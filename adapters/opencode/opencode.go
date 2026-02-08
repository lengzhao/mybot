package opencode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/lengzhao/mybot"
)

const uploadsDir = "uploads"

func init() {
	mybot.RegisterAdapterType("opencode", func(id string, config map[string]interface{}) (mybot.Adapter, error) {
		baseURL, _ := config["base_url"].(string)
		workdir, _ := config["workdir"].(string)
		if workdir == "" {
			workdir, _ = config["adapter_dir"].(string)
		}
		if workdir == "" {
			return nil, fmt.Errorf("opencode adapter requires adapter_dir (from main) or workdir")
		}
		absDir, err := filepath.Abs(workdir)
		if err != nil {
			return nil, err
		}
		workdir = absDir
		if err := os.MkdirAll(workdir, 0755); err != nil {
			return nil, err
		}
		apiKey, _ := config["api_key"].(string)
		bin, _ := config["opencode_bin"].(string)
		if bin == "" {
			bin = "opencode"
		}
		defaultTarget, _ := config["default_target"].(string)
		return &Adapter{
			id:            id,
			baseURL:       baseURL,
			directory:     workdir,
			apiKey:        apiKey,
			opencodeBin:   bin,
			tags:          []string{"type:ai", "service:opencode"},
			defaultTarget: defaultTarget,
			sessions:      make(map[string]string),
		}, nil
	})
}

// Adapter 将消息转发到 OpenCode；无 base_url 时自行启动 opencode 进程，文件落盘到 directory
type Adapter struct {
	id            string
	baseURL       string
	directory     string
	apiKey        string
	opencodeBin   string
	tags          []string
	defaultTarget string
	inbound       chan<- mybot.Message
	mu            sync.RWMutex
	sessions      map[string]string
	cmd           *exec.Cmd
}

func (a *Adapter) GetID() string {
	return a.id
}

func (a *Adapter) GetTags() []string {
	return a.tags
}

func (a *Adapter) GetDefaultTarget() string {
	return a.defaultTarget
}

func (a *Adapter) Start(ctx context.Context, inbound chan<- mybot.Message) error {
	a.inbound = inbound
	if a.baseURL != "" {
		return nil
	}
	// 自启动 opencode：确保目录存在，启动进程，解析 baseURL
	if err := os.MkdirAll(filepath.Join(a.directory, uploadsDir), 0755); err != nil {
		return err
	}
	listenURL, cmd, err := startOpencode(ctx, a.opencodeBin, a.directory)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.baseURL = listenURL
	a.cmd = cmd
	a.mu.Unlock()
	return nil
}

func (a *Adapter) ReceiveMessage(ctx context.Context, msg mybot.Message) error {
	sessionID, err := a.getOrCreateSession(ctx, msg.Channel)
	if err != nil {
		return err
	}

	files, err := a.ensureFilesInDirectory(ctx, msg.Channel, msg.Files)
	if err != nil {
		return err
	}
	parts := a.buildParts(msg.Content, files)
	resp, err := a.prompt(ctx, sessionID, parts)
	if err != nil {
		return err
	}

	content := a.collectTextParts(resp.Parts)

	// 如果消息没有明确的目标适配器，但当前适配器有默认目标，则使用默认目标
	targetAdapter := msg.SourceAdapter
	defaultTarget := a.GetDefaultTarget()
	if defaultTarget != "" {
		targetAdapter = defaultTarget
	}

	reply := mybot.Message{
		ID:            fmt.Sprintf("oc-%d", time.Now().UnixNano()),
		SourceAdapter: a.id,
		TargetAdapter: targetAdapter,
		Content:       content,
		Type:          mybot.TypeText,
		Timestamp:     time.Now().UnixMilli(),
		UserID:        "opencode",
		Channel:       msg.Channel,
	}

	select {
	case a.inbound <- reply:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (a *Adapter) Status() string {
	return "online"
}

// Stop 优雅地关闭 opencode 服务
func (a *Adapter) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.cmd != nil && a.cmd.Process != nil {
		// 发送 SIGTERM 信号优雅关闭
		if err := a.cmd.Process.Signal(os.Interrupt); err != nil {
			// 如果发送信号失败，强制 kill
			_ = a.cmd.Process.Kill()
		}
		// 等待进程退出
		done := make(chan error, 1)
		go func() {
			done <- a.cmd.Wait()
		}()

		select {
		case <-time.After(10 * time.Second):
			// 超时后强制 kill
			_ = a.cmd.Process.Kill()
		case <-done:
			// 正常退出
		}
		a.cmd = nil
	}
	return nil
}

func startOpencode(ctx context.Context, bin, workDir string) (listenURL string, cmd *exec.Cmd, err error) {
	cmd = exec.CommandContext(ctx, bin, "serve", "--hostname=127.0.0.1", "--port=4096")
	cmd.Dir = workDir

	// 重定向输出到文件而不是管道，避免阻塞
	logFile := filepath.Join(workDir, "opencode.log")
	logF, err := os.Create(logFile)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create log file: %w", err)
	}
	cmd.Stdout = logF
	cmd.Stderr = logF

	if err := cmd.Start(); err != nil {
		logF.Close()
		return "", nil, err
	}

	// 在单独的 goroutine 中等待进程结束并关闭日志文件
	go func() {
		cmd.Wait()
		logF.Close()
	}()

	// 等待服务启动完成
	deadline := time.Now().Add(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		select {
		case <-ticker.C:
			// 检查进程是否还在运行
			if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
				return "", nil, fmt.Errorf("opencode process exited unexpectedly")
			}

			// 检查日志中是否包含启动成功的消息
			logContent, err := os.ReadFile(logFile)
			if err != nil {
				continue
			}

			// 检查是否包含监听地址或启动成功的消息
			if bytes.Contains(logContent, []byte("opencode server listening on")) {
				return "http://127.0.0.1:4096", cmd, nil
			}

			// 检查是否有错误信息
			if bytes.Contains(logContent, []byte("Error:")) || bytes.Contains(logContent, []byte("error:")) {
				return "", nil, fmt.Errorf("opencode startup error detected in logs")
			}

		case <-ctx.Done():
			return "", nil, ctx.Err()
		}
	}

	return "", nil, fmt.Errorf("timeout waiting for opencode server to start")
}

func (a *Adapter) ensureFilesInDirectory(ctx context.Context, channel string, files []mybot.File) ([]mybot.File, error) {
	if len(files) == 0 {
		return nil, nil
	}
	dir := filepath.Join(a.directory, uploadsDir, channel)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	out := make([]mybot.File, 0, len(files))
	for i, f := range files {
		localPath, err := copyToDir(ctx, dir, f, i)
		if err != nil {
			return nil, err
		}
		out = append(out, mybot.File{
			Name:     f.Name,
			URL:      "file://" + filepath.ToSlash(localPath),
			MimeType: f.MimeType,
			Size:     f.Size,
		})
	}
	return out, nil
}

func copyToDir(ctx context.Context, dir string, f mybot.File, index int) (string, error) {
	name := f.Name
	if name == "" {
		name = fmt.Sprintf("file_%d", index)
	}
	localPath := filepath.Join(dir, filepath.Base(name))
	u, err := url.Parse(f.URL)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http", "https":
		req, err := http.NewRequestWithContext(ctx, "GET", f.URL, nil)
		if err != nil {
			return "", err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("download %s: %d", f.URL, resp.StatusCode)
		}
		w, err := os.Create(localPath)
		if err != nil {
			return "", err
		}
		_, err = io.Copy(w, resp.Body)
		w.Close()
		if err != nil {
			os.Remove(localPath)
			return "", err
		}
		return localPath, nil
	case "file":
		src := u.Path
		if u.Host != "" {
			src = u.Host + u.Path
		}
		data, err := os.ReadFile(src)
		if err != nil {
			return "", err
		}
		if err := os.WriteFile(localPath, data, 0644); err != nil {
			return "", err
		}
		return localPath, nil
	default:
		return "", fmt.Errorf("unsupported file URL scheme: %s", u.Scheme)
	}
}

func (a *Adapter) getOrCreateSession(ctx context.Context, channel string) (string, error) {
	a.mu.RLock()
	sid, ok := a.sessions[channel]
	a.mu.RUnlock()
	if ok {
		return sid, nil
	}

	body := map[string]interface{}{"title": "channel:" + channel}
	reqBody, _ := json.Marshal(body)
	u := a.baseURL + "/session"
	if a.directory != "" {
		u += "?directory=" + url.QueryEscape(a.directory)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(reqBody))
	if err != nil {
		return "", err
	}
	a.setAuth(req)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("opencode create session: %d %s", resp.StatusCode, string(b))
	}

	var ses struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ses); err != nil {
		return "", err
	}
	if ses.ID == "" {
		return "", fmt.Errorf("opencode create session: empty id")
	}

	a.mu.Lock()
	a.sessions[channel] = ses.ID
	a.mu.Unlock()
	return ses.ID, nil
}

func (a *Adapter) buildParts(content string, files []mybot.File) []map[string]interface{} {
	parts := make([]map[string]interface{}, 0, 1+len(files))
	if content != "" {
		parts = append(parts, map[string]interface{}{"type": "text", "text": content})
	}
	for _, f := range files {
		p := map[string]interface{}{
			"type": "file",
			"mime": f.MimeType,
			"url":  f.URL,
		}
		if f.Name != "" {
			p["filename"] = f.Name
		}
		parts = append(parts, p)
	}
	if len(parts) == 0 {
		parts = append(parts, map[string]interface{}{"type": "text", "text": ""})
	}
	return parts
}

func (a *Adapter) prompt(ctx context.Context, sessionID string, parts []map[string]interface{}) (*promptResponse, error) {
	body := map[string]interface{}{"parts": parts}
	reqBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	u := a.baseURL + "/session/" + sessionID + "/message"
	if a.directory != "" {
		u += "?directory=" + url.QueryEscape(a.directory)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	a.setAuth(req)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("opencode prompt: %d %s", resp.StatusCode, string(b))
	}

	var out promptResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (a *Adapter) setAuth(req *http.Request) {
	if a.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.apiKey)
	}
}

func (a *Adapter) collectTextParts(parts []partSchema) string {
	var b bytes.Buffer
	for _, p := range parts {
		if p.Type == "text" && p.Text != "" {
			if b.Len() > 0 {
				b.WriteString("\n")
			}
			b.WriteString(p.Text)
		}
	}
	return b.String()
}

type promptResponse struct {
	Info  map[string]interface{} `json:"info"`
	Parts []partSchema           `json:"parts"`
}

type partSchema struct {
	Type string `json:"type"`
	Text string `json:"text"`
}
