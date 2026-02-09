package opencode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lengzhao/mybot"
)

const uploadsDir = "uploads"

func init() {
	mybot.RegisterAdapterType("opencode", func(id string, config map[string]interface{}) (mybot.Adapter, error) {
		slog.Debug("Creating OpenCode Adapter", "id", id, "config", config)
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
	defaultTarget string
	inbound       chan<- mybot.Message
	mu            sync.RWMutex
	sessions      map[string]string
	cmd           *exec.Cmd
}

func (a *Adapter) GetID() string {
	return a.id
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
	slog.Debug("Received message from OpenCode", "channel", msg.Channel, "content", msg.Content, "files", msg.Files)

	// 处理 /reset：重置当前上下文对应的 Session，并不下发到 OpenCode
	if strings.TrimSpace(msg.Content) == "/reset" {
		oldID, newID, err := a.resetSession(ctx, msg.Channel)
		if err != nil {
			slog.Error("Failed to reset session", "channel", msg.Channel, "err", err)
		}

		var resetMsg string
		if err != nil {
			resetMsg = fmt.Sprintf("重置会话失败: %v", err)
		} else {
			resetMsg = "已重置当前对话上下文，新的 Session 已创建。"
		}

		// 如果有旧的 / 新的 sessionID，记录到日志文件中
		if oldID != "" || newID != "" {
			a.logSessionEvent(oldID, "SESSION_RESET_OLD channel=%s new_session=%s", msg.Channel, newID)
			a.logSessionEvent(newID, "SESSION_RESET_NEW channel=%s old_session=%s", msg.Channel, oldID)
		}

		// 选择目标适配器：优先使用默认目标
		targetAdapter := msg.SourceAdapter
		if dt := a.GetDefaultTarget(); dt != "" {
			targetAdapter = dt
		}

		reply := mybot.Message{
			ID:            fmt.Sprintf("oc-reset-%d", time.Now().UnixNano()),
			ParentID:      msg.ID,
			SourceAdapter: a.id,
			TargetAdapter: targetAdapter,
			Content:       resetMsg,
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
		return err
	}

	sessionID, err := a.getOrCreateSession(ctx, msg.Channel)
	if err != nil {
		slog.Error("Failed to get or create session", "channel", msg.Channel, "err", err)
		return err
	}

	files, err := a.ensureFilesInDirectory(ctx, msg.Channel, msg.Files)
	if err != nil {
		slog.Error("Failed to ensure files in directory", "channel", msg.Channel, "err", err)
		return err
	}
	parts := a.buildParts(msg.Content, files)

	// 记录请求日志
	a.logSessionEvent(sessionID, "REQUEST channel=%s content=%q files=%d", msg.Channel, msg.Content, len(files))

	resp, err := a.prompt(ctx, sessionID, parts)
	if err != nil {
		slog.Error("Failed to get response from OpenCode", "channel", msg.Channel, "err", err)
		a.logSessionEvent(sessionID, "RESPONSE_ERROR channel=%s error=%v", msg.Channel, err)
		return err
	}
	slog.Debug("Received response from OpenCode", "channel", msg.Channel, "response", resp)

	content := a.collectTextParts(resp.Parts)

	// 记录回复日志（只记录文本部分的汇总）
	a.logSessionEvent(sessionID, "RESPONSE channel=%s content=%q", msg.Channel, content)

	// 如果消息没有明确的目标适配器，但当前适配器有默认目标，则使用默认目标
	targetAdapter := msg.SourceAdapter
	defaultTarget := a.GetDefaultTarget()
	if defaultTarget != "" {
		targetAdapter = defaultTarget
	}

	reply := mybot.Message{
		ID:            fmt.Sprintf("oc-%d", time.Now().UnixNano()),
		ParentID:      msg.ID,
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
		a.logSessionEvent(sid, "REUSE_SESSION channel=%s directory=%s", channel, a.directory)
		return sid, nil
	}

	sid, err := a.createSession(ctx, channel)
	if err != nil {
		return "", err
	}

	a.mu.Lock()
	a.sessions[channel] = sid
	a.mu.Unlock()
	return sid, nil
}

// createSession 创建新的 OpenCode Session，不写入缓存映射，由调用方决定是否缓存。
func (a *Adapter) createSession(ctx context.Context, channel string) (string, error) {
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

	// 记录创建 Session 的日志
	a.logSessionEvent(ses.ID, "CREATE_SESSION channel=%s directory=%s", channel, a.directory)
	return ses.ID, nil
}

// resetSession 删除旧 Session（若存在）并创建新的 Session，更新 channel→sessionID 映射。
func (a *Adapter) resetSession(ctx context.Context, channel string) (oldID, newID string, err error) {
	a.mu.RLock()
	oldID = a.sessions[channel]
	a.mu.RUnlock()

	newID, err = a.createSession(ctx, channel)
	if err != nil {
		return oldID, "", err
	}

	a.mu.Lock()
	a.sessions[channel] = newID
	a.mu.Unlock()

	return oldID, newID, nil
}

// logSessionEvent 将交互记录写入工作目录下 logs/session_id.log
func (a *Adapter) logSessionEvent(sessionID, format string, args ...interface{}) {
	if sessionID == "" || a.directory == "" {
		return
	}
	logsDir := filepath.Join(a.directory, "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		slog.Error("Failed to create logs directory", "dir", logsDir, "err", err)
		return
	}
	path := filepath.Join(logsDir, sessionID+".log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("Failed to open session log file", "path", path, "err", err)
		return
	}
	defer f.Close()

	ts := time.Now().Format(time.RFC3339Nano)
	line := fmt.Sprintf("%s "+format+"\n", append([]interface{}{ts}, args...)...)
	if _, err := f.WriteString(line); err != nil {
		slog.Error("Failed to write session log", "path", path, "err", err)
	}
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
		slog.Error("Failed to marshal request body", "err", err)
		return nil, err
	}

	u := a.baseURL + "/session/" + sessionID + "/message"
	if a.directory != "" {
		u += "?directory=" + url.QueryEscape(a.directory)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(reqBody))
	if err != nil {
		slog.Error("Failed to create request", "err", err)
		return nil, err
	}
	a.setAuth(req)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		slog.Error("Failed to get response from OpenCode", "err", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		slog.Error("Failed to get response from OpenCode", "status_code", resp.StatusCode, "body", string(b))
		return nil, fmt.Errorf("opencode prompt: %d %s", resp.StatusCode, string(b))
	}

	var out promptResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		slog.Error("Failed to decode response body", "err", err)
		return nil, err
	}
	slog.Debug("Received response from OpenCode", "response", out)
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
