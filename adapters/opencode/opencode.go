package opencode

import (
	"bufio"
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
	"regexp"
	"sync"
	"time"

	"github.com/lengzhao/mybot"
)

const uploadsDir = "uploads"

var listenRE = regexp.MustCompile(`opencode server listening on (https?://[^\s]+)`)

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
		return &Adapter{
			id:          id,
			baseURL:     baseURL,
			directory:   workdir,
			apiKey:      apiKey,
			opencodeBin: bin,
			tags:        []string{"type:ai", "service:opencode"},
			sessions:    make(map[string]string),
		}, nil
	})
}

// Adapter 将消息转发到 OpenCode；无 base_url 时自行启动 opencode 进程，文件落盘到 directory
type Adapter struct {
	id          string
	baseURL     string
	directory   string
	apiKey      string
	opencodeBin string
	tags        []string
	inbound     chan<- mybot.Message
	mu          sync.RWMutex
	sessions    map[string]string
	cmd         *exec.Cmd
}

func (a *Adapter) GetID() string {
	return a.id
}

func (a *Adapter) GetTags() []string {
	return a.tags
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
	reply := mybot.Message{
		ID:            fmt.Sprintf("oc-%d", time.Now().UnixNano()),
		SourceAdapter: a.id,
		TargetAdapter: msg.SourceAdapter,
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

func startOpencode(ctx context.Context, bin, workDir string) (listenURL string, cmd *exec.Cmd, err error) {
	cmd = exec.CommandContext(ctx, bin, "serve", "--hostname=127.0.0.1", "--port=0")
	cmd.Dir = workDir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", nil, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return "", nil, err
	}
	scanner := bufio.NewScanner(stdout)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if !scanner.Scan() {
			_ = cmd.Process.Kill()
			return "", nil, fmt.Errorf("opencode exited before listening: %s", stderr.String())
		}
		line := scanner.Text()
		if m := listenRE.FindStringSubmatch(line); len(m) > 1 {
			return m[1], cmd, nil
		}
	}
	_ = cmd.Process.Kill()
	return "", nil, fmt.Errorf("timeout waiting for opencode server: %s", stderr.String())
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
