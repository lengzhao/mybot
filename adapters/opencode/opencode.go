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

type contextKey string

const workdirContextKey contextKey = "workdir"

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
		allowExecute := true
		if v, ok := config["allow_execute"].(bool); ok {
			allowExecute = v
		}
		return &Adapter{
			id:            id,
			baseURL:       baseURL,
			directory:     workdir,
			apiKey:        apiKey,
			opencodeBin:   bin,
			defaultTarget: defaultTarget,
			allowExecute:  allowExecute,
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
	allowExecute  bool // 为 true 时在创建 Session 时授予 bash/edit allow，便于写并执行 Python 等代码
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

// getWorkdir 从 ctx 读取当前请求的工作目录（按 channel 创建）；未设置时退回适配器默认 directory。
func (a *Adapter) getWorkdir(ctx context.Context) string {
	if v := ctx.Value(workdirContextKey); v != nil {
		return v.(string)
	}
	return a.directory
}

// workdirForChannel 根据 channel（优先）或 userID 计算该会话的工作目录，用于隔离不同会话的文件与 session。channel 为空时以 userID 区分。
func (a *Adapter) workdirForChannel(channel, userID string) string {
	safe := sanitizePathSegment(channel)
	if safe == "" {
		safe = sanitizePathSegment(userID)
	}
	if safe == "" {
		safe = "default"
	}
	return filepath.Join(a.directory, safe)
}

func sanitizePathSegment(s string) string {
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.ReplaceAll(s, "..", "_")
	return strings.TrimSpace(s)
}

// sessionFilePath 返回该 channel 对应工作目录下的 .session 文件路径，用于持久化 channel→sessionID 映射。
func (a *Adapter) sessionFilePath(channel string) string {
	return filepath.Join(a.workdirForChannel(channel, ""), ".session")
}

// sessionState 持久化在 .session 文件中的 JSON 结构：当前 session 与历史列表。
type sessionState struct {
	Current string   `json:"current"`
	History []string `json:"history"`
}

func (a *Adapter) loadSessionFromFile(channel string) (string, bool) {
	state, ok := a.loadSessionState(channel)
	if !ok {
		return "", false
	}
	return state.Current, true
}

func (a *Adapter) loadSessionState(channel string) (sessionState, bool) {
	path := a.sessionFilePath(channel)
	data, err := os.ReadFile(path)
	if err != nil {
		return sessionState{}, false
	}
	var state sessionState
	if err := json.Unmarshal(data, &state); err == nil && state.Current != "" {
		return state, true
	}
	return sessionState{}, false
}

func (a *Adapter) saveSessionToFile(channel, current string, history []string) error {
	dir := filepath.Dir(a.sessionFilePath(channel))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	state := sessionState{Current: current, History: history}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(a.sessionFilePath(channel), data, 0644)
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

	workdir := a.workdirForChannel(msg.Channel, msg.UserID)
	if err := os.MkdirAll(workdir, 0755); err != nil {
		slog.Error("Failed to create channel workdir", "channel", msg.Channel, "workdir", workdir, "err", err)
		return err
	}
	ctx = context.WithValue(ctx, workdirContextKey, workdir)

	handled, err := a.handleCommand(ctx, msg)
	if err != nil {
		slog.Error("Failed to handle command", "channel", msg.Channel, "err", err)
		return err
	}
	if handled {
		return nil
	}

	sessionID, err := a.getOrCreateSession(ctx, msg.Channel)
	if err != nil {
		slog.Error("Failed to get or create session", "channel", msg.Channel, "err", err)
		return err
	}

	files, err := a.ensureFilesInDirectory(ctx, msg.Files)
	if err != nil {
		slog.Error("Failed to ensure files in directory", "channel", msg.Channel, "err", err)
		return err
	}
	parts := a.buildParts(msg.Content, files)

	// 记录请求日志
	a.logSessionEvent(ctx, sessionID, "REQUEST channel=%s content=%q files=%d", msg.Channel, msg.Content, len(files))

	beforePrompt := time.Now()
	resp, err := a.prompt(ctx, sessionID, parts)
	if err != nil {
		slog.Error("Failed to get response from OpenCode", "channel", msg.Channel, "err", err)
		a.logSessionEvent(ctx, sessionID, "RESPONSE_ERROR channel=%s error=%v", msg.Channel, err)
		return err
	}
	slog.Debug("Received response from OpenCode", "channel", msg.Channel, "response", resp)

	content := a.collectTextParts(resp.Parts)
	replyFiles := a.collectFileParts(resp.Parts)
	// 补充工作目录中本次请求后新产生或修改的文件（如 agent 创建的 hello.txt），供 webchat 展示下载
	newFiles := a.listWorkdirFilesModifiedAfter(a.getWorkdir(ctx), beforePrompt)
	for _, f := range newFiles {
		replyFiles = append(replyFiles, f)
	}

	// 记录回复日志（只记录文本部分的汇总）
	a.logSessionEvent(ctx, sessionID, "RESPONSE channel=%s content=%q files=%d", msg.Channel, content, len(replyFiles))

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
		Files:         replyFiles,
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

func (a *Adapter) ensureFilesInDirectory(ctx context.Context, files []mybot.File) ([]mybot.File, error) {
	if len(files) == 0 {
		return nil, nil
	}
	dir := filepath.Join(a.getWorkdir(ctx), uploadsDir)
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
		a.logSessionEvent(ctx, sid, "REUSE_SESSION channel=%s directory=%s", channel, a.getWorkdir(ctx))
		return sid, nil
	}
	// 内存未命中时从 workdir 下的 .session 文件恢复
	if sid, ok = a.loadSessionFromFile(channel); ok {
		a.mu.Lock()
		a.sessions[channel] = sid
		a.mu.Unlock()
		a.logSessionEvent(ctx, sid, "REUSE_SESSION channel=%s directory=%s (from .session)", channel, a.getWorkdir(ctx))
		return sid, nil
	}

	sid, err := a.createSession(ctx, channel)
	if err != nil {
		return "", err
	}

	a.mu.Lock()
	a.sessions[channel] = sid
	a.mu.Unlock()
	if err := a.saveSessionToFile(channel, sid, nil); err != nil {
		slog.Warn("Failed to persist session to .session", "channel", channel, "err", err)
	}
	return sid, nil
}

// sessionPermission 创建 Session 时使用的权限规则。
// 目标：API 调用时 opencode 直接处理、不中断、必有响应（不出现等待用户点允许/回答的阻塞）。
//
// 策略：所有会用到的能力一律 allow；会阻塞的 question/plan 一律 deny；doom_loop 设为 allow 避免重复 3 次时弹确认。
func (a *Adapter) sessionPermission() []map[string]string {
	rules := []map[string]string{
		{"permission": "read", "pattern": "*", "action": "allow"},
		{"permission": "glob", "pattern": "*", "action": "allow"},
		{"permission": "grep", "pattern": "*", "action": "allow"},
		{"permission": "list", "pattern": "*", "action": "allow"},
		{"permission": "lsp", "pattern": "*", "action": "allow"},
		{"permission": "todowrite", "pattern": "*", "action": "allow"},
		{"permission": "todoread", "pattern": "*", "action": "allow"},
		{"permission": "question", "pattern": "*", "action": "deny"},
		{"permission": "plan_enter", "pattern": "*", "action": "deny"},
		{"permission": "plan_exit", "pattern": "*", "action": "deny"},
		{"permission": "doom_loop", "pattern": "*", "action": "allow"},
		{"permission": "skill", "pattern": "*", "action": "allow"},
		{"permission": "task", "pattern": "*", "action": "allow"},
		{"permission": "webfetch", "pattern": "*", "action": "allow"},
		{"permission": "websearch", "pattern": "*", "action": "allow"},
		{"permission": "codesearch", "pattern": "*", "action": "allow"},
		{"permission": "external_directory", "pattern": "*", "action": "allow"},
	}
	if a.allowExecute {
		// bash: 先允许全部，再拒绝 rm 等破坏性命令（后匹配优先）
		rules = append(rules,
			map[string]string{"permission": "bash", "pattern": "*", "action": "allow"},
			map[string]string{"permission": "bash", "pattern": "rm", "action": "deny"},
			map[string]string{"permission": "bash", "pattern": "rm *", "action": "deny"},
			map[string]string{"permission": "bash", "pattern": "rm -rf *", "action": "deny"},
			map[string]string{"permission": "bash", "pattern": "rm -r *", "action": "deny"},
			map[string]string{"permission": "bash", "pattern": "/bin/rm*", "action": "deny"},
			map[string]string{"permission": "edit", "pattern": "*", "action": "allow"},
		)
	}
	return rules
}

// createSession 创建新的 OpenCode Session，不写入缓存映射，由调用方决定是否缓存。
func (a *Adapter) createSession(ctx context.Context, channel string) (string, error) {
	body := map[string]interface{}{
		"title":      "channel:" + channel,
		"permission": a.sessionPermission(),
	}
	reqBody, _ := json.Marshal(body)
	dir := a.getWorkdir(ctx)
	u := a.baseURL + "/session"
	if dir != "" {
		u += "?directory=" + url.QueryEscape(dir)
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
	a.logSessionEvent(ctx, ses.ID, "CREATE_SESSION channel=%s directory=%s", channel, dir)
	return ses.ID, nil
}

// resetSession 创建新的 Session，更新 channel→sessionID 映射。
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
	history := []string{}
	if old, ok := a.loadSessionState(channel); ok && old.Current != "" {
		history = append(append([]string{}, old.History...), old.Current)
	}
	if err := a.saveSessionToFile(channel, newID, history); err != nil {
		slog.Warn("Failed to persist session to .session after reset", "channel", channel, "err", err)
	}
	return oldID, newID, nil
}

// handleCommand 解析并处理斜杠命令（/reset、/init、/history 等），若为命令则处理并返回 (true, nil)，否则返回 (false, nil)。
func (a *Adapter) handleCommand(ctx context.Context, msg mybot.Message) (bool, error) {
	cmd := strings.TrimSpace(msg.Content)
	if cmd == "" || !strings.HasPrefix(cmd, "/") {
		return false, nil
	}
	parts := strings.Fields(cmd)
	verb := parts[0]

	switch verb {
	case "/reset":
		return true, a.handleReset(ctx, msg)
	case "/init":
		return true, a.handleInit(ctx, msg)
	case "/history":
		return true, a.handleHistory(ctx, msg)
	case "/plan", "/status":
		return true, a.handlePlan(ctx, msg)
	default:
		_ = a.sendCommandReply(ctx, msg, "未知命令。支持: /reset（清理会话）、/init（初始化 agent.md）、/history（查询会话历史）、/plan（查询会话状态与待办）")
		return true, nil
	}
}

func (a *Adapter) sendCommandReply(ctx context.Context, msg mybot.Message, content string) error {
	targetAdapter := msg.SourceAdapter
	if dt := a.GetDefaultTarget(); dt != "" {
		targetAdapter = dt
	}
	reply := mybot.Message{
		ID:            fmt.Sprintf("oc-cmd-%d", time.Now().UnixNano()),
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
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *Adapter) handleReset(ctx context.Context, msg mybot.Message) error {
	oldID, newID, err := a.resetSession(ctx, msg.Channel)
	if err != nil {
		slog.Error("Failed to reset session", "channel", msg.Channel, "err", err)
	}
	var content string
	if err != nil {
		content = fmt.Sprintf("重置会话失败: %v", err)
	} else {
		content = "已重置当前对话上下文，新的 Session 已创建。"
	}
	if oldID != "" || newID != "" {
		a.logSessionEvent(ctx, oldID, "SESSION_RESET_OLD channel=%s new_session=%s", msg.Channel, newID)
		a.logSessionEvent(ctx, newID, "SESSION_RESET_NEW channel=%s old_session=%s", msg.Channel, oldID)
	}
	return a.sendCommandReply(ctx, msg, content)
}

func (a *Adapter) handleInit(ctx context.Context, msg mybot.Message) error {
	sessionID, err := a.getOrCreateSession(ctx, msg.Channel)
	if err != nil {
		slog.Error("Failed to get or create session for init", "channel", msg.Channel, "err", err)
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("初始化失败: 无法获取会话 (%v)", err))
	}
	dir := a.getWorkdir(ctx)
	u := a.baseURL + "/session/" + sessionID + "/init"
	if dir != "" {
		u += "?directory=" + url.QueryEscape(dir)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(nil))
	if err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("初始化请求构建失败: %v", err))
	}
	a.setAuth(req)
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("初始化请求失败: %v", err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("初始化 agent.md 失败: %d %s", resp.StatusCode, string(b)))
	}
	a.logSessionEvent(ctx, sessionID, "INIT channel=%s", msg.Channel)
	return a.sendCommandReply(ctx, msg, "已初始化 agent.md，当前会话将使用新的 Agent 配置。")
}

func (a *Adapter) handleHistory(ctx context.Context, msg mybot.Message) error {
	sessionID, err := a.getOrCreateSession(ctx, msg.Channel)
	if err != nil {
		slog.Error("Failed to get or create session for history", "channel", msg.Channel, "err", err)
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("查询历史失败: 无法获取会话 (%v)", err))
	}
	dir := a.getWorkdir(ctx)
	u := a.baseURL + "/session/" + sessionID + "/message?limit=20"
	if dir != "" {
		u += "&directory=" + url.QueryEscape(dir)
	}
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("请求历史失败: %v", err))
	}
	a.setAuth(req)
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("请求历史失败: %v", err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("获取会话历史失败: %d %s", resp.StatusCode, string(b)))
	}
	// OpenCode API: GET /session/{id}/message 返回 Array<{ info: Message, parts: Part[] }>
	var list []struct {
		Info struct {
			Role string `json:"role"`
		} `json:"info"`
		Parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"parts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("解析历史数据失败: %v", err))
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("当前 Session: %s\n最近 %d 条消息:\n", sessionID, len(list)))
	for i, m := range list {
		var text string
		for _, p := range m.Parts {
			if p.Type == "text" && p.Text != "" {
				text = p.Text
				if len(text) > 200 {
					text = text[:200] + "..."
				}
				break
			}
		}
		b.WriteString(fmt.Sprintf("%d. [%s] %s\n", i+1, m.Info.Role, text))
	}
	if len(list) == 0 {
		b.WriteString("（暂无消息）")
	}
	return a.sendCommandReply(ctx, msg, strings.TrimSuffix(b.String(), "\n"))
}

// handlePlan 查询当前会话状态：session 信息、运行状态、待办列表。
func (a *Adapter) handlePlan(ctx context.Context, msg mybot.Message) error {
	sessionID, err := a.getOrCreateSession(ctx, msg.Channel)
	if err != nil {
		slog.Error("Failed to get or create session for plan", "channel", msg.Channel, "err", err)
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("查询状态失败: 无法获取会话 (%v)", err))
	}
	dir := a.getWorkdir(ctx)
	q := ""
	if dir != "" {
		q = "?directory=" + url.QueryEscape(dir)
	}
	client := &http.Client{Timeout: 15 * time.Second}

	// GET session 详情
	uSession := a.baseURL + "/session/" + sessionID + q
	reqSession, _ := http.NewRequestWithContext(ctx, "GET", uSession, nil)
	a.setAuth(reqSession)
	respSession, err := client.Do(reqSession)
	if err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("获取会话详情失败: %v", err))
	}
	defer respSession.Body.Close()
	var session struct {
		ID        string `json:"id"`
		Directory string `json:"directory"`
		ParentID  string `json:"parentID"`
		ProjectID string `json:"projectID"`
		Summary   *struct {
			Additions int `json:"additions"`
			Deletions int `json:"deletions"`
			Files     int `json:"files"`
		} `json:"summary"`
	}
	_ = json.NewDecoder(respSession.Body).Decode(&session)

	// GET 运行状态
	uStatus := a.baseURL + "/session/status"
	if dir != "" {
		uStatus += "?directory=" + url.QueryEscape(dir)
	}
	reqStatus, _ := http.NewRequestWithContext(ctx, "GET", uStatus, nil)
	a.setAuth(reqStatus)
	respStatus, err := client.Do(reqStatus)
	if err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("获取会话状态失败: %v", err))
	}
	defer respStatus.Body.Close()
	var statusMap map[string]struct {
		Type    string  `json:"type"`
		Attempt *int    `json:"attempt,omitempty"`
		Message *string `json:"message,omitempty"`
		Next    *int    `json:"next,omitempty"`
	}
	_ = json.NewDecoder(respStatus.Body).Decode(&statusMap)
	statusStr := "unknown"
	if st, ok := statusMap[sessionID]; ok {
		statusStr = st.Type
		if st.Type == "retry" && st.Message != nil {
			statusStr += " (" + *st.Message + ")"
		}
	}

	// GET 待办
	uTodo := a.baseURL + "/session/" + sessionID + "/todo" + q
	reqTodo, _ := http.NewRequestWithContext(ctx, "GET", uTodo, nil)
	a.setAuth(reqTodo)
	respTodo, err := client.Do(reqTodo)
	if err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("获取待办失败: %v", err))
	}
	defer respTodo.Body.Close()
	var todos []struct {
		ID       string `json:"id"`
		Content  string `json:"content"`
		Status   string `json:"status"`
		Priority string `json:"priority"`
	}
	_ = json.NewDecoder(respTodo.Body).Decode(&todos)

	var b strings.Builder
	b.WriteString(fmt.Sprintf("**Session 状态**\n- ID: %s\n- 工作目录: %s\n- 运行状态: %s\n", sessionID, session.Directory, statusStr))
	if session.Summary != nil {
		b.WriteString(fmt.Sprintf("- 变更摘要: +%d -%d (%d 文件)\n", session.Summary.Additions, session.Summary.Deletions, session.Summary.Files))
	}
	b.WriteString("\n**待办**\n")
	if len(todos) == 0 {
		b.WriteString("（无）")
	} else {
		for i, t := range todos {
			icon := "[ ]"
			if t.Status == "completed" || t.Status == "cancelled" {
				icon = "[x]"
			}
			b.WriteString(fmt.Sprintf("%d. %s %s (%s)\n", i+1, icon, t.Content, t.Status))
		}
	}
	return a.sendCommandReply(ctx, msg, strings.TrimSuffix(b.String(), "\n"))
}

// logSessionEvent 将交互记录写入当前请求工作目录（ctx）下的 logs/session_id.log
func (a *Adapter) logSessionEvent(ctx context.Context, sessionID, format string, args ...interface{}) {
	dir := a.getWorkdir(ctx)
	if sessionID == "" || dir == "" {
		return
	}
	logsDir := filepath.Join(dir, "logs")
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
		text := f.URL
		if f.Name != "" {
			text = fmt.Sprintf("%s (%s)", f.Name, f.URL)
		}
		parts = append(parts, map[string]interface{}{
			"type": "text",
			"text": text,
		})
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

	dir := a.getWorkdir(ctx)
	u := a.baseURL + "/session/" + sessionID + "/message"
	if dir != "" {
		u += "?directory=" + url.QueryEscape(dir)
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

// collectFileParts 从 OpenCode 响应的 parts 中收集附件：顶层 type=file 的 part，以及 type=tool 的 state.attachments（工具返回的生成文件）。
func (a *Adapter) collectFileParts(parts []partSchema) []mybot.File {
	out := make([]mybot.File, 0)
	for _, p := range parts {
		if p.Type == "file" && p.URL != "" {
			out = append(out, filePartToMybot(p.Filename, p.URL, p.Mime, p.Size))
			continue
		}
		if p.Type == "tool" && p.State != nil {
			for _, att := range p.State.Attachments {
				if att.URL == "" {
					continue
				}
				out = append(out, filePartToMybot(att.Filename, att.URL, att.Mime, 0))
			}
		}
	}
	return out
}

func filePartToMybot(filename, url, mime string, size int64) mybot.File {
	name := filename
	if name == "" {
		name = filepath.Base(url)
	}
	if name == "" || name == "." {
		name = "file"
	}
	return mybot.File{Name: name, URL: url, MimeType: mime, Size: size}
}

// listWorkdirFilesModifiedAfter 扫描 workdir 下在 after 之后有修改的普通文件，排除 logs 等目录；包含 uploads（agent 常把生成文件写到 uploads），返回 mybot.File 列表（file:// URL）。
func (a *Adapter) listWorkdirFilesModifiedAfter(workdir string, after time.Time) []mybot.File {
	if workdir == "" {
		return nil
	}
	absWorkdir, err := filepath.Abs(workdir)
	if err != nil {
		return nil
	}
	out := make([]mybot.File, 0)
	skipDirs := map[string]bool{"logs": true, ".ruff_cache": true, "node_modules": true}
	filepath.WalkDir(absWorkdir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(absWorkdir, path)
		if rel == "." {
			return nil
		}
		base := filepath.Base(rel)
		firstSeg := strings.Split(rel, string(filepath.Separator))[0]
		if skipDirs[firstSeg] || skipDirs[base] {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.ModTime().Before(after) || info.ModTime().Equal(after) {
			return nil
		}
		out = append(out, mybot.File{
			Name: filepath.Base(path),
			URL:  "file://" + filepath.ToSlash(path),
			Size: info.Size(),
		})
		return nil
	})
	return out
}

type promptResponse struct {
	Info  map[string]interface{} `json:"info"`
	Parts []partSchema           `json:"parts"`
}

type partSchema struct {
	Type     string `json:"type"`
	Text     string `json:"text"`
	Filename string `json:"filename"`
	Mime     string `json:"mime"`
	URL      string `json:"url"`
	Size     int64  `json:"size"`
	State    *struct {
		Attachments []struct {
			URL      string `json:"url"`
			Filename string `json:"filename"`
			Mime     string `json:"mime"`
		} `json:"attachments"`
	} `json:"state,omitempty"`
}
