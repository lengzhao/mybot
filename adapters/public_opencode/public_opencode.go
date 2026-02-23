package public_opencode

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
	"github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
)

const uploadsDir = "uploads"
const sessionFile = ".session"

func init() {
	mybot.RegisterAdapterType("public_opencode", func(id string, config map[string]interface{}) (mybot.Adapter, error) {
		slog.Debug("Creating Public OpenCode Adapter", "id", id, "config", config)
		baseURL, _ := config["base_url"].(string)
		adapterDir, _ := config["adapter_dir"].(string)
		if adapterDir == "" {
			return nil, fmt.Errorf("public_opencode adapter requires adapter_dir (from main)")
		}
		absDir, err := filepath.Abs(adapterDir)
		if err != nil {
			return nil, err
		}
		adapterDir = absDir
		if err := os.MkdirAll(adapterDir, 0755); err != nil {
			return nil, err
		}
		apiKey, _ := config["api_key"].(string)
		defaultTarget, _ := config["default_target"].(string)
		allowExecute := true
		if v, ok := config["allow_execute"].(bool); ok {
			allowExecute = v
		}
		autoPermission, _ := config["auto_permission"].(string)
		opencodeBin, _ := config["opencode_bin"].(string)
		if opencodeBin == "" {
			opencodeBin = "opencode"
		}
		var client *opencode.Client
		if baseURL != "" {
			opts := []option.RequestOption{option.WithBaseURL(baseURL)}
			if apiKey != "" {
				opts = append(opts, option.WithHeader("Authorization", "Bearer "+apiKey))
			}
			client = opencode.NewClient(opts...)
		}

		return &Adapter{
			id:             id,
			client:         client,
			baseURL:        baseURL,
			apiKey:         apiKey,
			directory:      adapterDir,
			defaultTarget:  defaultTarget,
			allowExecute:   allowExecute,
			autoPermission: autoPermission,
			opencodeBin:    opencodeBin,
		}, nil
	})
}

type Adapter struct {
	id             string
	client         *opencode.Client // 有 base_url 时在工厂创建；否则在 Start 自启动后创建
	baseURL        string
	apiKey         string
	directory      string
	defaultTarget  string
	allowExecute   bool
	autoPermission string // "once" | "always" | "reject"，非空时订阅 event 并自动响应权限请求
	opencodeBin    string
	inbound        chan<- mybot.Message
	mu             sync.RWMutex
	sessionID      string
	eventCancel    context.CancelFunc
	cmd            *exec.Cmd
}

func (a *Adapter) GetID() string {
	return a.id
}

func (a *Adapter) GetDefaultTarget() string {
	return a.defaultTarget
}

func (a *Adapter) Start(ctx context.Context, inbound chan<- mybot.Message) error {
	a.inbound = inbound
	if a.client == nil {
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
		opts := []option.RequestOption{option.WithBaseURL(listenURL)}
		if a.apiKey != "" {
			opts = append(opts, option.WithHeader("Authorization", "Bearer "+a.apiKey))
		}
		a.client = opencode.NewClient(opts...)
		slog.Info("public_opencode started local opencode serve", "url", listenURL)
	}
	if sid, ok := a.loadSessionFromFile(); ok {
		a.mu.Lock()
		a.sessionID = sid
		a.mu.Unlock()
		slog.Debug("public_opencode restored session from file", "sessionID", sid)
	}
	if a.autoPermission != "" {
		eventCtx, cancel := context.WithCancel(context.Background())
		a.eventCancel = cancel
		go a.runEventLoop(eventCtx)
	}
	return nil
}

func (a *Adapter) ReceiveMessage(ctx context.Context, msg mybot.Message) error {
	slog.Debug("public_opencode received message", "channel", msg.Channel, "content", msg.Content, "files", len(msg.Files))

	handled, err := a.handleCommand(ctx, msg)
	if err != nil {
		return err
	}
	if handled {
		return nil
	}

	sessionID, err := a.getOrCreateSession(ctx)
	if err != nil {
		slog.Error("failed to get or create session", "err", err)
		return err
	}

	files, err := a.ensureFilesInDirectory(ctx, msg.Files)
	if err != nil {
		slog.Error("failed to ensure files in directory", "err", err)
		return err
	}
	parts := a.buildParts(msg.Content, files)

	beforePrompt := time.Now()
	resp, err := a.prompt(ctx, sessionID, parts)
	if err != nil {
		slog.Error("failed to get response from OpenCode", "err", err)
		return err
	}

	content := collectTextParts(resp.Parts)
	replyFiles := collectFileParts(resp.Parts)
	newFiles := listWorkdirFilesModifiedAfter(a.directory, beforePrompt)
	for _, f := range newFiles {
		replyFiles = append(replyFiles, f)
	}

	targetAdapter := msg.SourceAdapter
	if a.GetDefaultTarget() != "" {
		targetAdapter = a.GetDefaultTarget()
	}

	reply := mybot.Message{
		ID:            fmt.Sprintf("poc-%d", time.Now().UnixNano()),
		ParentID:      msg.ID,
		SourceAdapter: a.GetID(),
		TargetAdapter: targetAdapter,
		Content:       content,
		Type:          mybot.TypeText,
		Timestamp:     time.Now().UnixMilli(),
		UserID:        "public_opencode",
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

func (a *Adapter) Stop() error {
	if a.eventCancel != nil {
		a.eventCancel()
		a.eventCancel = nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cmd != nil && a.cmd.Process != nil {
		_ = a.cmd.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- a.cmd.Wait() }()
		select {
		case <-time.After(10 * time.Second):
			_ = a.cmd.Process.Kill()
		case <-done:
		}
		a.cmd = nil
	}
	return nil
}

func startOpencode(ctx context.Context, bin, workDir string) (listenURL string, cmd *exec.Cmd, err error) {
	cmd = exec.CommandContext(ctx, bin, "serve", "--hostname=127.0.0.1", "--port=4096")
	cmd.Dir = workDir
	logPath := filepath.Join(workDir, "opencode.log")
	logF, err := os.Create(logPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create log file: %w", err)
	}
	cmd.Stdout = logF
	cmd.Stderr = logF
	if err := cmd.Start(); err != nil {
		logF.Close()
		return "", nil, err
	}
	go func() {
		cmd.Wait()
		logF.Close()
	}()
	deadline := time.Now().Add(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		select {
		case <-ticker.C:
			if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
				return "", nil, fmt.Errorf("opencode process exited unexpectedly")
			}
			logContent, err := os.ReadFile(logPath)
			if err != nil {
				continue
			}
			if bytes.Contains(logContent, []byte("opencode server listening on")) {
				return "http://127.0.0.1:4096", cmd, nil
			}
			if bytes.Contains(logContent, []byte("Error:")) || bytes.Contains(logContent, []byte("error:")) {
				return "", nil, fmt.Errorf("opencode startup error detected in logs")
			}
		case <-ctx.Done():
			return "", nil, ctx.Err()
		}
	}
	return "", nil, fmt.Errorf("timeout waiting for opencode server to start")
}

const eventKeyMaxLen = 200

// publicOpencodeEventSummary 基于真实事件类型返回 type 与关键内容；未识别类型返回 type 与 properties 摘要。
func publicOpencodeEventSummary(ev opencode.EventListResponse) (eventType, key string) {
	eventType = string(ev.Type)
	u := ev.AsUnion()
	switch ev.Type {
	case opencode.EventListResponseTypePermissionUpdated:
		if p, ok := u.(opencode.EventListResponseEventPermissionUpdated); ok && p.Properties.SessionID != "" {
			return eventType, p.Properties.SessionID + "/" + p.Properties.ID
		}
		return eventType, ""
	case opencode.EventListResponseTypeSessionIdle:
		if e, ok := u.(opencode.EventListResponseEventSessionIdle); ok {
			return eventType, e.Properties.SessionID
		}
		return eventType, ""
	case opencode.EventListResponseTypeFileEdited:
		if e, ok := u.(opencode.EventListResponseEventFileEdited); ok {
			return eventType, e.Properties.File
		}
		return eventType, ""
	case opencode.EventListResponseTypeFileWatcherUpdated:
		if e, ok := u.(opencode.EventListResponseEventFileWatcherUpdated); ok {
			return eventType, fmt.Sprint(e.Properties.Event) + " " + e.Properties.File
		}
		return eventType, ""
	case opencode.EventListResponseTypeSessionUpdated:
		if e, ok := u.(opencode.EventListResponseEventSessionUpdated); ok && e.Properties.Info.ID != "" {
			return eventType, e.Properties.Info.ID
		}
		return eventType, ""
	case opencode.EventListResponseTypeSessionCreated:
		if e, ok := u.(opencode.EventListResponseEventSessionCreated); ok && e.Properties.Info.ID != "" {
			return eventType, e.Properties.Info.ID
		}
		return eventType, ""
	case opencode.EventListResponseTypeSessionDeleted:
		if e, ok := u.(opencode.EventListResponseEventSessionDeleted); ok {
			return eventType, e.Properties.Info.ID
		}
		return eventType, ""
	case opencode.EventListResponseTypeSessionError:
		if e, ok := u.(opencode.EventListResponseEventSessionError); ok {
			return eventType, e.Properties.SessionID
		}
		return eventType, ""
	case opencode.EventListResponseTypeMessageUpdated:
		if e, ok := u.(opencode.EventListResponseEventMessageUpdated); ok && e.Properties.Info.ID != "" {
			return eventType, e.Properties.Info.ID
		}
		return eventType, ""
	case opencode.EventListResponseTypeMessagePartUpdated:
		if e, ok := u.(opencode.EventListResponseEventMessagePartUpdated); ok && e.Properties.Part.ID != "" {
			return eventType, fmt.Sprint(e.Properties.Part.Type) + " " + e.Properties.Part.ID
		}
		return eventType, ""
	default:
		key = propertiesSummary(ev.Properties)
		return eventType, key
	}
}

// propertiesSummary 从 Properties 提取简短摘要，用于未识别或默认事件的 key。
func propertiesSummary(p interface{}) string {
	if p == nil {
		return ""
	}
	b, err := json.Marshal(p)
	if err != nil {
		return fmt.Sprint(p)
	}
	s := string(b)
	if len(s) > eventKeyMaxLen {
		return s[:eventKeyMaxLen] + "..."
	}
	return s
}

// runEventLoop 订阅 OpenCode event 流，收到 permission.updated 时按 autoPermission 自动授权或拒绝。
func (a *Adapter) runEventLoop(ctx context.Context) {
	query := opencode.EventListParams{Directory: opencode.F(a.directory)}
	stream := a.client.Event.ListStreaming(ctx, query)
	if stream == nil {
		slog.Error("public_opencode event stream is nil")
		return
	}
	if stream.Err() != nil {
		slog.Error("public_opencode event stream error", "err", stream.Err())
		return
	}
	defer stream.Close()

	response := a.autoPermissionResponse()
	if response == "" {
		return
	}

	for stream.Next() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		ev := stream.Current()
		u := ev.AsUnion()

		if eventType, key := publicOpencodeEventSummary(ev); eventType != "" {
			slog.Info("public_opencode event", "type", eventType, "key", key)
		}
		perm, ok := u.(opencode.EventListResponseEventPermissionUpdated)
		if !ok {
			continue
		}
		sessionID := perm.Properties.SessionID
		permissionID := perm.Properties.ID
		if sessionID == "" || permissionID == "" {
			continue
		}
		reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := a.client.Session.Permissions.Respond(reqCtx, sessionID, permissionID, opencode.SessionPermissionRespondParams{
			Response:  opencode.F(response),
			Directory: opencode.F(a.directory),
		})
		cancel()
		if err != nil {
			slog.Warn("public_opencode permission respond failed", "sessionID", sessionID, "permissionID", permissionID, "err", err)
			a.logEvent("PERMISSION_RESPOND_FAIL session=%s permission=%s type=%s title=%q err=%v", sessionID, permissionID, perm.Properties.Type, perm.Properties.Title, err)
			continue
		}
		slog.Info("public_opencode permission responded", "sessionID", sessionID, "permissionID", permissionID, "response", response, "type", perm.Properties.Type, "title", perm.Properties.Title)
		a.logEvent("PERMISSION_RESPOND session=%s permission=%s response=%s type=%s title=%q", sessionID, permissionID, response, perm.Properties.Type, perm.Properties.Title)
	}
	if stream.Err() != nil {
		slog.Error("public_opencode event loop error", "err", stream.Err())
	}
}

func (a *Adapter) autoPermissionResponse() opencode.SessionPermissionRespondParamsResponse {
	switch a.autoPermission {
	case "once":
		return opencode.SessionPermissionRespondParamsResponseOnce
	case "always":
		return opencode.SessionPermissionRespondParamsResponseAlways
	case "reject":
		return opencode.SessionPermissionRespondParamsResponseReject
	default:
		return ""
	}
}

func (a *Adapter) loadSessionFromFile() (string, bool) {
	path := filepath.Join(a.directory, sessionFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	var state struct {
		Current string `json:"current"`
	}
	if json.Unmarshal(data, &state) != nil || state.Current == "" {
		return "", false
	}
	return state.Current, true
}

func (a *Adapter) saveSessionToFile(sid string) error {
	dir := filepath.Dir(filepath.Join(a.directory, sessionFile))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	state := struct {
		Current string `json:"current"`
	}{Current: sid}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(a.directory, sessionFile), data, 0644)
}

func (a *Adapter) getOrCreateSession(ctx context.Context) (string, error) {
	a.mu.RLock()
	sid := a.sessionID
	a.mu.RUnlock()
	if sid != "" {
		return sid, nil
	}

	params := opencode.SessionNewParams{
		Title:     opencode.F("public_opencode"),
		Directory: opencode.F(a.directory),
	}
	ses, err := a.client.Session.New(ctx, params)
	if err != nil {
		return "", err
	}
	if ses == nil || ses.ID == "" {
		return "", fmt.Errorf("opencode create session: empty id")
	}

	a.mu.Lock()
	a.sessionID = ses.ID
	a.mu.Unlock()
	if err := a.saveSessionToFile(ses.ID); err != nil {
		slog.Warn("failed to persist session to .session", "err", err)
	}
	return ses.ID, nil
}

func (a *Adapter) ensureFilesInDirectory(ctx context.Context, files []mybot.File) ([]mybot.File, error) {
	if len(files) == 0 {
		return nil, nil
	}
	dir := filepath.Join(a.directory, uploadsDir)
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

func (a *Adapter) buildParts(content string, files []mybot.File) []opencode.SessionPromptParamsPartUnion {
	parts := make([]opencode.SessionPromptParamsPartUnion, 0, 1+len(files))
	if content != "" {
		parts = append(parts, opencode.TextPartInputParam{
			Type: opencode.F(opencode.TextPartInputTypeText),
			Text: opencode.F(content),
		})
	}
	for _, f := range files {
		text := f.URL
		if f.Name != "" {
			text = fmt.Sprintf("%s (%s)", f.Name, f.URL)
		}
		parts = append(parts, opencode.TextPartInputParam{
			Type: opencode.F(opencode.TextPartInputTypeText),
			Text: opencode.F(text),
		})
	}
	if len(parts) == 0 {
		parts = append(parts, opencode.TextPartInputParam{
			Type: opencode.F(opencode.TextPartInputTypeText),
			Text: opencode.F(""),
		})
	}
	return parts
}

func (a *Adapter) prompt(ctx context.Context, sessionID string, parts []opencode.SessionPromptParamsPartUnion) (*opencode.SessionPromptResponse, error) {
	params := opencode.SessionPromptParams{
		Parts:     opencode.F(parts),
		Directory: opencode.F(a.directory),
	}
	resp, err := a.client.Session.Prompt(ctx, sessionID, params)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func collectTextParts(parts []opencode.Part) string {
	var b strings.Builder
	for _, p := range parts {
		if p.Type == opencode.PartTypeText && p.Text != "" {
			if b.Len() > 0 {
				b.WriteString("\n")
			}
			b.WriteString(p.Text)
		}
	}
	return b.String()
}

func collectFileParts(parts []opencode.Part) []mybot.File {
	out := make([]mybot.File, 0)
	for _, p := range parts {
		if p.Type == opencode.PartTypeFile && p.URL != "" {
			out = append(out, filePartToMybot(p.Filename, p.URL, p.Mime, 0))
			continue
		}
		if p.Type == opencode.PartTypeTool && p.State != nil {
			st, ok := p.State.(opencode.ToolPartState)
			if !ok {
				continue
			}
			atts, ok := st.Attachments.([]opencode.FilePart)
			if !ok {
				continue
			}
			for _, att := range atts {
				if att.URL != "" {
					out = append(out, filePartToMybot(att.Filename, att.URL, att.Mime, 0))
				}
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

func listWorkdirFilesModifiedAfter(workdir string, after time.Time) []mybot.File {
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

func (a *Adapter) handleCommand(ctx context.Context, msg mybot.Message) (bool, error) {
	cmd := strings.TrimSpace(msg.Content)
	if cmd == "" || !strings.HasPrefix(cmd, "/") {
		return false, nil
	}
	parts := strings.Fields(cmd)
	verb := parts[0]

	switch verb {
	case "/help":
		return true, a.handleHelp(ctx, msg)
	case "/reset":
		return true, a.handleReset(ctx, msg)
	case "/init":
		scenario := strings.TrimSpace(strings.TrimPrefix(cmd, "/init"))
		return true, a.handleCreateAgent(ctx, msg, scenario)
	case "/history":
		return true, a.handleHistory(ctx, msg)
	case "/plan", "/status":
		return true, a.handlePlan(ctx, msg)
	default:
		_ = a.sendCommandReply(ctx, msg, "未知命令。发送 /help 查看支持的命令。")
		return true, nil
	}
}

func (a *Adapter) sendCommandReply(ctx context.Context, msg mybot.Message, content string) error {
	targetAdapter := msg.SourceAdapter
	if a.GetDefaultTarget() != "" {
		targetAdapter = a.GetDefaultTarget()
	}
	reply := mybot.Message{
		ID:            fmt.Sprintf("poc-cmd-%d", time.Now().UnixNano()),
		ParentID:      msg.ID,
		SourceAdapter: a.id,
		TargetAdapter: targetAdapter,
		Content:       content,
		Type:          mybot.TypeText,
		Timestamp:     time.Now().UnixMilli(),
		UserID:        "public_opencode",
		Channel:       msg.Channel,
	}
	select {
	case a.inbound <- reply:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

const helpText = `**命令说明：**
* /help — 显示本帮助
* /reset — 重置会话，创建新的 Session
* /init <需求描述> — 根据需求由 AI 生成并保存 .opencode/agent/agent.md；若文件已存在会参考后调整
* /history — 查看当前 Session 最近消息列表
* /plan、/status — 查看当前 Session 状态与待办`

func (a *Adapter) handleHelp(ctx context.Context, msg mybot.Message) error {
	return a.sendCommandReply(ctx, msg, helpText)
}

func (a *Adapter) handleReset(ctx context.Context, msg mybot.Message) error {
	a.mu.Lock()
	oldID := a.sessionID
	a.sessionID = ""
	a.mu.Unlock()

	sessionID, err := a.getOrCreateSession(ctx)
	var content string
	if err != nil {
		content = fmt.Sprintf("重置会话失败: %v", err)
	} else {
		content = "已重置当前对话上下文，新的 Session 已创建。"
	}
	if oldID != "" || sessionID != "" {
		a.logEvent("SESSION_RESET old=%s new=%s", oldID, sessionID)
	}
	return a.sendCommandReply(ctx, msg, content)
}

const agentMDPath = ".opencode/agent/agent.md"

const createAgentPrompt = "请根据以下用户需求，直接创建或更新 OpenCode agent 配置文件。\n\n" +
	"要求：\n" +
	"1. 必须使用写文件/编辑工具，将结果保存到项目下的 " + agentMDPath + "（不要只在对话里输出内容）。\n" +
	"2. 若该文件已存在，请先读取现有内容，在此基础上按用户需求调整；若不存在则新建。\n" +
	"3. 文件格式：第一行 ---，YAML frontmatter，再一行 ---，正文为 agent 的 system prompt。\n" +
	"4. frontmatter 约束：description 必填；mode 只能是 subagent、primary、all 之一（小写）；tools 必须是对象/键值对（如 write: true, edit: true, bash: false），不能是数组。\n" +
	"5. 完成后简短确认已保存到 " + agentMDPath + "。"

func (a *Adapter) handleCreateAgent(ctx context.Context, msg mybot.Message, scenario string) error {
	if scenario == "" {
		return a.sendCommandReply(ctx, msg, "用法: /init <需求描述>，例如: /init 我需要一个专门做代码审查的 agent")
	}
	agentDir := filepath.Join(a.directory, ".opencode", "agent")
	if err := os.MkdirAll(agentDir, 0755); err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("创建目录失败: %v", err))
	}
	sessionID, err := a.getOrCreateSession(ctx)
	if err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("创建 agent 失败: 无法获取会话 (%v)", err))
	}
	userContent := createAgentPrompt + "\n\n用户需求：\n" + scenario
	parts := a.buildParts(userContent, nil)
	_, err = a.prompt(ctx, sessionID, parts)
	if err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("生成 agent 失败: %v", err))
	}
	outPath := filepath.Join(a.directory, agentMDPath)
	a.logEvent("CREATE_AGENT path=%s", outPath)
	return a.sendCommandReply(ctx, msg, fmt.Sprintf("已根据需求创建/更新 %s。新会话将使用该 agent。", agentMDPath))
}

func (a *Adapter) handleHistory(ctx context.Context, msg mybot.Message) error {
	sessionID, err := a.getOrCreateSession(ctx)
	if err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("查询历史失败: 无法获取会话 (%v)", err))
	}
	query := opencode.SessionMessagesParams{Directory: opencode.F(a.directory)}
	list, err := a.client.Session.Messages(ctx, sessionID, query)
	if err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("获取会话历史失败: %v", err))
	}
	msgs := list
	if msgs == nil {
		msgs = &[]opencode.SessionMessagesResponse{}
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("当前 Session: %s\n最近 %d 条消息:\n", sessionID, len(*msgs)))
	for i, m := range *msgs {
		var text string
		for _, p := range m.Parts {
			if p.Type == opencode.PartTypeText && p.Text != "" {
				text = p.Text
				if len(text) > 200 {
					text = text[:200] + "..."
				}
				break
			}
		}
		b.WriteString(fmt.Sprintf("%d. [%s] %s\n", i+1, m.Info.Role, text))
	}
	if len(*msgs) == 0 {
		b.WriteString("（暂无消息）")
	}
	return a.sendCommandReply(ctx, msg, strings.TrimSuffix(b.String(), "\n"))
}

func (a *Adapter) handlePlan(ctx context.Context, msg mybot.Message) error {
	sessionID, err := a.getOrCreateSession(ctx)
	if err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("查询状态失败: 无法获取会话 (%v)", err))
	}
	query := opencode.SessionGetParams{Directory: opencode.F(a.directory)}
	ses, err := a.client.Session.Get(ctx, sessionID, query)
	if err != nil {
		return a.sendCommandReply(ctx, msg, fmt.Sprintf("获取会话详情失败: %v", err))
	}
	var statusMap map[string]struct {
		Type    string  `json:"type"`
		Attempt *int    `json:"attempt,omitempty"`
		Message *string `json:"message,omitempty"`
		Next    *int    `json:"next,omitempty"`
	}
	_ = a.client.Get(ctx, "session/status", query, &statusMap)
	statusStr := "unknown"
	if st, ok := statusMap[sessionID]; ok {
		statusStr = st.Type
		if st.Type == "retry" && st.Message != nil {
			statusStr += " (" + *st.Message + ")"
		}
	}
	var todos []struct {
		ID       string `json:"id"`
		Content  string `json:"content"`
		Status   string `json:"status"`
		Priority string `json:"priority"`
	}
	_ = a.client.Get(ctx, "session/"+sessionID+"/todo", query, &todos)

	var b strings.Builder
	b.WriteString(fmt.Sprintf("**Session 状态**\n- ID: %s\n- 工作目录: %s\n- 标题: %s\n- 运行状态: %s\n", ses.ID, ses.Directory, ses.Title, statusStr))
	if len(ses.Summary.Diffs) > 0 {
		var add, del float64
		for _, d := range ses.Summary.Diffs {
			add += d.Additions
			del += d.Deletions
		}
		b.WriteString(fmt.Sprintf("- 变更摘要: +%.0f -%.0f (%d 文件)\n", add, del, len(ses.Summary.Diffs)))
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

func (a *Adapter) logEvent(format string, args ...interface{}) {
	logsDir := filepath.Join(a.directory, "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		slog.Error("failed to create logs directory", "dir", logsDir, "err", err)
		return
	}
	path := filepath.Join(logsDir, "public_session.log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("failed to open session log file", "path", path, "err", err)
		return
	}
	defer f.Close()
	ts := time.Now().Format(time.RFC3339Nano)
	line := fmt.Sprintf("%s "+format+"\n", append([]interface{}{ts}, args...)...)
	if _, err := f.WriteString(line); err != nil {
		slog.Error("failed to write session log", "path", path, "err", err)
	}
}
