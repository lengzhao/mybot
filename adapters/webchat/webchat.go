package webchat

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"embed"

	"github.com/gorilla/websocket"
	"github.com/lengzhao/mybot"
)

//go:embed static/*
var staticFiles embed.FS

func init() {
	mybot.RegisterAdapterType("webchat", func(id string, config map[string]interface{}) (mybot.Adapter, error) {
		return NewAdapter(id, config), nil
	})
}

// Adapter Web聊天适配器
type Adapter struct {
	id             string
	defaultTarget  string
	port           string
	uploadDir      string
	fileNames      sync.Map // id -> string 原始文件名，用于 GET /files/:id 的 Content-Disposition
	server         *http.Server
	inbound        chan<- mybot.Message
	stateStore     mybot.StateStore // 状态存储引用
	messageMutex   sync.RWMutex
	activeSessions map[string]*Session // 存储活跃会话
	clients        map[*Client]bool    // WebSocket客户端连接
	broadcast      chan mybot.Message  // 广播消息通道
	register       chan *Client        // 注册客户端通道
	unregister     chan *Client        // 注销客户端通道
}

// Client WebSocket客户端结构
type Client struct {
	conn   *websocket.Conn
	send   chan mybot.Message
	userId string
}

// Session 会话结构
type Session struct {
	UserID     string
	LastActive time.Time
}

// NewAdapter 创建新的Web聊天适配器
func NewAdapter(id string, config map[string]interface{}) *Adapter {
	slog.Debug("Creating WebChat Adapter", "id", id, "config", config)

	defaultTarget, _ := config["default_target"].(string)
	port, _ := config["port"].(string)
	if port == "" {
		port = "8080" // 默认端口
	}
	adapterDir, _ := config["adapter_dir"].(string)
	uploadDir := filepath.Join(adapterDir, "uploads")
	if adapterDir == "" {
		uploadDir = filepath.Join(os.TempDir(), "webchat_uploads", id)
	}
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		slog.Warn("WebChat upload dir create failed, uploads may fail", "dir", uploadDir, "err", err)
	}

	adapter := &Adapter{
		id:             id,
		defaultTarget:  defaultTarget,
		port:           port,
		uploadDir:      uploadDir,
		activeSessions: make(map[string]*Session),
		clients:        make(map[*Client]bool),
		broadcast:      make(chan mybot.Message),
		register:       make(chan *Client),
		unregister:     make(chan *Client),
	}

	return adapter
}

func (w *Adapter) GetID() string {
	return w.id
}

func (w *Adapter) GetDefaultTarget() string {
	return w.defaultTarget
}

func (w *Adapter) Start(ctx context.Context, inbound chan<- mybot.Message) error {
	w.inbound = inbound

	// 设置HTTP路由处理器
	http.HandleFunc("/", w.handleIndex) // 添加主页路由
	http.HandleFunc("/ws", w.handleWebSocket)
	http.HandleFunc("/send", w.handlePostSend)
	http.HandleFunc("/upload", w.handleUpload)
	http.HandleFunc("/files/", w.handleFileServe)
	http.HandleFunc("/health", w.handleHealth)
	http.HandleFunc("/sessions", w.handleListSessions)
	// 提供静态文件服务
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFiles))))

	// 启动HTTP服务器
	w.server = &http.Server{
		Addr:    ":" + w.port,
		Handler: nil, // 使用默认的http.DefaultServeMux
	}

	slog.Info("Starting WebChat adapter", "id", w.id, "port", w.port)

	go func() {
		if err := w.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("WebChat server error", "adapter", w.id, "err", err)
		}
	}()

	// 定期清理过期会话
	go w.cleanupSessions(ctx)

	// 启动WebSocket管理协程
	go w.runWebSocketManager()

	return nil
}

// cleanupSessions 清理会话
func (w *Adapter) cleanupSessions(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.messageMutex.Lock()
			for sessionId, session := range w.activeSessions {
				if time.Since(session.LastActive) > 30*time.Minute { // 30分钟未活动则清除
					delete(w.activeSessions, sessionId)
				}
			}
			w.messageMutex.Unlock()
		}
	}
}

// runWebSocketManager WebSocket连接管理协程
func (w *Adapter) runWebSocketManager() {
	for {
		select {
		case client := <-w.register:
			w.messageMutex.Lock()
			w.clients[client] = true
			w.messageMutex.Unlock()
		case client := <-w.unregister:
			w.messageMutex.Lock()
			if _, ok := w.clients[client]; ok {
				delete(w.clients, client)
				// 关闭客户端的send通道
				close(client.send)
			}
			w.messageMutex.Unlock()
		case message := <-w.broadcast:
			w.messageMutex.RLock()
			for client := range w.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(w.clients, client)
				}
			}
			w.messageMutex.RUnlock()
		}
	}
}

// handleIndex 首页处理
func (w *Adapter) handleIndex(rw http.ResponseWriter, r *http.Request) {
	// 从嵌入的文件系统中读取index.html
	content, err := staticFiles.ReadFile("static/index.html")
	if err != nil {
		http.Error(rw, "File not found", http.StatusNotFound)
		slog.Error("Failed to read index.html", "err", err)
		return
	}

	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	rw.Write(content)
}

// handleWebSocket WebSocket连接处理
func (w *Adapter) handleWebSocket(rw http.ResponseWriter, r *http.Request) {
	// 设置WebSocket升级器
	var upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			// 允许从相同来源或localhost来的连接
			origin := r.Header.Get("Origin")
			if origin == "" {
				return true // 没有origin头，可能是非浏览器客户端
			}
			// 简单的origin检查，允许本地访问
			return strings.Contains(origin, "localhost") ||
				strings.Contains(origin, "127.0.0.1") ||
				strings.Contains(origin, r.Host) ||
				strings.HasPrefix(origin, "http://"+r.Host) ||
				strings.HasPrefix(origin, "https://"+r.Host)
		},
	}

	conn, err := upgrader.Upgrade(rw, r, nil)
	if err != nil {
		slog.Error("WebSocket upgrade failed", "err", err)
		return
	}

	client := &Client{
		conn:   conn,
		send:   make(chan mybot.Message, 256), // 缓冲通道，避免阻塞
		userId: r.URL.Query().Get("userId"),   // 从查询参数获取用户ID
	}

	// 注册客户端
	w.register <- client

	// 启动读取和写入协程
	go w.writePump(client)
	go w.readPump(client)
}

// writePump 向WebSocket客户端发送消息
func (w *Adapter) writePump(client *Client) {
	ticker := time.NewTicker(time.Second * 60) // ping周期
	defer func() {
		ticker.Stop()
		client.conn.Close()
	}()

	for {
		select {
		case message, ok := <-client.send:
			if !ok {
				// 发送通道已关闭，退出
				client.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			client.conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
			err := client.conn.WriteJSON(message)
			if err != nil {
				slog.Error("WebSocket write error", "err", err)
				return
			}
		case <-ticker.C:
			// 发送ping消息
			client.conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
			if err := client.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				slog.Error("WebSocket ping error", "err", err)
				return
			}
		}
	}
}

// readPump 从WebSocket客户端读取消息
func (w *Adapter) readPump(client *Client) {
	defer func() {
		w.unregister <- client
		client.conn.Close()
	}()

	client.conn.SetReadLimit(512 << 10) // 512KB限制
	client.conn.SetReadDeadline(time.Now().Add(time.Second * 60))
	client.conn.SetPongHandler(func(string) error {
		client.conn.SetReadDeadline(time.Now().Add(time.Second * 60))
		return nil
	})

	for {
		_, message, err := client.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				slog.Error("WebSocket read error", "err", err)
			}
			break
		}

		// 解析收到的消息
		var msgReq struct {
			SessionID string                 `json:"session_id"`
			Content   string                 `json:"content"`
			Files     []mybot.File           `json:"files,omitempty"`
			Extra     map[string]interface{} `json:"extra,omitempty"`
		}

		if err := json.Unmarshal(message, &msgReq); err != nil {
			slog.Error("WebSocket message parse error", "err", err)
			continue
		}

		if msgReq.SessionID == "" {
			msgReq.SessionID = "default"
		}

		// 更新会话信息
		w.messageMutex.Lock()
		if _, exists := w.activeSessions[msgReq.SessionID]; !exists {
			w.activeSessions[msgReq.SessionID] = &Session{
				UserID:     msgReq.SessionID,
				LastActive: time.Now(),
			}
		} else {
			w.activeSessions[msgReq.SessionID].LastActive = time.Now()
		}
		w.messageMutex.Unlock()

		// 生成唯一消息ID
		msgID := w.generateMessageID()

		// 创建消息（将 /files/:id 转为 file:// 绝对路径，下游无需再走 HTTP）
		msg := mybot.Message{
			ID:            msgID,
			SourceAdapter: w.id,
			TargetAdapter: w.GetDefaultTarget(),
			UserID:        msgReq.SessionID,
			Channel:       "webchat_ws",
			Content:       msgReq.Content,
			Type:          mybot.TypeText,
			Timestamp:     time.Now().UnixMilli(),
			Files:         w.resolveOutboundFiles(msgReq.Files),
			Extra:         msgReq.Extra,
		}

		// 发送到调度中心
		select {
		case w.inbound <- msg:
			slog.Debug("WebSocket message forwarded to inbound", "msg_id", msg.ID)
		case <-time.After(5 * time.Second): // 5秒超时
			slog.Warn("Timeout forwarding WebSocket message", "msg_id", msg.ID)
		}
	}
}

// handleHealth 健康检查
func (w *Adapter) handleHealth(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(map[string]interface{}{
		"status":  "ok",
		"adapter": w.id,
	})
}

// handleListSessions 列出活跃会话
func (w *Adapter) handleListSessions(rw http.ResponseWriter, r *http.Request) {
	w.messageMutex.RLock()
	sessionIDs := make([]string, 0, len(w.activeSessions))
	for sessionId := range w.activeSessions {
		sessionIDs = append(sessionIDs, sessionId)
	}
	w.messageMutex.RUnlock()

	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(map[string]interface{}{
		"sessions": sessionIDs,
		"count":    len(sessionIDs),
	})
}

// handlePostSend 处理POST发送消息请求
func (w *Adapter) handlePostSend(rw http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		rw.Header().Set("Allow", "POST")
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req struct {
		SessionID string                 `json:"session_id"`
		Content   string                 `json:"content"`
		Files     []mybot.File           `json:"files,omitempty"`
		Extra     map[string]interface{} `json:"extra,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	if req.SessionID == "" {
		req.SessionID = "default"
	}

	// 更新会话信息
	w.messageMutex.Lock()
	if _, exists := w.activeSessions[req.SessionID]; !exists {
		w.activeSessions[req.SessionID] = &Session{
			UserID:     req.SessionID,
			LastActive: time.Now(),
		}
	} else {
		w.activeSessions[req.SessionID].LastActive = time.Now()
	}
	w.messageMutex.Unlock()

	// 生成唯一消息ID
	msgID := w.generateMessageID()

	// 创建消息（将 /files/:id 转为 file:// 绝对路径，下游无需再走 HTTP）
	msg := mybot.Message{
		ID:            msgID,
		SourceAdapter: w.id,
		TargetAdapter: w.GetDefaultTarget(),
		UserID:        req.SessionID,
		Channel:       "webchat",
		Content:       req.Content,
		Type:          mybot.TypeText,
		Timestamp:     time.Now().UnixMilli(),
		Files:         w.resolveOutboundFiles(req.Files),
		Extra:         req.Extra,
	}

	// 发送到调度中心
	select {
	case w.inbound <- msg:
		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(map[string]interface{}{
			"message": "Message sent successfully",
			"msg_id":  msg.ID,
		})
	case <-time.After(5 * time.Second): // 5秒超时
		http.Error(rw, "Timeout sending message", http.StatusInternalServerError)
	}
}

// handleUpload 处理文档上传，返回 files 列表（含 url 供后续发消息携带）
func (w *Adapter) handleUpload(rw http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		rw.Header().Set("Allow", "POST")
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	const maxUploadMB = 20
	if err := r.ParseMultipartForm(maxUploadMB << 20); err != nil {
		http.Error(rw, "parse multipart: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.MultipartForm.RemoveAll()

	out := make([]mybot.File, 0)
	for _, headers := range r.MultipartForm.File {
		for _, hdr := range headers {
			f, err := hdr.Open()
			if err != nil {
				slog.Warn("Upload open file failed", "filename", hdr.Filename, "err", err)
				continue
			}
			id, path, err := w.saveUploadedFile(f, hdr)
			f.Close()
			if err != nil {
				slog.Warn("Upload save failed", "filename", hdr.Filename, "err", err)
				continue
			}
			w.fileNames.Store(id, hdr.Filename)
			baseURL := w.requestBaseURL(r)
			out = append(out, mybot.File{
				Name:     hdr.Filename,
				URL:      baseURL + "/files/" + id,
				MimeType: hdr.Header.Get("Content-Type"),
				Size:     hdr.Size,
			})
			_ = path
		}
	}
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(map[string]interface{}{"files": out})
}

func (w *Adapter) saveUploadedFile(f multipart.File, hdr *multipart.FileHeader) (id string, path string, err error) {
	b := make([]byte, 16)
	if _, err = rand.Read(b); err != nil {
		return "", "", err
	}
	id = hex.EncodeToString(b)
	ext := filepath.Ext(hdr.Filename)
	if ext != "" {
		id = id + ext
	}
	path = filepath.Join(w.uploadDir, id)
	dst, err := os.Create(path)
	if err != nil {
		return "", "", err
	}
	_, err = io.Copy(dst, f)
	dst.Close()
	if err != nil {
		os.Remove(path)
		return "", "", err
	}
	return id, path, nil
}

func (w *Adapter) requestBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if s := r.Header.Get("X-Forwarded-Proto"); s != "" {
		scheme = s
	}
	return scheme + "://" + r.Host
}

// resolveOutboundFiles 将消息里指向本适配器 /files/:id 的 URL 转为本地绝对路径 file://，便于下游（如 opencode）直接读文件而无需 HTTP。
func (w *Adapter) resolveOutboundFiles(files []mybot.File) []mybot.File {
	if len(files) == 0 {
		return files
	}
	out := make([]mybot.File, 0, len(files))
	for _, f := range files {
		u, err := url.Parse(f.URL)
		if err != nil {
			out = append(out, f)
			continue
		}
		// 只处理 path 为 /files/:id 的形式（含 http(s) 或相对路径）
		path := strings.TrimPrefix(strings.TrimSuffix(u.Path, "/"), "/")
		if !strings.HasPrefix(path, "files/") {
			out = append(out, f)
			continue
		}
		id := strings.TrimPrefix(path, "files/")
		if id == "" || strings.Contains(id, "/") || strings.Contains(id, "..") {
			out = append(out, f)
			continue
		}
		localPath := filepath.Join(w.uploadDir, id)
		absPath, err := filepath.Abs(localPath)
		if err != nil {
			out = append(out, f)
			continue
		}
		if _, err := os.Stat(absPath); err != nil {
			out = append(out, f)
			continue
		}
		out = append(out, mybot.File{
			Name:     f.Name,
			URL:      "file://" + filepath.ToSlash(absPath),
			MimeType: f.MimeType,
			Size:     f.Size,
		})
	}
	return out
}

// handleFileServe 提供已上传文件的下载
func (w *Adapter) handleFileServe(rw http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "HEAD" {
		rw.Header().Set("Allow", "GET, HEAD")
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/files/")
	if id == "" || strings.Contains(id, "/") || strings.Contains(id, "..") {
		http.Error(rw, "invalid file id", http.StatusBadRequest)
		return
	}
	path := filepath.Join(w.uploadDir, id)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			http.NotFound(rw, r)
			return
		}
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	if info.IsDir() {
		http.NotFound(rw, r)
		return
	}
	if name, ok := w.fileNames.Load(id); ok {
		rw.Header().Set("Content-Disposition", "attachment; filename=\""+strings.ReplaceAll(name.(string), "\"", "%22")+"\"")
	}
	http.ServeFile(rw, r, path)
}

// resolveMessageFiles 将消息中 file:// 附件复制到 webchat 的 upload 目录，并替换为可被前端下载的 URL（/files/:id）。
func (w *Adapter) resolveMessageFiles(ctx context.Context, msg mybot.Message) mybot.Message {
	if len(msg.Files) == 0 {
		return msg
	}
	resolved := make([]mybot.File, 0, len(msg.Files))
	for _, f := range msg.Files {
		u, err := url.Parse(f.URL)
		if err != nil || (u.Scheme != "file" && u.Scheme != "http" && u.Scheme != "https") {
			resolved = append(resolved, f)
			continue
		}
		if u.Scheme == "http" || u.Scheme == "https" {
			resolved = append(resolved, f)
			continue
		}
		path := u.Path
		if u.Host != "" {
			path = u.Host + u.Path
		}
		id, err := w.copyFileToUpload(ctx, path, f.Name)
		if err != nil {
			slog.Warn("WebChat resolve file failed", "url", f.URL, "err", err)
			resolved = append(resolved, f)
			continue
		}
		w.fileNames.Store(id, f.Name)
		resolved = append(resolved, mybot.File{
			Name:     f.Name,
			URL:      "/files/" + id,
			MimeType: f.MimeType,
			Size:     f.Size,
		})
	}
	msg.Files = resolved
	return msg
}

func (w *Adapter) copyFileToUpload(ctx context.Context, srcPath, name string) (id string, err error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return "", err
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return "", err
	}
	if info.IsDir() {
		return "", fmt.Errorf("cannot copy directory")
	}
	b := make([]byte, 16)
	if _, err = rand.Read(b); err != nil {
		return "", err
	}
	id = hex.EncodeToString(b)
	if ext := filepath.Ext(name); ext != "" {
		id = id + ext
	}
	dstPath := filepath.Join(w.uploadDir, id)
	dst, err := os.Create(dstPath)
	if err != nil {
		return "", err
	}
	_, err = io.Copy(dst, src)
	dst.Close()
	if err != nil {
		os.Remove(dstPath)
		return "", err
	}
	return id, nil
}

// generateMessageID 生成唯一消息ID
func (w *Adapter) generateMessageID() string {
	// 生成随机ID
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// 如果随机数生成失败，使用时间戳+随机数
		return fmt.Sprintf("webchat-%d-%d", time.Now().UnixNano(), time.Now().Nanosecond()%10000)
	}
	return "webchat-" + hex.EncodeToString(b)
}

func (w *Adapter) ReceiveMessage(ctx context.Context, msg mybot.Message) error {
	slog.Debug("WebChat adapter received message", "msg_id", msg.ID, "content", msg.Content)

	// 检查消息是否是发送给WebChat适配器的
	if msg.TargetAdapter == w.id || msg.TargetAdapter == "" {
		out := w.resolveMessageFiles(ctx, msg)
		select {
		case w.broadcast <- out:
			slog.Debug("Broadcasting message to clients", "msg_id", msg.ID)
		case <-time.After(time.Second):
			slog.Warn("Failed to broadcast message, channel blocked", "msg_id", msg.ID)
		}

		// 同时也可以考虑将消息存储到会话历史中
		w.messageMutex.Lock()
		if _, exists := w.activeSessions[msg.UserID]; !exists {
			w.activeSessions[msg.UserID] = &Session{
				UserID:     msg.UserID,
				LastActive: time.Now(),
			}
		} else {
			w.activeSessions[msg.UserID].LastActive = time.Now()
		}
		w.messageMutex.Unlock()
	}

	return nil
}

// SetStateStore 设置状态存储引用
func (w *Adapter) SetStateStore(store mybot.StateStore) {
	w.stateStore = store
}

// handleListMessages 处理消息列表查询
func (w *Adapter) handleListMessages(rw http.ResponseWriter, r *http.Request) {
	if w.stateStore == nil {
		http.Error(rw, "StateStore not available", http.StatusServiceUnavailable)
		return
	}

	// 解析查询参数
	filter := mybot.MessageFilter{
		SourceAdapter: r.URL.Query().Get("source_adapter"),
		TargetAdapter: r.URL.Query().Get("target_adapter"),
		UserID:        r.URL.Query().Get("user_id"),
		Channel:       r.URL.Query().Get("channel"),
		Type:          r.URL.Query().Get("type"),
	}

	// 解析时间范围
	if startTime := r.URL.Query().Get("start_time"); startTime != "" {
		if ts, err := strconv.ParseInt(startTime, 10, 64); err == nil {
			filter.StartTime = ts
		}
	}
	if endTime := r.URL.Query().Get("end_time"); endTime != "" {
		if ts, err := strconv.ParseInt(endTime, 10, 64); err == nil {
			filter.EndTime = ts
		}
	}

	// 解析分页参数
	if limit := r.URL.Query().Get("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil && l > 0 {
			filter.Limit = l
		}
	}
	if offset := r.URL.Query().Get("offset"); offset != "" {
		if o, err := strconv.Atoi(offset); err == nil && o >= 0 {
			filter.Offset = o
		}
	}

	// 默认限制
	if filter.Limit == 0 {
		filter.Limit = 50
	}
	if filter.Limit > 1000 {
		filter.Limit = 1000 // 最大1000条
	}

	// 查询消息
	messages, err := w.stateStore.QueryMessages(filter)
	if err != nil {
		slog.Error("Failed to query messages", "err", err)
		http.Error(rw, "Failed to query messages", http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(map[string]interface{}{
		"messages": messages,
		"count":    len(messages),
		"filter":   filter,
	})
}

func (w *Adapter) Status() string {
	return fmt.Sprintf("running on port %s", w.port)
}
