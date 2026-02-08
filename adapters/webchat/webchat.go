package webchat

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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

	adapter := &Adapter{
		id:             id,
		defaultTarget:  defaultTarget,
		port:           port,
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
	http.HandleFunc("/health", w.handleHealth)
	http.HandleFunc("/sessions", w.handleListSessions)
	// StateStore管理API
	http.HandleFunc("/api/messages", w.handleListMessages)
	http.HandleFunc("/api/stats", w.handleGetStats)
	http.HandleFunc("/admin", w.handleAdminPage)
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

		// 创建消息
		msg := mybot.Message{
			ID:            msgID,
			SourceAdapter: w.id,
			TargetAdapter: w.GetDefaultTarget(),
			UserID:        msgReq.SessionID,
			Channel:       "webchat_ws",
			Content:       msgReq.Content,
			Type:          mybot.TypeText,
			Timestamp:     time.Now().UnixMilli(),
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

	// 创建消息
	msg := mybot.Message{
		ID:            msgID,
		SourceAdapter: w.id,
		TargetAdapter: w.GetDefaultTarget(),
		UserID:        req.SessionID,
		Channel:       "webchat",
		Content:       req.Content,
		Type:          mybot.TypeText,
		Timestamp:     time.Now().UnixMilli(),
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
		// 将消息广播给所有连接的客户端
		select {
		case w.broadcast <- msg:
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

// handleGetStats 处理统计信息查询
func (w *Adapter) handleGetStats(rw http.ResponseWriter, r *http.Request) {
	if w.stateStore == nil {
		http.Error(rw, "StateStore not available", http.StatusServiceUnavailable)
		return
	}

	stats, err := w.stateStore.GetStats()
	if err != nil {
		slog.Error("Failed to get stats", "err", err)
		http.Error(rw, "Failed to get stats", http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(stats)
}

// handleAdminPage 处理管理页面
func (w *Adapter) handleAdminPage(rw http.ResponseWriter, r *http.Request) {
	adminPageHTML := `<!DOCTYPE html><html><head><meta charset="utf-8"><title>MyBot StateStore 管理页面</title></head>" +
		"<body><h1>MyBot StateStore 管理面板</h1>" +
		"<p><a href="/api/stats">查看统计信息</a> | <a href="/api/messages?limit=50">查看最新消息</a></p>" +
		"<h2>API接口:</h2>" +
		"<ul><li>GET /api/stats - 获取统计信息</li>" +
		"<li>GET /api/messages - 查询消息 (支持参数: source_adapter, target_adapter, user_id, channel, type, limit, offset)</li></ul>" +
		"</body></html>`

	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	rw.Write([]byte(adminPageHTML))
}

func (w *Adapter) Status() string {
	return fmt.Sprintf("running on port %s", w.port)
}
