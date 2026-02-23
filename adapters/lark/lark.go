package lark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	mybot "github.com/lengzhao/mybot"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkapplication "github.com/larksuite/oapi-sdk-go/v3/service/application/v6"
	larkdrive "github.com/larksuite/oapi-sdk-go/v3/service/drive/v1"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const uploadsDir = "uploads"

// Lark 适配器配置键：
// - app_id            (string, 必填)
// - app_secret        (string, 必填)
// - default_target    (string, 可选) 默认转发目标适配器
// - adapter_dir       (string, 可选) 工作目录，用于存放用户上传的文件；不设则无法接收文件
// - drive_parent_node (string, 可选) 云文档父目录 token，用于回复时上传附件；不设则回复仅发文本不发文件

func init() {
	mybot.RegisterAdapterType("lark", func(id string, config map[string]interface{}) (mybot.Adapter, error) {
		return NewAdapter(id, config)
	})
}

// Adapter Lark IM 适配器
type Adapter struct {
	id            string
	defaultTarget string

	appID     string
	appSecret string

	directory       string // 工作目录，用于存放下载的用户文件
	driveParentNode string // 云文档父目录 token，回复时上传附件用

	inbound chan<- mybot.Message

	botName   string
	apiClient *lark.Client
	wsClient  *larkws.Client
	db        *gorm.DB
	dbMu      sync.Mutex
}

// aclEntry 记录 Lark 侧的管理员和白名单信息。
// - 若 UserID 不为空且 IsAdmin=true，则代表该用户是管理员；
// - 若 Allowed=true：
//   - 且 UserID 不为空、ChatID 为空：该用户在所有会话中允许；
//   - 且 ChatID 不为空、UserID 为空：该会话中的所有用户允许；
//   - 且 UserID 与 ChatID 都不为空：只允许该用户在该会话中使用。
type aclEntry struct {
	ID        uint   `gorm:"primaryKey"`
	UserID    string `gorm:"index"`
	ChatID    string `gorm:"index"`
	IsAdmin   bool   `gorm:"index"`
	Allowed   bool   `gorm:"index"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

// initACL 初始化 / 打开 ACL 所在的 SQLite 数据库。
func (a *Adapter) initACL() error {
	a.dbMu.Lock()
	defer a.dbMu.Unlock()

	if a.db != nil {
		return nil
	}

	// 优先使用适配器工作目录；若未配置，则退化到系统临时目录。
	baseDir := a.directory
	if baseDir == "" {
		baseDir = filepath.Join(os.TempDir(), "mybot_lark")
	}
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return fmt.Errorf("create acl dir: %w", err)
	}

	dbPath := filepath.Join(baseDir, fmt.Sprintf("lark_acl_%s.db", a.id))
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("open acl db: %w", err)
	}
	if err := db.AutoMigrate(&aclEntry{}); err != nil {
		return fmt.Errorf("migrate acl db: %w", err)
	}

	a.db = db
	slog.Info("lark: acl db initialized", "adapter", a.id, "path", dbPath)
	return nil
}

// ensureFirstAdmin 如果还没有管理员，则将当前用户设为首个管理员（同时加入白名单）。
func (a *Adapter) ensureFirstAdmin(userID, chatID string) {
	if userID == "" {
		return
	}
	if err := a.initACL(); err != nil {
		slog.Warn("lark: init ACL db failed when ensuring first admin", "err", err)
		return
	}

	var count int64
	if err := a.db.Model(&aclEntry{}).
		Where("is_admin = ?", true).
		Count(&count).Error; err != nil {
		slog.Warn("lark: query admin count failed", "err", err)
		return
	}
	if count > 0 {
		return
	}

	entry := &aclEntry{
		UserID:  userID,
		ChatID:  chatID,
		IsAdmin: true,
		Allowed: true,
	}
	if err := a.db.Create(entry).Error; err != nil {
		slog.Warn("lark: create first admin failed", "user_id", userID, "err", err)
		return
	}
	slog.Info("lark: first admin created", "user_id", userID, "chat_id", chatID)
}

func (a *Adapter) isAdmin(userID string) bool {
	if userID == "" {
		return false
	}
	if err := a.initACL(); err != nil {
		slog.Warn("lark: init ACL db failed when checking admin", "err", err)
		return false
	}

	var count int64
	if err := a.db.Model(&aclEntry{}).
		Where("user_id = ? AND is_admin = ?", userID, true).
		Count(&count).Error; err != nil {
		slog.Warn("lark: query admin failed", "err", err)
		return false
	}
	return count > 0
}

func (a *Adapter) isAllowed(userID, chatID string) bool {
	if err := a.initACL(); err != nil {
		// 如果 ACL 系统都初始化不了，避免直接拒绝所有消息，这里选择放行并记录日志。
		slog.Warn("lark: init ACL db failed when checking allow, fallback to allow", "err", err)
		return true
	}

	// 管理员始终允许。
	if a.isAdmin(userID) {
		return true
	}

	if userID == "" && chatID == "" {
		return false
	}

	var count int64
	q := a.db.Model(&aclEntry{}).Where("allowed = ?", true).Where(
		a.db.Where("user_id = ? AND (chat_id = '' OR chat_id = ?)", userID, chatID).
			Or("chat_id = ? AND user_id = ''", chatID),
	)
	if err := q.Count(&count).Error; err != nil {
		slog.Warn("lark: query allowed failed, fallback to allow", "err", err)
		return true
	}
	return count > 0
}

// addChatToWhitelist 将当前会话加入白名单。
func (a *Adapter) addChatToWhitelist(chatID string) error {
	if chatID == "" {
		return fmt.Errorf("empty chatID")
	}
	if err := a.initACL(); err != nil {
		return err
	}

	var entry aclEntry
	err := a.db.Where("chat_id = ? AND user_id = ''", chatID).First(&entry).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			entry = aclEntry{
				UserID:  "",
				ChatID:  chatID,
				Allowed: true,
			}
			return a.db.Create(&entry).Error
		}
		return err
	}
	entry.Allowed = true
	return a.db.Save(&entry).Error
}

// handleAdminCommand 处理管理员在 Lark 中发送的管理指令。
// 当前支持：
// - "#allow"：将当前会话加入白名单。
// 返回值表示该消息是否被当作管理员指令消费（true 时不会再转发到调度中心）。
func (a *Adapter) handleAdminCommand(ctx context.Context, userID, chatID, content string) bool {
	if !a.isAdmin(userID) {
		return false
	}

	content = strings.TrimSpace(content)
	switch content {
	case "#allow":
		if err := a.addChatToWhitelist(chatID); err != nil {
			slog.Warn("lark: add chat to whitelist failed", "chat_id", chatID, "err", err)
			a.sendText(ctx, chatID, "添加白名单失败，请查看服务端日志")
		} else {
			a.sendText(ctx, chatID, "当前会话已加入白名单")
		}
		return true
	}

	return false
}

// sendText 直接向指定会话发送一条文本消息，用于权限提示等系统信息。
func (a *Adapter) sendText(ctx context.Context, chatID, text string) {
	if a.apiClient == nil || chatID == "" || strings.TrimSpace(text) == "" {
		return
	}

	contentStruct := struct {
		Text string `json:"text"`
	}{Text: text}
	contentBytes, err := json.Marshal(contentStruct)
	if err != nil {
		return
	}

	req := larkim.NewCreateMessageReqBuilder().
		ReceiveIdType(larkim.ReceiveIdTypeChatId).
		Body(larkim.NewCreateMessageReqBodyBuilder().
			MsgType(larkim.MsgTypeText).
			ReceiveId(chatID).
			Content(string(contentBytes)).
			Build()).
		Build()

	resp, err := a.apiClient.Im.V1.Message.Create(ctx, req)
	if err != nil {
		slog.Warn("lark: sendText failed", "chat_id", chatID, "err", err)
		return
	}
	if !resp.Success() {
		slog.Warn("lark: sendText failed", "chat_id", chatID, "code", resp.Code, "msg", resp.Msg)
	}
}

// MessageText Lark 文本消息结构
type MessageText struct {
	Text string `json:"text"`
}

// NewAdapter 创建 Lark 适配器
func NewAdapter(id string, config map[string]interface{}) (*Adapter, error) {
	appID, _ := config["app_id"].(string)
	appSecret, _ := config["app_secret"].(string)
	if appID == "" || appSecret == "" {
		return nil, fmt.Errorf("lark adapter requires app_id and app_secret")
	}

	defaultTarget, _ := config["default_target"].(string)
	directory, _ := config["adapter_dir"].(string)
	if directory != "" {
		if abs, err := filepath.Abs(directory); err == nil {
			directory = abs
		}
	}
	driveParentNode, _ := config["drive_parent_node"].(string)

	return &Adapter{
		id:              id,
		defaultTarget:   defaultTarget,
		appID:           appID,
		appSecret:       appSecret,
		directory:       directory,
		driveParentNode: driveParentNode,
	}, nil
}

func (a *Adapter) GetID() string {
	return a.id
}

func (a *Adapter) GetDefaultTarget() string {
	return a.defaultTarget
}

// Start 启动 Lark WebSocket 客户端并监听消息
func (a *Adapter) Start(ctx context.Context, inbound chan<- mybot.Message) error {
	a.inbound = inbound

	if a.directory != "" {
		if err := os.MkdirAll(filepath.Join(a.directory, uploadsDir), 0755); err != nil {
			slog.Warn("lark: failed to create uploads dir", "dir", a.directory, "err", err)
		}
	}

	// 初始化 Lark Client
	a.apiClient = lark.NewClient(a.appID, a.appSecret)

	// 获取机器人应用信息，主要是名称，用于 @ 过滤等
	resp, err := a.apiClient.Application.Application.Get(ctx, larkapplication.NewGetApplicationReqBuilder().
		AppId(a.appID).Lang("zh_cn").Build())
	if err != nil || !resp.Success() {
		slog.Error("lark: failed to get application info", "err", err)
	} else if resp.Data != nil && resp.Data.App != nil && resp.Data.App.AppName != nil {
		a.botName = larkcore.StringValue(resp.Data.App.AppName)
		slog.Info("Lark adapter started",
			"adapter", a.id,
			"bot_name", a.botName,
			"app_id", a.appID,
		)
	}

	// 事件分发器：只关心消息事件
	eventHandler := dispatcher.NewEventDispatcher("", "").
		OnP2MessageReceiveV1(a.handleMessage)

	// WebSocket 客户端
	a.wsClient = larkws.NewClient(a.appID, a.appSecret,
		larkws.WithEventHandler(eventHandler),
		larkws.WithLogLevel(larkcore.LogLevelInfo),
	)

	go func() {
		if err := a.wsClient.Start(ctx); err != nil {
			slog.Error("lark: ws client exited with error", "adapter", a.id, "err", err)
		}
	}()

	return nil
}

// handleMessage 将 Lark 消息转换为 mybot.Message 推送到调度中心
func (a *Adapter) handleMessage(ctx context.Context, message *larkim.P2MessageReceiveV1) error {
	if message == nil || message.Event == nil || message.Event.Message == nil {
		return nil
	}

	msg := message.Event.Message
	msgType := larkcore.StringValue(msg.MessageType)
	chatID := larkcore.StringValue(msg.ChatId)
	userID := ""
	if message.Event.Sender != nil && message.Event.Sender.SenderId != nil {
		sid := message.Event.Sender.SenderId
		// 优先使用 UserId，其次 OpenId，最后 UnionId，保证能拿到一个稳定的用户标识
		if sid.UserId != nil && larkcore.StringValue(sid.UserId) != "" {
			userID = larkcore.StringValue(sid.UserId)
		} else if sid.OpenId != nil && larkcore.StringValue(sid.OpenId) != "" {
			userID = larkcore.StringValue(sid.OpenId)
		} else if sid.UnionId != nil && larkcore.StringValue(sid.UnionId) != "" {
			userID = larkcore.StringValue(sid.UnionId)
		}
	}

	// 确保首个管理员存在：第一条消息的发送者会被自动设为管理员并加入白名单。
	a.ensureFirstAdmin(userID, chatID)

	extra := map[string]interface{}{
		"lark_chat_id":   chatID,
		"lark_msg_id":    larkcore.StringValue(msg.MessageId),
		"lark_msg_type":  msgType,
		"lark_chat_type": larkcore.StringValue(msg.ChatType),
	}

	switch msgType {
	case larkim.MsgTypeText:
		textMsg := new(MessageText)
		if err := json.Unmarshal([]byte(larkcore.StringValue(msg.Content)), textMsg); err != nil {
			slog.Error("lark: failed to unmarshal text content", "err", err)
			return nil
		}

		// 优先处理管理员指令（不会转发到调度中心）
		if a.handleAdminCommand(ctx, userID, chatID, textMsg.Text) {
			return nil
		}

		// 非白名单用户 / 会话先拦截，不转发给调度中心。
		if !a.isAllowed(userID, chatID) {
			a.sendText(ctx, chatID, "你还未被授权使用此机器人，请联系管理员在当前会话发送 #allow 进行授权。")
			return nil
		}

		mbMsg := mybot.Message{
			ID:            larkcore.StringValue(msg.MessageId),
			SourceAdapter: a.GetID(),
			TargetAdapter: a.defaultTarget,
			UserID:        userID,
			Channel:       chatID,
			Content:       textMsg.Text,
			Type:          mybot.TypeText,
			Timestamp:     time.Now().UnixMilli(),
			Extra:         extra,
		}
		select {
		case a.inbound <- mbMsg:
		case <-ctx.Done():
		}
		return nil

	case larkim.MsgTypeFile:
		fileMsg := new(larkim.MessageFile)
		if err := json.Unmarshal([]byte(larkcore.StringValue(msg.Content)), fileMsg); err != nil || fileMsg.FileKey == "" {
			slog.Warn("lark: invalid file message content", "err", err)
			return nil
		}
		a.handleMessageResource(ctx, msg, userID, chatID, extra, fileMsg.FileKey, "file", "文件")
		return nil

	case larkim.MsgTypeImage:
		imageMsg := new(larkim.MessageImage)
		if err := json.Unmarshal([]byte(larkcore.StringValue(msg.Content)), imageMsg); err != nil || imageMsg.ImageKey == "" {
			slog.Warn("lark: invalid image message content", "err", err)
			return nil
		}
		a.handleMessageResource(ctx, msg, userID, chatID, extra, imageMsg.ImageKey, "image", "图片")
		return nil

	case larkim.MsgTypeAudio:
		audioMsg := new(larkim.MessageAudio)
		if err := json.Unmarshal([]byte(larkcore.StringValue(msg.Content)), audioMsg); err != nil || audioMsg.FileKey == "" {
			slog.Warn("lark: invalid audio message content", "err", err)
			return nil
		}
		a.handleMessageResource(ctx, msg, userID, chatID, extra, audioMsg.FileKey, "file", "音频")
		return nil

	case larkim.MsgTypeMedia:
		mediaMsg := new(larkim.MessageMedia)
		if err := json.Unmarshal([]byte(larkcore.StringValue(msg.Content)), mediaMsg); err != nil || mediaMsg.FileKey == "" {
			slog.Warn("lark: invalid media message content", "err", err)
			return nil
		}
		a.handleMessageResource(ctx, msg, userID, chatID, extra, mediaMsg.FileKey, "file", "视频")
		return nil
	}

	// 其他类型暂不处理
	return nil
}

// handleMessageResource 校验白名单与工作目录后，下载消息中的资源（文件/图片/音频/视频）并转发到调度中心。
func (a *Adapter) handleMessageResource(ctx context.Context, msg *larkim.EventMessage, userID, chatID string, extra map[string]interface{}, resourceKey, resourceType, contentLabel string) {
	if !a.isAllowed(userID, chatID) {
		a.sendText(ctx, chatID, "当前会话或用户尚未被授权接收"+contentLabel+"消息，请联系管理员在会话中发送 #allow。")
		return
	}
	if a.directory == "" {
		a.sendText(ctx, chatID, "当前未配置工作目录，无法接收"+contentLabel+"。请在 system.work_dir 或适配器配置中设置 adapter_dir。")
		return
	}
	files, err := a.downloadMessageResource(ctx, larkcore.StringValue(msg.MessageId), resourceKey, resourceType)
	if err != nil {
		slog.Error("lark: download message resource failed", "key", resourceKey, "type", resourceType, "err", err)
		a.sendText(ctx, chatID, contentLabel+"下载失败，请稍后重试。")
		return
	}
	content := "用户发送了" + contentLabel
	if len(files) > 0 && files[0].Name != "" {
		content = "用户发送了" + contentLabel + ": " + files[0].Name
	}
	mbMsg := mybot.Message{
		ID:            larkcore.StringValue(msg.MessageId),
		SourceAdapter: a.GetID(),
		TargetAdapter: a.defaultTarget,
		UserID:        userID,
		Channel:       chatID,
		Content:       content,
		Type:          mybot.TypeText,
		Timestamp:     time.Now().UnixMilli(),
		Files:         files,
		Extra:         extra,
	}
	select {
	case a.inbound <- mbMsg:
	case <-ctx.Done():
	}
}

// downloadMessageResource 通过 message_id + key 下载消息中的资源，resourceType 为 "file"（文件/音频/视频）或 "image"（图片），保存到 directory/uploads，返回 mybot.File 列表。
func (a *Adapter) downloadMessageResource(ctx context.Context, messageID, key, resourceType string) ([]mybot.File, error) {
	if a.apiClient == nil || a.directory == "" {
		return nil, fmt.Errorf("adapter not ready or no directory")
	}
	// 飞书 API 要求必传 type：file 表示消息中的文件/音频/视频，image 表示图片
	resp, err := a.apiClient.Im.V1.MessageResource.Get(ctx, larkim.NewGetMessageResourceReqBuilder().
		MessageId(messageID).
		FileKey(key).
		Type(resourceType).
		Build())
	if err != nil {
		return nil, err
	}
	if !resp.Success() {
		return nil, fmt.Errorf("get message resource: code=%d, msg=%s", resp.Code, resp.Msg)
	}
	name := resp.FileName
	if name == "" {
		name = key
	}
	name = sanitizeFileName(name)
	dir := filepath.Join(a.directory, uploadsDir)
	localPath := filepath.Join(dir, name)
	if err := resp.WriteFile(localPath); err != nil {
		return nil, err
	}
	absPath, _ := filepath.Abs(localPath)
	var size int64
	if info, err := os.Stat(localPath); err == nil {
		size = info.Size()
	}
	return []mybot.File{{
		Name: name,
		URL:  "file://" + filepath.ToSlash(absPath),
		Size: size,
	}}, nil
}

func sanitizeFileName(s string) string {
	s = strings.ReplaceAll(s, "..", "_")
	s = strings.TrimSpace(s)
	if s == "" {
		return "file"
	}
	return s
}

// ReceiveMessage 将调度中心的消息发送回 Lark
func (a *Adapter) ReceiveMessage(ctx context.Context, msg mybot.Message) error {
	// 只处理发给自己的消息
	if msg.TargetAdapter != "" && msg.TargetAdapter != a.id {
		return nil
	}

	if a.apiClient == nil {
		return fmt.Errorf("lark adapter not started")
	}

	chatID := msg.Channel
	if chatID == "" && msg.Extra != nil {
		if v, ok := msg.Extra["lark_chat_id"].(string); ok {
			chatID = v
		}
	}
	if chatID == "" {
		slog.Warn("lark: missing chat id, skip sending", "msg_id", msg.ID)
		return nil
	}

	// 1. 先发文本消息（若有内容）
	if msg.Content != "" {
		contentStruct := struct {
			Text string `json:"text"`
		}{Text: msg.Content}
		contentBytes, err := json.Marshal(contentStruct)
		if err != nil {
			return err
		}
		req := larkim.NewCreateMessageReqBuilder().
			ReceiveIdType(larkim.ReceiveIdTypeChatId).
			Body(larkim.NewCreateMessageReqBodyBuilder().
				MsgType(larkim.MsgTypeText).
				ReceiveId(chatID).
				Content(string(contentBytes)).
				Build()).
			Build()
		resp, err := a.apiClient.Im.V1.Message.Create(ctx, req)
		if err != nil {
			return err
		}
		if !resp.Success() {
			return fmt.Errorf("lark send text failed: code=%d, msg=%s", resp.Code, resp.Msg)
		}
	}

	// 2. 若有附件且配置了云文档父目录，则上传并逐条发送文件消息
	if len(msg.Files) > 0 {
		if a.driveParentNode == "" {
			slog.Debug("lark: skip sending files (drive_parent_node not set)", "count", len(msg.Files))
		} else {
			for _, f := range msg.Files {
				fileKey, err := a.uploadFileToDrive(ctx, f)
				if err != nil {
					slog.Warn("lark: upload file failed", "name", f.Name, "err", err)
					continue
				}
				fileContent := struct {
					FileKey string `json:"file_key"`
				}{FileKey: fileKey}
				contentBytes, _ := json.Marshal(fileContent)
				req := larkim.NewCreateMessageReqBuilder().
					ReceiveIdType(larkim.ReceiveIdTypeChatId).
					Body(larkim.NewCreateMessageReqBodyBuilder().
						MsgType(larkim.MsgTypeFile).
						ReceiveId(chatID).
						Content(string(contentBytes)).
						Build()).
					Build()
				resp, err := a.apiClient.Im.V1.Message.Create(ctx, req)
				if err != nil {
					slog.Warn("lark: send file message failed", "name", f.Name, "err", err)
					continue
				}
				if !resp.Success() {
					slog.Warn("lark: send file message failed", "name", f.Name, "code", resp.Code, "msg", resp.Msg)
				}
			}
		}
	}

	return nil
}

// uploadFileToDrive 将 mybot.File（file:// 或 http(s)）上传到云文档，返回 file_token 供发消息使用。
func (a *Adapter) uploadFileToDrive(ctx context.Context, f mybot.File) (string, error) {
	if a.apiClient == nil || a.driveParentNode == "" {
		return "", fmt.Errorf("adapter not ready or no drive_parent_node")
	}
	reader, size, err := openFileReader(ctx, f)
	if err != nil {
		return "", err
	}
	if c, ok := reader.(io.Closer); ok {
		defer c.Close()
	}
	name := f.Name
	if name == "" {
		name = "file"
	}
	body := larkdrive.NewUploadAllFileReqBodyBuilder().
		FileName(name).
		ParentType(larkdrive.ParentTypeExplorer).
		ParentNode(a.driveParentNode).
		Size(int(size)).
		File(reader).
		Build()
	req := larkdrive.NewUploadAllFileReqBuilder().Body(body).Build()
	resp, err := a.apiClient.Drive.V1.File.UploadAll(ctx, req)
	if err != nil {
		return "", err
	}
	if !resp.Success() {
		return "", fmt.Errorf("drive upload: code=%d, msg=%s", resp.Code, resp.Msg)
	}
	if resp.Data == nil || resp.Data.FileToken == nil {
		return "", fmt.Errorf("drive upload: no file_token in response")
	}
	return *resp.Data.FileToken, nil
}

// openFileReader 根据 File.URL 打开可读流并返回大小。调用方负责关闭 reader（若为 *os.File）。
func openFileReader(ctx context.Context, f mybot.File) (io.Reader, int64, error) {
	u, err := url.Parse(f.URL)
	if err != nil {
		return nil, 0, err
	}
	switch u.Scheme {
	case "file":
		path := u.Path
		if u.Host != "" {
			path = u.Host + u.Path
		}
		file, err := os.Open(path)
		if err != nil {
			return nil, 0, err
		}
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, 0, err
		}
		return file, info.Size(), nil
	case "http", "https":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.URL, nil)
		if err != nil {
			return nil, 0, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, 0, err
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, 0, fmt.Errorf("download %s: %d", f.URL, resp.StatusCode)
		}
		size := resp.ContentLength
		if size < 0 {
			size = 0
		}
		return resp.Body, size, nil
	default:
		return nil, 0, fmt.Errorf("unsupported URL scheme: %s", u.Scheme)
	}
}

func (a *Adapter) Status() string {
	if a.wsClient == nil {
		return "not_started"
	}
	return "running"
}
