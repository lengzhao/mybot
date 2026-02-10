package lark

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	mybot "github.com/lengzhao/mybot"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkapplication "github.com/larksuite/oapi-sdk-go/v3/service/application/v6"
	larkdrive "github.com/larksuite/oapi-sdk-go/v3/service/drive/v1"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
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
	if d, ok := config["workdir"].(string); ok && d != "" {
		directory = d
	}
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
	if message.Event.Sender != nil && message.Event.Sender.SenderId != nil && message.Event.Sender.SenderId.UserId != nil {
		userID = larkcore.StringValue(message.Event.Sender.SenderId.UserId)
	}
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
		mbMsg := mybot.Message{
			ID:            larkcore.StringValue(msg.MessageId),
			SourceAdapter: a.id,
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
		files, err := a.downloadMessageFile(ctx, larkcore.StringValue(msg.MessageId), fileMsg.FileKey)
		if err != nil {
			slog.Error("lark: download message file failed", "file_key", fileMsg.FileKey, "err", err)
			return nil
		}
		content := "用户发送了一个文件"
		if len(files) > 0 && files[0].Name != "" {
			content = "用户发送了文件: " + files[0].Name
		}
		mbMsg := mybot.Message{
			ID:            larkcore.StringValue(msg.MessageId),
			SourceAdapter: a.id,
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
		return nil
	}

	// 其他类型暂不处理
	return nil
}

// downloadMessageFile 通过 message_id + file_key 下载用户发送的文件，保存到 directory/uploads，返回 mybot.File 列表。
func (a *Adapter) downloadMessageFile(ctx context.Context, messageID, fileKey string) ([]mybot.File, error) {
	if a.apiClient == nil || a.directory == "" {
		return nil, fmt.Errorf("adapter not ready or no directory")
	}
	resp, err := a.apiClient.Im.V1.MessageResource.Get(ctx, larkim.NewGetMessageResourceReqBuilder().
		MessageId(messageID).
		FileKey(fileKey).
		Build())
	if err != nil {
		return nil, err
	}
	if !resp.Success() {
		return nil, fmt.Errorf("get message resource: code=%d, msg=%s", resp.Code, resp.Msg)
	}
	name := resp.FileName
	if name == "" {
		name = fileKey
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
