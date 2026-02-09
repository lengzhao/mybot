package lark

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	mybot "github.com/lengzhao/mybot"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkapplication "github.com/larksuite/oapi-sdk-go/v3/service/application/v6"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

// Lark 适配器配置键：
// - app_id        (string, 必填)
// - app_secret    (string, 必填)
// - default_target(string, 可选) 默认转发目标适配器

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

	return &Adapter{
		id:            id,
		defaultTarget: defaultTarget,
		appID:         appID,
		appSecret:     appSecret,
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

	// 只处理文本消息，其他类型后续再扩展
	if larkcore.StringValue(msg.MessageType) != larkim.MsgTypeText {
		return nil
	}

	textMsg := new(MessageText)
	if err := json.Unmarshal([]byte(larkcore.StringValue(msg.Content)), textMsg); err != nil {
		slog.Error("lark: failed to unmarshal text content", "err", err)
		return nil
	}

	chatID := larkcore.StringValue(msg.ChatId)
	userID := ""
	if message.Event.Sender != nil && message.Event.Sender.SenderId != nil && message.Event.Sender.SenderId.UserId != nil {
		userID = larkcore.StringValue(message.Event.Sender.SenderId.UserId)
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
		Extra: map[string]interface{}{
			"lark_chat_id":   chatID,
			"lark_msg_id":    larkcore.StringValue(msg.MessageId),
			"lark_msg_type":  larkcore.StringValue(msg.MessageType),
			"lark_chat_type": larkcore.StringValue(msg.ChatType),
		},
	}

	select {
	case a.inbound <- mbMsg:
	case <-ctx.Done():
	}

	return nil
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

	// 使用文本消息回复
	contentStruct := struct {
		Text string `json:"text"`
	}{
		Text: msg.Content,
	}
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
		return fmt.Errorf("lark send message failed: code=%d, msg=%s", resp.Code, resp.Msg)
	}

	return nil
}

func (a *Adapter) Status() string {
	if a.wsClient == nil {
		return "not_started"
	}
	return "running"
}
