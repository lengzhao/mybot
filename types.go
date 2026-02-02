package mybot

import (
	"context"
)

// MessageType 定义消息载荷类型
type MessageType string

const (
	TypeText    MessageType = "text"
	TypeImage   MessageType = "image"
	TypeFile    MessageType = "file"
	TypeCommand MessageType = "command"
)

// Message 统一消息模型
type Message struct {
	ID            string   `json:"id"`             // 消息唯一标识
	SourceAdapter string   `json:"source_adapter"` // 来源适配器 ID
	TargetAdapter string   `json:"target_adapter"` // 目标适配器 ID (可选，P2P 路由)
	Tags          []string `json:"tags"`           // 标签列表 (可选，用于分组路由)

	// 核心业务上下文
	UserID  string `json:"user_id"` // 发送者唯一 ID
	Channel string `json:"channel"` // 频道/群组/会话 ID

	// 核心载荷
	Content   string      `json:"content"`   // 文本内容或主要 Payload
	Type      MessageType `json:"type"`      // 消息类型
	Timestamp int64       `json:"timestamp"` // 发生时间 (Unix 毫秒)

	// 扩展字段
	Extra map[string]interface{} `json:"extra"`
}

// Adapter 适配器接口
type Adapter interface {
	// GetID 注册标识符
	GetID() string

	// GetTags 返回该适配器的属性标签 (用于标签路由)
	GetTags() []string

	// Start 启动监听：接收端上事件 -> 封装 Message -> 投递至 inbound
	Start(ctx context.Context, inbound chan<- Message) error

	// ReceiveMessage 接收调度中心转发的消息并执行具体逻辑
	ReceiveMessage(ctx context.Context, msg Message) error

	// Status 返回该适配器的当前状态
	Status() string
}
