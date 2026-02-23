package mybot

import (
	"context"
)

// MessageType 定义消息载荷类型
// 附件统一走 Message.Files，此处仅表示载荷语义（文本/命令/交互/事件/思考等）
type MessageType string

const (
	TypeText        MessageType = "text"        // 普通文本
	TypeCommand     MessageType = "command"     // 用户命令（如 /reset）
	TypeInteraction MessageType = "interaction" // 交互：权限确认、问答选择等，可映射为按钮/卡片
	TypeEvent       MessageType = "event"       // 平台或系统事件
	TypeThinking    MessageType = "thinking"    // AI 思考过程/中间状态（如流式推理片段）
)

// StateStore 状态存储接口
type StateStore interface {
	// RecordMessage 记录消息
	RecordMessage(msg Message) error

	// QueryMessages 查询消息历史
	QueryMessages(filter MessageFilter) ([]MessageRecord, error)

	// GetStats 获取统计信息
	GetStats() (Stats, error)

	// Start 启动存储服务
	Start() error

	// Stop 停停止存储服务
	Stop() error
}

// MessageRecord 消息记录：内嵌 Message，仅增加存储相关字段
type MessageRecord struct {
	Message
	ProcessedAt int64 `json:"processed_at" db:"processed_at"` // 处理时间
}

// MessageFilter 消息查询过滤器
type MessageFilter struct {
	SourceAdapter string `json:"source_adapter"`
	TargetAdapter string `json:"target_adapter"`
	UserID        string `json:"user_id"`
	Channel       string `json:"channel"`
	Type          string `json:"type"`

	// 时间范围
	StartTime int64 `json:"start_time"`
	EndTime   int64 `json:"end_time"`

	// 分页
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

// Stats 统计信息
type Stats struct {
	TotalMessages     int64            `json:"total_messages"`
	MessagesByAdapter map[string]int64 `json:"messages_by_adapter"`
	MessagesByUser    map[string]int64 `json:"messages_by_user"`
	MessagesByChannel map[string]int64 `json:"messages_by_channel"`
	MessagesByType    map[string]int64 `json:"messages_by_type"`
}

// File 消息附件
// 约定：Message 中的 Files 默认应存于「处理该消息的 adapter 的 directory」下；
// 能处理文件的 adapter 在接收消息时，应先把附件转存到自己的工作目录再处理。
type File struct {
	Name     string `json:"name"      db:"name"`      // 文件名
	URL      string `json:"url"       db:"url"`       // 文件地址 (http(s) 或 file://)
	MimeType string `json:"mime_type" db:"mime_type"` // MIME 类型
	Size     int64  `json:"size"      db:"size"`      // 字节数，可选
}

// Message 统一消息模型
type Message struct {
	ID            string `json:"id"              db:"id"`
	ParentID      string `json:"parent_id"       db:"parent_id"` // 所回复的消息 ID，空表示根消息
	SourceAdapter string `json:"source_adapter"  db:"source_adapter"`
	TargetAdapter string `json:"target_adapter"  db:"target_adapter"`

	// 核心业务上下文
	UserID  string `json:"user_id"  db:"user_id"` // 发送者唯一 ID
	Channel string `json:"channel"  db:"channel"` // 频道/群组/会话 ID

	// 核心载荷
	Content   string      `json:"content"   db:"content"`   // 文本内容或主要 Payload
	Type      MessageType `json:"type"      db:"type"`      // 消息类型
	Timestamp int64       `json:"timestamp" db:"timestamp"` // 发生时间 (Unix 毫秒)
	Files     []File      `json:"files"     db:"files"`     // 附件列表

	// 扩展字段
	Extra map[string]interface{} `json:"extra" db:"extra"`
}

// Adapter 适配器接口
type Adapter interface {
	// GetID 注册标识符
	GetID() string

	// GetDefaultTarget 返回该适配器的默认目标适配器ID
	GetDefaultTarget() string

	// Start 启动监听：接收端上事件 -> 封装 Message -> 投递至 inbound
	Start(ctx context.Context, inbound chan<- Message) error

	// ReceiveMessage 接收调度中心转发的消息并执行具体逻辑
	ReceiveMessage(ctx context.Context, msg Message) error

	// Status 返回该适配器的当前状态
	Status() string
}
