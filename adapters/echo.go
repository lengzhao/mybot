package adapters

import (
	"context"
	"fmt"
	"time"

	"github.com/lengzhao/mybot"
)

func init() {
	mybot.RegisterAdapterType("echo", func(id string, config map[string]interface{}) (mybot.Adapter, error) {
		return NewEchoAdapter(id), nil
	})
}

// EchoAdapter 回显适配器 (用于测试)
type EchoAdapter struct {
	id      string
	tags    []string
	inbound chan<- mybot.Message
}

func NewEchoAdapter(id string) *EchoAdapter {
	return &EchoAdapter{
		id:   id,
		tags: []string{"type:ai", "service:echo"},
	}
}

func (e *EchoAdapter) GetID() string {
	return e.id
}

func (e *EchoAdapter) GetTags() []string {
	return e.tags
}

func (e *EchoAdapter) Start(ctx context.Context, inbound chan<- mybot.Message) error {
	e.inbound = inbound
	return nil
}

func (e *EchoAdapter) ReceiveMessage(ctx context.Context, msg mybot.Message) error {
	// 简单的回显逻辑
	response := mybot.Message{
		ID:            fmt.Sprintf("echo-%d", time.Now().UnixNano()),
		SourceAdapter: e.id,
		TargetAdapter: msg.SourceAdapter, // 回复给发送者
		Content:       "[Echo] " + msg.Content,
		Type:          mybot.TypeText,
		Timestamp:     time.Now().UnixMilli(),
		UserID:        "echo-bot",
		Channel:       msg.Channel,
	}

	select {
	case e.inbound <- response:
	case <-ctx.Done():
	}

	return nil
}

func (e *EchoAdapter) Status() string {
	return "online"
}
