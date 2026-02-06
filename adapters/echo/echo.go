package echo

import (
	"context"
	"fmt"
	"time"

	"github.com/lengzhao/mybot"
)

func init() {
	mybot.RegisterAdapterType("echo", func(id string, config map[string]interface{}) (mybot.Adapter, error) {
		return NewAdapter(id), nil
	})
}

// Adapter 回显适配器 (用于测试)
type Adapter struct {
	id      string
	tags    []string
	inbound chan<- mybot.Message
}

func NewAdapter(id string) *Adapter {
	return &Adapter{
		id:   id,
		tags: []string{"type:ai", "service:echo"},
	}
}

func (e *Adapter) GetID() string {
	return e.id
}

func (e *Adapter) GetTags() []string {
	return e.tags
}

func (e *Adapter) Start(ctx context.Context, inbound chan<- mybot.Message) error {
	e.inbound = inbound
	return nil
}

func (e *Adapter) ReceiveMessage(ctx context.Context, msg mybot.Message) error {
	response := mybot.Message{
		ID:            fmt.Sprintf("echo-%d", time.Now().UnixNano()),
		SourceAdapter: e.id,
		TargetAdapter: msg.SourceAdapter,
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

func (e *Adapter) Status() string {
	return "online"
}
