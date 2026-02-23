package echo

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/lengzhao/mybot"
)

func init() {
	mybot.RegisterAdapterType("echo", func(id string, config map[string]interface{}) (mybot.Adapter, error) {
		return NewAdapter(id, config), nil
	})
}

// Adapter 回显适配器 (用于测试)
type Adapter struct {
	id            string
	defaultTarget string
	inbound       chan<- mybot.Message
}

func NewAdapter(id string, config map[string]interface{}) *Adapter {
	slog.Debug("Creating Echo Adapter", "id", id, "config", config)

	defaultTarget, _ := config["default_target"].(string)
	return &Adapter{
		id:            id,
		defaultTarget: defaultTarget,
	}
}

func (e *Adapter) GetID() string {
	return e.id
}

func (e *Adapter) GetDefaultTarget() string {
	return e.defaultTarget
}

func (e *Adapter) Start(ctx context.Context, inbound chan<- mybot.Message) error {
	e.inbound = inbound
	return nil
}

func (e *Adapter) ReceiveMessage(ctx context.Context, msg mybot.Message) error {
	// 如果消息没有明确的目标适配器，但当前适配器有默认目标，则使用默认目标
	targetAdapter := msg.SourceAdapter
	defaultTarget := e.GetDefaultTarget()
	if defaultTarget != "" {
		targetAdapter = defaultTarget
	}

	response := mybot.Message{
		ID:            fmt.Sprintf("echo-%d", time.Now().UnixNano()),
		ParentID:      msg.ID,
		SourceAdapter: e.GetID(),
		TargetAdapter: targetAdapter,
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
