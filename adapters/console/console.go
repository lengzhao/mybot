package console

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/lengzhao/mybot"
)

func init() {
	mybot.RegisterAdapterType("console", func(id string, config map[string]interface{}) (mybot.Adapter, error) {
		return NewAdapter(id, config), nil
	})
}

// Adapter 终端适配器
type Adapter struct {
	id            string
	tags          []string
	defaultTarget string
}

func NewAdapter(id string, config map[string]interface{}) *Adapter {
	defaultTarget, _ := config["default_target"].(string)
	return &Adapter{
		id:            id,
		tags:          []string{"platform:console", "type:terminal"},
		defaultTarget: defaultTarget,
	}
}

func (c *Adapter) GetID() string {
	return c.id
}

func (c *Adapter) GetTags() []string {
	return c.tags
}

func (c *Adapter) GetDefaultTarget() string {
	return c.defaultTarget
}

func (c *Adapter) Start(ctx context.Context, inbound chan<- mybot.Message) error {
	fmt.Printf("[ConsoleAdapter:%s] Started. Type message and press Enter.\n", c.id)

	scanner := bufio.NewScanner(os.Stdin)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if scanner.Scan() {
					text := scanner.Text()
					if text == "" {
						continue
					}

					msg := mybot.Message{
						ID:            fmt.Sprintf("console-%d", time.Now().UnixNano()),
						SourceAdapter: c.id,
						Content:       text,
						Type:          mybot.TypeText,
						Timestamp:     time.Now().UnixMilli(),
						UserID:        "local-user",
						Channel:       "terminal",
					}
					inbound <- msg
				}
			}
		}
	}()

	return nil
}

func (c *Adapter) ReceiveMessage(ctx context.Context, msg mybot.Message) error {
	fmt.Printf("\n[ConsoleAdapter:%s] Received from %s: %s\n", c.id, msg.SourceAdapter, msg.Content)
	fmt.Print("> ")
	return nil
}

func (c *Adapter) Status() string {
	return "running"
}
