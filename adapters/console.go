package adapters

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
		return NewConsoleAdapter(id), nil
	})
}

// ConsoleAdapter 终端适配器
type ConsoleAdapter struct {
	id   string
	tags []string
}

func NewConsoleAdapter(id string) *ConsoleAdapter {
	return &ConsoleAdapter{
		id:   id,
		tags: []string{"platform:console", "type:terminal"},
	}
}

func (c *ConsoleAdapter) GetID() string {
	return c.id
}

func (c *ConsoleAdapter) GetTags() []string {
	return c.tags
}

func (c *ConsoleAdapter) Start(ctx context.Context, inbound chan<- mybot.Message) error {
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

func (c *ConsoleAdapter) ReceiveMessage(ctx context.Context, msg mybot.Message) error {
	fmt.Printf("\n[ConsoleAdapter:%s] Received from %s: %s\n", c.id, msg.SourceAdapter, msg.Content)
	fmt.Print("> ") // 重新打印提示符
	return nil
}

func (c *ConsoleAdapter) Status() string {
	return "running"
}
