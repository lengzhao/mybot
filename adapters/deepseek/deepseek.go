package deepseek

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/lengzhao/mybot"
)

func init() {
	mybot.RegisterAdapterType("deepseek", func(id string, config map[string]interface{}) (mybot.Adapter, error) {
		slog.Debug("Creating DeepSeek Adapter", "id", id, "config", config)
		apiKey, _ := config["api_key"].(string)
		baseURL, _ := config["base_url"].(string)
		model, _ := config["model"].(string)

		if apiKey == "" {
			return nil, fmt.Errorf("api_key is required for deepseek adapter")
		}
		if baseURL == "" {
			baseURL = "https://api.deepseek.com/v1"
		}
		if model == "" {
			model = "deepseek-chat"
		}

		defaultTarget, _ := config["default_target"].(string)
		return &Adapter{
			id:            id,
			apiKey:        apiKey,
			baseURL:       baseURL,
			model:         model,
			defaultTarget: defaultTarget,
		}, nil
	})
}

// Adapter DeepSeek 对话适配器
type Adapter struct {
	id            string
	apiKey        string
	baseURL       string
	model         string
	defaultTarget string
	inbound       chan<- mybot.Message
}

type deepSeekRequest struct {
	Model    string            `json:"model"`
	Messages []deepSeekMessage `json:"messages"`
}

type deepSeekMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type deepSeekResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (a *Adapter) GetID() string {
	return "deepseek." + a.id
}

func (a *Adapter) GetDefaultTarget() string {
	return a.defaultTarget
}

func (a *Adapter) Start(ctx context.Context, inbound chan<- mybot.Message) error {
	a.inbound = inbound
	return nil
}

func (a *Adapter) ReceiveMessage(ctx context.Context, msg mybot.Message) error {
	reqBody := deepSeekRequest{
		Model: a.model,
		Messages: []deepSeekMessage{
			{Role: "user", Content: msg.Content},
		},
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/chat/completions", a.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.apiKey))

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("deepseek api error (status %d): %s", resp.StatusCode, string(body))
	}

	var dsResp deepSeekResponse
	if err := json.Unmarshal(body, &dsResp); err != nil {
		return err
	}

	if dsResp.Error.Message != "" {
		return fmt.Errorf("deepseek api business error: %s", dsResp.Error.Message)
	}

	if len(dsResp.Choices) == 0 {
		return fmt.Errorf("deepseek api returned no choices")
	}

	// 如果消息没有明确的目标适配器，但当前适配器有默认目标，则使用默认目标
	targetAdapter := msg.SourceAdapter
	defaultTarget := a.GetDefaultTarget()
	if defaultTarget != "" {
		targetAdapter = defaultTarget
	}

	response := mybot.Message{
		ID:            fmt.Sprintf("ds-%d", time.Now().UnixNano()),
		ParentID:      msg.ID,
		SourceAdapter: a.GetID(),
		TargetAdapter: targetAdapter,
		Content:       dsResp.Choices[0].Message.Content,
		Type:          mybot.TypeText,
		Timestamp:     time.Now().UnixMilli(),
		UserID:        "deepseek-bot",
		Channel:       msg.Channel,
	}

	select {
	case a.inbound <- response:
	case <-ctx.Done():
	}

	return nil
}

func (a *Adapter) Status() string {
	return "online"
}
