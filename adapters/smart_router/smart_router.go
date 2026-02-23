package smart_router

import (
	"context"
	"log/slog"
	"regexp"

	"github.com/lengzhao/mybot"
)

func init() {
	mybot.RegisterAdapterType("smart_router", func(id string, config map[string]interface{}) (mybot.Adapter, error) {
		return NewAdapter(id, config)
	})
}

// RouteRule 单条路由规则
type RouteRule struct {
	// 匹配类型: content_prefix, content_regex, user_id, channel, default
	MatchType  string `json:"match_type"  yaml:"match_type"`
	MatchValue string `json:"match_value" yaml:"match_value"`
	Target     string `json:"target"      yaml:"target"`
}

// Adapter 智能路由适配器：根据规则识别消息并转发到目标 adapter
type Adapter struct {
	id            string
	rules         []RouteRule
	defaultTarget string
	inbound       chan<- mybot.Message
}

// NewAdapter 从 config 创建。配置示例：
//
//	rules:
//	  - match_type: content_prefix
//	    match_value: "/code"
//	    target: opencode_1
//	  - match_type: content_regex
//	    match_value: "(?i)bug|error|fix"
//	    target: opencode_1
//	  - match_type: user_id
//	    match_value: "user_abc"
//	    target: deepseek_chat
//	  - match_type: channel
//	    match_value: "channel_xyz"
//	    target: lark_bot
//	default_target: echo_service
func NewAdapter(id string, config map[string]interface{}) (*Adapter, error) {
	slog.Debug("Creating SmartRouter Adapter", "id", id, "config", config)

	defaultTarget, _ := config["default_target"].(string)

	var rules []RouteRule
	if r, ok := config["rules"]; ok {
		switch rr := r.(type) {
		case []interface{}:
			for _, item := range rr {
				m, _ := item.(map[string]interface{})
				if m == nil {
					continue
				}
				rule := RouteRule{}
				if v, _ := m["match_type"].(string); v != "" {
					rule.MatchType = v
				}
				if v, _ := m["match_value"].(string); v != "" {
					rule.MatchValue = v
				}
				if v, _ := m["target"].(string); v != "" {
					rule.Target = v
				}
				// 禁止自指，防止死循环
				if rule.Target == id {
					slog.Warn("smart_router: skip self-targeting rule", "adapter", id, "target", rule.Target)
					continue
				}
				if rule.MatchType != "" && rule.Target != "" {
					rules = append(rules, rule)
				}
			}
		}
	}

	return &Adapter{
		id:            id,
		rules:         rules,
		defaultTarget: defaultTarget,
	}, nil
}

func (a *Adapter) GetID() string {
	return a.id
}

func (a *Adapter) GetDefaultTarget() string {
	return a.defaultTarget
}

func (a *Adapter) Start(ctx context.Context, inbound chan<- mybot.Message) error {
	a.inbound = inbound
	return nil
}

// resolveTarget 按规则顺序匹配，返回目标 adapter id；无匹配时返回 default_target；禁止自指
func (a *Adapter) resolveTarget(msg mybot.Message) string {
	for _, r := range a.rules {
		if r.Target == a.id {
			continue // 运行时再次跳过自指（兼容历史配置）
		}
		switch r.MatchType {
		case "content_prefix":
			if r.MatchValue != "" && len(msg.Content) >= len(r.MatchValue) && msg.Content[:len(r.MatchValue)] == r.MatchValue {
				return r.Target
			}
		case "content_regex":
			if r.MatchValue != "" {
				if re, err := regexp.Compile(r.MatchValue); err == nil && re.MatchString(msg.Content) {
					return r.Target
				}
			}
		case "user_id":
			if r.MatchValue != "" && msg.UserID == r.MatchValue {
				return r.Target
			}
		case "channel":
			if r.MatchValue != "" && msg.Channel == r.MatchValue {
				return r.Target
			}
		case "default":
			return r.Target
		}
	}
	return a.defaultTarget
}

func (a *Adapter) ReceiveMessage(ctx context.Context, msg mybot.Message) error {
	target := a.resolveTarget(msg)
	if target == "" {
		slog.Warn("smart_router: no target resolved, drop message", "msg_id", msg.ID, "content_preview", preview(msg.Content))
		return nil
	}

	// 仅设置目标，保持 SourceAdapter 等不变，便于下游回复回源
	forward := msg
	forward.TargetAdapter = target

	select {
	case a.inbound <- forward:
		slog.Debug("smart_router: forwarded", "msg_id", msg.ID, "target", target)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (a *Adapter) Status() string {
	return "online"
}

func preview(s string) string {
	const max = 40
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
