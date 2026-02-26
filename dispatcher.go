package mybot

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// HeartbeatUserID 心跳消息的固定 user_id，便于 adapter 区分系统触发与真实用户消息
const HeartbeatUserID = "heartbeat"

// defaultHeartbeatContent 未配置 heartbeat.content 时使用的心跳指令文案
const defaultHeartbeatContent = "请进行一次自主轮次（Think-Act-Observe）：根据当前 SOUL/状态与 Cron 计划，决定是否生成日报、发送提醒、或创建/修改 Cron 任务；可立刻执行或留待 Cron 到点再发。历史对话见 **.mybot/conversation.md**，请先阅读再执行自主轮次。"

// Dispatcher 核心路由分发器：管理适配器生命周期，按 TargetAdapter / 默认 优先级路由消息
type Dispatcher struct {
	adapters       map[string]Adapter
	defaultAdapter string // 无 Target 时的兜底适配器 ID
	maxHops        int    // 消息最大跳数，0 表示不限制，用于防循环
	inbound        chan Message
	stateStore     StateStore      // 状态存储
	heartbeatCfg   HeartbeatConfig // 心跳配置

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
}

// NewDispatcher 创建调度器
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		adapters: make(map[string]Adapter),
		maxHops:  20, // 默认防循环跳数，0 表示不限制
		inbound:  make(chan Message, 100),
	}
}

// SetDefaultAdapter 设置默认兜底适配器 ID（无 P2P 且标签无匹配时投递）
func (d *Dispatcher) SetDefaultAdapter(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.defaultAdapter = id
}

// SetMaxHops 设置消息最大转发跳数，防循环；0 表示不限制。未设置时默认 20。
func (d *Dispatcher) SetMaxHops(n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.maxHops = n
}

// Register 注册适配器并更新 tagIndex
func (d *Dispatcher) Register(adapter Adapter) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	id := adapter.GetID()
	if _, exists := d.adapters[id]; exists {
		return fmt.Errorf("adapter with id %s already exists", id)
	}

	d.adapters[id] = adapter
	return nil
}

// Unregister 注销适配器并重建 tagIndex
func (d *Dispatcher) Unregister(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.adapters[id]; !exists {
		return fmt.Errorf("adapter with id %s not found", id)
	}

	delete(d.adapters, id)
	return nil
}

// Start 启动调度器主循环
func (d *Dispatcher) Start(ctx context.Context) error {
	d.ctx, d.cancel = context.WithCancel(ctx)

	// 启动所有已注册适配器的监听
	d.mu.RLock()
	for _, adapter := range d.adapters {
		go func(a Adapter) {
			if err := a.Start(d.ctx, d.inbound); err != nil {
				slog.Error("adapter start failed", "adapter", a.GetID(), "err", err)
			}
		}(adapter)
	}
	d.mu.RUnlock()

	// 主路由循环
	go d.routeLoop()

	// 心跳：若启用且存储支持心跳状态，则启动心跳 goroutine
	if d.heartbeatCfg.Enabled && d.heartbeatCfg.IntervalSec > 0 {
		if hs, ok := d.stateStore.(HeartbeatStateStore); ok {
			go d.heartbeatLoop(hs)
		} else {
			slog.Warn("heartbeat enabled but StateStore does not implement HeartbeatStateStore, heartbeat disabled")
		}
	}

	return nil
}

// Push 外部手动推送消息到调度中心
func (d *Dispatcher) Push(msg Message) {
	d.inbound <- msg
}

func (d *Dispatcher) routeLoop() {
	for {
		select {
		case <-d.ctx.Done():
			return
		case msg := <-d.inbound:
			go d.dispatch(msg)
		}
	}
}

func (d *Dispatcher) dispatch(msg Message) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// 防循环：跳数检查
	hop := 0
	if msg.Extra != nil {
		if v, ok := msg.Extra["_hop"].(int); ok {
			hop = v
		}
	}
	hop++
	if d.maxHops > 0 && hop > d.maxHops {
		slog.Warn("dispatch dropped: max hops exceeded", "msg_id", msg.ID, "hop", hop, "max_hops", d.maxHops)
		return
	}
	// 投递时使用带 _hop 的副本，避免篡改原始消息
	deliverMsg := msg
	if deliverMsg.Extra == nil {
		deliverMsg.Extra = make(map[string]interface{})
	} else {
		extraCopy := make(map[string]interface{}, len(deliverMsg.Extra)+1)
		for k, v := range deliverMsg.Extra {
			extraCopy[k] = v
		}
		deliverMsg.Extra = extraCopy
	}
	deliverMsg.Extra["_hop"] = hop

	// 解析实际投递目标（P2P 或默认），用于落库与投递；落库时使用该目标便于 GetActiveChannelsForTarget 正确统计各 adapter 下的 channel
	resolvedTarget := ""
	if deliverMsg.TargetAdapter != "" {
		if _, ok := d.adapters[deliverMsg.TargetAdapter]; ok {
			resolvedTarget = deliverMsg.TargetAdapter
		}
	}
	if resolvedTarget == "" && d.defaultAdapter != "" {
		if _, ok := d.adapters[d.defaultAdapter]; ok {
			resolvedTarget = d.defaultAdapter
		}
	}

	// 记录消息到状态存储：每条经 dispatch 的消息（各 adapter 收到的与回复的）均落库，供管理查询与心跳触发后 adapter 分析处理；使用 resolvedTarget 以便心跳能按 target 找到 channel
	if d.stateStore != nil {
		recordMsg := msg
		if resolvedTarget != "" {
			recordMsg.TargetAdapter = resolvedTarget
		}
		if err := d.stateStore.RecordMessage(recordMsg); err != nil {
			slog.Error("failed to record message to state store", "err", err, "msg_id", msg.ID)
		}
		// 非心跳来源且带 channel 时，恢复该 channel 的已取消心跳（有新对话则重新纳入心跳）
		if msg.SourceAdapter != "heartbeat" && msg.Channel != "" {
			if hs, ok := d.stateStore.(HeartbeatStateStore); ok {
				_ = hs.UncancelChannelHeartbeat(msg.Channel, msg.UserID)
			}
		}
	}
	slog.Debug("dispatch message", "msg", msg)

	// 1. P2P 投递优先
	if deliverMsg.TargetAdapter != "" {
		if adapter, ok := d.adapters[deliverMsg.TargetAdapter]; ok {
			d.deliver(deliverMsg, deliverMsg.TargetAdapter, adapter)
		} else {
			slog.Warn("dispatch P2P target not found", "target", deliverMsg.TargetAdapter, "msg_id", deliverMsg.ID)
		}
		return
	}

	// 2. 默认兜底
	if d.defaultAdapter != "" {
		if adapter, ok := d.adapters[d.defaultAdapter]; ok {
			d.deliver(deliverMsg, d.defaultAdapter, adapter)
		} else {
			slog.Warn("dispatch default adapter not found", "default", d.defaultAdapter, "msg_id", deliverMsg.ID)
		}
	}
}

func (d *Dispatcher) deliver(msg Message, targetID string, adapter Adapter) {
	// 若源 adapter 实现了 SourceAckAdapter，先通知“消息已被目标接收”，便于在源端做“处理中”反馈（如 Lark 加表情）
	if msg.SourceAdapter != "" {
		if source, ok := d.adapters[msg.SourceAdapter]; ok {
			if ack, ok := source.(SourceAckAdapter); ok {
				ack.OnMessageDispatchedToTarget(d.ctx, msg, targetID)
			}
		}
	}
	if err := adapter.ReceiveMessage(d.ctx, msg); err != nil {
		slog.Error("dispatch deliver failed", "target", targetID, "msg_id", msg.ID, "err", err)
	}
}

// SetStateStore 设置状态存储
func (d *Dispatcher) SetStateStore(store StateStore) {
	d.stateStore = store
}

// SetHeartbeatConfig 设置心跳配置
func (d *Dispatcher) SetHeartbeatConfig(cfg HeartbeatConfig) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.heartbeatCfg = cfg
}

// GetStateStore 获取状态存储
func (d *Dispatcher) GetStateStore() StateStore {
	return d.stateStore
}

// heartbeatLoop 心跳循环：按间隔判断是否触发自主轮次，触发时向 Inbound 投递 Message
func (d *Dispatcher) heartbeatLoop(store HeartbeatStateStore) {
	d.mu.RLock()
	interval := time.Duration(d.heartbeatCfg.IntervalSec) * time.Second
	minInterval := time.Duration(d.heartbeatCfg.MinIntervalSec) * time.Second
	d.mu.RUnlock()

	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			d.tryTriggerHeartbeat(store, minInterval)
		}
	}
}

// tryTriggerHeartbeat 全局心跳：1）全局状态与间隔校验 2）遍历每个 adapter，未启用跳过 3）查询当前 adapter 的本轮 channel 列表 4）调用 adapter 心跳接口；channel 列表为默认 + 到期的历史 channel（全局一份，各 adapter 共用）
func (d *Dispatcher) tryTriggerHeartbeat(store HeartbeatStateStore, minInterval time.Duration) {
	d.mu.RLock()
	intervalSec := d.heartbeatCfg.IntervalSec
	contentCfg := d.heartbeatCfg.Content
	d.mu.RUnlock()

	content := contentCfg
	if strings.TrimSpace(content) == "" {
		content = defaultHeartbeatContent
	}

	state, err := store.GetHeartbeatState()
	if err != nil {
		slog.Error("heartbeat: get state failed", "err", err)
		return
	}
	if state == nil || !state.Enabled {
		return
	}
	now := time.Now()
	nowMs := now.UnixMilli()
	if minInterval > 0 && state.LastTriggerAt > 0 {
		if time.Duration(nowMs-state.LastTriggerAt)*time.Millisecond < minInterval {
			return
		}
	}

	// 更新全局心跳状态
	state.LastTriggerAt = nowMs
	state.TriggerCount++
	state.NextDueAt = nowMs + int64(intervalSec)*1000
	if err := store.UpdateHeartbeatState(*state); err != nil {
		slog.Error("heartbeat: update state failed", "err", err)
	}

	// 1. 遍历每个 adapter
	d.mu.RLock()
	adapters := make([]Adapter, 0, len(d.adapters))
	for _, a := range d.adapters {
		adapters = append(adapters, a)
	}
	d.mu.RUnlock()

	for _, a := range adapters {
		// 2. 未实现 HeartbeatHandler 或未启用心跳则跳过
		h, ok := a.(HeartbeatHandler)
		if !ok {
			continue
		}
		if !h.HeartbeatEnabled() {
			continue
		}
		// 3. 查询当前 adapter 下的各 channel（仅统计投递到该 adapter 的消息），得到本轮要触发的 channel
		triggerChannels := d.collectTriggerChannelsForAdapter(store, a.GetID(), intervalSec, nowMs)
		opts := HeartbeatOptions{Content: content, TriggerChannels: triggerChannels}
		// 4. 调用当前 adapter 的心跳接口
		if err := h.OnHeartbeat(d.ctx, opts); err != nil {
			slog.Error("heartbeat: adapter OnHeartbeat failed", "adapter", a.GetID(), "err", err)
		} else {
			slog.Debug("heartbeat: adapter OnHeartbeat ok", "adapter", a.GetID(), "channels", len(triggerChannels))
		}
		// 更新本轮触发的 channel 的 next_due_at（仅限本 adapter 的 channel）
		for _, c := range triggerChannels {
			if c.Channel == "" {
				continue
			}
			chState, _ := store.GetChannelHeartbeatState(c.Channel, c.UserID)
			if chState != nil {
				chState.LastTriggerAt = nowMs
				chState.NextDueAt = nowMs + int64(chState.IntervalSec)*1000
				_ = store.UpsertChannelHeartbeatState(*chState)
			}
		}
	}
	slog.Info("heartbeat triggered", "trigger_count", state.TriggerCount)
}

// collectTriggerChannelsForAdapter 收集指定 adapter 本轮要触发的 channel：该 adapter 下到期的历史 channel（仅统计 target_adapter=adapterID 的消息）
func (d *Dispatcher) collectTriggerChannelsForAdapter(store HeartbeatStateStore, adapterID string, intervalSec int, nowMs int64) []ChannelInfo {
	var out []ChannelInfo
	active, err := store.GetActiveChannelsForTarget(adapterID, 0)
	if err != nil {
		slog.Error("heartbeat: get active channels for target failed", "adapter", adapterID, "err", err)
		return out
	}
	for _, c := range active {
		if c.Channel == "" {
			continue
		}
		chState, err := store.GetChannelHeartbeatState(c.Channel, c.UserID)
		if err != nil {
			continue
		}
		if chState == nil {
			// 新 channel 首次视为到期，加入本轮触发；落库后下次按间隔判断
			chState = &ChannelHeartbeatState{
				Channel:     c.Channel,
				UserID:      c.UserID,
				NextDueAt:   nowMs + int64(intervalSec)*1000,
				IntervalSec: intervalSec,
			}
			_ = store.UpsertChannelHeartbeatState(*chState)
			out = append(out, c)
			continue
		}
		if chState.CancelledAt > 0 || nowMs < chState.NextDueAt {
			continue
		}
		out = append(out, c)
	}
	return out
}

// Stop 停止调度器
func (d *Dispatcher) Stop() {
	if d.cancel != nil {
		d.cancel()
	}

	// 停止状态存储
	if d.stateStore != nil {
		if err := d.stateStore.Stop(); err != nil {
			slog.Error("failed to stop state store", "err", err)
		}
	}
}
