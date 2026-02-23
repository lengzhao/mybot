package mybot

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// Dispatcher 核心路由分发器：管理适配器生命周期，按 TargetAdapter / 默认 优先级路由消息
type Dispatcher struct {
	adapters       map[string]Adapter
	defaultAdapter string // 无 Target 时的兜底适配器 ID
	maxHops        int    // 消息最大跳数，0 表示不限制，用于防循环
	inbound        chan Message
	stateStore     StateStore // 状态存储

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
}

// NewDispatcher 创建调度器
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		adapters:       make(map[string]Adapter),
		maxHops:        20, // 默认防循环跳数，0 表示不限制
		inbound:        make(chan Message, 100),
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

	// 记录消息到状态存储（使用原始 msg，不记录 _hop）
	if d.stateStore != nil {
		if err := d.stateStore.RecordMessage(msg); err != nil {
			slog.Error("failed to record message to state store", "err", err, "msg_id", msg.ID)
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

// GetStateStore 获取状态存储
func (d *Dispatcher) GetStateStore() StateStore {
	return d.stateStore
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
