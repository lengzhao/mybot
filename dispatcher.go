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
	inbound        chan Message

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
}

// NewDispatcher 创建调度器
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		adapters: make(map[string]Adapter),

		inbound: make(chan Message, 100),
	}
}

// SetDefaultAdapter 设置默认兜底适配器 ID（无 P2P 且标签无匹配时投递）
func (d *Dispatcher) SetDefaultAdapter(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.defaultAdapter = id
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

	// 1. P2P 投递优先
	if msg.TargetAdapter != "" {
		if adapter, ok := d.adapters[msg.TargetAdapter]; ok {
			d.deliver(msg, msg.TargetAdapter, adapter)
		} else {
			slog.Warn("dispatch P2P target not found", "target", msg.TargetAdapter, "msg_id", msg.ID)
		}
		return
	}

	// 2. 默认兜底
	if d.defaultAdapter != "" {
		if adapter, ok := d.adapters[d.defaultAdapter]; ok {
			d.deliver(msg, d.defaultAdapter, adapter)
		} else {
			slog.Warn("dispatch default adapter not found", "default", d.defaultAdapter, "msg_id", msg.ID)
		}
	}
}

func (d *Dispatcher) deliver(msg Message, targetID string, adapter Adapter) {
	if err := adapter.ReceiveMessage(d.ctx, msg); err != nil {
		slog.Error("dispatch deliver failed", "target", targetID, "msg_id", msg.ID, "err", err)
	}
}

// Stop 停止调度器
func (d *Dispatcher) Stop() {
	if d.cancel != nil {
		d.cancel()
	}
}
