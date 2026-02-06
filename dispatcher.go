package mybot

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
)

// Dispatcher 核心路由分发器：管理适配器生命周期，按 TargetAdapter / Tags / 默认 优先级路由消息
type Dispatcher struct {
	adapters        map[string]Adapter
	tagIndex        map[string][]string   // tag -> adapter IDs，用于标签快速匹配
	defaultAdapter  string                // 无 Target 且无 Tags 或标签无匹配时的兜底适配器 ID
	inbound         chan Message

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
}

// NewDispatcher 创建调度器
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		adapters: make(map[string]Adapter),
		tagIndex: make(map[string][]string),
		inbound:  make(chan Message, 100),
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
	for _, tag := range adapter.GetTags() {
		d.tagIndex[tag] = append(d.tagIndex[tag], id)
	}
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
	d.rebuildTagIndex()
	return nil
}

func (d *Dispatcher) rebuildTagIndex() {
	d.tagIndex = make(map[string][]string)
	for id, adapter := range d.adapters {
		for _, tag := range adapter.GetTags() {
			d.tagIndex[tag] = append(d.tagIndex[tag], id)
		}
	}
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
			// 2. 富化 (Enrich)
			msg = d.enrich(msg)
			go d.dispatch(msg)
		}
	}
}

func (d *Dispatcher) enrich(msg Message) Message {
	// 简单的指令识别逻辑
	if msg.TargetAdapter == "" && len(msg.Tags) == 0 {
		if strings.HasPrefix(msg.Content, "/echo") {
			msg.Tags = append(msg.Tags, "service:echo")
		} else {
			// 默认交给 AI (这里暂时用 echo 代替)
			msg.Tags = append(msg.Tags, "type:ai")
		}
	}
	return msg
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

	// 2. 标签路由：匹配「拥有消息全部 Tags」的适配器（tagIndex 交集）
	if len(msg.Tags) > 0 {
		ids := d.matchTagIds(msg.Tags)
		for _, id := range ids {
			adapter := d.adapters[id]
			d.deliver(msg, id, adapter)
		}
		return
	}

	// 3. 默认兜底
	if d.defaultAdapter != "" {
		if adapter, ok := d.adapters[d.defaultAdapter]; ok {
			d.deliver(msg, d.defaultAdapter, adapter)
		} else {
			slog.Warn("dispatch default adapter not found", "default", d.defaultAdapter, "msg_id", msg.ID)
		}
	}
}

// matchTagIds 返回同时拥有 msg.Tags 中所有 tag 的适配器 ID 列表（去重）
func (d *Dispatcher) matchTagIds(messageTags []string) []string {
	if len(messageTags) == 0 {
		return nil
	}
	var set map[string]int
	for i, tag := range messageTags {
		ids := d.tagIndex[tag]
		if i == 0 {
			set = make(map[string]int)
			for _, id := range ids {
				set[id] = 1
			}
			continue
		}
		for id := range set {
			found := false
			for _, x := range ids {
				if x == id {
					found = true
					break
				}
			}
			if !found {
				delete(set, id)
			}
		}
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out
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
