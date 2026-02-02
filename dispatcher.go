package mybot

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// Dispatcher 核心路由分发器
type Dispatcher struct {
	adapters map[string]Adapter
	inbound  chan Message

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
}

// NewDispatcher 创建一个新的调度器
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		adapters: make(map[string]Adapter),
		inbound:  make(chan Message, 100),
	}
}

// Register 注册适配器
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

// Unregister 注销适配器
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
				fmt.Printf("adapter %s failed to start: %v\n", a.GetID(), err)
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

	// 1. P2P 路由优先
	if msg.TargetAdapter != "" {
		if adapter, ok := d.adapters[msg.TargetAdapter]; ok {
			_ = adapter.ReceiveMessage(d.ctx, msg)
		}
		return
	}

	// 2. 标签路由 (Tag-based Multicast)
	if len(msg.Tags) > 0 {
		for _, adapter := range d.adapters {
			if d.matchTags(msg.Tags, adapter) {
				_ = adapter.ReceiveMessage(d.ctx, msg)
			}
		}
		return
	}

	// 3. 默认分发 (如果没有 Target 且没有 Tags)
	// 这里可以定义一个默认的 AI 适配器或日志记录器，暂时不实现具体逻辑
}

// matchTags 检查适配器是否匹配消息标签
// 这里的匹配算法可以后续优化，目前简单遍历
func (d *Dispatcher) matchTags(messageTags []string, adapter Adapter) bool {
	adapterTags := adapter.GetTags()
	if len(adapterTags) == 0 {
		return false
	}

	// 检查消息的所有 Tags 是否都在适配器的标签中 (子集匹配)
	for _, mTag := range messageTags {
		found := false
		for _, aTag := range adapterTags {
			if mTag == aTag {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Stop 停止调度器
func (d *Dispatcher) Stop() {
	if d.cancel != nil {
		d.cancel()
	}
}
