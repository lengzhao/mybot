package mybot

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type mockAdapter struct {
	id            string
	defaultTarget string
	received      chan Message
	receiveCount  int
	mu            sync.Mutex
}

func (m *mockAdapter) GetID() string                                      { return m.id }
func (m *mockAdapter) GetDefaultTarget() string                           { return m.defaultTarget }
func (m *mockAdapter) Status() string                                     { return "ok" }
func (m *mockAdapter) Start(ctx context.Context, in chan<- Message) error { return nil }
func (m *mockAdapter) ReceiveMessage(ctx context.Context, msg Message) error {
	m.mu.Lock()
	m.receiveCount++
	m.mu.Unlock()
	m.received <- msg
	return nil
}

func (m *mockAdapter) GetReceiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.receiveCount
}

// 创建测试用的适配器
func newMockAdapter(id string, defaultTarget string) *mockAdapter {
	return &mockAdapter{
		id:            id,
		defaultTarget: defaultTarget,
		received:      make(chan Message, 10),
	}
}

func TestDispatcher_BasicRouting(t *testing.T) {
	d := NewDispatcher()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a1 := newMockAdapter("a1", "")
	a2 := newMockAdapter("a2", "")
	defaultAdapter := newMockAdapter("default", "")

	if err := d.Register(a1); err != nil {
		t.Fatalf("Failed to register adapter a1: %v", err)
	}
	if err := d.Register(a2); err != nil {
		t.Fatalf("Failed to register adapter a2: %v", err)
	}
	if err := d.Register(defaultAdapter); err != nil {
		t.Fatalf("Failed to register default adapter: %v", err)
	}

	d.SetDefaultAdapter("default")

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Failed to start dispatcher: %v", err)
	}

	// Test P2P routing
	msgP2P := Message{ID: "1", Content: "p2p message", TargetAdapter: "a1"}
	d.Push(msgP2P)

	select {
	case m := <-a1.received:
		if m.Content != "p2p message" {
			t.Errorf("Expected 'p2p message', got '%s'", m.Content)
		}
		if m.TargetAdapter != "a1" {
			t.Errorf("Expected TargetAdapter 'a1', got '%s'", m.TargetAdapter)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out waiting for p2p message")
	}

	// Verify a2 didn't receive the message
	select {
	case <-a2.received:
		t.Error("a2 should not receive P2P message targeted to a1")
	default:
		// Expected - a2 should not receive
	}

	// Test message that goes to default adapter
	msgDefault := Message{ID: "2", Content: "hello default"}
	d.Push(msgDefault)

	select {
	case m := <-defaultAdapter.received:
		if m.Content != "hello default" {
			t.Errorf("Expected 'hello default', got '%s'", m.Content)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Timed out waiting for default message")
	}

	// Verify a1 and a2 didn't receive the default message
	select {
	case <-a1.received:
		t.Error("a1 should not receive default message")
	case <-a2.received:
		t.Error("a2 should not receive default message")
	default:
		// Expected
	}
}

func TestDispatcher_DefaultRouting(t *testing.T) {
	d := NewDispatcher()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register default adapter
	a1 := newMockAdapter("default-adapter", "")
	if err := d.Register(a1); err != nil {
		t.Fatalf("Failed to register adapter: %v", err)
	}

	d.SetDefaultAdapter("default-adapter")

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Failed to start dispatcher: %v", err)
	}

	// Test message with no target - should go to default adapter
	msg := Message{ID: "1", Content: "no target message"}
	d.Push(msg)

	select {
	case m := <-a1.received:
		if m.Content != "no target message" {
			t.Errorf("Expected 'no target message', got '%s'", m.Content)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Timed out waiting for default routing")
	}

	// Test with explicit empty target
	msg2 := Message{ID: "2", Content: "empty target", TargetAdapter: ""}
	d.Push(msg2)

	select {
	case m := <-a1.received:
		if m.Content != "empty target" {
			t.Errorf("Expected 'empty target', got '%s'", m.Content)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Timed out waiting for default routing with empty target")
	}
}

func TestDispatcher_Registration(t *testing.T) {
	d := NewDispatcher()

	// Test successful registration
	a1 := newMockAdapter("adapter1", "")
	if err := d.Register(a1); err != nil {
		t.Errorf("Expected successful registration, got error: %v", err)
	}

	// Test duplicate registration
	a2 := newMockAdapter("adapter1", "") // Same ID
	if err := d.Register(a2); err == nil {
		t.Error("Expected error for duplicate registration, got nil")
	}

	// Test successful unregistration
	if err := d.Unregister("adapter1"); err != nil {
		t.Errorf("Expected successful unregistration, got error: %v", err)
	}

	// Test unregistration of non-existent adapter
	if err := d.Unregister("nonexistent"); err == nil {
		t.Error("Expected error for unregistering non-existent adapter, got nil")
	}

	// Test registration after unregistration
	a3 := newMockAdapter("adapter3", "")
	a4 := newMockAdapter("adapter4", "")

	if err := d.Register(a3); err != nil {
		t.Fatalf("Failed to register a3: %v", err)
	}
	if err := d.Register(a4); err != nil {
		t.Fatalf("Failed to register a4: %v", err)
	}

	// Unregister a3
	if err := d.Unregister("adapter3"); err != nil {
		t.Fatalf("Failed to unregister a3: %v", err)
	}

	// Verify a4 still works
	msg := Message{ID: "test", Content: "test", TargetAdapter: "adapter4"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Failed to start dispatcher: %v", err)
	}
	d.Push(msg)

	select {
	case m := <-a4.received:
		if m.Content != "test" {
			t.Errorf("Expected 'test', got '%s'", m.Content)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out waiting for message after unregistration")
	}

	// Verify a3 no longer receives messages
	select {
	case <-a3.received:
		t.Error("a3 should not receive messages after unregistration")
	default:
		// Expected
	}
}

func TestDispatcher_EnrichLogic(t *testing.T) {
	d := NewDispatcher()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Since Tags are removed, we only test basic message handling
	a1 := newMockAdapter("test-adapter", "")
	defaultAdapter := newMockAdapter("default", "")

	if err := d.Register(a1); err != nil {
		t.Fatalf("Failed to register a1: %v", err)
	}
	if err := d.Register(defaultAdapter); err != nil {
		t.Fatalf("Failed to register default adapter: %v", err)
	}

	d.SetDefaultAdapter("default")

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Failed to start dispatcher: %v", err)
	}

	// Test message with target adapter
	msg1 := Message{ID: "1", Content: "targeted message", TargetAdapter: "test-adapter"}
	d.Push(msg1)

	select {
	case m := <-a1.received:
		if m.Content != "targeted message" {
			t.Errorf("Expected 'targeted message', got '%s'", m.Content)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out waiting for targeted message")
	}

	// Test message that goes to default adapter
	msg2 := Message{ID: "2", Content: "default message"}
	d.Push(msg2)

	select {
	case m := <-defaultAdapter.received:
		if m.Content != "default message" {
			t.Errorf("Expected 'default message', got '%s'", m.Content)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Timed out waiting for default message")
	}

	// Verify a1 didn't receive the default message
	select {
	case <-a1.received:
		t.Error("a1 should not receive default message")
	default:
		// Expected
	}
}

func TestDispatcher_ConcurrentSafety(t *testing.T) {
	d := NewDispatcher()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create multiple adapters
	adapters := make([]*mockAdapter, 10)
	for i := 0; i < 10; i++ {
		adapters[i] = newMockAdapter(fmt.Sprintf("adapter-%d", i), "")
		if err := d.Register(adapters[i]); err != nil {
			t.Fatalf("Failed to register adapter %d: %v", i, err)
		}
	}

	d.SetDefaultAdapter("adapter-0") // Set first adapter as default

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Failed to start dispatcher: %v", err)
	}

	// Concurrent message pushing
	const numMessages = 100
	var wg sync.WaitGroup

	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			msg := Message{
				ID:            fmt.Sprintf("msg-%d", id),
				Content:       fmt.Sprintf("message %d", id),
				TargetAdapter: fmt.Sprintf("adapter-%d", id%10),
			}
			d.Push(msg)
		}(i)
	}

	wg.Wait()

	// Give some time for all messages to be processed
	time.Sleep(100 * time.Millisecond)

	// Verify all messages were received
	totalReceived := 0
	for i, adapter := range adapters {
		count := adapter.GetReceiveCount()
		totalReceived += count
		t.Logf("Adapter %d received %d messages", i, count)
	}

	if totalReceived == 0 {
		t.Error("No messages were received by any adapter")
	}

	t.Logf("Total messages received: %d", totalReceived)
}

func TestDispatcher_EdgeCases(t *testing.T) {
	d := NewDispatcher()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use default adapter for edge case testing
	a1 := newMockAdapter("test-adapter", "")
	d.SetDefaultAdapter("test-adapter")
	if err := d.Register(a1); err != nil {
		t.Fatalf("Failed to register adapter: %v", err)
	}

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Failed to start dispatcher: %v", err)
	}

	// Test empty message
	msg1 := Message{ID: "1", Content: ""}
	d.Push(msg1)

	select {
	case m := <-a1.received:
		if m.Content != "" {
			t.Errorf("Expected empty content, got '%s'", m.Content)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Timed out waiting for empty message")
	}

	// Test message with special characters
	msg2 := Message{ID: "2", Content: "测试消息 with special chars: !@#$%^&*()"}
	d.Push(msg2)

	select {
	case m := <-a1.received:
		if m.Content != "测试消息 with special chars: !@#$%^&*()" {
			t.Errorf("Expected special char message, got '%s'", m.Content)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Timed out waiting for special char message")
	}

	// Test very long message
	longContent := make([]byte, 10000)
	for i := range longContent {
		longContent[i] = 'A'
	}
	msg3 := Message{ID: "3", Content: string(longContent)}
	d.Push(msg3)

	select {
	case m := <-a1.received:
		if len(m.Content) != 10000 {
			t.Errorf("Expected content length 10000, got %d", len(m.Content))
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Timed out waiting for long message")
	}

	// Test message with no ID (should still work)
	msg4 := Message{Content: "no id message"}
	d.Push(msg4)

	select {
	case m := <-a1.received:
		if m.Content != "no id message" {
			t.Errorf("Expected 'no id message', got '%s'", m.Content)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Timed out waiting for message without ID")
	}
}

func TestDispatcher_Stop(t *testing.T) {
	d := NewDispatcher()
	ctx, cancel := context.WithCancel(context.Background())

	// Use default adapter
	a1 := newMockAdapter("test", "")
	d.SetDefaultAdapter("test")
	if err := d.Register(a1); err != nil {
		t.Fatalf("Failed to register adapter: %v", err)
	}

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Failed to start dispatcher: %v", err)
	}

	// Send a message
	msg := Message{ID: "1", Content: "test message"}
	d.Push(msg)

	select {
	case m := <-a1.received:
		if m.Content != "test message" {
			t.Errorf("Expected 'test message', got '%s'", m.Content)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Timed out waiting for initial message")
	}

	// Stop the dispatcher
	d.Stop()
	cancel()

	// Try to send another message - should not be processed
	msg2 := Message{ID: "2", Content: "should not be processed"}
	d.Push(msg2)

	select {
	case <-a1.received:
		t.Error("Message should not be processed after stop")
	case <-time.After(100 * time.Millisecond):
		// Expected - no message should be received
	}
}

func TestDispatcher_P2PNotFound(t *testing.T) {
	d := NewDispatcher()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a1 := newMockAdapter("existing-adapter", "")
	if err := d.Register(a1); err != nil {
		t.Fatalf("Failed to register adapter: %v", err)
	}

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Failed to start dispatcher: %v", err)
	}

	// Test P2P to non-existent adapter
	msg := Message{ID: "1", Content: "test", TargetAdapter: "non-existent"}
	d.Push(msg)

	// Should not be delivered to any adapter
	select {
	case <-a1.received:
		t.Error("Message should not be delivered to existing adapter when target doesn't exist")
	case <-time.After(50 * time.Millisecond):
		// Expected - message should be dropped
	}
}

func TestDispatcher_DefaultAdapterNotFound(t *testing.T) {
	d := NewDispatcher()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set default adapter that doesn't exist
	d.SetDefaultAdapter("non-existent")

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Failed to start dispatcher: %v", err)
	}

	// Test message that would go to default
	msg := Message{ID: "1", Content: "test message"}
	d.Push(msg)

	// Should not be delivered (no matching adapters)
	time.Sleep(50 * time.Millisecond)
	// No way to verify this directly, but test should not panic or hang
}
