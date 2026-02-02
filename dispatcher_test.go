package mybot

import (
	"context"
	"testing"
	"time"
)

type mockAdapter struct {
	id       string
	tags     []string
	received chan Message
}

func (m *mockAdapter) GetID() string                                      { return m.id }
func (m *mockAdapter) GetTags() []string                                  { return m.tags }
func (m *mockAdapter) Status() string                                     { return "ok" }
func (m *mockAdapter) Start(ctx context.Context, in chan<- Message) error { return nil }
func (m *mockAdapter) ReceiveMessage(ctx context.Context, msg Message) error {
	m.received <- msg
	return nil
}

func TestDispatcher_Routing(t *testing.T) {
	d := NewDispatcher()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a1 := &mockAdapter{id: "a1", tags: []string{"type:ai"}, received: make(chan Message, 1)}
	a2 := &mockAdapter{id: "a2", tags: []string{"type:tool"}, received: make(chan Message, 1)}

	_ = d.Register(a1)
	_ = d.Register(a2)
	_ = d.Start(ctx)

	// Test P2P
	msgP2P := Message{Content: "p2p", TargetAdapter: "a1"}
	d.Push(msgP2P)

	select {
	case m := <-a1.received:
		if m.Content != "p2p" {
			t.Errorf("Expected p2p message, got %s", m.Content)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out waiting for p2p message")
	}

	// Test Tag Routing (via enrich)
	msgTag := Message{Content: "/echo test"}
	d.Push(msgTag)

	// Since we don't have an adapter with "service:echo" tag, it won't be delivered to anyone in the current test setup unless we add one.
	// But let's at least consume the msgTag to avoid unused variable error.

	// Let's test the default AI routing
	msgAI := Message{Content: "hello ai"}
	d.Push(msgAI)

	select {
	case m := <-a1.received:
		if m.Content != "hello ai" {
			t.Errorf("Expected ai message, got %s", m.Content)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out waiting for ai message")
	}
}
