// Package event provides tests for the event bus implementation.
package event

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// Test Helpers
// ============================================================================

// mockLogger captures log messages for testing
type mockLogger struct {
	mu       sync.Mutex
	messages []string
}

func (l *mockLogger) Printf(format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, format)
}

func (l *mockLogger) MessageCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.messages)
}

// ============================================================================
// Unit Tests - Publish/Subscribe
// ============================================================================

func TestMemoryEventBus_Subscribe(t *testing.T) {
	bus := NewMemoryEventBus()

	handler := func(ctx context.Context, event Event) error {
		return nil
	}

	err := bus.Subscribe(EventTxCreated, handler)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if bus.HandlerCount(EventTxCreated) != 1 {
		t.Errorf("expected 1 handler, got %d", bus.HandlerCount(EventTxCreated))
	}
}

func TestMemoryEventBus_PublishToSubscriber(t *testing.T) {
	bus := NewMemoryEventBus()

	var received Event
	var called bool

	handler := func(ctx context.Context, event Event) error {
		received = event
		called = true
		return nil
	}

	bus.Subscribe(EventTxCreated, handler)

	event := NewEvent(EventTxCreated).WithTxID("tx-123")
	err := bus.Publish(context.Background(), event)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if !called {
		t.Error("expected handler to be called")
	}

	if received.TxID != "tx-123" {
		t.Errorf("expected TxID 'tx-123', got '%s'", received.TxID)
	}
}

func TestMemoryEventBus_NoSubscribers(t *testing.T) {
	bus := NewMemoryEventBus()

	event := NewEvent(EventTxCreated).WithTxID("tx-123")
	err := bus.Publish(context.Background(), event)

	if err != nil {
		t.Errorf("expected no error when no subscribers, got %v", err)
	}
}

func TestMemoryEventBus_SubscribeAll(t *testing.T) {
	bus := NewMemoryEventBus()

	var callCount int32

	handler := func(ctx context.Context, event Event) error {
		atomic.AddInt32(&callCount, 1)
		return nil
	}

	bus.SubscribeAll(handler)

	// Publish different event types
	bus.Publish(context.Background(), NewEvent(EventTxCreated))
	bus.Publish(context.Background(), NewEvent(EventTxCompleted))
	bus.Publish(context.Background(), NewEvent(EventStepStarted))

	if atomic.LoadInt32(&callCount) != 3 {
		t.Errorf("expected handler to be called 3 times, got %d", callCount)
	}
}

// ============================================================================
// Unit Tests - Multiple Handlers
// ============================================================================

func TestMemoryEventBus_MultipleHandlers(t *testing.T) {
	bus := NewMemoryEventBus()

	var callCount int32

	handler1 := func(ctx context.Context, event Event) error {
		atomic.AddInt32(&callCount, 1)
		return nil
	}

	handler2 := func(ctx context.Context, event Event) error {
		atomic.AddInt32(&callCount, 1)
		return nil
	}

	handler3 := func(ctx context.Context, event Event) error {
		atomic.AddInt32(&callCount, 1)
		return nil
	}

	// Subscribe multiple handlers to the same event type
	bus.Subscribe(EventTxCreated, handler1)
	bus.Subscribe(EventTxCreated, handler2)
	bus.Subscribe(EventTxCreated, handler3)

	if bus.HandlerCount(EventTxCreated) != 3 {
		t.Errorf("expected 3 handlers, got %d", bus.HandlerCount(EventTxCreated))
	}

	// Publish event
	bus.Publish(context.Background(), NewEvent(EventTxCreated))

	if atomic.LoadInt32(&callCount) != 3 {
		t.Errorf("expected all 3 handlers to be called, got %d", callCount)
	}
}

func TestMemoryEventBus_MultipleHandlersWithAllHandler(t *testing.T) {
	bus := NewMemoryEventBus()

	var typeHandlerCalls int32
	var allHandlerCalls int32

	typeHandler := func(ctx context.Context, event Event) error {
		atomic.AddInt32(&typeHandlerCalls, 1)
		return nil
	}

	allHandler := func(ctx context.Context, event Event) error {
		atomic.AddInt32(&allHandlerCalls, 1)
		return nil
	}

	bus.Subscribe(EventTxCreated, typeHandler)
	bus.SubscribeAll(allHandler)

	bus.Publish(context.Background(), NewEvent(EventTxCreated))

	if atomic.LoadInt32(&typeHandlerCalls) != 1 {
		t.Errorf("expected type handler to be called once, got %d", typeHandlerCalls)
	}

	if atomic.LoadInt32(&allHandlerCalls) != 1 {
		t.Errorf("expected all handler to be called once, got %d", allHandlerCalls)
	}
}

// ============================================================================
// Unit Tests - Error Handling
// ============================================================================

func TestMemoryEventBus_HandlerErrorDoesNotBlock(t *testing.T) {
	logger := &mockLogger{}
	bus := NewMemoryEventBus(WithLogger(logger))

	var handler2Called bool

	handler1 := func(ctx context.Context, event Event) error {
		return errors.New("handler error")
	}

	handler2 := func(ctx context.Context, event Event) error {
		handler2Called = true
		return nil
	}

	bus.Subscribe(EventTxCreated, handler1)
	bus.Subscribe(EventTxCreated, handler2)

	err := bus.Publish(context.Background(), NewEvent(EventTxCreated).WithTxID("tx-123"))

	// Publish should not return error even if handler fails
	if err != nil {
		t.Errorf("expected no error from Publish, got %v", err)
	}

	// Second handler should still be called
	if !handler2Called {
		t.Error("expected handler2 to be called despite handler1 error")
	}

	// Error should be logged
	if logger.MessageCount() == 0 {
		t.Error("expected error to be logged")
	}
}

func TestMemoryEventBus_HandlerPanicDoesNotBlock(t *testing.T) {
	logger := &mockLogger{}
	bus := NewMemoryEventBus(WithLogger(logger))

	var handler2Called bool

	handler1 := func(ctx context.Context, event Event) error {
		panic("handler panic")
	}

	handler2 := func(ctx context.Context, event Event) error {
		handler2Called = true
		return nil
	}

	bus.Subscribe(EventTxCreated, handler1)
	bus.Subscribe(EventTxCreated, handler2)

	// Should not panic
	err := bus.Publish(context.Background(), NewEvent(EventTxCreated))

	if err != nil {
		t.Errorf("expected no error from Publish, got %v", err)
	}

	// Second handler should still be called
	if !handler2Called {
		t.Error("expected handler2 to be called despite handler1 panic")
	}

	// Panic should be logged
	if logger.MessageCount() == 0 {
		t.Error("expected panic to be logged")
	}
}

// ============================================================================
// Unit Tests - Unsubscribe
// ============================================================================

func TestMemoryEventBus_Unsubscribe(t *testing.T) {
	bus := NewMemoryEventBus()

	var called bool
	handler := func(ctx context.Context, event Event) error {
		called = true
		return nil
	}

	bus.Subscribe(EventTxCreated, handler)
	bus.Unsubscribe(EventTxCreated)

	bus.Publish(context.Background(), NewEvent(EventTxCreated))

	if called {
		t.Error("expected handler not to be called after unsubscribe")
	}

	if bus.HandlerCount(EventTxCreated) != 0 {
		t.Errorf("expected 0 handlers after unsubscribe, got %d", bus.HandlerCount(EventTxCreated))
	}
}

func TestMemoryEventBus_UnsubscribeAll(t *testing.T) {
	bus := NewMemoryEventBus()

	var callCount int32
	handler := func(ctx context.Context, event Event) error {
		atomic.AddInt32(&callCount, 1)
		return nil
	}

	bus.Subscribe(EventTxCreated, handler)
	bus.Subscribe(EventTxCompleted, handler)
	bus.SubscribeAll(handler)

	bus.UnsubscribeAll()

	bus.Publish(context.Background(), NewEvent(EventTxCreated))
	bus.Publish(context.Background(), NewEvent(EventTxCompleted))

	if atomic.LoadInt32(&callCount) != 0 {
		t.Errorf("expected no handlers to be called after UnsubscribeAll, got %d", callCount)
	}
}

// ============================================================================
// Unit Tests - Event Data
// ============================================================================

func TestMemoryEventBus_EventContainsRequiredData(t *testing.T) {
	bus := NewMemoryEventBus()

	var received Event

	handler := func(ctx context.Context, event Event) error {
		received = event
		return nil
	}

	bus.Subscribe(EventTxCreated, handler)

	event := NewEvent(EventTxCreated).
		WithTxID("tx-123").
		WithTxType("transfer").
		WithData("amount", 100.0)

	bus.Publish(context.Background(), event)

	if received.TxID != "tx-123" {
		t.Errorf("expected TxID 'tx-123', got '%s'", received.TxID)
	}

	if received.TxType != "transfer" {
		t.Errorf("expected TxType 'transfer', got '%s'", received.TxType)
	}

	if received.Timestamp.IsZero() {
		t.Error("expected Timestamp to be set")
	}

	if received.Data["amount"] != 100.0 {
		t.Errorf("expected Data['amount'] = 100.0, got %v", received.Data["amount"])
	}
}

// ============================================================================
// Unit Tests - Concurrent Safety
// ============================================================================

func TestMemoryEventBus_ConcurrentPublish(t *testing.T) {
	bus := NewMemoryEventBus()

	var callCount int64

	handler := func(ctx context.Context, event Event) error {
		atomic.AddInt64(&callCount, 1)
		return nil
	}

	bus.Subscribe(EventTxCreated, handler)

	var wg sync.WaitGroup
	numGoroutines := 10
	numPublishes := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numPublishes; j++ {
				bus.Publish(context.Background(), NewEvent(EventTxCreated))
			}
		}()
	}

	wg.Wait()

	expected := int64(numGoroutines * numPublishes)
	if atomic.LoadInt64(&callCount) != expected {
		t.Errorf("expected %d calls, got %d", expected, callCount)
	}
}

func TestMemoryEventBus_ConcurrentSubscribeAndPublish(t *testing.T) {
	bus := NewMemoryEventBus()

	var wg sync.WaitGroup

	// Concurrent subscribes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler := func(ctx context.Context, event Event) error {
				return nil
			}
			bus.Subscribe(EventTxCreated, handler)
		}()
	}

	// Concurrent publishes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				bus.Publish(context.Background(), NewEvent(EventTxCreated))
			}
		}()
	}

	wg.Wait()

	// Should not panic or deadlock
	if bus.HandlerCount(EventTxCreated) != 10 {
		t.Errorf("expected 10 handlers, got %d", bus.HandlerCount(EventTxCreated))
	}
}

// ============================================================================
// Unit Tests - NoOpEventBus
// ============================================================================

func TestNoOpEventBus_DoesNothing(t *testing.T) {
	bus := NewNoOpEventBus()

	err := bus.Subscribe(EventTxCreated, func(ctx context.Context, event Event) error {
		t.Error("handler should not be called")
		return nil
	})

	if err != nil {
		t.Errorf("expected no error from Subscribe, got %v", err)
	}

	err = bus.SubscribeAll(func(ctx context.Context, event Event) error {
		t.Error("handler should not be called")
		return nil
	})

	if err != nil {
		t.Errorf("expected no error from SubscribeAll, got %v", err)
	}

	err = bus.Publish(context.Background(), NewEvent(EventTxCreated))

	if err != nil {
		t.Errorf("expected no error from Publish, got %v", err)
	}
}

// ============================================================================
// Unit Tests - Event Builder
// ============================================================================

func TestEvent_Builder(t *testing.T) {
	err := errors.New("test error")

	event := NewEvent(EventStepFailed).
		WithTxID("tx-123").
		WithTxType("transfer").
		WithStepName("debit").
		WithError(err).
		WithData("key1", "value1").
		WithData("key2", 42)

	if event.Type != EventStepFailed {
		t.Errorf("expected Type EventStepFailed, got %s", event.Type)
	}

	if event.TxID != "tx-123" {
		t.Errorf("expected TxID 'tx-123', got '%s'", event.TxID)
	}

	if event.TxType != "transfer" {
		t.Errorf("expected TxType 'transfer', got '%s'", event.TxType)
	}

	if event.StepName != "debit" {
		t.Errorf("expected StepName 'debit', got '%s'", event.StepName)
	}

	if event.Error != err {
		t.Errorf("expected Error to be set")
	}

	if event.Timestamp.IsZero() {
		t.Error("expected Timestamp to be set")
	}

	if event.Data["key1"] != "value1" {
		t.Errorf("expected Data['key1'] = 'value1', got %v", event.Data["key1"])
	}

	if event.Data["key2"] != 42 {
		t.Errorf("expected Data['key2'] = 42, got %v", event.Data["key2"])
	}
}

func TestEventType_String(t *testing.T) {
	if EventTxCreated.String() != "tx.created" {
		t.Errorf("expected 'tx.created', got '%s'", EventTxCreated.String())
	}

	if EventStepCompleted.String() != "step.completed" {
		t.Errorf("expected 'step.completed', got '%s'", EventStepCompleted.String())
	}
}

// ============================================================================
// Unit Tests - Handler Execution Order
// ============================================================================

func TestMemoryEventBus_HandlersExecuteInOrder(t *testing.T) {
	bus := NewMemoryEventBus()

	var order []int
	var mu sync.Mutex

	for i := 1; i <= 3; i++ {
		idx := i
		handler := func(ctx context.Context, event Event) error {
			mu.Lock()
			order = append(order, idx)
			mu.Unlock()
			return nil
		}
		bus.Subscribe(EventTxCreated, handler)
	}

	bus.Publish(context.Background(), NewEvent(EventTxCreated))

	// Give handlers time to complete
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(order) != 3 {
		t.Errorf("expected 3 handlers to be called, got %d", len(order))
	}

	// Handlers should execute in subscription order
	for i, v := range order {
		if v != i+1 {
			t.Errorf("expected handler %d at position %d, got %d", i+1, i, v)
		}
	}
}
