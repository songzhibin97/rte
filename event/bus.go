package event

import (
	"context"
	"log"
	"sync"
)

// EventHandler 事件处理器
type EventHandler func(ctx context.Context, event Event) error

// EventBus 事件总线接口
type EventBus interface {
	// Publish 发布事件
	Publish(ctx context.Context, event Event) error
	// Subscribe 订阅事件
	Subscribe(eventType EventType, handler EventHandler) error
	// SubscribeAll 订阅所有事件
	SubscribeAll(handler EventHandler) error
}

// MemoryEventBus 内存事件总线实现
type MemoryEventBus struct {
	mu          sync.RWMutex
	handlers    map[EventType][]EventHandler
	allHandlers []EventHandler
	logger      Logger
}

// Logger 日志接口
type Logger interface {
	Printf(format string, v ...any)
}

// defaultLogger 默认日志实现
type defaultLogger struct{}

func (l *defaultLogger) Printf(format string, v ...any) {
	log.Printf(format, v...)
}

// MemoryEventBusOption 配置选项
type MemoryEventBusOption func(*MemoryEventBus)

// WithLogger sets a custom logger for the event bus.
func WithLogger(logger Logger) MemoryEventBusOption {
	return func(b *MemoryEventBus) {
		b.logger = logger
	}
}

// NewMemoryEventBus creates a new in-memory event bus.
func NewMemoryEventBus(opts ...MemoryEventBusOption) *MemoryEventBus {
	bus := &MemoryEventBus{
		handlers:    make(map[EventType][]EventHandler),
		allHandlers: make([]EventHandler, 0),
		logger:      &defaultLogger{},
	}

	for _, opt := range opts {
		opt(bus)
	}

	return bus
}

// Publish publishes an event to all subscribed handlers.
// Handler errors are logged but do not block transaction execution.
func (b *MemoryEventBus) Publish(ctx context.Context, event Event) error {
	b.mu.RLock()
	// Copy handlers to avoid holding lock during execution
	typeHandlers := make([]EventHandler, len(b.handlers[event.Type]))
	copy(typeHandlers, b.handlers[event.Type])
	allHandlers := make([]EventHandler, len(b.allHandlers))
	copy(allHandlers, b.allHandlers)
	b.mu.RUnlock()

	// Execute type-specific handlers
	for _, handler := range typeHandlers {
		b.executeHandler(ctx, handler, event)
	}

	// Execute all-event handlers
	for _, handler := range allHandlers {
		b.executeHandler(ctx, handler, event)
	}

	return nil
}

// executeHandler executes a single handler and logs any errors.
// Errors do not propagate to prevent blocking transaction execution.
func (b *MemoryEventBus) executeHandler(ctx context.Context, handler EventHandler, event Event) {
	defer func() {
		if r := recover(); r != nil {
			b.logger.Printf("[EventBus] handler panic for event %s: %v", event.Type, r)
		}
	}()

	if err := handler(ctx, event); err != nil {
		b.logger.Printf("[EventBus] handler error for event %s (tx=%s): %v", event.Type, event.TxID, err)
	}
}

// Subscribe subscribes a handler to a specific event type.
// Multiple handlers can be registered for the same event type.
func (b *MemoryEventBus) Subscribe(eventType EventType, handler EventHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.handlers[eventType] = append(b.handlers[eventType], handler)
	return nil
}

// SubscribeAll subscribes a handler to all events.
func (b *MemoryEventBus) SubscribeAll(handler EventHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.allHandlers = append(b.allHandlers, handler)
	return nil
}

// Unsubscribe removes all handlers for a specific event type.
// This is useful for testing and cleanup.
func (b *MemoryEventBus) Unsubscribe(eventType EventType) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.handlers, eventType)
}

// UnsubscribeAll removes all handlers (both type-specific and all-event handlers).
func (b *MemoryEventBus) UnsubscribeAll() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.handlers = make(map[EventType][]EventHandler)
	b.allHandlers = make([]EventHandler, 0)
}

// HandlerCount returns the number of handlers for a specific event type.
func (b *MemoryEventBus) HandlerCount(eventType EventType) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.handlers[eventType])
}

// AllHandlerCount returns the number of all-event handlers.
func (b *MemoryEventBus) AllHandlerCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.allHandlers)
}

// NoOpEventBus 空操作事件总线（用于测试或禁用事件）
type NoOpEventBus struct{}

// NewNoOpEventBus creates a new no-op event bus.
func NewNoOpEventBus() *NoOpEventBus {
	return &NoOpEventBus{}
}

// Publish does nothing.
func (b *NoOpEventBus) Publish(_ context.Context, _ Event) error {
	return nil
}

// Subscribe does nothing.
func (b *NoOpEventBus) Subscribe(_ EventType, _ EventHandler) error {
	return nil
}

// SubscribeAll does nothing.
func (b *NoOpEventBus) SubscribeAll(_ EventHandler) error {
	return nil
}
