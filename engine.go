package rte

import (
	"context"
	"sync"

	"rte/circuit"
	"rte/event"
	"rte/idempotency"
	"rte/lock"
)

// Engine is the main entry point for the RTE (Reliable Transaction Engine).
// It provides methods for creating and executing transactions, registering steps,
// and subscribing to events.
type Engine struct {
	// coordinator handles transaction execution
	coordinator *Coordinator

	// Step registry
	steps map[string]Step
	mu    sync.RWMutex

	// Dependencies
	store   TxStore
	locker  lock.Locker
	breaker circuit.Breaker
	events  event.EventBus
	checker idempotency.Checker

	// Configuration
	config Config
}

// EngineOption is a function that configures the Engine.
type EngineOption func(*Engine)

// WithEngineStore sets the store for the engine.
func WithEngineStore(s TxStore) EngineOption {
	return func(e *Engine) {
		e.store = s
	}
}

// WithEngineLocker sets the locker for the engine.
func WithEngineLocker(l lock.Locker) EngineOption {
	return func(e *Engine) {
		e.locker = l
	}
}

// WithEngineBreaker sets the circuit breaker for the engine.
func WithEngineBreaker(b circuit.Breaker) EngineOption {
	return func(e *Engine) {
		e.breaker = b
	}
}

// WithEngineEventBus sets the event bus for the engine.
func WithEngineEventBus(eb event.EventBus) EngineOption {
	return func(e *Engine) {
		e.events = eb
	}
}

// WithEngineChecker sets the idempotency checker for the engine.
func WithEngineChecker(ch idempotency.Checker) EngineOption {
	return func(e *Engine) {
		e.checker = ch
	}
}

// WithEngineConfig sets the configuration for the engine.
func WithEngineConfig(cfg Config) EngineOption {
	return func(e *Engine) {
		e.config = cfg
	}
}

// NewEngine creates a new Engine with the given options.
// The engine must be configured with at least a store before executing transactions.
func NewEngine(opts ...EngineOption) *Engine {
	e := &Engine{
		steps:  make(map[string]Step),
		config: DefaultConfig(),
	}

	for _, opt := range opts {
		opt(e)
	}

	// Create coordinator with the same dependencies
	e.coordinator = NewCoordinator(
		WithStore(e.store),
		WithLocker(e.locker),
		WithBreaker(e.breaker),
		WithEventBus(e.events),
		WithChecker(e.checker),
		WithCoordinatorConfig(e.config),
	)

	return e
}

// RegisterStep registers a step with the engine.
// Steps must be registered before they can be used in transactions.
func (e *Engine) RegisterStep(step Step) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.steps[step.Name()] = step
	// Also register with coordinator
	e.coordinator.RegisterStep(step)
}

// GetStep returns a step by name, or nil if not found.
func (e *Engine) GetStep(name string) Step {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.steps[name]
}

// HasStep returns true if a step with the given name is registered.
func (e *Engine) HasStep(name string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	_, ok := e.steps[name]
	return ok
}

// NewTransaction creates a new transaction builder with the given type.
// The returned builder uses a fluent API for configuration.
func (e *Engine) NewTransaction(txType string) *TransactionBuilder {
	return NewTransaction(txType).WithStepRegistry(e)
}

// NewTransactionWithID creates a new transaction builder with a specific ID.
// This is useful for idempotent transaction creation.
func (e *Engine) NewTransactionWithID(txID, txType string) *TransactionBuilder {
	return NewTransactionWithID(txID, txType).WithStepRegistry(e)
}

// Execute executes a transaction and returns the result.
// This is the main entry point for transaction execution.
func (e *Engine) Execute(ctx context.Context, tx *Transaction) (*TxResult, error) {
	return e.coordinator.Execute(ctx, tx)
}

// Subscribe subscribes a handler to a specific event type.
// Multiple handlers can be registered for the same event type.
func (e *Engine) Subscribe(eventType event.EventType, handler event.EventHandler) error {
	if e.events == nil {
		return nil
	}
	return e.events.Subscribe(eventType, handler)
}

// SubscribeAll subscribes a handler to all events.
func (e *Engine) SubscribeAll(handler event.EventHandler) error {
	if e.events == nil {
		return nil
	}
	return e.events.SubscribeAll(handler)
}

// Coordinator returns the underlying coordinator.
// This is useful for advanced use cases like recovery.
func (e *Engine) Coordinator() *Coordinator {
	return e.coordinator
}

// Store returns the underlying store.
func (e *Engine) Store() TxStore {
	return e.store
}

// Config returns the engine configuration.
func (e *Engine) Config() Config {
	return e.config
}
