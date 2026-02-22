package rte

import (
	"context"

	"rte/circuit"
	"rte/event"
	"rte/idempotency"
	"rte/lock"
)

// Engine is the main entry point for the RTE (Reliable Transaction Engine).
// It provides methods for creating and executing transactions, registering steps,
// and subscribing to events.
type Engine struct {
	// coordinator handles transaction execution and holds all dependencies
	coordinator *Coordinator
}

// EngineOption is a function that configures the Engine.
type EngineOption func(*Engine, *engineConfig)

// engineConfig holds the configuration options that will be passed to coordinator
type engineConfig struct {
	locker  lock.Locker
	breaker circuit.Breaker
	events  event.EventBus
	checker idempotency.Checker
	config  Config
}

// WithEngineLocker sets the locker for the engine.
func WithEngineLocker(l lock.Locker) EngineOption {
	return func(e *Engine, cfg *engineConfig) {
		cfg.locker = l
	}
}

// WithEngineBreaker sets the circuit breaker for the engine.
func WithEngineBreaker(b circuit.Breaker) EngineOption {
	return func(e *Engine, cfg *engineConfig) {
		cfg.breaker = b
	}
}

// WithEngineEventBus sets the event bus for the engine.
func WithEngineEventBus(eb event.EventBus) EngineOption {
	return func(e *Engine, cfg *engineConfig) {
		cfg.events = eb
	}
}

// WithEngineChecker sets the idempotency checker for the engine.
func WithEngineChecker(ch idempotency.Checker) EngineOption {
	return func(e *Engine, cfg *engineConfig) {
		cfg.checker = ch
	}
}

// WithEngineConfig sets the configuration for the engine.
func WithEngineConfig(c Config) EngineOption {
	return func(e *Engine, cfg *engineConfig) {
		cfg.config = c
	}
}

// NewEngine creates a new Engine with the given store and options.
// Store is a required dependency for transaction persistence.
func NewEngine(store TxStore, opts ...EngineOption) *Engine {
	cfg := &engineConfig{
		config: DefaultConfig(),
	}

	e := &Engine{}

	for _, opt := range opts {
		opt(e, cfg)
	}

	// Create coordinator with store as required parameter
	e.coordinator = NewCoordinator(
		store,
		WithLocker(cfg.locker),
		WithBreaker(cfg.breaker),
		WithEventBus(cfg.events),
		WithChecker(cfg.checker),
		WithCoordinatorConfig(cfg.config),
	)

	return e
}

// RegisterStep registers a step with the engine.
// Steps must be registered before they can be used in transactions.
func (e *Engine) RegisterStep(step Step) {
	e.coordinator.RegisterStep(step)
}

// GetStep returns a step by name, or nil if not found.
func (e *Engine) GetStep(name string) Step {
	return e.coordinator.GetStep(name)
}

// HasStep returns true if a step with the given name is registered.
func (e *Engine) HasStep(name string) bool {
	return e.coordinator.HasStep(name)
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
	if e.coordinator.events == nil {
		return nil
	}
	return e.coordinator.events.Subscribe(eventType, handler)
}

// SubscribeAll subscribes a handler to all events.
func (e *Engine) SubscribeAll(handler event.EventHandler) error {
	if e.coordinator.events == nil {
		return nil
	}
	return e.coordinator.events.SubscribeAll(handler)
}

// Coordinator returns the underlying coordinator.
// This is useful for advanced use cases like recovery.
func (e *Engine) Coordinator() *Coordinator {
	return e.coordinator
}

// Store returns the underlying store.
func (e *Engine) Store() TxStore {
	return e.coordinator.store
}

// Config returns the engine configuration.
func (e *Engine) Config() Config {
	return e.coordinator.config
}
