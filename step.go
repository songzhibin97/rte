package rte

import (
	"context"
	"time"
)

// StepConfig represents per-step configuration options
type StepConfig struct {
	// Timeout is the step execution timeout, 0 means use global config
	Timeout time.Duration
	// MaxRetries is the maximum retry count, 0 means use global config
	MaxRetries int
	// RetryInterval is the interval between retries, 0 means use global config
	RetryInterval time.Duration
}

// Step defines the interface for transaction steps
type Step interface {
	// Name returns the step name
	Name() string

	// Execute executes the step
	Execute(ctx context.Context, txCtx *TxContext) error

	// Compensate performs the compensation operation
	Compensate(ctx context.Context, txCtx *TxContext) error

	// SupportsCompensation returns whether this step supports compensation
	SupportsCompensation() bool

	// IdempotencyKey returns the idempotency key for this step execution
	IdempotencyKey(txCtx *TxContext) string

	// SupportsIdempotency returns whether this step supports idempotency checking
	SupportsIdempotency() bool

	// Config returns the step-level configuration, nil means use global config
	Config() *StepConfig
}

// BaseStep provides a base implementation of the Step interface
// that can be embedded in custom step implementations
type BaseStep struct {
	name   string
	config *StepConfig
}

// NewBaseStep creates a new BaseStep with the given name
func NewBaseStep(name string) *BaseStep {
	return &BaseStep{
		name: name,
	}
}

// NewBaseStepWithConfig creates a new BaseStep with the given name and config
func NewBaseStepWithConfig(name string, config *StepConfig) *BaseStep {
	return &BaseStep{
		name:   name,
		config: config,
	}
}

// Name returns the step name
func (s *BaseStep) Name() string {
	return s.name
}

// Execute is a no-op implementation that should be overridden
func (s *BaseStep) Execute(ctx context.Context, txCtx *TxContext) error {
	return nil
}

// Compensate returns ErrCompensationNotSupported by default
func (s *BaseStep) Compensate(ctx context.Context, txCtx *TxContext) error {
	return ErrCompensationNotSupported
}

// SupportsCompensation returns false by default
func (s *BaseStep) SupportsCompensation() bool {
	return false
}

// IdempotencyKey returns an empty string by default
func (s *BaseStep) IdempotencyKey(txCtx *TxContext) string {
	return ""
}

// SupportsIdempotency returns false by default
func (s *BaseStep) SupportsIdempotency() bool {
	return false
}

// Config returns the step configuration
func (s *BaseStep) Config() *StepConfig {
	return s.config
}

// SetConfig sets the step configuration
func (s *BaseStep) SetConfig(config *StepConfig) {
	s.config = config
}
