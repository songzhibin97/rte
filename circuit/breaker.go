package circuit

import (
	"context"
	"time"
)

// State represents the circuit breaker state
type State int

const (
	// StateClosed is the normal state where requests are allowed
	StateClosed State = iota
	// StateOpen is the state where requests are blocked
	StateOpen
	// StateHalfOpen is the state where limited requests are allowed to test recovery
	StateHalfOpen
)

// String returns the string representation of the state
func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// BreakerConfig holds the configuration for a circuit breaker
type BreakerConfig struct {
	// Threshold is the number of consecutive failures before opening the circuit
	Threshold int
	// Timeout is the duration to wait before transitioning from OPEN to HALF_OPEN
	Timeout time.Duration
	// HalfOpenMaxReqs is the maximum number of requests allowed in HALF_OPEN state
	HalfOpenMaxReqs int
}

// DefaultBreakerConfig returns the default circuit breaker configuration
func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		Threshold:       5,
		Timeout:         30 * time.Second,
		HalfOpenMaxReqs: 3,
	}
}

// BreakerCounts holds the statistics for a circuit breaker
type BreakerCounts struct {
	// Requests is the total number of requests
	Requests int64
	// TotalSuccesses is the total number of successful requests
	TotalSuccesses int64
	// TotalFailures is the total number of failed requests
	TotalFailures int64
	// ConsecutiveSuccesses is the number of consecutive successful requests
	ConsecutiveSuccesses int64
	// ConsecutiveFailures is the number of consecutive failed requests
	ConsecutiveFailures int64
}

// Breaker is the circuit breaker manager interface
type Breaker interface {
	// Get returns the circuit breaker for the specified service with default config
	Get(service string) CircuitBreaker
	// GetWithConfig returns the circuit breaker for the specified service with custom config
	GetWithConfig(service string, config BreakerConfig) CircuitBreaker
}

// CircuitBreaker is the interface for a single circuit breaker
type CircuitBreaker interface {
	// Execute executes the given function with circuit breaker protection
	Execute(ctx context.Context, fn func() error) error
	// State returns the current state of the circuit breaker
	State() State
	// Reset manually resets the circuit breaker to closed state
	Reset()
	// Counts returns the current statistics
	Counts() BreakerCounts
}
