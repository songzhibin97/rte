package memory

import (
	"context"
	"sync"
	"time"

	"rte/circuit"

	rte "rte"
)

// MemoryBreaker is an in-memory implementation of the Breaker interface
type MemoryBreaker struct {
	mu            sync.RWMutex
	breakers      map[string]*memoryCircuitBreaker
	defaultConfig circuit.BreakerConfig
}

// NewMemoryBreaker creates a new MemoryBreaker with default configuration
func NewMemoryBreaker() *MemoryBreaker {
	return NewMemoryBreakerWithConfig(circuit.DefaultBreakerConfig())
}

// NewMemoryBreakerWithConfig creates a new MemoryBreaker with custom default configuration
func NewMemoryBreakerWithConfig(config circuit.BreakerConfig) *MemoryBreaker {
	return &MemoryBreaker{
		breakers:      make(map[string]*memoryCircuitBreaker),
		defaultConfig: config,
	}
}

// Get returns the circuit breaker for the specified service with default config
func (m *MemoryBreaker) Get(service string) circuit.CircuitBreaker {
	return m.GetWithConfig(service, m.defaultConfig)
}

// GetWithConfig returns the circuit breaker for the specified service with custom config
func (m *MemoryBreaker) GetWithConfig(service string, config circuit.BreakerConfig) circuit.CircuitBreaker {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cb, exists := m.breakers[service]; exists {
		return cb
	}

	cb := newMemoryCircuitBreaker(service, config)
	m.breakers[service] = cb
	return cb
}

// memoryCircuitBreaker is an in-memory implementation of CircuitBreaker
type memoryCircuitBreaker struct {
	mu      sync.RWMutex
	service string
	config  circuit.BreakerConfig
	state   circuit.State
	counts  circuit.BreakerCounts

	// openedAt is the time when the circuit was opened
	openedAt time.Time
	// halfOpenRequests tracks the number of requests in half-open state
	halfOpenRequests int
}

// newMemoryCircuitBreaker creates a new memoryCircuitBreaker
func newMemoryCircuitBreaker(service string, config circuit.BreakerConfig) *memoryCircuitBreaker {
	return &memoryCircuitBreaker{
		service: service,
		config:  config,
		state:   circuit.StateClosed,
	}
}

// Execute executes the given function with circuit breaker protection
func (cb *memoryCircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	// Check if we can proceed
	if err := cb.beforeRequest(); err != nil {
		return err
	}

	// Execute the function
	err := fn()

	// Record the result
	cb.afterRequest(err == nil)

	return err
}

// beforeRequest checks if the request can proceed and updates state if needed
func (cb *memoryCircuitBreaker) beforeRequest() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case circuit.StateClosed:
		// Allow request
		cb.counts.Requests++
		return nil

	case circuit.StateOpen:
		// Check if timeout has passed
		if now.Sub(cb.openedAt) >= cb.config.Timeout {
			// Transition to half-open
			cb.state = circuit.StateHalfOpen
			cb.halfOpenRequests = 0
			cb.counts.Requests++
			cb.halfOpenRequests++
			return nil
		}
		// Still open, reject request
		return rte.ErrCircuitOpen

	case circuit.StateHalfOpen:
		// Check if we've reached the max requests for half-open
		if cb.halfOpenRequests >= cb.config.HalfOpenMaxReqs {
			return rte.ErrCircuitOpen
		}
		cb.counts.Requests++
		cb.halfOpenRequests++
		return nil

	default:
		return rte.ErrCircuitOpen
	}
}

// afterRequest records the result of the request and updates state
func (cb *memoryCircuitBreaker) afterRequest(success bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if success {
		cb.onSuccess()
	} else {
		cb.onFailure()
	}
}

// onSuccess handles a successful request
func (cb *memoryCircuitBreaker) onSuccess() {
	cb.counts.TotalSuccesses++
	cb.counts.ConsecutiveSuccesses++
	cb.counts.ConsecutiveFailures = 0

	switch cb.state {
	case circuit.StateClosed:
		// Stay closed
	case circuit.StateHalfOpen:
		// If we've had enough successes in half-open, close the circuit
		if cb.counts.ConsecutiveSuccesses >= int64(cb.config.HalfOpenMaxReqs) {
			cb.state = circuit.StateClosed
			cb.halfOpenRequests = 0
		}
	case circuit.StateOpen:
		// Should not happen, but handle gracefully
	}
}

// onFailure handles a failed request
func (cb *memoryCircuitBreaker) onFailure() {
	cb.counts.TotalFailures++
	cb.counts.ConsecutiveFailures++
	cb.counts.ConsecutiveSuccesses = 0

	switch cb.state {
	case circuit.StateClosed:
		// Check if we should open the circuit
		if cb.counts.ConsecutiveFailures >= int64(cb.config.Threshold) {
			cb.state = circuit.StateOpen
			cb.openedAt = time.Now()
		}
	case circuit.StateHalfOpen:
		// Any failure in half-open state opens the circuit again
		cb.state = circuit.StateOpen
		cb.openedAt = time.Now()
		cb.halfOpenRequests = 0
	case circuit.StateOpen:
		// Should not happen, but handle gracefully
	}
}

// State returns the current state of the circuit breaker
func (cb *memoryCircuitBreaker) State() circuit.State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	// Check if we should transition from open to half-open
	if cb.state == circuit.StateOpen && time.Since(cb.openedAt) >= cb.config.Timeout {
		// Note: We don't actually transition here to avoid write lock
		// The transition will happen on the next request
		return circuit.StateHalfOpen
	}

	return cb.state
}

// Reset manually resets the circuit breaker to closed state
func (cb *memoryCircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = circuit.StateClosed
	cb.counts = circuit.BreakerCounts{}
	cb.halfOpenRequests = 0
}

// Counts returns the current statistics
func (cb *memoryCircuitBreaker) Counts() circuit.BreakerCounts {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.counts
}
