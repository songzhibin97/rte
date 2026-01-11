// Package memory provides tests for the in-memory circuit breaker implementation.
package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"pgregory.net/rapid"

	"rte"
	"rte/circuit"
)

// ============================================================================
// Test Helpers
// ============================================================================

var errSimulatedFailure = errors.New("simulated failure")

// ============================================================================
// Unit Tests
// ============================================================================

func TestMemoryBreaker_InitialState(t *testing.T) {
	breaker := NewMemoryBreaker()
	cb := breaker.Get("test-service")

	if cb.State() != circuit.StateClosed {
		t.Errorf("expected initial state CLOSED, got %s", cb.State())
	}

	counts := cb.Counts()
	if counts.Requests != 0 || counts.TotalSuccesses != 0 || counts.TotalFailures != 0 {
		t.Errorf("expected zero counts, got %+v", counts)
	}
}

func TestMemoryBreaker_SuccessfulExecution(t *testing.T) {
	breaker := NewMemoryBreaker()
	cb := breaker.Get("test-service")

	err := cb.Execute(context.Background(), func() error {
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	counts := cb.Counts()
	if counts.TotalSuccesses != 1 {
		t.Errorf("expected 1 success, got %d", counts.TotalSuccesses)
	}
	if cb.State() != circuit.StateClosed {
		t.Errorf("expected state CLOSED, got %s", cb.State())
	}
}

func TestMemoryBreaker_FailedExecution(t *testing.T) {
	breaker := NewMemoryBreaker()
	cb := breaker.Get("test-service")

	err := cb.Execute(context.Background(), func() error {
		return errSimulatedFailure
	})

	if !errors.Is(err, errSimulatedFailure) {
		t.Errorf("expected simulated failure, got %v", err)
	}

	counts := cb.Counts()
	if counts.TotalFailures != 1 {
		t.Errorf("expected 1 failure, got %d", counts.TotalFailures)
	}
}

func TestMemoryBreaker_OpensAfterThreshold(t *testing.T) {
	config := circuit.BreakerConfig{
		Threshold:       3,
		Timeout:         100 * time.Millisecond,
		HalfOpenMaxReqs: 1,
	}
	breaker := NewMemoryBreakerWithConfig(config)
	cb := breaker.Get("test-service")

	// Cause threshold failures
	for i := 0; i < 3; i++ {
		cb.Execute(context.Background(), func() error {
			return errSimulatedFailure
		})
	}

	if cb.State() != circuit.StateOpen {
		t.Errorf("expected state OPEN after %d failures, got %s", config.Threshold, cb.State())
	}
}

func TestMemoryBreaker_RejectsWhenOpen(t *testing.T) {
	config := circuit.BreakerConfig{
		Threshold:       1,
		Timeout:         1 * time.Hour, // Long timeout to keep it open
		HalfOpenMaxReqs: 1,
	}
	breaker := NewMemoryBreakerWithConfig(config)
	cb := breaker.Get("test-service")

	// Open the circuit
	cb.Execute(context.Background(), func() error {
		return errSimulatedFailure
	})

	// Try to execute when open
	err := cb.Execute(context.Background(), func() error {
		return nil
	})

	if !errors.Is(err, rte.ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestMemoryBreaker_TransitionsToHalfOpen(t *testing.T) {
	config := circuit.BreakerConfig{
		Threshold:       1,
		Timeout:         50 * time.Millisecond,
		HalfOpenMaxReqs: 1,
	}
	breaker := NewMemoryBreakerWithConfig(config)
	cb := breaker.Get("test-service")

	// Open the circuit
	cb.Execute(context.Background(), func() error {
		return errSimulatedFailure
	})

	if cb.State() != circuit.StateOpen {
		t.Errorf("expected state OPEN, got %s", cb.State())
	}

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// State() should report HALF_OPEN after timeout
	if cb.State() != circuit.StateHalfOpen {
		t.Errorf("expected state HALF_OPEN after timeout, got %s", cb.State())
	}
}

func TestMemoryBreaker_ClosesOnHalfOpenSuccess(t *testing.T) {
	config := circuit.BreakerConfig{
		Threshold:       1,
		Timeout:         10 * time.Millisecond,
		HalfOpenMaxReqs: 1,
	}
	breaker := NewMemoryBreakerWithConfig(config)
	cb := breaker.Get("test-service")

	// Open the circuit
	cb.Execute(context.Background(), func() error {
		return errSimulatedFailure
	})

	// Wait for timeout to transition to half-open
	time.Sleep(20 * time.Millisecond)

	// Execute successful request in half-open state
	err := cb.Execute(context.Background(), func() error {
		return nil
	})

	if err != nil {
		t.Errorf("expected no error in half-open, got %v", err)
	}

	if cb.State() != circuit.StateClosed {
		t.Errorf("expected state CLOSED after half-open success, got %s", cb.State())
	}
}

func TestMemoryBreaker_ReopensOnHalfOpenFailure(t *testing.T) {
	config := circuit.BreakerConfig{
		Threshold:       1,
		Timeout:         10 * time.Millisecond,
		HalfOpenMaxReqs: 1,
	}
	breaker := NewMemoryBreakerWithConfig(config)
	cb := breaker.Get("test-service")

	// Open the circuit
	cb.Execute(context.Background(), func() error {
		return errSimulatedFailure
	})

	// Wait for timeout to transition to half-open
	time.Sleep(20 * time.Millisecond)

	// Execute failed request in half-open state
	cb.Execute(context.Background(), func() error {
		return errSimulatedFailure
	})

	if cb.State() != circuit.StateOpen {
		t.Errorf("expected state OPEN after half-open failure, got %s", cb.State())
	}
}

func TestMemoryBreaker_Reset(t *testing.T) {
	config := circuit.BreakerConfig{
		Threshold:       1,
		Timeout:         1 * time.Hour,
		HalfOpenMaxReqs: 1,
	}
	breaker := NewMemoryBreakerWithConfig(config)
	cb := breaker.Get("test-service")

	// Open the circuit
	cb.Execute(context.Background(), func() error {
		return errSimulatedFailure
	})

	if cb.State() != circuit.StateOpen {
		t.Errorf("expected state OPEN, got %s", cb.State())
	}

	// Reset
	cb.Reset()

	if cb.State() != circuit.StateClosed {
		t.Errorf("expected state CLOSED after reset, got %s", cb.State())
	}

	counts := cb.Counts()
	if counts.Requests != 0 || counts.TotalFailures != 0 {
		t.Errorf("expected zero counts after reset, got %+v", counts)
	}
}

// ============================================================================
// Property-Based Tests
// ============================================================================


// For any circuit breaker, state transitions SHALL follow:
// - CLOSED → OPEN (on threshold failures)
// - OPEN → HALF_OPEN (on timeout)
// - HALF_OPEN → CLOSED (on success)
// - HALF_OPEN → OPEN (on failure)
func TestProperty_CircuitBreakerStateTransitions(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate random configuration
		threshold := rapid.IntRange(1, 10).Draw(t, "threshold")
		halfOpenMaxReqs := rapid.IntRange(1, 5).Draw(t, "halfOpenMaxReqs")

		config := circuit.BreakerConfig{
			Threshold:       threshold,
			Timeout:         10 * time.Millisecond, // Short timeout for testing
			HalfOpenMaxReqs: halfOpenMaxReqs,
		}

		breaker := NewMemoryBreakerWithConfig(config)
		cb := breaker.Get("test-service")

		
		if cb.State() != circuit.StateClosed {
			t.Fatalf("initial state should be CLOSED, got %s", cb.State())
		}

		
		for i := 0; i < threshold; i++ {
			cb.Execute(context.Background(), func() error {
				return errSimulatedFailure
			})
		}

		if cb.State() != circuit.StateOpen {
			t.Fatalf("state should be OPEN after %d consecutive failures, got %s", threshold, cb.State())
		}

		
		err := cb.Execute(context.Background(), func() error {
			return nil
		})
		if !errors.Is(err, rte.ErrCircuitOpen) {
			t.Fatalf("OPEN state should reject requests with ErrCircuitOpen, got %v", err)
		}

		
		time.Sleep(15 * time.Millisecond)
		if cb.State() != circuit.StateHalfOpen {
			t.Fatalf("state should be HALF_OPEN after timeout, got %s", cb.State())
		}

		// Reset for next test
		cb.Reset()

		// Test HALF_OPEN → CLOSED transition (Req 7.5, 7.6)
		// First, get to HALF_OPEN state
		for i := 0; i < threshold; i++ {
			cb.Execute(context.Background(), func() error {
				return errSimulatedFailure
			})
		}
		time.Sleep(15 * time.Millisecond)

		// Execute successful requests in HALF_OPEN
		for i := 0; i < halfOpenMaxReqs; i++ {
			err := cb.Execute(context.Background(), func() error {
				return nil
			})
			if err != nil {
				t.Fatalf("HALF_OPEN should allow requests, got error: %v", err)
			}
		}

		if cb.State() != circuit.StateClosed {
			t.Fatalf("state should be CLOSED after %d successful requests in HALF_OPEN, got %s", halfOpenMaxReqs, cb.State())
		}

		// Reset for next test
		cb.Reset()

		// Test HALF_OPEN → OPEN transition on failure (Req 7.7)
		// First, get to HALF_OPEN state
		for i := 0; i < threshold; i++ {
			cb.Execute(context.Background(), func() error {
				return errSimulatedFailure
			})
		}
		time.Sleep(15 * time.Millisecond)

		// Execute failed request in HALF_OPEN
		cb.Execute(context.Background(), func() error {
			return errSimulatedFailure
		})

		if cb.State() != circuit.StateOpen {
			t.Fatalf("state should be OPEN after failure in HALF_OPEN, got %s", cb.State())
		}
	})
}

// Additional property: Consecutive failures counter resets on success
func TestProperty_ConsecutiveFailuresResetOnSuccess(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		threshold := rapid.IntRange(2, 10).Draw(t, "threshold")
		failuresBeforeSuccess := rapid.IntRange(1, threshold-1).Draw(t, "failuresBeforeSuccess")

		config := circuit.BreakerConfig{
			Threshold:       threshold,
			Timeout:         100 * time.Millisecond,
			HalfOpenMaxReqs: 1,
		}

		breaker := NewMemoryBreakerWithConfig(config)
		cb := breaker.Get("test-service")

		// Cause some failures (but less than threshold)
		for i := 0; i < failuresBeforeSuccess; i++ {
			cb.Execute(context.Background(), func() error {
				return errSimulatedFailure
			})
		}

		// Circuit should still be closed
		if cb.State() != circuit.StateClosed {
			t.Fatalf("state should be CLOSED with %d failures (threshold=%d), got %s",
				failuresBeforeSuccess, threshold, cb.State())
		}

		// Execute a success
		cb.Execute(context.Background(), func() error {
			return nil
		})

		// Verify consecutive failures reset
		counts := cb.Counts()
		if counts.ConsecutiveFailures != 0 {
			t.Fatalf("consecutive failures should be 0 after success, got %d", counts.ConsecutiveFailures)
		}

		// Now we should need threshold failures again to open
		for i := 0; i < threshold; i++ {
			cb.Execute(context.Background(), func() error {
				return errSimulatedFailure
			})
		}

		if cb.State() != circuit.StateOpen {
			t.Fatalf("state should be OPEN after %d consecutive failures, got %s", threshold, cb.State())
		}
	})
}


// For any circuit breaker, the state transitions SHALL follow:
// CLOSED → OPEN (after threshold failures) → HALF_OPEN (after timeout) → CLOSED (after success) or OPEN (after failure)
func TestProperty_CircuitBreakerStateMachine(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate random configuration within reasonable bounds
		threshold := rapid.IntRange(1, 10).Draw(t, "threshold")
		halfOpenMaxReqs := rapid.IntRange(1, 5).Draw(t, "halfOpenMaxReqs")

		config := circuit.BreakerConfig{
			Threshold:       threshold,
			Timeout:         10 * time.Millisecond, // Short timeout for testing
			HalfOpenMaxReqs: halfOpenMaxReqs,
		}

		breaker := NewMemoryBreakerWithConfig(config)
		cb := breaker.Get("test-service")

		// ========================================================================
		
		// THE Circuit_Breaker SHALL open and reject subsequent requests
		// ========================================================================

		// Verify initial state is CLOSED
		if cb.State() != circuit.StateClosed {
			t.Fatalf("initial state should be CLOSED, got %s", cb.State())
		}

		// Cause exactly threshold consecutive failures
		for i := 0; i < threshold; i++ {
			cb.Execute(context.Background(), func() error {
				return errSimulatedFailure
			})
		}

		// Verify circuit is now OPEN
		if cb.State() != circuit.StateOpen {
			t.Fatalf("Req 5.1: state should be OPEN after %d consecutive failures, got %s", threshold, cb.State())
		}

		// ========================================================================
		
		// SHALL reject requests immediately without calling the downstream service
		// ========================================================================

		// Verify OPEN state rejects requests
		executed := false
		err := cb.Execute(context.Background(), func() error {
			executed = true
			return nil
		})

		if !errors.Is(err, rte.ErrCircuitOpen) {
			t.Fatalf("Req 5.2: OPEN state should reject with ErrCircuitOpen, got %v", err)
		}
		if executed {
			t.Fatalf("Req 5.2: OPEN state should NOT execute the function")
		}

		// ========================================================================
		
		// THE Circuit_Breaker SHALL transition to half-open state
		// ========================================================================

		// Wait for timeout to expire
		time.Sleep(15 * time.Millisecond)

		// Verify state is now HALF_OPEN
		if cb.State() != circuit.StateHalfOpen {
			t.Fatalf("Req 5.3: state should be HALF_OPEN after timeout, got %s", cb.State())
		}

		// ========================================================================
		
		// allow limited requests to test service recovery
		// ========================================================================

		// Reset and get back to HALF_OPEN state for testing limited requests
		cb.Reset()
		for i := 0; i < threshold; i++ {
			cb.Execute(context.Background(), func() error {
				return errSimulatedFailure
			})
		}
		time.Sleep(15 * time.Millisecond)

		// Verify HALF_OPEN allows limited requests
		allowedRequests := 0
		for i := 0; i < halfOpenMaxReqs+2; i++ {
			err := cb.Execute(context.Background(), func() error {
				return nil
			})
			if err == nil {
				allowedRequests++
			}
		}

		// Should have allowed at least halfOpenMaxReqs requests
		// (may close after success, allowing more)
		if allowedRequests < halfOpenMaxReqs {
			t.Fatalf("Req 5.4: HALF_OPEN should allow at least %d requests, allowed %d",
				halfOpenMaxReqs, allowedRequests)
		}

		// ========================================================================
		// Test HALF_OPEN → CLOSED transition 
		// ========================================================================

		cb.Reset()
		// Get to HALF_OPEN state
		for i := 0; i < threshold; i++ {
			cb.Execute(context.Background(), func() error {
				return errSimulatedFailure
			})
		}
		time.Sleep(15 * time.Millisecond)

		// Execute successful requests in HALF_OPEN
		for i := 0; i < halfOpenMaxReqs; i++ {
			cb.Execute(context.Background(), func() error {
				return nil
			})
		}

		// Verify circuit is now CLOSED
		if cb.State() != circuit.StateClosed {
			t.Fatalf("Req 5.5: state should be CLOSED after %d successful requests in HALF_OPEN, got %s",
				halfOpenMaxReqs, cb.State())
		}

		// ========================================================================
		// Test HALF_OPEN → OPEN transition on failure 
		// ========================================================================

		cb.Reset()
		// Get to HALF_OPEN state
		for i := 0; i < threshold; i++ {
			cb.Execute(context.Background(), func() error {
				return errSimulatedFailure
			})
		}
		time.Sleep(15 * time.Millisecond)

		// Execute failed request in HALF_OPEN
		cb.Execute(context.Background(), func() error {
			return errSimulatedFailure
		})

		// Verify circuit is now OPEN again
		if cb.State() != circuit.StateOpen {
			t.Fatalf("Req 5.6: state should be OPEN after failure in HALF_OPEN, got %s", cb.State())
		}
	})
}


func TestProperty_CountsConsistency(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		numOperations := rapid.IntRange(1, 50).Draw(t, "numOperations")
		successRate := rapid.Float64Range(0, 1).Draw(t, "successRate")

		breaker := NewMemoryBreaker()
		cb := breaker.Get("test-service")

		expectedSuccesses := 0
		expectedFailures := 0

		for i := 0; i < numOperations; i++ {
			shouldSucceed := rapid.Float64Range(0, 1).Draw(t, "shouldSucceed") < successRate

			// Skip if circuit is open
			if cb.State() == circuit.StateOpen {
				// Wait for timeout to allow more operations
				time.Sleep(35 * time.Millisecond)
			}

			err := cb.Execute(context.Background(), func() error {
				if shouldSucceed {
					return nil
				}
				return errSimulatedFailure
			})

			// Only count if request was actually executed (not rejected by open circuit)
			if !errors.Is(err, rte.ErrCircuitOpen) {
				if shouldSucceed {
					expectedSuccesses++
				} else {
					expectedFailures++
				}
			}
		}

		counts := cb.Counts()

		
		if counts.TotalSuccesses+counts.TotalFailures != counts.Requests {
			t.Fatalf("successes(%d) + failures(%d) should equal requests(%d)",
				counts.TotalSuccesses, counts.TotalFailures, counts.Requests)
		}

		
		if counts.Requests < 0 || counts.TotalSuccesses < 0 || counts.TotalFailures < 0 {
			t.Fatalf("counts should be non-negative: %+v", counts)
		}

		
		if counts.ConsecutiveSuccesses > counts.TotalSuccesses {
			t.Fatalf("consecutive successes(%d) should not exceed total successes(%d)",
				counts.ConsecutiveSuccesses, counts.TotalSuccesses)
		}
		if counts.ConsecutiveFailures > counts.TotalFailures {
			t.Fatalf("consecutive failures(%d) should not exceed total failures(%d)",
				counts.ConsecutiveFailures, counts.TotalFailures)
		}
	})
}
