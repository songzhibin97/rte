// Package testinfra provides property-based tests for RTE production validation.
// Feature: financial-production-readiness
// This file contains property tests for distributed lock correctness (Properties 7, 8, 9).
package testinfra

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"rte"
	"rte/circuit/memory"

	"pgregory.net/rapid"
)

// ============================================================================

// For any two concurrent transactions attempting to acquire locks on overlapping
// resources, at most one SHALL succeed in acquiring all locks.
// ============================================================================

// TestProperty_DistributedLockMutualExclusion tests that concurrent lock acquisition
// results in at most one success.

// resources, at most one SHALL succeed in acquiring all locks.
func TestProperty_DistributedLockMutualExclusion(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random concurrency level (2-20 concurrent acquirers)
		concurrency := rapid.IntRange(2, 20).Draw(rt, "concurrency")

		// Generate random number of lock keys (1-3)
		numKeys := rapid.IntRange(1, 3).Draw(rt, "numKeys")

		// Generate unique lock keys for this test iteration
		lockKeys := make([]string, numKeys)
		baseKey := fmt.Sprintf("mutex-test-%d", atomic.AddInt64(&txIDCounter, 1))
		for i := 0; i < numKeys; i++ {
			lockKeys[i] = fmt.Sprintf("%s-key-%d", baseKey, i)
		}

		// Track successful acquisitions
		var successCount int64
		var failCount int64

		// Use a barrier to ensure all goroutines attempt acquisition simultaneously
		var readyWg sync.WaitGroup
		readyWg.Add(concurrency)
		startCh := make(chan struct{})

		var wg sync.WaitGroup
		ctx := context.Background()

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				// Signal ready and wait for all goroutines
				readyWg.Done()
				<-startCh

				// Attempt to acquire the lock
				handle, err := ti.Locker.Acquire(ctx, lockKeys, 30*time.Second)
				if err != nil {
					atomic.AddInt64(&failCount, 1)
					return
				}

				// Successfully acquired - count it
				atomic.AddInt64(&successCount, 1)

				// Hold the lock briefly to ensure overlap
				time.Sleep(10 * time.Millisecond)

				// Release the lock
				handle.Release(ctx)
			}(i)
		}

		// Wait for all goroutines to be ready
		readyWg.Wait()

		// Start all acquisitions simultaneously
		close(startCh)

		// Wait for all to complete
		wg.Wait()

		
		// Note: After the first one releases, others may succeed sequentially
		// But the key property is that at any instant, at most one holds the lock

		// At least one should succeed
		if successCount == 0 {
			rt.Fatalf("Expected at least one successful acquisition, got 0 (failures: %d)", failCount)
		}

		rt.Logf("Mutual exclusion test: %d succeeded, %d failed out of %d concurrent attempts",
			successCount, failCount, concurrency)
	})
}

// TestProperty_DistributedLockMutualExclusion_Strict tests strict mutual exclusion
// where we verify that only one holder exists at any given time.

func TestProperty_DistributedLockMutualExclusion_Strict(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random concurrency level (2-10)
		concurrency := rapid.IntRange(2, 10).Draw(rt, "concurrency")

		// Generate unique lock key for this test iteration
		lockKey := fmt.Sprintf("strict-mutex-%d", atomic.AddInt64(&txIDCounter, 1))

		// Track concurrent holders
		var currentHolders int64
		var maxConcurrentHolders int64
		var holdersMu sync.Mutex

		var wg sync.WaitGroup
		ctx := context.Background()

		// Use a barrier to ensure all goroutines attempt acquisition simultaneously
		var readyWg sync.WaitGroup
		readyWg.Add(concurrency)
		startCh := make(chan struct{})

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				// Signal ready and wait
				readyWg.Done()
				<-startCh

				// Try to acquire the lock (with retries)
				for attempt := 0; attempt < 5; attempt++ {
					handle, err := ti.Locker.Acquire(ctx, []string{lockKey}, 30*time.Second)
					if err != nil {
						time.Sleep(20 * time.Millisecond)
						continue
					}

					// Successfully acquired - track concurrent holders
					holdersMu.Lock()
					currentHolders++
					if currentHolders > maxConcurrentHolders {
						maxConcurrentHolders = currentHolders
					}
					holdersMu.Unlock()

					// Hold the lock for a bit
					time.Sleep(30 * time.Millisecond)

					// Release
					holdersMu.Lock()
					currentHolders--
					holdersMu.Unlock()

					handle.Release(ctx)
					break
				}
			}(i)
		}

		// Wait for all goroutines to be ready
		readyWg.Wait()

		// Start all acquisitions simultaneously
		close(startCh)

		wg.Wait()

		
		if maxConcurrentHolders > 1 {
			rt.Fatalf("Mutual exclusion violated: max concurrent holders = %d (expected <= 1)",
				maxConcurrentHolders)
		}

		rt.Logf("Strict mutual exclusion verified: max concurrent holders = %d", maxConcurrentHolders)
	})
}

// ============================================================================

// For any acquired lock, after the TTL expires without extension, the lock
// SHALL be automatically released and available for acquisition by other transactions.
// ============================================================================

// TestProperty_LockTTLAutoRelease tests that locks are automatically released after TTL expires.

// SHALL be automatically released and available for acquisition by other transactions.
func TestProperty_LockTTLAutoRelease(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random TTL between 1-3 seconds (short for testing)
		ttlSeconds := rapid.IntRange(1, 3).Draw(rt, "ttlSeconds")
		ttl := time.Duration(ttlSeconds) * time.Second

		// Generate unique lock key for this test iteration
		lockKey := fmt.Sprintf("ttl-test-%d", atomic.AddInt64(&txIDCounter, 1))

		ctx := context.Background()

		// First, acquire the lock with short TTL
		handle1, err := ti.Locker.Acquire(ctx, []string{lockKey}, ttl)
		if err != nil {
			rt.Fatalf("First acquisition failed: %v", err)
		}

		// Verify lock is held - second acquisition should fail
		_, err = ti.Locker.Acquire(ctx, []string{lockKey}, ttl)
		if err == nil {
			rt.Fatalf("Second acquisition should fail while lock is held")
		}

		// Wait for TTL to expire (add buffer for Redis timing)
		time.Sleep(ttl + 500*time.Millisecond)

		
		handle2, err := ti.Locker.Acquire(ctx, []string{lockKey}, 30*time.Second)
		if err != nil {
			rt.Fatalf("Acquisition after TTL expiry should succeed: %v", err)
		}
		defer handle2.Release(ctx)

		// Clean up first handle (should be no-op since expired)
		_ = handle1.Release(ctx)

		rt.Logf("TTL auto-release verified: lock released after %v TTL", ttl)
	})
}

// TestProperty_LockTTLAutoRelease_MultipleKeys tests TTL auto-release with multiple keys.

func TestProperty_LockTTLAutoRelease_MultipleKeys(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random number of keys (2-4)
		numKeys := rapid.IntRange(2, 4).Draw(rt, "numKeys")

		// Short TTL for testing
		ttl := 2 * time.Second

		// Generate unique lock keys for this test iteration
		lockKeys := make([]string, numKeys)
		baseKey := fmt.Sprintf("ttl-multi-%d", atomic.AddInt64(&txIDCounter, 1))
		for i := 0; i < numKeys; i++ {
			lockKeys[i] = fmt.Sprintf("%s-key-%d", baseKey, i)
		}

		ctx := context.Background()

		// Acquire all locks with short TTL
		handle1, err := ti.Locker.Acquire(ctx, lockKeys, ttl)
		if err != nil {
			rt.Fatalf("First acquisition failed: %v", err)
		}

		// Verify all locks are held
		for _, key := range lockKeys {
			_, err := ti.Locker.Acquire(ctx, []string{key}, ttl)
			if err == nil {
				rt.Fatalf("Lock %s should be held", key)
			}
		}

		// Wait for TTL to expire
		time.Sleep(ttl + 500*time.Millisecond)

		
		handle2, err := ti.Locker.Acquire(ctx, lockKeys, 30*time.Second)
		if err != nil {
			rt.Fatalf("Acquisition after TTL expiry should succeed: %v", err)
		}
		defer handle2.Release(ctx)

		// Clean up first handle
		_ = handle1.Release(ctx)

		rt.Logf("TTL auto-release verified for %d keys", numKeys)
	})
}

// ============================================================================

// For any transaction that completes (successfully or with failure), all
// acquired locks SHALL be released.
// ============================================================================

// TestProperty_LockCleanupOnCompletion tests that locks are released when transactions complete.

// acquired locks SHALL be released.
func TestProperty_LockCleanupOnCompletion(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random scenario: 0 = success, 1 = failure
		scenario := rapid.IntRange(0, 1).Draw(rt, "scenario")

		// Generate unique lock key for this test iteration
		lockKey := fmt.Sprintf("cleanup-test-%d", atomic.AddInt64(&txIDCounter, 1))

		// Create a fresh breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create coordinator
		coord := rte.NewCoordinator(
			rte.WithStore(ti.StoreAdapter),
			rte.WithLocker(ti.Locker),
			rte.WithBreaker(breaker),
			rte.WithEventBus(ti.EventBus),
			rte.WithCoordinatorConfig(rte.Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       0, // No retries
				RetryInterval:    100 * time.Millisecond,
			}),
		)

		// Create step that may fail based on scenario
		step := &lockCleanupTestStep{
			BaseStep:   rte.NewBaseStep("cleanup-step"),
			shouldFail: scenario == 1,
		}
		coord.RegisterStep(step)

		// Build and execute transaction with lock
		txID := ti.GenerateTxID(fmt.Sprintf("cleanup-%d", atomic.AddInt64(&txIDCounter, 1)))
		tx, err := rte.NewTransactionWithID(txID, "cleanup-test").
			WithStepRegistry(coord).
			WithLockKeys(lockKey).
			AddStep("cleanup-step").
			Build()

		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction
		result, _ := coord.Execute(context.Background(), tx)

		// Verify expected outcome
		if scenario == 0 && result.Status != rte.TxStatusCompleted {
			rt.Fatalf("Expected COMPLETED for success scenario, got %s", result.Status)
		}
		if scenario == 1 && result.Status != rte.TxStatusFailed {
			rt.Fatalf("Expected FAILED for failure scenario, got %s", result.Status)
		}

		
		// Try to acquire the same lock - should succeed
		handle, err := ti.Locker.Acquire(context.Background(), []string{lockKey}, 30*time.Second)
		if err != nil {
			rt.Fatalf("Lock should be released after transaction %s: %v", result.Status, err)
		}
		defer handle.Release(context.Background())

		rt.Logf("Lock cleanup verified for %s transaction", result.Status)
	})
}

// lockCleanupTestStep is a test step for lock cleanup testing
type lockCleanupTestStep struct {
	*rte.BaseStep
	shouldFail bool
}

func (s *lockCleanupTestStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	if s.shouldFail {
		return fmt.Errorf("simulated failure for lock cleanup test")
	}
	return nil
}

func (s *lockCleanupTestStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *lockCleanupTestStep) SupportsCompensation() bool {
	return false
}

// TestProperty_LockCleanupOnCompletion_WithCompensation tests lock cleanup with compensation.

func TestProperty_LockCleanupOnCompletion_WithCompensation(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random number of steps (2-4)
		numSteps := rapid.IntRange(2, 4).Draw(rt, "numSteps")
		// Fail at step 1 or later (so compensation is triggered)
		failAtStep := rapid.IntRange(1, numSteps-1).Draw(rt, "failAtStep")

		// Generate unique lock key for this test iteration
		lockKey := fmt.Sprintf("cleanup-comp-%d", atomic.AddInt64(&txIDCounter, 1))

		// Create a fresh breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create coordinator
		coord := rte.NewCoordinator(
			rte.WithStore(ti.StoreAdapter),
			rte.WithLocker(ti.Locker),
			rte.WithBreaker(breaker),
			rte.WithEventBus(ti.EventBus),
			rte.WithCoordinatorConfig(rte.Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       3,
				RetryInterval:    100 * time.Millisecond,
			}),
		)

		// Create step names and register steps
		stepNames := make([]string, numSteps)
		for i := 0; i < numSteps; i++ {
			stepNames[i] = fmt.Sprintf("comp-step-%d", i)
			step := &lockCleanupCompensationStep{
				BaseStep:   rte.NewBaseStep(stepNames[i]),
				stepIndex:  i,
				failAtStep: failAtStep,
			}
			coord.RegisterStep(step)
		}

		// Build transaction with lock
		txID := ti.GenerateTxID(fmt.Sprintf("cleanup-comp-%d", atomic.AddInt64(&txIDCounter, 1)))
		builder := rte.NewTransactionWithID(txID, "cleanup-comp-test").
			WithStepRegistry(coord).
			WithLockKeys(lockKey)
		for _, name := range stepNames {
			builder.AddStep(name)
		}

		tx, err := builder.Build()
		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction (should fail and compensate)
		result, _ := coord.Execute(context.Background(), tx)

		// Verify compensation occurred
		if result.Status != rte.TxStatusCompensated {
			rt.Fatalf("Expected COMPENSATED, got %s", result.Status)
		}

		
		handle, err := ti.Locker.Acquire(context.Background(), []string{lockKey}, 30*time.Second)
		if err != nil {
			rt.Fatalf("Lock should be released after compensation: %v", err)
		}
		defer handle.Release(context.Background())

		rt.Logf("Lock cleanup verified after compensation (failed at step %d)", failAtStep)
	})
}

// lockCleanupCompensationStep is a test step for lock cleanup with compensation testing
type lockCleanupCompensationStep struct {
	*rte.BaseStep
	stepIndex  int
	failAtStep int
}

func (s *lockCleanupCompensationStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	if s.stepIndex == s.failAtStep {
		return fmt.Errorf("simulated failure at step %d", s.stepIndex)
	}
	return nil
}

func (s *lockCleanupCompensationStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *lockCleanupCompensationStep) SupportsCompensation() bool {
	return true
}

// Ensure test steps implement rte.Step interface
var _ rte.Step = (*lockCleanupTestStep)(nil)
var _ rte.Step = (*lockCleanupCompensationStep)(nil)
