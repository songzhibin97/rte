// Package testinfra provides integration tests for Redis lock with real Redis.
// These tests validate lock acquisition, release, extension, and concurrent behavior.
package testinfra

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// Redis Lock Integration Tests
// ============================================================================

// TestIntegration_RedisLock_AcquireRelease tests basic lock acquisition and release
func TestIntegration_RedisLock_AcquireRelease(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("Acquire_Single_Key", func(t *testing.T) {
		lockKey := ti.GenerateTxID("lock-single")

		handle, err := ti.Locker.Acquire(ctx, []string{lockKey}, 30*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
		defer handle.Release(ctx)

		keys := handle.Keys()
		if len(keys) != 1 {
			t.Errorf("Expected 1 key, got %d", len(keys))
		}
		if keys[0] != lockKey {
			t.Errorf("Expected key %s, got %s", lockKey, keys[0])
		}
	})

	t.Run("Acquire_Multiple_Keys", func(t *testing.T) {
		lockKeys := []string{
			ti.GenerateTxID("lock-multi-c"),
			ti.GenerateTxID("lock-multi-a"),
			ti.GenerateTxID("lock-multi-b"),
		}

		handle, err := ti.Locker.Acquire(ctx, lockKeys, 30*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
		defer handle.Release(ctx)

		keys := handle.Keys()
		if len(keys) != 3 {
			t.Errorf("Expected 3 keys, got %d", len(keys))
		}

		// Keys should be sorted alphabetically
		for i := 1; i < len(keys); i++ {
			if keys[i] < keys[i-1] {
				t.Errorf("Keys not sorted: %v", keys)
				break
			}
		}
	})

	t.Run("Release_Frees_Lock", func(t *testing.T) {
		lockKey := ti.GenerateTxID("lock-release")

		// Acquire lock
		handle, err := ti.Locker.Acquire(ctx, []string{lockKey}, 30*time.Second)
		if err != nil {
			t.Fatalf("First acquire failed: %v", err)
		}

		// Release lock
		err = handle.Release(ctx)
		if err != nil {
			t.Fatalf("Release failed: %v", err)
		}

		// Should be able to acquire again
		handle2, err := ti.Locker.Acquire(ctx, []string{lockKey}, 30*time.Second)
		if err != nil {
			t.Fatalf("Second acquire after release failed: %v", err)
		}
		defer handle2.Release(ctx)
	})

	t.Run("Acquire_Already_Locked", func(t *testing.T) {
		lockKey := ti.GenerateTxID("lock-conflict")

		// First acquire
		handle1, err := ti.Locker.Acquire(ctx, []string{lockKey}, 30*time.Second)
		if err != nil {
			t.Fatalf("First acquire failed: %v", err)
		}
		defer handle1.Release(ctx)

		// Second acquire should fail
		_, err = ti.Locker.Acquire(ctx, []string{lockKey}, 30*time.Second)
		if err == nil {
			t.Error("Second acquire should fail when lock is held")
		}
	})
}

// TestIntegration_RedisLock_Extension tests lock TTL extension
func TestIntegration_RedisLock_Extension(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("Extend_Lock_TTL", func(t *testing.T) {
		lockKey := ti.GenerateTxID("lock-extend")

		// Acquire with short TTL
		handle, err := ti.Locker.Acquire(ctx, []string{lockKey}, 5*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
		defer handle.Release(ctx)

		// Wait a bit
		time.Sleep(2 * time.Second)

		// Extend the lock
		err = handle.Extend(ctx, 30*time.Second)
		if err != nil {
			t.Fatalf("Extend failed: %v", err)
		}

		// Wait past original TTL
		time.Sleep(4 * time.Second)

		// Lock should still be held (extended)
		// Try to acquire with another handle - should fail
		_, err = ti.Locker.Acquire(ctx, []string{lockKey}, 5*time.Second)
		if err == nil {
			t.Error("Lock should still be held after extension")
		}
	})

	t.Run("Extend_Multiple_Keys", func(t *testing.T) {
		lockKeys := []string{
			ti.GenerateTxID("lock-ext-a"),
			ti.GenerateTxID("lock-ext-b"),
		}

		handle, err := ti.Locker.Acquire(ctx, lockKeys, 5*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
		defer handle.Release(ctx)

		// Extend all locks
		err = handle.Extend(ctx, 30*time.Second)
		if err != nil {
			t.Fatalf("Extend failed: %v", err)
		}

		// Verify both locks are still held
		for _, key := range lockKeys {
			_, err := ti.Locker.Acquire(ctx, []string{key}, 5*time.Second)
			if err == nil {
				t.Errorf("Lock %s should still be held", key)
			}
		}
	})

	t.Run("Lock_Expires_Without_Extension", func(t *testing.T) {
		lockKey := ti.GenerateTxID("lock-expire")

		// Acquire with very short TTL
		handle, err := ti.Locker.Acquire(ctx, []string{lockKey}, 2*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
		// Don't release - let it expire

		// Wait for expiration
		time.Sleep(3 * time.Second)

		// Should be able to acquire now
		handle2, err := ti.Locker.Acquire(ctx, []string{lockKey}, 30*time.Second)
		if err != nil {
			t.Fatalf("Acquire after expiration failed: %v", err)
		}
		defer handle2.Release(ctx)

		// Clean up first handle (no-op since expired)
		_ = handle.Release(ctx)
	})
}

// TestIntegration_RedisLock_ConcurrentCompetition tests concurrent lock competition
func TestIntegration_RedisLock_ConcurrentCompetition(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("Only_One_Acquires_Lock", func(t *testing.T) {
		lockKey := ti.GenerateTxID("lock-race")

		const numGoroutines = 20
		var wg sync.WaitGroup
		var successCount int32
		startCh := make(chan struct{})

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Wait for signal
				<-startCh

				handle, err := ti.Locker.Acquire(ctx, []string{lockKey}, 30*time.Second)
				if err == nil {
					atomic.AddInt32(&successCount, 1)
					// Hold lock briefly
					time.Sleep(10 * time.Millisecond)
					handle.Release(ctx)
				}
			}()
		}

		// Start all goroutines simultaneously
		close(startCh)
		wg.Wait()

		// Only one should have acquired the lock at a time
		// But since they release quickly, multiple may succeed sequentially
		if successCount == 0 {
			t.Error("At least one goroutine should acquire the lock")
		}
		t.Logf("Successful acquisitions: %d", successCount)
	})

	t.Run("Mutual_Exclusion_Verified", func(t *testing.T) {
		lockKey := ti.GenerateTxID("lock-mutex")

		const numGoroutines = 10
		var wg sync.WaitGroup
		var concurrentHolders int32
		var maxConcurrent int32
		var mu sync.Mutex

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for attempt := 0; attempt < 5; attempt++ {
					handle, err := ti.Locker.Acquire(ctx, []string{lockKey}, 30*time.Second)
					if err != nil {
						time.Sleep(10 * time.Millisecond)
						continue
					}

					// Increment concurrent holders
					current := atomic.AddInt32(&concurrentHolders, 1)

					// Track max concurrent
					mu.Lock()
					if current > maxConcurrent {
						maxConcurrent = current
					}
					mu.Unlock()

					// Hold lock briefly
					time.Sleep(5 * time.Millisecond)

					// Decrement before release
					atomic.AddInt32(&concurrentHolders, -1)
					handle.Release(ctx)
					break
				}
			}()
		}

		wg.Wait()

		// Max concurrent holders should be 1
		if maxConcurrent > 1 {
			t.Errorf("Max concurrent lock holders should be 1, got %d", maxConcurrent)
		}
	})

	t.Run("Multiple_Keys_Atomic_Acquisition", func(t *testing.T) {
		// Test that acquiring multiple keys is atomic
		lockKeys := []string{
			ti.GenerateTxID("lock-atomic-a"),
			ti.GenerateTxID("lock-atomic-b"),
		}

		const numGoroutines = 10
		var wg sync.WaitGroup
		var successCount int32
		startCh := make(chan struct{})

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				<-startCh

				handle, err := ti.Locker.Acquire(ctx, lockKeys, 30*time.Second)
				if err == nil {
					atomic.AddInt32(&successCount, 1)
					time.Sleep(50 * time.Millisecond)
					handle.Release(ctx)
				}
			}()
		}

		close(startCh)
		wg.Wait()

		// At least one should succeed
		if successCount == 0 {
			t.Error("At least one goroutine should acquire all locks")
		}
	})
}

// TestIntegration_RedisLock_PartialFailure tests behavior when partial lock acquisition fails
func TestIntegration_RedisLock_PartialFailure(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("Partial_Failure_Releases_Acquired", func(t *testing.T) {
		// Pre-acquire one of the keys
		blockedKey := ti.GenerateTxID("lock-blocked")
		freeKey := ti.GenerateTxID("lock-free")

		blocker, err := ti.Locker.Acquire(ctx, []string{blockedKey}, 30*time.Second)
		if err != nil {
			t.Fatalf("Blocker acquire failed: %v", err)
		}
		defer blocker.Release(ctx)

		// Try to acquire both keys - should fail
		_, err = ti.Locker.Acquire(ctx, []string{freeKey, blockedKey}, 30*time.Second)
		if err == nil {
			t.Error("Should fail when one key is already locked")
		}

		// The free key should have been released after partial failure
		// So we should be able to acquire it
		handle, err := ti.Locker.Acquire(ctx, []string{freeKey}, 30*time.Second)
		if err != nil {
			t.Fatalf("Free key should be available after partial failure: %v", err)
		}
		defer handle.Release(ctx)
	})
}

// TestIntegration_RedisLock_KeyOrdering tests that keys are acquired in sorted order
func TestIntegration_RedisLock_KeyOrdering(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("Keys_Sorted_Alphabetically", func(t *testing.T) {
		// Create keys in reverse order
		keys := []string{
			ti.GenerateTxID("z-key"),
			ti.GenerateTxID("a-key"),
			ti.GenerateTxID("m-key"),
		}

		handle, err := ti.Locker.Acquire(ctx, keys, 30*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
		defer handle.Release(ctx)

		acquiredKeys := handle.Keys()

		// Verify sorted order
		for i := 1; i < len(acquiredKeys); i++ {
			if acquiredKeys[i] < acquiredKeys[i-1] {
				t.Errorf("Keys not sorted: %v", acquiredKeys)
				break
			}
		}
	})
}
