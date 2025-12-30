// Package store provides tests for the store-based idempotency checker implementation.
package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// ============================================================================
// Mock Store for Testing
// ============================================================================

// mockIdempotencyStore is an in-memory implementation of IdempotencyStore for testing.
type mockIdempotencyStore struct {
	mu      sync.RWMutex
	records map[string]idempotencyRecord
}

type idempotencyRecord struct {
	result    []byte
	expiresAt time.Time
}

func newMockIdempotencyStore() *mockIdempotencyStore {
	return &mockIdempotencyStore{
		records: make(map[string]idempotencyRecord),
	}
}

func (m *mockIdempotencyStore) CheckIdempotency(ctx context.Context, key string) (bool, []byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	record, exists := m.records[key]
	if !exists {
		return false, nil, nil
	}

	// Check if expired
	if time.Now().After(record.expiresAt) {
		return false, nil, nil
	}

	return true, record.result, nil
}

func (m *mockIdempotencyStore) MarkIdempotency(ctx context.Context, key string, result []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.records[key] = idempotencyRecord{
		result:    result,
		expiresAt: time.Now().Add(ttl),
	}
	return nil
}

// ============================================================================
// Unit Tests
// ============================================================================

func TestStoreChecker_CheckNotExists(t *testing.T) {
	store := newMockIdempotencyStore()
	checker := New(store)

	exists, result, err := checker.Check(context.Background(), "non-existent-key")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if exists {
		t.Error("expected exists=false for non-existent key")
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestStoreChecker_MarkAndCheck(t *testing.T) {
	store := newMockIdempotencyStore()
	checker := New(store)

	key := "test-key"
	expectedResult := []byte(`{"status":"success"}`)

	// Mark the operation
	err := checker.Mark(context.Background(), key, expectedResult, time.Hour)
	if err != nil {
		t.Errorf("expected no error on mark, got %v", err)
	}

	// Check should return exists=true with the result
	exists, result, err := checker.Check(context.Background(), key)
	if err != nil {
		t.Errorf("expected no error on check, got %v", err)
	}
	if !exists {
		t.Error("expected exists=true after marking")
	}
	if string(result) != string(expectedResult) {
		t.Errorf("expected result %s, got %s", expectedResult, result)
	}
}

func TestStoreChecker_ExpiredRecord(t *testing.T) {
	store := newMockIdempotencyStore()
	checker := New(store)

	key := "expiring-key"
	result := []byte(`{"status":"success"}`)

	// Mark with very short TTL
	err := checker.Mark(context.Background(), key, result, 1*time.Millisecond)
	if err != nil {
		t.Errorf("expected no error on mark, got %v", err)
	}

	// Wait for expiration
	time.Sleep(5 * time.Millisecond)

	// Check should return exists=false after expiration
	exists, _, err := checker.Check(context.Background(), key)
	if err != nil {
		t.Errorf("expected no error on check, got %v", err)
	}
	if exists {
		t.Error("expected exists=false after expiration")
	}
}

// ============================================================================
// Property-Based Tests
// ============================================================================

// Property 4: Idempotency Guarantee
// For any step with idempotency support, executing the same step multiple times
// with the same idempotency key SHALL produce the same result.
func TestProperty_IdempotencyGuarantee(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockIdempotencyStore()
		checker := New(store)
		ctx := context.Background()

		// Generate random idempotency key (simulating unique tx+step combination per Req 6.5)
		txID := rapid.StringMatching(`tx-[a-z0-9]{8}`).Draw(t, "txID")
		stepIndex := rapid.IntRange(0, 10).Draw(t, "stepIndex")
		key := txID + ":" + rapid.StringMatching(`step-[a-z]{4}`).Draw(t, "stepName") + ":" + string(rune('0'+stepIndex))

		// Generate random result data
		resultData := rapid.SliceOfN(rapid.Byte(), 1, 100).Draw(t, "resultData")
		ttlSeconds := rapid.IntRange(1, 3600).Draw(t, "ttlSeconds")
		ttl := time.Duration(ttlSeconds) * time.Second

		// First check should return not exists
		exists, _, err := checker.Check(ctx, key)
		if err != nil {
			t.Fatalf("first check failed: %v", err)
		}
		if exists {
			t.Fatal("first check should return exists=false")
		}

		// Mark the operation as executed
		err = checker.Mark(ctx, key, resultData, ttl)
		if err != nil {
			t.Fatalf("mark failed: %v", err)
		}

		// Property: Multiple checks with the same key should return the same result
		numChecks := rapid.IntRange(2, 10).Draw(t, "numChecks")
		for i := 0; i < numChecks; i++ {
			exists, result, err := checker.Check(ctx, key)
			if err != nil {
				t.Fatalf("check %d failed: %v", i, err)
			}
			if !exists {
				t.Fatalf("check %d: expected exists=true", i)
			}
			if string(result) != string(resultData) {
				t.Fatalf("check %d: expected result %v, got %v", i, resultData, result)
			}
		}
	})
}

// Property: Idempotency keys are unique per transaction and step combination
// Different keys should have independent idempotency records
func TestProperty_IdempotencyKeyUniqueness(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockIdempotencyStore()
		checker := New(store)
		ctx := context.Background()

		// Generate two different keys
		key1 := rapid.StringMatching(`tx-[a-z0-9]{8}:step-[a-z]{4}:[0-9]`).Draw(t, "key1")
		key2 := rapid.StringMatching(`tx-[a-z0-9]{8}:step-[a-z]{4}:[0-9]`).Draw(t, "key2")

		// Ensure keys are different
		if key1 == key2 {
			t.Skip("generated identical keys, skipping")
		}

		result1 := rapid.SliceOfN(rapid.Byte(), 1, 50).Draw(t, "result1")
		result2 := rapid.SliceOfN(rapid.Byte(), 1, 50).Draw(t, "result2")
		ttl := time.Hour

		// Mark both keys with different results
		err := checker.Mark(ctx, key1, result1, ttl)
		if err != nil {
			t.Fatalf("mark key1 failed: %v", err)
		}
		err = checker.Mark(ctx, key2, result2, ttl)
		if err != nil {
			t.Fatalf("mark key2 failed: %v", err)
		}

		// Property: Each key should return its own result
		exists1, gotResult1, err := checker.Check(ctx, key1)
		if err != nil {
			t.Fatalf("check key1 failed: %v", err)
		}
		if !exists1 {
			t.Fatal("key1 should exist")
		}
		if string(gotResult1) != string(result1) {
			t.Fatalf("key1: expected result %v, got %v", result1, gotResult1)
		}

		exists2, gotResult2, err := checker.Check(ctx, key2)
		if err != nil {
			t.Fatalf("check key2 failed: %v", err)
		}
		if !exists2 {
			t.Fatal("key2 should exist")
		}
		if string(gotResult2) != string(result2) {
			t.Fatalf("key2: expected result %v, got %v", result2, gotResult2)
		}
	})
}

// Property: Mark is idempotent - marking the same key multiple times
// should preserve the first result (or update it, depending on implementation)
// This tests that the system handles duplicate marks gracefully
func TestProperty_MarkIdempotence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockIdempotencyStore()
		checker := New(store)
		ctx := context.Background()

		key := rapid.StringMatching(`tx-[a-z0-9]{8}:step-[a-z]{4}:[0-9]`).Draw(t, "key")
		result1 := rapid.SliceOfN(rapid.Byte(), 1, 50).Draw(t, "result1")
		result2 := rapid.SliceOfN(rapid.Byte(), 1, 50).Draw(t, "result2")
		ttl := time.Hour

		// Mark the key first time
		err := checker.Mark(ctx, key, result1, ttl)
		if err != nil {
			t.Fatalf("first mark failed: %v", err)
		}

		// Mark the same key again (simulating retry scenario)
		err = checker.Mark(ctx, key, result2, ttl)
		if err != nil {
			t.Fatalf("second mark failed: %v", err)
		}

		// Check should return a valid result (either first or second, depending on implementation)
		exists, result, err := checker.Check(ctx, key)
		if err != nil {
			t.Fatalf("check failed: %v", err)
		}
		if !exists {
			t.Fatal("key should exist after marking")
		}

		// Property: Result should be one of the marked values
		isResult1 := string(result) == string(result1)
		isResult2 := string(result) == string(result2)
		if !isResult1 && !isResult2 {
			t.Fatalf("result should be either result1 or result2, got %v", result)
		}
	})
}

// Property: Check-then-Mark pattern guarantees idempotency
// This simulates the actual usage pattern in the coordinator
func TestProperty_CheckThenMarkPattern(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockIdempotencyStore()
		checker := New(store)
		ctx := context.Background()

		key := rapid.StringMatching(`tx-[a-z0-9]{8}:step-[a-z]{4}:[0-9]`).Draw(t, "key")
		ttl := time.Hour

		// Simulate step execution counter
		executionCount := 0

		// Simulate the coordinator's idempotency pattern multiple times
		numAttempts := rapid.IntRange(2, 5).Draw(t, "numAttempts")
		var firstResult []byte

		for attempt := 0; attempt < numAttempts; attempt++ {
			// Check if already executed
			exists, cachedResult, err := checker.Check(ctx, key)
			if err != nil {
				t.Fatalf("attempt %d: check failed: %v", attempt, err)
			}

			if exists {
				// Use cached result (Req 6.4)
				if firstResult == nil {
					t.Fatalf("attempt %d: got cached result but firstResult is nil", attempt)
				}
				if string(cachedResult) != string(firstResult) {
					t.Fatalf("attempt %d: cached result mismatch, expected %v, got %v",
						attempt, firstResult, cachedResult)
				}
			} else {
				// Execute the step (only happens once)
				executionCount++
				result := rapid.SliceOfN(rapid.Byte(), 1, 50).Draw(t, "stepResult")
				firstResult = result

				// Mark as executed
				err = checker.Mark(ctx, key, result, ttl)
				if err != nil {
					t.Fatalf("attempt %d: mark failed: %v", attempt, err)
				}
			}
		}

		// Property: Step should only be executed once
		if executionCount != 1 {
			t.Fatalf("step should be executed exactly once, got %d executions", executionCount)
		}
	})
}
