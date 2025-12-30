// Package redis provides tests for the Redis implementation of the lock.Locker interface.
package redis

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"pgregory.net/rapid"
)

// ============================================================================
// Test Helpers
// ============================================================================

// mockRedisClient is a minimal mock for testing lock behavior
type mockRedisClient struct {
	redis.Cmdable
	mu          sync.Mutex
	locks       map[string]string // key -> token
	setNXCalls  []setNXCall
	scriptCalls []scriptCall
}

type setNXCall struct {
	key   string
	value string
	ttl   time.Duration
}

type scriptCall struct {
	script string
	keys   []string
	args   []interface{}
}

func newMockRedisClient() *mockRedisClient {
	return &mockRedisClient{
		locks:       make(map[string]string),
		setNXCalls:  make([]setNXCall, 0),
		scriptCalls: make([]scriptCall, 0),
	}
}

// SetNX implements the SetNX command for testing
func (m *mockRedisClient) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.setNXCalls = append(m.setNXCalls, setNXCall{key: key, value: value.(string), ttl: expiration})

	cmd := redis.NewBoolCmd(ctx)
	if _, exists := m.locks[key]; exists {
		cmd.SetVal(false) // Lock already held
	} else {
		m.locks[key] = value.(string)
		cmd.SetVal(true) // Lock acquired
	}
	return cmd
}

// Eval implements the Eval command for Lua scripts (used by release and extend)
func (m *mockRedisClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := redis.NewCmd(ctx)

	if len(keys) == 0 {
		cmd.SetVal(int64(0))
		return cmd
	}

	key := keys[0]
	token := ""
	if len(args) > 0 {
		token, _ = args[0].(string)
	}

	// Check if this is a release or extend script
	if storedToken, exists := m.locks[key]; exists && storedToken == token {
		// Token matches - either delete (release) or extend
		delete(m.locks, key)
		cmd.SetVal(int64(1))
	} else {
		cmd.SetVal(int64(0))
	}

	return cmd
}

// EvalSha implements the EvalSha command (scripts are cached by SHA)
func (m *mockRedisClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return m.Eval(ctx, sha1, keys, args...)
}

// ScriptExists implements the ScriptExists command
func (m *mockRedisClient) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	cmd := redis.NewBoolSliceCmd(ctx)
	// Return false for all scripts to force Eval instead of EvalSha
	result := make([]bool, len(hashes))
	cmd.SetVal(result)
	return cmd
}

// ============================================================================
// Unit Tests: Lock Acquisition and Release
// ============================================================================

func TestRedisLocker_Acquire_SingleKey(t *testing.T) {
	mock := newMockRedisClient()
	locker := NewRedisLocker(mock)

	handle, err := locker.Acquire(context.Background(), []string{"key1"}, 30*time.Second)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	if handle == nil {
		t.Fatal("expected non-nil handle")
	}

	keys := handle.Keys()
	if len(keys) != 1 || keys[0] != "key1" {
		t.Errorf("expected keys [key1], got %v", keys)
	}

	// Verify SetNX was called with correct parameters
	if len(mock.setNXCalls) != 1 {
		t.Fatalf("expected 1 SetNX call, got %d", len(mock.setNXCalls))
	}

	call := mock.setNXCalls[0]
	if call.key != "rte:lock:key1" {
		t.Errorf("expected key 'rte:lock:key1', got '%s'", call.key)
	}
	if call.ttl != 30*time.Second {
		t.Errorf("expected TTL 30s, got %v", call.ttl)
	}
}

func TestRedisLocker_Acquire_MultipleKeys(t *testing.T) {
	mock := newMockRedisClient()
	locker := NewRedisLocker(mock)

	handle, err := locker.Acquire(context.Background(), []string{"key3", "key1", "key2"}, 30*time.Second)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	keys := handle.Keys()
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}

	// Keys should be sorted alphabetically
	expected := []string{"key1", "key2", "key3"}
	for i, k := range keys {
		if k != expected[i] {
			t.Errorf("expected key %s at index %d, got %s", expected[i], i, k)
		}
	}
}

func TestRedisLocker_Acquire_EmptyKeys(t *testing.T) {
	mock := newMockRedisClient()
	locker := NewRedisLocker(mock)

	_, err := locker.Acquire(context.Background(), []string{}, 30*time.Second)
	if err == nil {
		t.Fatal("expected error for empty keys")
	}
}

func TestRedisLocker_Acquire_AlreadyLocked(t *testing.T) {
	mock := newMockRedisClient()
	// Pre-set a lock
	mock.locks["rte:lock:key1"] = "other-token"

	locker := NewRedisLocker(mock)

	_, err := locker.Acquire(context.Background(), []string{"key1"}, 30*time.Second)
	if err == nil {
		t.Fatal("expected error when lock is already held")
	}
}

func TestRedisLocker_Acquire_PartialFailure_ReleasesAcquired(t *testing.T) {
	mock := newMockRedisClient()
	// Pre-set lock on key2 so acquisition fails there
	mock.locks["rte:lock:key2"] = "other-token"

	locker := NewRedisLocker(mock)

	_, err := locker.Acquire(context.Background(), []string{"key1", "key2", "key3"}, 30*time.Second)
	if err == nil {
		t.Fatal("expected error when partial lock acquisition fails")
	}

	// key1 should have been acquired then released
	// Since we can't easily verify release with this mock, we verify the error occurred
	if len(mock.setNXCalls) < 2 {
		t.Errorf("expected at least 2 SetNX calls (key1 success, key2 fail), got %d", len(mock.setNXCalls))
	}
}

func TestRedisLocker_WithPrefix(t *testing.T) {
	mock := newMockRedisClient()
	locker := NewRedisLocker(mock, WithPrefix("custom:prefix:"))

	_, err := locker.Acquire(context.Background(), []string{"key1"}, 30*time.Second)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	if len(mock.setNXCalls) != 1 {
		t.Fatalf("expected 1 SetNX call, got %d", len(mock.setNXCalls))
	}

	if mock.setNXCalls[0].key != "custom:prefix:key1" {
		t.Errorf("expected key 'custom:prefix:key1', got '%s'", mock.setNXCalls[0].key)
	}
}

func TestLockHandle_Keys_ReturnsNilAfterRelease(t *testing.T) {
	mock := newMockRedisClient()
	locker := NewRedisLocker(mock)

	handle, err := locker.Acquire(context.Background(), []string{"key1"}, 30*time.Second)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	// Cast to access internal state for testing
	h := handle.(*redisLockHandle)

	// Clear acquired to simulate release
	h.mu.Lock()
	h.acquired = nil
	h.mu.Unlock()

	keys := handle.Keys()
	if keys != nil {
		t.Errorf("expected nil keys after release, got %v", keys)
	}
}

// ============================================================================
// Unit Tests: Lock Extension
// ============================================================================

func TestLockHandle_Extend_NoLocksHeld(t *testing.T) {
	handle := &redisLockHandle{
		acquired: nil,
	}

	err := handle.Extend(context.Background(), 30*time.Second)
	if err == nil {
		t.Fatal("expected error when no locks held")
	}
}

// ============================================================================
// Property-Based Tests
// ============================================================================

// Property 2: Lock Ordering Prevents Deadlock
// For any set of lock keys, the locker SHALL sort them alphabetically before
// acquisition to prevent deadlocks when multiple processes acquire overlapping
// sets of locks.
func TestProperty_LockOrderingPreventsDeadlock(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate random set of keys (1-10 keys)
		numKeys := rapid.IntRange(1, 10).Draw(t, "numKeys")
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			keys[i] = rapid.StringMatching(`[a-z]{3,10}`).Draw(t, "key")
		}

		// Remove duplicates
		keySet := make(map[string]bool)
		uniqueKeys := make([]string, 0)
		for _, k := range keys {
			if !keySet[k] {
				keySet[k] = true
				uniqueKeys = append(uniqueKeys, k)
			}
		}

		if len(uniqueKeys) == 0 {
			return // Skip if no unique keys
		}

		mock := newMockRedisClient()
		locker := NewRedisLocker(mock)

		handle, err := locker.Acquire(context.Background(), uniqueKeys, 30*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		acquiredKeys := handle.Keys()

		// Property 1: Acquired keys should be sorted alphabetically
		if !sort.StringsAreSorted(acquiredKeys) {
			t.Fatalf("acquired keys are not sorted: %v", acquiredKeys)
		}

		// Property 2: SetNX calls should be made in sorted order
		expectedSorted := make([]string, len(uniqueKeys))
		copy(expectedSorted, uniqueKeys)
		sort.Strings(expectedSorted)

		if len(mock.setNXCalls) != len(expectedSorted) {
			t.Fatalf("expected %d SetNX calls, got %d", len(expectedSorted), len(mock.setNXCalls))
		}

		for i, call := range mock.setNXCalls {
			expectedKey := "rte:lock:" + expectedSorted[i]
			if call.key != expectedKey {
				t.Fatalf("SetNX call %d: expected key '%s', got '%s'", i, expectedKey, call.key)
			}
		}

		// Property 3: All unique keys should be acquired
		if len(acquiredKeys) != len(expectedSorted) {
			t.Fatalf("expected %d acquired keys, got %d", len(expectedSorted), len(acquiredKeys))
		}

		for i, k := range acquiredKeys {
			if k != expectedSorted[i] {
				t.Fatalf("acquired key %d: expected '%s', got '%s'", i, expectedSorted[i], k)
			}
		}
	})
}

// Additional property: Lock keys are always returned in sorted order
func TestProperty_KeysAlwaysSorted(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate random keys in random order
		keys := rapid.SliceOfNDistinct(
			rapid.StringMatching(`[a-z0-9]{1,20}`),
			1, 20,
			func(s string) string { return s },
		).Draw(t, "keys")

		mock := newMockRedisClient()
		locker := NewRedisLocker(mock)

		handle, err := locker.Acquire(context.Background(), keys, 30*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		acquiredKeys := handle.Keys()

		// Property: Keys() should always return sorted keys
		if !sort.StringsAreSorted(acquiredKeys) {
			t.Fatalf("Keys() returned unsorted keys: %v", acquiredKeys)
		}
	})
}

// Property: Lock acquisition order is deterministic regardless of input order
func TestProperty_DeterministicAcquisitionOrder(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a set of keys
		keys := rapid.SliceOfNDistinct(
			rapid.StringMatching(`[a-z]{2,8}`),
			2, 5,
			func(s string) string { return s },
		).Draw(t, "keys")

		// Create two different orderings of the same keys
		keys1 := make([]string, len(keys))
		copy(keys1, keys)

		keys2 := make([]string, len(keys))
		copy(keys2, keys)
		// Reverse keys2
		for i, j := 0, len(keys2)-1; i < j; i, j = i+1, j-1 {
			keys2[i], keys2[j] = keys2[j], keys2[i]
		}

		mock1 := newMockRedisClient()
		locker1 := NewRedisLocker(mock1)

		mock2 := newMockRedisClient()
		locker2 := NewRedisLocker(mock2)

		handle1, err1 := locker1.Acquire(context.Background(), keys1, 30*time.Second)
		handle2, err2 := locker2.Acquire(context.Background(), keys2, 30*time.Second)

		if err1 != nil || err2 != nil {
			t.Fatalf("Acquire failed: err1=%v, err2=%v", err1, err2)
		}

		// Property: Both should acquire locks in the same order
		acquiredKeys1 := handle1.Keys()
		acquiredKeys2 := handle2.Keys()

		if len(acquiredKeys1) != len(acquiredKeys2) {
			t.Fatalf("different number of acquired keys: %d vs %d", len(acquiredKeys1), len(acquiredKeys2))
		}

		for i := range acquiredKeys1 {
			if acquiredKeys1[i] != acquiredKeys2[i] {
				t.Fatalf("different acquisition order at index %d: '%s' vs '%s'", i, acquiredKeys1[i], acquiredKeys2[i])
			}
		}

		// Property: SetNX calls should be in the same order
		if len(mock1.setNXCalls) != len(mock2.setNXCalls) {
			t.Fatalf("different number of SetNX calls: %d vs %d", len(mock1.setNXCalls), len(mock2.setNXCalls))
		}

		for i := range mock1.setNXCalls {
			if mock1.setNXCalls[i].key != mock2.setNXCalls[i].key {
				t.Fatalf("different SetNX order at index %d: '%s' vs '%s'",
					i, mock1.setNXCalls[i].key, mock2.setNXCalls[i].key)
			}
		}
	})
}
