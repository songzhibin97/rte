package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"rte/lock"

	"github.com/redis/go-redis/v9"
)

// Ensure RedisLocker implements lock.Locker
var _ lock.Locker = (*RedisLocker)(nil)

// Ensure redisLockHandle implements lock.LockHandle
var _ lock.LockHandle = (*redisLockHandle)(nil)

// RedisLocker implements distributed locking using Redis
type RedisLocker struct {
	client redis.Cmdable
	prefix string
}

// Option is a functional option for configuring RedisLocker
type Option func(*RedisLocker)

// WithPrefix sets the key prefix for locks
func WithPrefix(prefix string) Option {
	return func(l *RedisLocker) {
		l.prefix = prefix
	}
}

// NewRedisLocker creates a new Redis-based distributed locker
func NewRedisLocker(client redis.Cmdable, opts ...Option) *RedisLocker {
	l := &RedisLocker{
		client: client,
		prefix: "rte:lock:",
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Acquire acquires locks on the given keys
// Keys are sorted alphabetically before acquisition to prevent deadlocks.
// Each lock has a TTL to prevent indefinite holding.
func (l *RedisLocker) Acquire(ctx context.Context, keys []string, ttl time.Duration) (lock.LockHandle, error) {
	if len(keys) == 0 {
		return nil, errors.New("no keys provided")
	}

	// Sort keys alphabetically to prevent deadlocks
	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	sort.Strings(sortedKeys)

	// Generate a unique token for this lock holder
	token, err := generateToken()
	if err != nil {
		return nil, fmt.Errorf("failed to generate lock token: %w", err)
	}

	handle := &redisLockHandle{
		client:   l.client,
		prefix:   l.prefix,
		keys:     sortedKeys,
		token:    token,
		acquired: make([]string, 0, len(sortedKeys)),
	}

	// Acquire locks in sorted order
	for _, key := range sortedKeys {
		lockKey := l.prefix + key
		ok, err := l.tryAcquire(ctx, lockKey, token, ttl)
		if err != nil {
			// Release any acquired locks on error
			handle.Release(ctx)
			return nil, fmt.Errorf("lock acquisition failed for key %s: %w", key, err)
		}
		if !ok {
			// Lock is held by another process, release acquired locks
			handle.Release(ctx)
			return nil, fmt.Errorf("lock acquisition failed for key %s: lock is held by another process", key)
		}
		handle.acquired = append(handle.acquired, key)
	}

	return handle, nil
}

// tryAcquire attempts to acquire a single lock using SET NX with expiration
func (l *RedisLocker) tryAcquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	// Use SET NX EX for atomic lock acquisition
	result, err := l.client.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return false, err
	}
	return result, nil
}

// redisLockHandle represents a handle to acquired Redis locks
type redisLockHandle struct {
	client   redis.Cmdable
	prefix   string
	keys     []string // Original sorted keys
	token    string   // Unique token for this lock holder
	acquired []string // Keys that were successfully acquired
	mu       sync.Mutex
}

// Extend extends the TTL of all held locks
func (h *redisLockHandle) Extend(ctx context.Context, ttl time.Duration) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.acquired) == 0 {
		return errors.New("no locks held")
	}

	var extendErr error
	for _, key := range h.acquired {
		lockKey := h.prefix + key
		err := h.extendSingle(ctx, lockKey, ttl)
		if err != nil {
			extendErr = errors.Join(extendErr, fmt.Errorf("failed to extend lock %s: %w", key, err))
		}
	}

	return extendErr
}

// extendSingle extends a single lock using a Lua script to ensure atomicity
func (h *redisLockHandle) extendSingle(ctx context.Context, key string, ttl time.Duration) error {
	// Lua script to extend lock only if we still hold it
	script := redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)

	result, err := script.Run(ctx, h.client, []string{key}, h.token, ttl.Milliseconds()).Int()
	if err != nil {
		return err
	}
	if result == 0 {
		return errors.New("lock not held or expired")
	}
	return nil
}

// Release releases all held locks
// Attempts to release all locks even if some releases fail
func (h *redisLockHandle) Release(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.acquired) == 0 {
		return nil
	}

	var releaseErr error
	// Release in reverse order (though order doesn't matter for release)
	for i := len(h.acquired) - 1; i >= 0; i-- {
		key := h.acquired[i]
		lockKey := h.prefix + key
		err := h.releaseSingle(ctx, lockKey)
		if err != nil {
			releaseErr = errors.Join(releaseErr, fmt.Errorf("failed to release lock %s: %w", key, err))
		}
	}

	// Clear acquired locks regardless of errors
	h.acquired = nil

	return releaseErr
}

// releaseSingle releases a single lock using a Lua script to ensure we only release our own lock
func (h *redisLockHandle) releaseSingle(ctx context.Context, key string) error {
	// Lua script to release lock only if we hold it
	script := redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`)

	_, err := script.Run(ctx, h.client, []string{key}, h.token).Result()
	return err
}

// Keys returns the keys that are locked
func (h *redisLockHandle) Keys() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.acquired == nil {
		return nil
	}

	keys := make([]string, len(h.acquired))
	copy(keys, h.acquired)
	return keys
}

// generateToken generates a unique token for lock ownership
func generateToken() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
