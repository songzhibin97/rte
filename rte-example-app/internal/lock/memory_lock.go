// Package lock 提供分布式锁实现
package lock

import (
	"context"
	"sync"
	"time"

	"rte"
	"rte/lock"
)

// MemoryLocker 内存锁实现
// 用于演示和测试，生产环境应使用Redis等分布式锁
type MemoryLocker struct {
	mu    sync.Mutex
	locks map[string]*lockEntry
}

type lockEntry struct {
	holder    string
	expiresAt time.Time
}

// NewMemoryLocker 创建内存锁
func NewMemoryLocker() *MemoryLocker {
	return &MemoryLocker{
		locks: make(map[string]*lockEntry),
	}
}

// Acquire 获取锁
func (l *MemoryLocker) Acquire(ctx context.Context, keys []string, ttl time.Duration) (lock.LockHandle, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	holder := generateHolder()

	// 检查所有键是否可用
	for _, key := range keys {
		if entry, exists := l.locks[key]; exists {
			if now.Before(entry.expiresAt) {
				return nil, rte.ErrLockAcquisitionFailed
			}
		}
	}

	// 获取所有锁
	expiresAt := now.Add(ttl)
	for _, key := range keys {
		l.locks[key] = &lockEntry{
			holder:    holder,
			expiresAt: expiresAt,
		}
	}

	return &memoryLockHandle{
		locker: l,
		keys:   keys,
		holder: holder,
	}, nil
}

type memoryLockHandle struct {
	locker *MemoryLocker
	keys   []string
	holder string
}

// Extend 延长锁TTL
func (h *memoryLockHandle) Extend(ctx context.Context, ttl time.Duration) error {
	h.locker.mu.Lock()
	defer h.locker.mu.Unlock()

	expiresAt := time.Now().Add(ttl)
	for _, key := range h.keys {
		if entry, exists := h.locker.locks[key]; exists && entry.holder == h.holder {
			entry.expiresAt = expiresAt
		} else {
			return rte.ErrLockNotHeld
		}
	}
	return nil
}

// Release 释放锁
func (h *memoryLockHandle) Release(ctx context.Context) error {
	h.locker.mu.Lock()
	defer h.locker.mu.Unlock()

	for _, key := range h.keys {
		if entry, exists := h.locker.locks[key]; exists && entry.holder == h.holder {
			delete(h.locker.locks, key)
		}
	}
	return nil
}

// Keys 返回锁定的键
func (h *memoryLockHandle) Keys() []string {
	return h.keys
}

var holderCounter int64
var holderMu sync.Mutex

func generateHolder() string {
	holderMu.Lock()
	defer holderMu.Unlock()
	holderCounter++
	return time.Now().Format("20060102150405") + "-" + string(rune(holderCounter))
}
