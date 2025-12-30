package lock

import (
	"context"
	"time"
)

// Locker is the distributed lock interface
// It provides methods to acquire locks on multiple keys atomically
type Locker interface {
	// Acquire acquires locks on the given keys
	// Keys are sorted alphabetically before acquisition to prevent deadlocks
	// Returns a LockHandle for extending and releasing the locks
	// Returns ErrLockAcquisitionFailed if any lock cannot be acquired
	Acquire(ctx context.Context, keys []string, ttl time.Duration) (LockHandle, error)
}

// LockHandle represents a handle to acquired locks
// It provides methods to extend the TTL and release the locks
type LockHandle interface {
	// Extend extends the TTL of all held locks
	// Returns ErrLockExtensionFailed if extension fails
	Extend(ctx context.Context, ttl time.Duration) error

	// Release releases all held locks
	// Attempts to release all locks even if some releases fail
	// Returns ErrLockReleaseFailed if any release fails
	Release(ctx context.Context) error

	// Keys returns the keys that are locked
	Keys() []string
}
