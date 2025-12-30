// Package idempotency provides idempotency checking functionality for the RTE engine.
package idempotency

import (
	"context"
	"time"
)

// Checker defines the interface for idempotency checking.
// It provides methods to check if an operation was already executed
// and to mark an operation as executed.
type Checker interface {
	// Check checks if an operation was already executed.
	// Returns:
	//   - exists: true if the operation was already executed
	//   - result: the cached result of the operation (if exists is true)
	//   - err: any error that occurred during the check
	Check(ctx context.Context, key string) (exists bool, result []byte, err error)

	// Mark marks an operation as executed with its result.
	// The result will be stored with the given TTL.
	// Parameters:
	//   - key: the idempotency key (unique per transaction and step combination)
	//   - result: the serialized result of the operation
	//   - ttl: how long to keep the idempotency record
	Mark(ctx context.Context, key string, result []byte, ttl time.Duration) error
}
