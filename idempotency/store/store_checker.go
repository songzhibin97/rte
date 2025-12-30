// Package store provides a store-based implementation of the idempotency.Checker interface.
package store

import (
	"context"
	"time"

	"rte/idempotency"
)

// IdempotencyStore defines the storage operations required for idempotency checking.
// This interface is a subset of store.Store, allowing for flexible implementations.
type IdempotencyStore interface {
	// CheckIdempotency checks if an operation was already executed.
	CheckIdempotency(ctx context.Context, key string) (exists bool, result []byte, err error)

	// MarkIdempotency marks an operation as executed with its result.
	MarkIdempotency(ctx context.Context, key string, result []byte, ttl time.Duration) error
}

// StoreChecker implements the idempotency.Checker interface using a store backend.
type StoreChecker struct {
	store IdempotencyStore
}

// New creates a new StoreChecker with the given store.
func New(store IdempotencyStore) *StoreChecker {
	return &StoreChecker{
		store: store,
	}
}

// Check checks if an operation was already executed.
// It delegates to the underlying store's CheckIdempotency method.
func (c *StoreChecker) Check(ctx context.Context, key string) (bool, []byte, error) {
	return c.store.CheckIdempotency(ctx, key)
}

// Mark marks an operation as executed with its result.
// It delegates to the underlying store's MarkIdempotency method.
func (c *StoreChecker) Mark(ctx context.Context, key string, result []byte, ttl time.Duration) error {
	return c.store.MarkIdempotency(ctx, key, result, ttl)
}

// Ensure StoreChecker implements idempotency.Checker interface.
var _ idempotency.Checker = (*StoreChecker)(nil)
