// Package store 提供RTE存储实现
package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"rte"
)

// MemoryStore 内存存储实现
// 用于演示和测试，生产环境应使用MySQL等持久化存储
type MemoryStore struct {
	mu          sync.Mutex
	txs         map[string]*rte.StoreTx
	steps       map[string]*rte.StoreStepRecord
	idempotency map[string]idempotencyRecord
}

type idempotencyRecord struct {
	result    []byte
	expiresAt time.Time
}

// NewMemoryStore 创建内存存储
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		txs:         make(map[string]*rte.StoreTx),
		steps:       make(map[string]*rte.StoreStepRecord),
		idempotency: make(map[string]idempotencyRecord),
	}
}

// CreateTransaction 创建事务
func (s *MemoryStore) CreateTransaction(ctx context.Context, tx *rte.StoreTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.txs[tx.TxID]; exists {
		return rte.ErrTransactionAlreadyExists
	}

	// 深拷贝
	copy := *tx
	s.txs[tx.TxID] = &copy
	return nil
}

// GetTransaction 获取事务
func (s *MemoryStore) GetTransaction(ctx context.Context, txID string) (*rte.StoreTx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.txs[txID]
	if !ok {
		return nil, rte.ErrTransactionNotFound
	}

	// 深拷贝
	copy := *tx
	return &copy, nil
}

// UpdateTransaction 更新事务
func (s *MemoryStore) UpdateTransaction(ctx context.Context, tx *rte.StoreTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.txs[tx.TxID]
	if !ok {
		return rte.ErrTransactionNotFound
	}

	// 乐观锁检查
	if existing.Version != tx.Version-1 {
		return rte.ErrVersionConflict
	}

	// 深拷贝
	copy := *tx
	s.txs[tx.TxID] = &copy
	return nil
}

// CreateStep 创建步骤记录
func (s *MemoryStore) CreateStep(ctx context.Context, step *rte.StoreStepRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%s:%d", step.TxID, step.StepIndex)
	copy := *step
	s.steps[key] = &copy
	return nil
}

// GetStep 获取步骤记录
func (s *MemoryStore) GetStep(ctx context.Context, txID string, stepIndex int) (*rte.StoreStepRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%s:%d", txID, stepIndex)
	step, ok := s.steps[key]
	if !ok {
		return nil, rte.ErrStepNotFound
	}

	copy := *step
	return &copy, nil
}

// UpdateStep 更新步骤记录
func (s *MemoryStore) UpdateStep(ctx context.Context, step *rte.StoreStepRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%s:%d", step.TxID, step.StepIndex)
	copy := *step
	s.steps[key] = &copy
	return nil
}

// GetSteps 获取事务的所有步骤
func (s *MemoryStore) GetSteps(ctx context.Context, txID string) ([]*rte.StoreStepRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []*rte.StoreStepRecord
	prefix := txID + ":"
	for key, step := range s.steps {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			copy := *step
			result = append(result, &copy)
		}
	}
	return result, nil
}

// GetPendingTransactions 获取待处理事务
func (s *MemoryStore) GetPendingTransactions(ctx context.Context, olderThan time.Duration) ([]*rte.StoreTx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	threshold := time.Now().Add(-olderThan)
	var result []*rte.StoreTx
	for _, tx := range s.txs {
		if !tx.IsTerminal() && tx.UpdatedAt.Before(threshold) {
			copy := *tx
			result = append(result, &copy)
		}
	}
	return result, nil
}

// GetStuckTransactions 获取卡住的事务
func (s *MemoryStore) GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*rte.StoreTx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	threshold := time.Now().Add(-olderThan)
	var result []*rte.StoreTx
	for _, tx := range s.txs {
		if (tx.Status == rte.TxStatusLocked || tx.Status == rte.TxStatusExecuting) &&
			tx.UpdatedAt.Before(threshold) {
			copy := *tx
			result = append(result, &copy)
		}
	}
	return result, nil
}

// GetRetryableTransactions 获取可重试事务
func (s *MemoryStore) GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*rte.StoreTx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []*rte.StoreTx
	for _, tx := range s.txs {
		if tx.Status == rte.TxStatusFailed && tx.RetryCount < maxRetries {
			copy := *tx
			result = append(result, &copy)
		}
	}
	return result, nil
}

// ListTransactions 列出事务
func (s *MemoryStore) ListTransactions(ctx context.Context, filter *rte.StoreTxFilter) ([]*rte.StoreTx, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []*rte.StoreTx
	for _, tx := range s.txs {
		// 应用过滤器
		if filter != nil {
			if len(filter.Status) > 0 {
				found := false
				for _, status := range filter.Status {
					if tx.Status == status {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}
			if filter.TxType != "" && tx.TxType != filter.TxType {
				continue
			}
		}
		copy := *tx
		result = append(result, &copy)
	}

	total := int64(len(result))

	// 应用分页
	if filter != nil && filter.Limit > 0 {
		start := filter.Offset
		if start >= len(result) {
			return nil, total, nil
		}
		end := start + filter.Limit
		if end > len(result) {
			end = len(result)
		}
		result = result[start:end]
	}

	return result, total, nil
}

// CheckIdempotency 检查幂等性
func (s *MemoryStore) CheckIdempotency(ctx context.Context, key string) (bool, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.idempotency[key]
	if !ok {
		return false, nil, nil
	}

	// 检查是否过期
	if time.Now().After(record.expiresAt) {
		delete(s.idempotency, key)
		return false, nil, nil
	}

	return true, record.result, nil
}

// MarkIdempotency 标记幂等性
func (s *MemoryStore) MarkIdempotency(ctx context.Context, key string, result []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.idempotency[key] = idempotencyRecord{
		result:    result,
		expiresAt: time.Now().Add(ttl),
	}
	return nil
}

// DeleteExpiredIdempotency 删除过期的幂等性记录
func (s *MemoryStore) DeleteExpiredIdempotency(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var count int64
	for key, record := range s.idempotency {
		if now.After(record.expiresAt) {
			delete(s.idempotency, key)
			count++
		}
	}
	return count, nil
}
