// Package main provides the entry point for the RTE Admin Dashboard server.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"rte"
	"syscall"
	"time"

	"rte/admin"
	"rte/circuit/memory"
	"rte/event"
)

func main() {
	// Create in-memory store for demo
	store := newDemoStore()

	// Create event bus and event store
	eventBus := event.NewMemoryEventBus()
	eventStore := admin.NewEventStore(1000)

	// Subscribe event store to event bus
	eventBus.SubscribeAll(eventStore.EventHandler())

	// Create circuit breaker
	breaker := memory.NewMemoryBreaker()

	// Create admin implementation
	adminImpl := admin.NewAdmin(
		admin.WithAdminStore(store),
		admin.WithAdminEventBus(eventBus),
		admin.WithAdminBreaker(breaker),
	)

	// Create admin server
	server := admin.NewAdminServer(
		admin.WithAddr(":8080"),
		admin.WithAdminImpl(adminImpl),
		admin.WithServerStore(store),
		admin.WithServerBreaker(breaker),
		admin.WithServerEventBus(eventBus),
		admin.WithEventStore(eventStore),
	)

	// Add some demo data
	addDemoData(store, eventStore)

	// Start server in goroutine
	go func() {
		fmt.Println("🚀 RTE Admin Dashboard 启动中...")
		fmt.Println("📍 访问地址: http://localhost:8080")
		fmt.Println("按 Ctrl+C 停止服务器")
		if err := server.Start(); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("\n正在关闭服务器...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Stop(ctx)
	fmt.Println("服务器已停止")
}

// demoStore is a simple in-memory store for demonstration
type demoStore struct {
	transactions map[string]*rte.StoreTx
	steps        map[string][]*rte.StoreStepRecord
}

func newDemoStore() *demoStore {
	return &demoStore{
		transactions: make(map[string]*rte.StoreTx),
		steps:        make(map[string][]*rte.StoreStepRecord),
	}
}

func (s *demoStore) CreateTransaction(ctx context.Context, tx *rte.StoreTx) error {
	s.transactions[tx.TxID] = tx
	return nil
}

func (s *demoStore) UpdateTransaction(ctx context.Context, tx *rte.StoreTx) error {
	s.transactions[tx.TxID] = tx
	return nil
}

func (s *demoStore) GetTransaction(ctx context.Context, txID string) (*rte.StoreTx, error) {
	tx, ok := s.transactions[txID]
	if !ok {
		return nil, nil
	}
	return tx, nil
}

func (s *demoStore) CreateStep(ctx context.Context, step *rte.StoreStepRecord) error {
	s.steps[step.TxID] = append(s.steps[step.TxID], step)
	return nil
}

func (s *demoStore) UpdateStep(ctx context.Context, step *rte.StoreStepRecord) error {
	steps := s.steps[step.TxID]
	for i, st := range steps {
		if st.StepIndex == step.StepIndex {
			steps[i] = step
			break
		}
	}
	return nil
}

func (s *demoStore) GetStep(ctx context.Context, txID string, stepIndex int) (*rte.StoreStepRecord, error) {
	steps := s.steps[txID]
	for _, step := range steps {
		if step.StepIndex == stepIndex {
			return step, nil
		}
	}
	return nil, nil
}

func (s *demoStore) GetSteps(ctx context.Context, txID string) ([]*rte.StoreStepRecord, error) {
	return s.steps[txID], nil
}

func (s *demoStore) GetPendingTransactions(ctx context.Context, olderThan time.Duration) ([]*rte.StoreTx, error) {
	return nil, nil
}

func (s *demoStore) GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*rte.StoreTx, error) {
	return nil, nil
}

func (s *demoStore) GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*rte.StoreTx, error) {
	return nil, nil
}

func (s *demoStore) ListTransactions(ctx context.Context, filter *rte.StoreTxFilter) ([]*rte.StoreTx, int64, error) {
	var result []*rte.StoreTx
	for _, tx := range s.transactions {
		// Apply status filter
		if len(filter.Status) > 0 {
			matched := false
			for _, status := range filter.Status {
				if tx.Status == status {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		// Apply type filter
		if filter.TxType != "" && tx.TxType != filter.TxType {
			continue
		}
		result = append(result, tx)
	}
	total := int64(len(result))
	// Apply pagination
	if filter.Offset > 0 && filter.Offset < len(result) {
		result = result[filter.Offset:]
	}
	if filter.Limit > 0 && filter.Limit < len(result) {
		result = result[:filter.Limit]
	}
	return result, total, nil
}

func (s *demoStore) CheckIdempotency(ctx context.Context, key string) (bool, []byte, error) {
	return false, nil, nil
}

func (s *demoStore) MarkIdempotency(ctx context.Context, key string, result []byte, ttl time.Duration) error {
	return nil
}

func (s *demoStore) DeleteExpiredIdempotency(ctx context.Context) (int64, error) {
	return 0, nil
}

// addDemoData adds sample data for demonstration
func addDemoData(store *demoStore, eventStore *admin.EventStore) {
	now := time.Now()

	// Create demo transactions
	transactions := []struct {
		id       string
		txType   string
		status   rte.TxStatus
		steps    []string
		errorMsg string
	}{
		{"tx-001", "transfer", rte.TxStatusCompleted, []string{"validate", "debit", "credit"}, ""},
		{"tx-002", "transfer", rte.TxStatusCompleted, []string{"validate", "debit", "credit"}, ""},
		{"tx-003", "payment", rte.TxStatusExecuting, []string{"auth", "capture"}, ""},
		{"tx-004", "payment", rte.TxStatusFailed, []string{"auth", "capture"}, "支付网关超时"},
		{"tx-005", "refund", rte.TxStatusCompensated, []string{"validate", "refund"}, ""},
		{"tx-006", "transfer", rte.TxStatusLocked, []string{"validate", "debit", "credit"}, ""},
		{"tx-007", "payment", rte.TxStatusCreated, []string{"auth", "capture"}, ""},
		{"tx-008", "refund", rte.TxStatusTimeout, []string{"validate", "refund"}, "事务执行超时"},
	}

	for i, t := range transactions {
		tx := rte.NewStoreTx(t.id, t.txType, t.steps)
		tx.Status = t.status
		tx.ErrorMsg = t.errorMsg
		tx.CreatedAt = now.Add(-time.Duration(i) * time.Hour)
		tx.UpdatedAt = now.Add(-time.Duration(i) * time.Minute)
		tx.Context = &rte.StoreTxContext{
			TxID:   t.id,
			TxType: t.txType,
			Input:  map[string]any{"amount": 100 * (i + 1), "currency": "CNY"},
			Output: map[string]any{},
		}

		if t.status == rte.TxStatusCompleted {
			completedAt := now.Add(-time.Duration(i) * time.Minute)
			tx.CompletedAt = &completedAt
			tx.CurrentStep = len(t.steps)
		}

		store.CreateTransaction(context.Background(), tx)

		// Create steps
		for j, stepName := range t.steps {
			step := rte.NewStoreStepRecord(t.id, j, stepName)
			if j < tx.CurrentStep {
				step.Status = rte.StepStatusCompleted
				startedAt := now.Add(-time.Duration(i*10+j) * time.Minute)
				completedAt := now.Add(-time.Duration(i*10+j-1) * time.Minute)
				step.StartedAt = &startedAt
				step.CompletedAt = &completedAt
			}
			store.CreateStep(context.Background(), step)
		}
	}

	// Add demo events with detailed data
	events := []struct {
		eventType event.EventType
		txID      string
		txType    string
		stepName  string
		data      map[string]any
		err       error
	}{
		{event.EventTxCreated, "tx-001", "transfer", "", map[string]any{
			"input": map[string]any{"from": "account-A", "to": "account-B", "amount": 100},
		}, nil},
		{event.EventStepStarted, "tx-001", "transfer", "validate", map[string]any{
			"step_index": 0,
		}, nil},
		{event.EventStepCompleted, "tx-001", "transfer", "validate", map[string]any{
			"step_index": 0,
			"duration":   "15ms",
			"output":     map[string]any{"valid": true},
		}, nil},
		{event.EventStepStarted, "tx-001", "transfer", "debit", map[string]any{
			"step_index": 1,
		}, nil},
		{event.EventStepCompleted, "tx-001", "transfer", "debit", map[string]any{
			"step_index":     1,
			"duration":       "45ms",
			"debit_amount":   100,
			"source_account": "account-A",
		}, nil},
		{event.EventStepStarted, "tx-001", "transfer", "credit", map[string]any{
			"step_index": 2,
		}, nil},
		{event.EventStepCompleted, "tx-001", "transfer", "credit", map[string]any{
			"step_index":     2,
			"duration":       "32ms",
			"credit_amount":  100,
			"target_account": "account-B",
		}, nil},
		{event.EventTxCompleted, "tx-001", "transfer", "", map[string]any{
			"total_duration": "120ms",
			"steps_executed": 3,
		}, nil},
		{event.EventTxCreated, "tx-004", "payment", "", map[string]any{
			"input": map[string]any{"amount": 400, "currency": "CNY", "gateway": "alipay"},
		}, nil},
		{event.EventStepStarted, "tx-004", "payment", "auth", map[string]any{
			"step_index": 0,
			"gateway":    "alipay",
		}, nil},
		{event.EventStepFailed, "tx-004", "payment", "auth", map[string]any{
			"step_index":   0,
			"retry_count":  3,
			"last_attempt": "2026-01-06T22:26:00Z",
		}, fmt.Errorf("支付网关超时: 连接超时 30s")},
		{event.EventTxFailed, "tx-004", "payment", "", map[string]any{
			"failed_step":    "auth",
			"total_duration": "90s",
			"will_retry":     false,
		}, fmt.Errorf("支付网关超时")},
	}

	for i, e := range events {
		eventStore.Store(event.Event{
			Type:      e.eventType,
			TxID:      e.txID,
			TxType:    e.txType,
			StepName:  e.stepName,
			Timestamp: now.Add(-time.Duration(len(events)-i) * time.Minute),
			Data:      e.data,
			Error:     e.err,
		})
	}
}
