// Package admin provides property-based tests for the Admin API.
// Feature: admin-dashboard
package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"rte/circuit"
	"rte/event"
	"rte/recovery"
	"strings"
	"testing"
	"time"

	"rte"

	"pgregory.net/rapid"
)

// ============================================================================

// transactions SHALL satisfy the filter conditions.
// ============================================================================

func TestProperty_FilterResultConsistency(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		store := newMockStore()
		admin := NewAdmin(WithAdminStore(store))

		// Generate random transactions with various statuses and types
		numTxs := rapid.IntRange(5, 20).Draw(rt, "numTxs")
		txTypes := []string{"type-a", "type-b", "type-c"}
		statuses := []rte.TxStatus{
			rte.TxStatusCreated,
			rte.TxStatusCompleted,
			rte.TxStatusFailed,
			rte.TxStatusExecuting,
			rte.TxStatusCompensated,
		}

		// Create transactions with random attributes
		for i := 0; i < numTxs; i++ {
			txType := txTypes[rapid.IntRange(0, len(txTypes)-1).Draw(rt, fmt.Sprintf("txType_%d", i))]
			status := statuses[rapid.IntRange(0, len(statuses)-1).Draw(rt, fmt.Sprintf("status_%d", i))]

			tx := rte.NewStoreTx(fmt.Sprintf("tx-%d", i), txType, []string{"step1"})
			tx.Status = status
			// Randomize creation time within last 7 days
			daysAgo := rapid.IntRange(0, 7).Draw(rt, fmt.Sprintf("daysAgo_%d", i))
			tx.CreatedAt = time.Now().Add(-time.Duration(daysAgo) * 24 * time.Hour)
			store.CreateTransaction(context.Background(), tx)
		}

		// Generate random filter
		filterByStatus := rapid.Bool().Draw(rt, "filterByStatus")
		filterByType := rapid.Bool().Draw(rt, "filterByType")
		filterByTime := rapid.Bool().Draw(rt, "filterByTime")

		filter := &rte.StoreTxFilter{Limit: 100}

		var expectedStatus rte.TxStatus
		var expectedType string
		var startTime, endTime time.Time

		if filterByStatus {
			expectedStatus = statuses[rapid.IntRange(0, len(statuses)-1).Draw(rt, "filterStatus")]
			filter.Status = []rte.TxStatus{expectedStatus}
		}

		if filterByType {
			expectedType = txTypes[rapid.IntRange(0, len(txTypes)-1).Draw(rt, "filterType")]
			filter.TxType = expectedType
		}

		if filterByTime {
			daysAgoStart := rapid.IntRange(3, 7).Draw(rt, "daysAgoStart")
			daysAgoEnd := rapid.IntRange(0, 2).Draw(rt, "daysAgoEnd")
			startTime = time.Now().Add(-time.Duration(daysAgoStart) * 24 * time.Hour)
			endTime = time.Now().Add(-time.Duration(daysAgoEnd) * 24 * time.Hour)
			filter.StartTime = startTime
			filter.EndTime = endTime
		}

		// Execute query
		result, err := admin.ListTransactions(context.Background(), filter)
		if err != nil {
			rt.Fatalf("failed to list transactions: %v", err)
		}

		
		for _, tx := range result.Transactions {
			if filterByStatus && tx.Status != expectedStatus {
				rt.Fatalf("transaction %s has status %s, expected %s", tx.TxID, tx.Status, expectedStatus)
			}

			if filterByType && tx.TxType != expectedType {
				rt.Fatalf("transaction %s has type %s, expected %s", tx.TxID, tx.TxType, expectedType)
			}

			if filterByTime {
				if tx.CreatedAt.Before(startTime) {
					rt.Fatalf("transaction %s created at %v is before start time %v", tx.TxID, tx.CreatedAt, startTime)
				}
				if tx.CreatedAt.After(endTime) {
					rt.Fatalf("transaction %s created at %v is after end time %v", tx.TxID, tx.CreatedAt, endTime)
				}
			}
		}
	})
}

// ============================================================================

// (ID, type, status, time info, input/output).
// ============================================================================

func TestProperty_TransactionDetailCompleteness(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		store := newMockStore()
		admin := NewAdmin(WithAdminStore(store))

		// Generate random transaction
		txID := fmt.Sprintf("tx-%d", rapid.IntRange(1, 1000).Draw(rt, "txID"))
		txType := rapid.StringMatching(`[a-z]{3,10}`).Draw(rt, "txType")
		status := []rte.TxStatus{
			rte.TxStatusCreated,
			rte.TxStatusCompleted,
			rte.TxStatusFailed,
		}[rapid.IntRange(0, 2).Draw(rt, "status")]

		numSteps := rapid.IntRange(1, 5).Draw(rt, "numSteps")
		stepNames := make([]string, numSteps)
		for i := 0; i < numSteps; i++ {
			stepNames[i] = fmt.Sprintf("step-%d", i)
		}

		tx := rte.NewStoreTx(txID, txType, stepNames)
		tx.Status = status
		tx.Context = &rte.StoreTxContext{
			TxID:   txID,
			TxType: txType,
			Input:  map[string]any{"key": "value"},
			Output: map[string]any{"result": "success"},
		}
		store.CreateTransaction(context.Background(), tx)

		// Create step records
		for i, name := range stepNames {
			step := rte.NewStoreStepRecord(txID, i, name)
			store.CreateStep(context.Background(), step)
		}

		// Get transaction detail
		detail, err := admin.GetTransaction(context.Background(), txID)
		if err != nil {
			rt.Fatalf("failed to get transaction: %v", err)
		}

		
		if detail.Transaction == nil {
			rt.Fatal("transaction detail is nil")
		}

		if detail.Transaction.TxID != txID {
			rt.Fatalf("expected TxID %s, got %s", txID, detail.Transaction.TxID)
		}

		if detail.Transaction.TxType != txType {
			rt.Fatalf("expected TxType %s, got %s", txType, detail.Transaction.TxType)
		}

		if detail.Transaction.Status != status {
			rt.Fatalf("expected Status %s, got %s", status, detail.Transaction.Status)
		}

		
		if detail.Transaction.CreatedAt.IsZero() {
			rt.Fatal("CreatedAt is not set")
		}

		
		if len(detail.Steps) != numSteps {
			rt.Fatalf("expected %d steps, got %d", numSteps, len(detail.Steps))
		}
	})
}

// ============================================================================

// the transaction's TotalSteps field.
// ============================================================================

func TestProperty_StepCountConsistency(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		store := newMockStore()
		admin := NewAdmin(WithAdminStore(store))

		// Generate random transaction with random number of steps
		txID := fmt.Sprintf("tx-%d", rapid.IntRange(1, 1000).Draw(rt, "txID"))
		numSteps := rapid.IntRange(1, 10).Draw(rt, "numSteps")

		stepNames := make([]string, numSteps)
		for i := 0; i < numSteps; i++ {
			stepNames[i] = fmt.Sprintf("step-%d", i)
		}

		tx := rte.NewStoreTx(txID, "test-type", stepNames)
		store.CreateTransaction(context.Background(), tx)

		// Create step records
		for i, name := range stepNames {
			step := rte.NewStoreStepRecord(txID, i, name)
			store.CreateStep(context.Background(), step)
		}

		// Get transaction detail
		detail, err := admin.GetTransaction(context.Background(), txID)
		if err != nil {
			rt.Fatalf("failed to get transaction: %v", err)
		}

		
		if len(detail.Steps) != detail.Transaction.TotalSteps {
			rt.Fatalf("step count %d does not match TotalSteps %d",
				len(detail.Steps), detail.Transaction.TotalSteps)
		}

		
		if len(detail.Steps) != len(stepNames) {
			rt.Fatalf("step count %d does not match expected %d",
				len(detail.Steps), len(stepNames))
		}
	})
}

// ============================================================================

// force complete SHALL transition it to COMPLETED.
// ============================================================================

func TestProperty_ForceCompleteStateTransition(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		store := newMockStore()
		admin := NewAdmin(WithAdminStore(store))

		// Generate transaction in a stuck state
		txID := fmt.Sprintf("tx-%d", rapid.IntRange(1, 1000).Draw(rt, "txID"))
		stuckStatuses := []rte.TxStatus{
			rte.TxStatusLocked,
			rte.TxStatusExecuting,
			rte.TxStatusConfirming,
		}
		status := stuckStatuses[rapid.IntRange(0, len(stuckStatuses)-1).Draw(rt, "status")]

		tx := rte.NewStoreTx(txID, "test-type", []string{"step1"})
		tx.Status = status
		store.CreateTransaction(context.Background(), tx)

		// Force complete
		reason := rapid.StringMatching(`[a-z ]{5,20}`).Draw(rt, "reason")
		err := admin.ForceComplete(context.Background(), txID, reason)
		if err != nil {
			rt.Fatalf("failed to force complete: %v", err)
		}

		
		updatedTx, _ := store.GetTransaction(context.Background(), txID)
		if updatedTx.Status != rte.TxStatusCompleted {
			rt.Fatalf("expected COMPLETED status, got %s", updatedTx.Status)
		}

		
		if updatedTx.CompletedAt == nil {
			rt.Fatal("CompletedAt should be set after force complete")
		}
	})
}

// ============================================================================

// force cancel SHALL transition it to CANCELLED or COMPENSATED.
// ============================================================================

func TestProperty_ForceCancelStateTransition(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		store := newMockStore()
		admin := NewAdmin(WithAdminStore(store))

		// Generate transaction in a cancellable state
		txID := fmt.Sprintf("tx-%d", rapid.IntRange(1, 1000).Draw(rt, "txID"))
		cancellableStatuses := []rte.TxStatus{
			rte.TxStatusCreated,
			rte.TxStatusLocked,
			rte.TxStatusExecuting,
			rte.TxStatusFailed,
		}
		status := cancellableStatuses[rapid.IntRange(0, len(cancellableStatuses)-1).Draw(rt, "status")]

		tx := rte.NewStoreTx(txID, "test-type", []string{"step1"})
		tx.Status = status
		store.CreateTransaction(context.Background(), tx)

		// Force cancel
		reason := rapid.StringMatching(`[a-z ]{5,20}`).Draw(rt, "reason")
		err := admin.ForceCancel(context.Background(), txID, reason)
		if err != nil {
			rt.Fatalf("failed to force cancel: %v", err)
		}

		
		updatedTx, _ := store.GetTransaction(context.Background(), txID)
		validStatuses := map[rte.TxStatus]bool{
			rte.TxStatusCancelled:   true,
			rte.TxStatusCompensated: true,
		}
		if !validStatuses[updatedTx.Status] {
			rt.Fatalf("expected CANCELLED or COMPENSATED status, got %s", updatedTx.Status)
		}
	})
}

// ============================================================================

// ============================================================================

func TestProperty_RetryCountIncrement(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		store := newMockStore()

		// Create a coordinator with a simple step that succeeds
		coord := rte.NewCoordinator(
			rte.WithStore(store),
			rte.WithCoordinatorConfig(rte.Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       10,
				RetryInterval:    100 * time.Millisecond,
			}),
		)

		// Register a step that always succeeds
		testStep := newTestStep("step1")
		testStep.executeFunc = func(ctx context.Context, txCtx *rte.TxContext) error {
			return nil
		}
		coord.RegisterStep(testStep)

		admin := NewAdmin(
			WithAdminStore(store),
			WithAdminCoordinator(coord),
		)

		// Generate failed transaction with random initial retry count
		txID := fmt.Sprintf("tx-%d", rapid.IntRange(1, 1000).Draw(rt, "txID"))
		initialRetryCount := rapid.IntRange(0, 5).Draw(rt, "initialRetryCount")

		tx := rte.NewStoreTx(txID, "test-type", []string{"step1"})
		tx.Status = rte.TxStatusFailed
		tx.RetryCount = initialRetryCount
		tx.MaxRetries = 10
		tx.Context = &rte.StoreTxContext{
			TxID:   txID,
			TxType: "test-type",
			Input:  make(map[string]any),
			Output: make(map[string]any),
		}
		store.CreateTransaction(context.Background(), tx)

		// Create step record
		step := rte.NewStoreStepRecord(txID, 0, "step1")
		store.CreateStep(context.Background(), step)

		// Retry transaction
		err := admin.RetryTransaction(context.Background(), txID)
		if err != nil {
			rt.Fatalf("failed to retry transaction: %v", err)
		}

		
		updatedTx, _ := store.GetTransaction(context.Background(), txID)
		if updatedTx.RetryCount <= initialRetryCount {
			rt.Fatalf("expected retry count > %d, got %d", initialRetryCount, updatedTx.RetryCount)
		}
	})
}

// ============================================================================

// ============================================================================

func TestProperty_StatsDataConsistency(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		store := newMockStore()
		admin := NewAdmin(WithAdminStore(store))

		// Generate random transactions with various statuses
		numTxs := rapid.IntRange(10, 50).Draw(rt, "numTxs")

		// Track expected counts
		var expectedPending, expectedFailed, expectedCompleted, expectedCompensated int64

		for i := 0; i < numTxs; i++ {
			statusIdx := rapid.IntRange(0, 10).Draw(rt, fmt.Sprintf("status_%d", i))
			var status rte.TxStatus

			switch statusIdx {
			case 0:
				status = rte.TxStatusCreated
				expectedPending++
			case 1:
				status = rte.TxStatusLocked
				expectedPending++
			case 2:
				status = rte.TxStatusExecuting
				expectedPending++
			case 3:
				status = rte.TxStatusConfirming
				expectedPending++
			case 4:
				status = rte.TxStatusCompleted
				expectedCompleted++
			case 5:
				status = rte.TxStatusFailed
				expectedFailed++
			case 6:
				status = rte.TxStatusCompensationFailed
				expectedFailed++
			case 7:
				status = rte.TxStatusTimeout
				expectedFailed++
			case 8:
				status = rte.TxStatusCompensated
				expectedCompensated++
			case 9:
				status = rte.TxStatusCompensating
				// Not counted in any category
			case 10:
				status = rte.TxStatusCancelled
				// Not counted in any category
			}

			tx := rte.NewStoreTx(fmt.Sprintf("tx-%d", i), "test-type", []string{"step1"})
			tx.Status = status
			store.CreateTransaction(context.Background(), tx)
		}

		// Get stats
		stats, err := admin.GetStats(context.Background())
		if err != nil {
			rt.Fatalf("failed to get stats: %v", err)
		}

		
		if stats.TotalTransactions != int64(numTxs) {
			rt.Fatalf("expected total %d, got %d", numTxs, stats.TotalTransactions)
		}

		
		if stats.PendingTransactions != expectedPending {
			rt.Fatalf("expected pending %d, got %d", expectedPending, stats.PendingTransactions)
		}

		
		if stats.FailedTransactions != expectedFailed {
			rt.Fatalf("expected failed %d, got %d", expectedFailed, stats.FailedTransactions)
		}

		
		if stats.CompletedTransactions != expectedCompleted {
			rt.Fatalf("expected completed %d, got %d", expectedCompleted, stats.CompletedTransactions)
		}

		
		if stats.CompensatedTransactions != expectedCompensated {
			rt.Fatalf("expected compensated %d, got %d", expectedCompensated, stats.CompensatedTransactions)
		}
	})
}

// ============================================================================

// ============================================================================

func TestProperty_RecoveryWorkerStatsConsistency(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Create a real recovery worker
		worker := recovery.NewWorker()

		// Get stats from worker
		workerStats := worker.Stats()

		server := NewAdminServer(WithServerRecovery(worker))
		handler := server.Handler()

		req := httptest.NewRequest("GET", "/api/recovery/stats", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			rt.Fatalf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			rt.Fatalf("failed to decode response: %v", err)
		}

		if !resp.Success {
			rt.Fatal("expected success to be true")
		}

		// Extract stats from response
		dataBytes, _ := json.Marshal(resp.Data)
		var stats RecoveryStatsResponse
		json.Unmarshal(dataBytes, &stats)

		
		if stats.IsRunning != workerStats.IsRunning {
			rt.Fatalf("expected IsRunning=%v, got %v", workerStats.IsRunning, stats.IsRunning)
		}

		
		if stats.ScannedCount != workerStats.ScannedCount {
			rt.Fatalf("expected ScannedCount=%d, got %d", workerStats.ScannedCount, stats.ScannedCount)
		}

		
		if stats.ProcessedCount != workerStats.ProcessedCount {
			rt.Fatalf("expected ProcessedCount=%d, got %d", workerStats.ProcessedCount, stats.ProcessedCount)
		}

		
		if stats.FailedCount != workerStats.FailedCount {
			rt.Fatalf("expected FailedCount=%d, got %d", workerStats.FailedCount, stats.FailedCount)
		}
	})
}

// ============================================================================

// ============================================================================

func TestProperty_CircuitBreakerStateValidity(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		breaker := newMockBreaker()

		// Generate random services
		numServices := rapid.IntRange(1, 10).Draw(rt, "numServices")
		for i := 0; i < numServices; i++ {
			serviceName := fmt.Sprintf("service-%d", i)
			cb := breaker.Get(serviceName)

			
			state := cb.State()
			validStates := map[circuit.State]bool{
				circuit.StateClosed:   true,
				circuit.StateOpen:     true,
				circuit.StateHalfOpen: true,
			}
			if !validStates[state] {
				rt.Fatalf("invalid circuit breaker state: %s", state)
			}
		}
	})
}

// ============================================================================

// ============================================================================

func TestProperty_CircuitBreakerReset(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		breaker := newMockBreaker()
		server := NewAdminServer(WithServerBreaker(breaker))
		handler := server.Handler()

		// Generate random service name
		serviceName := rapid.StringMatching(`[a-z]{3,10}`).Draw(rt, "serviceName")

		// Get circuit breaker and potentially set it to non-CLOSED state
		cb := breaker.Get(serviceName).(*mockCircuitBreaker)

		// Randomly set initial state
		initialStateIdx := rapid.IntRange(0, 2).Draw(rt, "initialState")
		switch initialStateIdx {
		case 1:
			cb.state = circuit.StateOpen
		case 2:
			cb.state = circuit.StateHalfOpen
		}

		// Reset via API
		req := httptest.NewRequest("POST", fmt.Sprintf("/api/circuit-breakers/%s/reset", serviceName), nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			rt.Fatalf("expected status 200, got %d", w.Code)
		}

		
		if cb.State() != circuit.StateClosed {
			rt.Fatalf("expected CLOSED state after reset, got %s", cb.State())
		}
	})
}

// ============================================================================

// (containing success, data or error fields).
// ============================================================================

func TestProperty_APIResponseFormatConsistency(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		store := newMockStore()
		admin := NewAdmin(WithAdminStore(store))
		breaker := newMockBreaker()
		eventStore := NewEventStore(100)
		recoveryWorker := recovery.NewWorker()
		server := NewAdminServer(
			WithAdminImpl(admin),
			WithServerBreaker(breaker),
			WithEventStore(eventStore),
			WithServerRecovery(recoveryWorker),
		)
		handler := server.Handler()

		// Create test transactions with various statuses for different operations
		// tx-1: COMPLETED - for GET operations
		createTestTransaction(store, "tx-1", "type-a", rte.TxStatusCompleted, []string{"step1"})
		// tx-stuck: LOCKED - for force-complete operation
		createTestTransaction(store, "tx-stuck", "type-a", rte.TxStatusLocked, []string{"step1"})
		// tx-cancel: CREATED - for force-cancel operation
		createTestTransaction(store, "tx-cancel", "type-a", rte.TxStatusCreated, []string{"step1"})
		// tx-failed: FAILED - for retry operation
		createTestTransaction(store, "tx-failed", "type-a", rte.TxStatusFailed, []string{"step1"})

		// Add some test events
		eventStore.Store(event.NewEvent(event.EventTxCreated).WithTxID("tx-1").WithTxType("type-a"))

		// Test all API endpoints - comprehensive coverage for requirement 10.8
		endpoints := []struct {
			method string
			path   string
			body   string
		}{
			// Transaction list and detail APIs 
			{"GET", "/api/transactions", ""},
			{"GET", "/api/transactions?status=COMPLETED", ""},
			{"GET", "/api/transactions?tx_type=type-a", ""},
			{"GET", "/api/transactions/tx-1", ""},
			{"GET", "/api/transactions/non-existent", ""},

			// Transaction operation APIs 
			{"POST", "/api/transactions/tx-stuck/force-complete", `{"reason":"test force complete"}`},
			{"POST", "/api/transactions/tx-cancel/force-cancel", `{"reason":"test force cancel"}`},
			{"POST", "/api/transactions/non-existent/force-complete", `{"reason":"test"}`},
			{"POST", "/api/transactions/non-existent/force-cancel", `{"reason":"test"}`},
			{"POST", "/api/transactions/non-existent/retry", ""},

			// Stats APIs 
			{"GET", "/api/stats", ""},
			{"GET", "/api/recovery/stats", ""},

			// Circuit breaker APIs
			{"GET", "/api/circuit-breakers", ""},
			{"POST", "/api/circuit-breakers/test-service/reset", ""},

			// Events API
			{"GET", "/api/events", ""},
			{"GET", "/api/events?type=tx_created", ""},
		}

		endpointIdx := rapid.IntRange(0, len(endpoints)-1).Draw(rt, "endpoint")
		ep := endpoints[endpointIdx]

		var req *http.Request
		if ep.body != "" {
			req = httptest.NewRequest(ep.method, ep.path, strings.NewReader(ep.body))
			req.Header.Set("Content-Type", "application/json")
		} else {
			req = httptest.NewRequest(ep.method, ep.path, nil)
		}
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		
		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			rt.Fatalf("endpoint %s %s: response is not valid JSON: %v, body: %s",
				ep.method, ep.path, err, w.Body.String())
		}

		

		
		if !resp.Success {
			if resp.Error == nil {
				rt.Fatalf("endpoint %s %s: failed response must have error field",
					ep.method, ep.path)
			}
			if resp.Error.Code == "" {
				rt.Fatalf("endpoint %s %s: error must have code",
					ep.method, ep.path)
			}
			if resp.Error.Message == "" {
				rt.Fatalf("endpoint %s %s: error must have message",
					ep.method, ep.path)
			}
		}

		
		// Success responses: 200
		// Client errors (not found, invalid request): 400, 404
		// Server errors: 500
		validStatusCodes := map[int]bool{
			http.StatusOK:                  true,
			http.StatusBadRequest:          true,
			http.StatusNotFound:            true,
			http.StatusInternalServerError: true,
		}
		if !validStatusCodes[w.Code] {
			rt.Fatalf("endpoint %s %s: unexpected status code %d",
				ep.method, ep.path, w.Code)
		}

		
		if resp.Success && w.Code != http.StatusOK {
			rt.Fatalf("endpoint %s %s: success=true but status code is %d",
				ep.method, ep.path, w.Code)
		}
		if !resp.Success && w.Code == http.StatusOK {
			rt.Fatalf("endpoint %s %s: success=false but status code is 200",
				ep.method, ep.path)
		}
	})
}

// ============================================================================

// ============================================================================

func TestProperty_EventFilterConsistency(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Create event store
		eventStore := NewEventStore(1000)

		// Define event types to use
		eventTypes := []event.EventType{
			event.EventTxCreated,
			event.EventTxCompleted,
			event.EventTxFailed,
			event.EventStepStarted,
			event.EventStepCompleted,
			event.EventStepFailed,
			event.EventCircuitOpened,
			event.EventCircuitClosed,
		}

		// Generate random events
		numEvents := rapid.IntRange(10, 50).Draw(rt, "numEvents")
		for i := 0; i < numEvents; i++ {
			eventTypeIdx := rapid.IntRange(0, len(eventTypes)-1).Draw(rt, fmt.Sprintf("eventType_%d", i))
			eventType := eventTypes[eventTypeIdx]

			txID := fmt.Sprintf("tx-%d", rapid.IntRange(1, 10).Draw(rt, fmt.Sprintf("txID_%d", i)))
			txType := rapid.StringMatching(`[a-z]{3,8}`).Draw(rt, fmt.Sprintf("txType_%d", i))

			e := event.NewEvent(eventType).
				WithTxID(txID).
				WithTxType(txType)

			// Add step name for step events
			if eventType == event.EventStepStarted || eventType == event.EventStepCompleted || eventType == event.EventStepFailed {
				stepName := fmt.Sprintf("step-%d", rapid.IntRange(1, 5).Draw(rt, fmt.Sprintf("stepName_%d", i)))
				e = e.WithStepName(stepName)
			}

			eventStore.Store(e)
		}

		// Generate random filter type
		filterTypeIdx := rapid.IntRange(0, len(eventTypes)-1).Draw(rt, "filterType")
		filterType := string(eventTypes[filterTypeIdx])

		// Query with filter
		filter := EventFilter{
			Type:  filterType,
			Limit: 1000,
		}
		results := eventStore.List(filter)

		
		for _, e := range results {
			if e.Type != filterType {
				rt.Fatalf("event type %s does not match filter type %s", e.Type, filterType)
			}
		}

		
		count := eventStore.Count(filter)
		if count != len(results) {
			rt.Fatalf("count %d does not match results length %d", count, len(results))
		}
	})
}

// ============================================================================
// Additional Event Store Property Tests
// ============================================================================

// TestProperty_EventStoreTxIDFilter tests that tx_id filtering works correctly
func TestProperty_EventStoreTxIDFilter(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		eventStore := NewEventStore(1000)

		// Generate events for multiple transactions
		numTxs := rapid.IntRange(3, 10).Draw(rt, "numTxs")
		txIDs := make([]string, numTxs)
		for i := 0; i < numTxs; i++ {
			txIDs[i] = fmt.Sprintf("tx-%d", i)
		}

		// Create events for each transaction
		numEventsPerTx := rapid.IntRange(2, 5).Draw(rt, "numEventsPerTx")
		for _, txID := range txIDs {
			for j := 0; j < numEventsPerTx; j++ {
				e := event.NewEvent(event.EventTxCreated).
					WithTxID(txID).
					WithTxType("test-type")
				eventStore.Store(e)
			}
		}

		// Pick a random transaction to filter by
		filterTxIdx := rapid.IntRange(0, numTxs-1).Draw(rt, "filterTxIdx")
		filterTxID := txIDs[filterTxIdx]

		// Query with tx_id filter
		filter := EventFilter{
			TxID:  filterTxID,
			Limit: 1000,
		}
		results := eventStore.List(filter)

		
		for _, e := range results {
			if e.TxID != filterTxID {
				rt.Fatalf("event tx_id %s does not match filter tx_id %s", e.TxID, filterTxID)
			}
		}

		
		if len(results) != numEventsPerTx {
			rt.Fatalf("expected %d events for tx %s, got %d", numEventsPerTx, filterTxID, len(results))
		}
	})
}

// TestProperty_EventStoreMaxEvents tests that max events limit is enforced
func TestProperty_EventStoreMaxEvents(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		maxEvents := rapid.IntRange(10, 50).Draw(rt, "maxEvents")
		eventStore := NewEventStore(maxEvents)

		// Add more events than max
		numEvents := rapid.IntRange(maxEvents+1, maxEvents*2).Draw(rt, "numEvents")
		for i := 0; i < numEvents; i++ {
			e := event.NewEvent(event.EventTxCreated).
				WithTxID(fmt.Sprintf("tx-%d", i)).
				WithTxType("test-type")
			eventStore.Store(e)
		}

		
		if eventStore.Len() > maxEvents {
			rt.Fatalf("event store has %d events, exceeds max %d", eventStore.Len(), maxEvents)
		}

		
		if eventStore.Len() != maxEvents {
			rt.Fatalf("expected %d events, got %d", maxEvents, eventStore.Len())
		}
	})
}

// TestProperty_EventStoreOrdering tests that events are returned in reverse chronological order
func TestProperty_EventStoreOrdering(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		eventStore := NewEventStore(1000)

		// Add events with sequential IDs
		numEvents := rapid.IntRange(5, 20).Draw(rt, "numEvents")
		for i := 0; i < numEvents; i++ {
			e := event.NewEvent(event.EventTxCreated).
				WithTxID(fmt.Sprintf("tx-%d", i)).
				WithTxType("test-type")
			eventStore.Store(e)
		}

		// Get all events
		results := eventStore.List(EventFilter{Limit: 1000})

		
		for i := 1; i < len(results); i++ {
			if results[i].ID > results[i-1].ID {
				rt.Fatalf("events not in reverse order: event %d (id=%d) comes after event %d (id=%d)",
					i, results[i].ID, i-1, results[i-1].ID)
			}
		}
	})
}

// TestProperty_EventHandlerIntegration tests that EventHandler correctly stores events
func TestProperty_EventHandlerIntegration(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		eventStore := NewEventStore(1000)
		handler := eventStore.EventHandler()

		// Generate random events and store via handler
		numEvents := rapid.IntRange(5, 20).Draw(rt, "numEvents")
		expectedTxIDs := make(map[string]bool)

		for i := 0; i < numEvents; i++ {
			txID := fmt.Sprintf("tx-%d", i)
			expectedTxIDs[txID] = true

			e := event.NewEvent(event.EventTxCreated).
				WithTxID(txID).
				WithTxType("test-type")

			// Store via handler
			err := handler(context.Background(), e)
			if err != nil {
				rt.Fatalf("handler returned error: %v", err)
			}
		}

		
		if eventStore.Len() != numEvents {
			rt.Fatalf("expected %d events, got %d", numEvents, eventStore.Len())
		}

		
		results := eventStore.List(EventFilter{Limit: 1000})
		for _, e := range results {
			if !expectedTxIDs[e.TxID] {
				rt.Fatalf("unexpected tx_id %s in results", e.TxID)
			}
		}
	})
}

// ============================================================================

// the transaction is in a valid state before proceeding.
// ============================================================================

func TestProperty_AdminForceOperationValidation(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		store := newMockStore()
		eventBus := event.NewMemoryEventBus()
		admin := NewAdmin(
			WithAdminStore(store),
			WithAdminEventBus(eventBus),
		)

		// Define all possible transaction statuses
		allStatuses := []rte.TxStatus{
			rte.TxStatusCreated,
			rte.TxStatusLocked,
			rte.TxStatusExecuting,
			rte.TxStatusConfirming,
			rte.TxStatusCompleted,
			rte.TxStatusFailed,
			rte.TxStatusCompensating,
			rte.TxStatusCompensated,
			rte.TxStatusCompensationFailed,
			rte.TxStatusTimeout,
			rte.TxStatusCancelled,
		}

		// Valid states for force-complete: LOCKED, EXECUTING, CONFIRMING
		validForceCompleteStates := map[rte.TxStatus]bool{
			rte.TxStatusLocked:     true,
			rte.TxStatusExecuting:  true,
			rte.TxStatusConfirming: true,
		}

		// Generate random transaction with random status
		txID := fmt.Sprintf("tx-%d", rapid.IntRange(1, 10000).Draw(rt, "txID"))
		statusIdx := rapid.IntRange(0, len(allStatuses)-1).Draw(rt, "statusIdx")
		status := allStatuses[statusIdx]

		tx := rte.NewStoreTx(txID, "test-type", []string{"step1"})
		tx.Status = status
		store.CreateTransaction(context.Background(), tx)

		// Create step record
		step := rte.NewStoreStepRecord(txID, 0, "step1")
		store.CreateStep(context.Background(), step)

		// Test force-complete operation
		reason := rapid.StringMatching(`[a-z ]{5,20}`).Draw(rt, "reason")
		forceCompleteErr := admin.ForceComplete(context.Background(), txID, reason)

		
		if validForceCompleteStates[status] {
			// Should succeed
			if forceCompleteErr != nil {
				rt.Fatalf("force-complete should succeed for status %s, but got error: %v", status, forceCompleteErr)
			}
			// Verify status changed to COMPLETED
			updatedTx, _ := store.GetTransaction(context.Background(), txID)
			if updatedTx.Status != rte.TxStatusCompleted {
				rt.Fatalf("expected COMPLETED status after force-complete, got %s", updatedTx.Status)
			}
		} else {
			// Should fail with invalid state error
			if forceCompleteErr == nil {
				rt.Fatalf("force-complete should fail for status %s, but succeeded", status)
			}
			if !strings.Contains(forceCompleteErr.Error(), "invalid transaction state") &&
				!strings.Contains(forceCompleteErr.Error(), "cannot force complete") {
				rt.Fatalf("expected invalid state error for status %s, got: %v", status, forceCompleteErr)
			}
		}
	})
}

func TestProperty_AdminForceCancelValidation(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		store := newMockStore()
		eventBus := event.NewMemoryEventBus()
		admin := NewAdmin(
			WithAdminStore(store),
			WithAdminEventBus(eventBus),
		)

		// Define all possible transaction statuses
		allStatuses := []rte.TxStatus{
			rte.TxStatusCreated,
			rte.TxStatusLocked,
			rte.TxStatusExecuting,
			rte.TxStatusConfirming,
			rte.TxStatusCompleted,
			rte.TxStatusFailed,
			rte.TxStatusCompensating,
			rte.TxStatusCompensated,
			rte.TxStatusCompensationFailed,
			rte.TxStatusTimeout,
			rte.TxStatusCancelled,
		}

		// Valid states for force-cancel: CREATED, LOCKED, EXECUTING, FAILED
		validForceCancelStates := map[rte.TxStatus]bool{
			rte.TxStatusCreated:   true,
			rte.TxStatusLocked:    true,
			rte.TxStatusExecuting: true,
			rte.TxStatusFailed:    true,
		}

		// Generate random transaction with random status
		txID := fmt.Sprintf("tx-%d", rapid.IntRange(1, 10000).Draw(rt, "txID"))
		statusIdx := rapid.IntRange(0, len(allStatuses)-1).Draw(rt, "statusIdx")
		status := allStatuses[statusIdx]

		tx := rte.NewStoreTx(txID, "test-type", []string{"step1"})
		tx.Status = status
		store.CreateTransaction(context.Background(), tx)

		// Create step record
		step := rte.NewStoreStepRecord(txID, 0, "step1")
		store.CreateStep(context.Background(), step)

		// Test force-cancel operation
		reason := rapid.StringMatching(`[a-z ]{5,20}`).Draw(rt, "reason")
		forceCancelErr := admin.ForceCancel(context.Background(), txID, reason)

		
		if validForceCancelStates[status] {
			// Should succeed
			if forceCancelErr != nil {
				rt.Fatalf("force-cancel should succeed for status %s, but got error: %v", status, forceCancelErr)
			}
			// Verify status changed to CANCELLED or COMPENSATED
			updatedTx, _ := store.GetTransaction(context.Background(), txID)
			validEndStates := map[rte.TxStatus]bool{
				rte.TxStatusCancelled:   true,
				rte.TxStatusCompensated: true,
			}
			if !validEndStates[updatedTx.Status] {
				rt.Fatalf("expected CANCELLED or COMPENSATED status after force-cancel, got %s", updatedTx.Status)
			}
		} else {
			// Should fail with invalid state error
			if forceCancelErr == nil {
				rt.Fatalf("force-cancel should fail for status %s, but succeeded", status)
			}
			if !strings.Contains(forceCancelErr.Error(), "invalid transaction state") &&
				!strings.Contains(forceCancelErr.Error(), "cannot cancel") {
				rt.Fatalf("expected invalid state error for status %s, got: %v", status, forceCancelErr)
			}
		}
	})
}

func TestProperty_AdminRetryValidation(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		store := newMockStore()
		eventBus := event.NewMemoryEventBus()

		// Create coordinator with test step
		coord := rte.NewCoordinator(
			rte.WithStore(store),
			rte.WithEventBus(eventBus),
			rte.WithCoordinatorConfig(rte.Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       10,
				RetryInterval:    100 * time.Millisecond,
			}),
		)

		// Register a step that always succeeds
		testStep := newTestStep("step1")
		testStep.executeFunc = func(ctx context.Context, txCtx *rte.TxContext) error {
			return nil
		}
		coord.RegisterStep(testStep)

		admin := NewAdmin(
			WithAdminStore(store),
			WithAdminEventBus(eventBus),
			WithAdminCoordinator(coord),
		)

		// Define all possible transaction statuses
		allStatuses := []rte.TxStatus{
			rte.TxStatusCreated,
			rte.TxStatusLocked,
			rte.TxStatusExecuting,
			rte.TxStatusConfirming,
			rte.TxStatusCompleted,
			rte.TxStatusFailed,
			rte.TxStatusCompensating,
			rte.TxStatusCompensated,
			rte.TxStatusCompensationFailed,
			rte.TxStatusTimeout,
			rte.TxStatusCancelled,
		}

		// Only FAILED status is valid for retry
		validRetryStates := map[rte.TxStatus]bool{
			rte.TxStatusFailed: true,
		}

		// Generate random transaction with random status
		txID := fmt.Sprintf("tx-%d", rapid.IntRange(1, 10000).Draw(rt, "txID"))
		statusIdx := rapid.IntRange(0, len(allStatuses)-1).Draw(rt, "statusIdx")
		status := allStatuses[statusIdx]

		tx := rte.NewStoreTx(txID, "test-type", []string{"step1"})
		tx.Status = status
		tx.MaxRetries = 10
		tx.RetryCount = rapid.IntRange(0, 5).Draw(rt, "retryCount")
		tx.Context = &rte.StoreTxContext{
			TxID:   txID,
			TxType: "test-type",
			Input:  make(map[string]any),
			Output: make(map[string]any),
		}
		store.CreateTransaction(context.Background(), tx)

		// Create step record
		step := rte.NewStoreStepRecord(txID, 0, "step1")
		store.CreateStep(context.Background(), step)

		// Test retry operation
		retryErr := admin.RetryTransaction(context.Background(), txID)

		
		if validRetryStates[status] {
			// Should succeed
			if retryErr != nil {
				rt.Fatalf("retry should succeed for status %s, but got error: %v", status, retryErr)
			}
		} else {
			// Should fail with invalid state error
			if retryErr == nil {
				rt.Fatalf("retry should fail for status %s, but succeeded", status)
			}
			if !strings.Contains(retryErr.Error(), "invalid transaction state") &&
				!strings.Contains(retryErr.Error(), "can only retry failed") {
				rt.Fatalf("expected invalid state error for status %s, got: %v", status, retryErr)
			}
		}
	})
}

// TestProperty_AdminRetryMaxRetriesValidation tests that retry is rejected when max retries exceeded
func TestProperty_AdminRetryMaxRetriesValidation(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		store := newMockStore()
		eventBus := event.NewMemoryEventBus()

		// Create coordinator with test step
		coord := rte.NewCoordinator(
			rte.WithStore(store),
			rte.WithEventBus(eventBus),
			rte.WithCoordinatorConfig(rte.Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       10,
				RetryInterval:    100 * time.Millisecond,
			}),
		)

		// Register a step that always succeeds
		testStep := newTestStep("step1")
		testStep.executeFunc = func(ctx context.Context, txCtx *rte.TxContext) error {
			return nil
		}
		coord.RegisterStep(testStep)

		admin := NewAdmin(
			WithAdminStore(store),
			WithAdminEventBus(eventBus),
			WithAdminCoordinator(coord),
		)

		// Generate random max retries and retry count
		maxRetries := rapid.IntRange(1, 10).Draw(rt, "maxRetries")
		// Generate retry count that may or may not exceed max
		retryCount := rapid.IntRange(0, maxRetries+5).Draw(rt, "retryCount")

		txID := fmt.Sprintf("tx-%d", rapid.IntRange(1, 10000).Draw(rt, "txID"))

		tx := rte.NewStoreTx(txID, "test-type", []string{"step1"})
		tx.Status = rte.TxStatusFailed
		tx.MaxRetries = maxRetries
		tx.RetryCount = retryCount
		tx.Context = &rte.StoreTxContext{
			TxID:   txID,
			TxType: "test-type",
			Input:  make(map[string]any),
			Output: make(map[string]any),
		}
		store.CreateTransaction(context.Background(), tx)

		// Create step record
		step := rte.NewStoreStepRecord(txID, 0, "step1")
		store.CreateStep(context.Background(), step)

		// Test retry operation
		retryErr := admin.RetryTransaction(context.Background(), txID)

		
		if retryCount >= maxRetries {
			// Should fail with max retries exceeded error
			if retryErr == nil {
				rt.Fatalf("retry should fail when retryCount(%d) >= maxRetries(%d), but succeeded", retryCount, maxRetries)
			}
			if !strings.Contains(retryErr.Error(), "max retries") &&
				!strings.Contains(retryErr.Error(), "exceeded") &&
				!strings.Contains(retryErr.Error(), "retry limit") {
				rt.Fatalf("expected max retries exceeded error, got: %v", retryErr)
			}
		} else {
			// Should succeed
			if retryErr != nil {
				rt.Fatalf("retry should succeed when retryCount(%d) < maxRetries(%d), but got error: %v", retryCount, maxRetries, retryErr)
			}
		}
	})
}
