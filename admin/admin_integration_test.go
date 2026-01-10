// Package admin provides integration tests for the RTE Admin Dashboard.
// These tests validate complete HTTP request/response flows and page rendering.
package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"rte"
	"strings"
	"testing"
	"time"

	"rte/event"
)

// ============================================================================
// Integration Test Helpers
// ============================================================================

// testServerSetup creates a fully configured AdminServer for integration testing
func testServerSetup(t *testing.T) (*AdminServer, *mockStore, *EventStore) {
	t.Helper()

	store := newMockStore()
	eventBus := event.NewMemoryEventBus()
	breaker := newMockBreaker()
	eventStore := NewEventStore(100)

	admin := NewAdmin(
		WithAdminStore(store),
		WithAdminEventBus(eventBus),
		WithAdminBreaker(breaker),
	)

	server := NewAdminServer(
		WithAdminImpl(admin),
		WithServerStore(store),
		WithServerBreaker(breaker),
		WithServerEventBus(eventBus),
		WithEventStore(eventStore),
	)

	return server, store, eventStore
}

// ============================================================================
// API Integration Tests - Transaction List
// ============================================================================

func TestIntegration_API_TransactionList(t *testing.T) {
	server, store, _ := testServerSetup(t)
	handler := server.Handler()

	// Create test transactions with various statuses and types
	createTestTransaction(store, "tx-1", "transfer", rte.TxStatusCompleted, []string{"step1", "step2"})
	createTestTransaction(store, "tx-2", "transfer", rte.TxStatusFailed, []string{"step1"})
	createTestTransaction(store, "tx-3", "payment", rte.TxStatusExecuting, []string{"step1", "step2", "step3"})
	createTestTransaction(store, "tx-4", "payment", rte.TxStatusCompleted, []string{"step1"})
	createTestTransaction(store, "tx-5", "refund", rte.TxStatusCompensated, []string{"step1", "step2"})

	t.Run("List_All_Transactions", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if !resp.Success {
			t.Error("expected success to be true")
		}

		// Verify response structure
		data, ok := resp.Data.(map[string]interface{})
		if !ok {
			t.Fatal("expected data to be a map")
		}

		transactions, ok := data["transactions"].([]interface{})
		if !ok {
			t.Fatal("expected transactions to be an array")
		}

		if len(transactions) != 5 {
			t.Errorf("expected 5 transactions, got %d", len(transactions))
		}

		// Verify pagination info
		if total, ok := data["total"].(float64); !ok || total != 5 {
			t.Errorf("expected total to be 5, got %v", data["total"])
		}
	})

	t.Run("Filter_By_Status", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions?status=COMPLETED", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)

		data := resp.Data.(map[string]interface{})
		if total := data["total"].(float64); total != 2 {
			t.Errorf("expected 2 completed transactions, got %v", total)
		}
	})

	t.Run("Filter_By_Type", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions?tx_type=transfer", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)

		data := resp.Data.(map[string]interface{})
		if total := data["total"].(float64); total != 2 {
			t.Errorf("expected 2 transfer transactions, got %v", total)
		}
	})

	t.Run("Pagination", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions?page=1&page_size=2", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)

		data := resp.Data.(map[string]interface{})
		transactions := data["transactions"].([]interface{})

		if len(transactions) != 2 {
			t.Errorf("expected 2 transactions per page, got %d", len(transactions))
		}

		if pageSize := data["page_size"].(float64); pageSize != 2 {
			t.Errorf("expected page_size 2, got %v", pageSize)
		}
	})
}

// ============================================================================
// API Integration Tests - Transaction Detail
// ============================================================================

func TestIntegration_API_TransactionDetail(t *testing.T) {
	server, store, _ := testServerSetup(t)
	handler := server.Handler()

	// Create a transaction with steps and context
	tx := rte.NewStoreTx("tx-detail-1", "transfer", []string{"validate", "debit", "credit"})
	tx.Status = rte.TxStatusExecuting
	tx.Context = &rte.StoreTxContext{
		TxID:   "tx-detail-1",
		TxType: "transfer",
		Input:  map[string]any{"amount": 100, "from": "A", "to": "B"},
		Output: map[string]any{"result": "pending"},
	}
	store.CreateTransaction(context.Background(), tx)

	// Create step records
	for i, name := range []string{"validate", "debit", "credit"} {
		step := rte.NewStoreStepRecord("tx-detail-1", i, name)
		if i == 0 {
			step.Status = rte.StepStatusCompleted
			step.Input = []byte(`{"amount": 100}`)
			step.Output = []byte(`{"valid": true}`)
		}
		store.CreateStep(context.Background(), step)
	}

	t.Run("Get_Transaction_Detail", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions/tx-detail-1", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)

		if !resp.Success {
			t.Error("expected success to be true")
		}

		data := resp.Data.(map[string]interface{})
		txData := data["transaction"].(map[string]interface{})

		// Verify transaction fields
		if txData["tx_id"] != "tx-detail-1" {
			t.Errorf("expected tx_id tx-detail-1, got %v", txData["tx_id"])
		}
		if txData["tx_type"] != "transfer" {
			t.Errorf("expected tx_type transfer, got %v", txData["tx_type"])
		}
		if txData["status"] != "EXECUTING" {
			t.Errorf("expected status EXECUTING, got %v", txData["status"])
		}

		// Verify steps
		steps := data["steps"].([]interface{})
		if len(steps) != 3 {
			t.Errorf("expected 3 steps, got %d", len(steps))
		}

		// Verify input/output
		input := txData["input"].(map[string]interface{})
		if input["amount"].(float64) != 100 {
			t.Errorf("expected input amount 100, got %v", input["amount"])
		}
	})

	t.Run("Get_NonExistent_Transaction", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions/non-existent", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Code)
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)

		if resp.Success {
			t.Error("expected success to be false")
		}
		if resp.Error.Code != ErrCodeTransactionNotFound {
			t.Errorf("expected error code %s, got %s", ErrCodeTransactionNotFound, resp.Error.Code)
		}
	})
}

// ============================================================================
// API Integration Tests - Transaction Operations
// ============================================================================

func TestIntegration_API_TransactionOperations(t *testing.T) {
	server, store, _ := testServerSetup(t)
	handler := server.Handler()

	t.Run("Force_Complete_Stuck_Transaction", func(t *testing.T) {
		createTestTransaction(store, "tx-stuck-1", "transfer", rte.TxStatusExecuting, []string{"step1"})

		body := strings.NewReader(`{"reason": "manual intervention by admin"}`)
		req := httptest.NewRequest("POST", "/api/transactions/tx-stuck-1/force-complete", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)

		if !resp.Success {
			t.Error("expected success to be true")
		}

		// Verify transaction status changed
		tx, _ := store.GetTransaction(context.Background(), "tx-stuck-1")
		if tx.Status != rte.TxStatusCompleted {
			t.Errorf("expected status COMPLETED, got %s", tx.Status)
		}
	})

	t.Run("Force_Complete_Invalid_Status", func(t *testing.T) {
		createTestTransaction(store, "tx-completed-1", "transfer", rte.TxStatusCompleted, []string{"step1"})

		body := strings.NewReader(`{"reason": "test"}`)
		req := httptest.NewRequest("POST", "/api/transactions/tx-completed-1/force-complete", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Code)
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)

		if resp.Success {
			t.Error("expected success to be false")
		}
	})

	t.Run("Force_Cancel_Transaction", func(t *testing.T) {
		createTestTransaction(store, "tx-cancel-1", "transfer", rte.TxStatusCreated, []string{"step1"})

		body := strings.NewReader(`{"reason": "user requested cancellation"}`)
		req := httptest.NewRequest("POST", "/api/transactions/tx-cancel-1/force-cancel", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
		}

		// Verify transaction status changed
		tx, _ := store.GetTransaction(context.Background(), "tx-cancel-1")
		if tx.Status != rte.TxStatusCancelled {
			t.Errorf("expected status CANCELLED, got %s", tx.Status)
		}
	})

	t.Run("Force_Cancel_Invalid_Status", func(t *testing.T) {
		createTestTransaction(store, "tx-done-1", "transfer", rte.TxStatusCompleted, []string{"step1"})

		body := strings.NewReader(`{"reason": "test"}`)
		req := httptest.NewRequest("POST", "/api/transactions/tx-done-1/force-cancel", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Code)
		}
	})
}

// ============================================================================
// API Integration Tests - Statistics
// ============================================================================

func TestIntegration_API_Stats(t *testing.T) {
	server, store, _ := testServerSetup(t)
	handler := server.Handler()

	// Create transactions with various statuses
	createTestTransaction(store, "stat-1", "transfer", rte.TxStatusCompleted, []string{"step1"})
	createTestTransaction(store, "stat-2", "transfer", rte.TxStatusCompleted, []string{"step1"})
	createTestTransaction(store, "stat-3", "payment", rte.TxStatusFailed, []string{"step1"})
	createTestTransaction(store, "stat-4", "payment", rte.TxStatusExecuting, []string{"step1"})
	createTestTransaction(store, "stat-5", "refund", rte.TxStatusCompensated, []string{"step1"})
	createTestTransaction(store, "stat-6", "refund", rte.TxStatusCreated, []string{"step1"})

	t.Run("Get_Stats", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/stats", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)

		if !resp.Success {
			t.Error("expected success to be true")
		}

		data := resp.Data.(map[string]interface{})

		// Verify stats
		if total := data["total_transactions"].(float64); total != 6 {
			t.Errorf("expected total_transactions 6, got %v", total)
		}
		if completed := data["completed_transactions"].(float64); completed != 2 {
			t.Errorf("expected completed_transactions 2, got %v", completed)
		}
		if failed := data["failed_transactions"].(float64); failed != 1 {
			t.Errorf("expected failed_transactions 1, got %v", failed)
		}
		if pending := data["pending_transactions"].(float64); pending != 2 {
			t.Errorf("expected pending_transactions 2, got %v", pending)
		}
		if compensated := data["compensated_transactions"].(float64); compensated != 1 {
			t.Errorf("expected compensated_transactions 1, got %v", compensated)
		}
	})
}

// ============================================================================
// API Integration Tests - Circuit Breakers
// ============================================================================

func TestIntegration_API_CircuitBreakers(t *testing.T) {
	server, _, _ := testServerSetup(t)
	handler := server.Handler()

	t.Run("Get_Circuit_Breakers", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/circuit-breakers", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)

		if !resp.Success {
			t.Error("expected success to be true")
		}
	})

	t.Run("Reset_Circuit_Breaker", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/circuit-breakers/payment-service/reset", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)

		if !resp.Success {
			t.Error("expected success to be true")
		}
	})
}

// ============================================================================
// API Integration Tests - Events
// ============================================================================

func TestIntegration_API_Events(t *testing.T) {
	server, _, eventStore := testServerSetup(t)
	handler := server.Handler()

	// Add test events
	eventStore.Store(event.Event{
		Type:      event.EventTxCreated,
		TxID:      "tx-event-1",
		TxType:    "transfer",
		Timestamp: time.Now(),
	})
	eventStore.Store(event.Event{
		Type:      event.EventStepCompleted,
		TxID:      "tx-event-1",
		TxType:    "transfer",
		StepName:  "validate",
		Timestamp: time.Now(),
	})
	eventStore.Store(event.Event{
		Type:      event.EventTxCompleted,
		TxID:      "tx-event-1",
		TxType:    "transfer",
		Timestamp: time.Now(),
	})
	eventStore.Store(event.Event{
		Type:      event.EventTxFailed,
		TxID:      "tx-event-2",
		TxType:    "payment",
		Timestamp: time.Now(),
		Error:     errMockFailure,
	})

	t.Run("List_All_Events", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/events", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)

		if !resp.Success {
			t.Error("expected success to be true")
		}

		data := resp.Data.(map[string]interface{})
		events := data["events"].([]interface{})

		if len(events) != 4 {
			t.Errorf("expected 4 events, got %d", len(events))
		}
	})

	t.Run("Filter_Events_By_Type", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/events?type=tx.completed", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)

		data := resp.Data.(map[string]interface{})
		events := data["events"].([]interface{})

		if len(events) != 1 {
			t.Errorf("expected 1 tx.completed event, got %d", len(events))
		}
	})

	t.Run("Filter_Events_By_TxID", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/events?tx_id=tx-event-1", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)

		data := resp.Data.(map[string]interface{})
		events := data["events"].([]interface{})

		if len(events) != 3 {
			t.Errorf("expected 3 events for tx-event-1, got %d", len(events))
		}
	})
}

// ============================================================================
// Page Rendering Integration Tests
// ============================================================================

func TestIntegration_Page_Rendering(t *testing.T) {
	server, store, eventStore := testServerSetup(t)
	handler := server.Handler()

	// Create test data
	createTestTransaction(store, "page-tx-1", "transfer", rte.TxStatusCompleted, []string{"step1"})
	createTestTransaction(store, "page-tx-2", "payment", rte.TxStatusFailed, []string{"step1", "step2"})

	eventStore.Store(event.Event{
		Type:      event.EventTxCreated,
		TxID:      "page-tx-1",
		TxType:    "transfer",
		Timestamp: time.Now(),
	})

	t.Run("Index_Page", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		body := w.Body.String()

		// Verify page contains required elements
		if !strings.Contains(body, "RTE 管理控制台") {
			t.Error("expected page to contain 'RTE 管理控制台'")
		}

		// Verify navigation
		if !strings.Contains(body, "仪表盘") {
			t.Error("expected page to contain '仪表盘' navigation")
		}
		if !strings.Contains(body, "事务列表") {
			t.Error("expected page to contain '事务列表' navigation")
		}
	})

	t.Run("Transactions_Page", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/transactions", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		body := w.Body.String()

		// Verify page contains transaction list elements
		if !strings.Contains(body, "事务列表") {
			t.Error("expected page to contain '事务列表'")
		}
	})

	t.Run("Transaction_Detail_Page", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/transactions/page-tx-1", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		body := w.Body.String()

		// Debug: print the response if it's not what we expect
		if w.Code != http.StatusOK {
			t.Logf("Response body: %s", body)
			t.Errorf("expected status 200, got %d", w.Code)
			return
		}

		// Verify page contains transaction detail elements
		if !strings.Contains(body, "事务详情") {
			t.Logf("Response body: %s", body[:min(len(body), 500)])
			t.Error("expected page to contain '事务详情'")
		}
	})

	t.Run("Recovery_Page", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/recovery", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		body := w.Body.String()

		// Verify page contains recovery elements
		if !strings.Contains(body, "恢复监控") {
			t.Error("expected page to contain '恢复监控'")
		}
	})

	t.Run("Circuit_Breakers_Page", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/circuit-breakers", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		body := w.Body.String()

		// Verify page contains circuit breaker elements
		if !strings.Contains(body, "熔断器") {
			t.Error("expected page to contain '熔断器'")
		}
	})

	t.Run("Events_Page", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/events", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		body := w.Body.String()

		// Verify page contains event log elements
		if !strings.Contains(body, "事件日志") {
			t.Error("expected page to contain '事件日志'")
		}
	})

	t.Run("NonExistent_Transaction_Page", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/transactions/non-existent", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Code)
		}
	})
}

// ============================================================================
// API Response Format Integration Tests
// ============================================================================

func TestIntegration_API_ResponseFormat(t *testing.T) {
	server, store, _ := testServerSetup(t)
	handler := server.Handler()

	createTestTransaction(store, "format-tx-1", "transfer", rte.TxStatusCompleted, []string{"step1"})

	t.Run("Success_Response_Format", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		// Verify Content-Type
		contentType := w.Header().Get("Content-Type")
		if !strings.Contains(contentType, "application/json") {
			t.Errorf("expected Content-Type application/json, got %s", contentType)
		}

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		// Verify success response structure
		if !resp.Success {
			t.Error("expected success to be true")
		}
		if resp.Data == nil {
			t.Error("expected data to be present")
		}
		if resp.Error != nil {
			t.Error("expected error to be nil for success response")
		}
	})

	t.Run("Error_Response_Format", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions/non-existent", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		// Verify error response structure
		if resp.Success {
			t.Error("expected success to be false")
		}
		if resp.Error == nil {
			t.Error("expected error to be present")
		}
		if resp.Error.Code == "" {
			t.Error("expected error code to be present")
		}
		if resp.Error.Message == "" {
			t.Error("expected error message to be present")
		}
	})

	t.Run("Invalid_Request_Error_Format", func(t *testing.T) {
		body := strings.NewReader(`invalid json`)
		req := httptest.NewRequest("POST", "/api/transactions/format-tx-1/force-complete", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Code)
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)

		if resp.Success {
			t.Error("expected success to be false")
		}
		if resp.Error == nil || resp.Error.Code != ErrCodeInvalidRequest {
			t.Errorf("expected error code %s", ErrCodeInvalidRequest)
		}
	})
}

// ============================================================================
// Navigation Integration Tests
// ============================================================================

func TestIntegration_Navigation(t *testing.T) {
	server, _, _ := testServerSetup(t)
	handler := server.Handler()

	pages := []struct {
		path        string
		currentPage string
		title       string
	}{
		{"/", "index", "仪表盘"},
		{"/transactions", "transactions", "事务列表"},
		{"/recovery", "recovery", "恢复监控"},
		{"/circuit-breakers", "circuit-breakers", "熔断器"},
		{"/events", "events", "事件日志"},
	}

	for _, page := range pages {
		t.Run("Navigation_"+page.currentPage, func(t *testing.T) {
			req := httptest.NewRequest("GET", page.path, nil)
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Errorf("expected status 200, got %d", w.Code)
			}

			body := w.Body.String()

			// Verify navigation links are present
			navLinks := []string{"仪表盘", "事务列表", "恢复监控", "熔断器", "事件日志"}
			for _, link := range navLinks {
				if !strings.Contains(body, link) {
					t.Errorf("expected navigation to contain '%s'", link)
				}
			}

			// Verify current page is highlighted
			// The active class should be present for the current page
			if !strings.Contains(body, "active") {
				t.Error("expected active navigation item")
			}
		})
	}
}

// ============================================================================
// End-to-End Workflow Integration Tests
// ============================================================================

func TestIntegration_EndToEnd_TransactionWorkflow(t *testing.T) {
	server, store, eventStore := testServerSetup(t)
	handler := server.Handler()

	// Subscribe to events
	eventStore.Store(event.Event{
		Type:      event.EventTxCreated,
		TxID:      "e2e-tx-1",
		TxType:    "transfer",
		Timestamp: time.Now(),
	})

	t.Run("Complete_Transaction_Workflow", func(t *testing.T) {
		// Step 1: Create a transaction in EXECUTING state
		createTestTransaction(store, "e2e-tx-1", "transfer", rte.TxStatusExecuting, []string{"validate", "execute"})

		// Step 2: View transaction list via API
		req := httptest.NewRequest("GET", "/api/transactions", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("list transactions failed: %d", w.Code)
		}

		// Step 3: View transaction detail via API
		req = httptest.NewRequest("GET", "/api/transactions/e2e-tx-1", nil)
		w = httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("get transaction detail failed: %d", w.Code)
		}

		// Step 4: Force complete the transaction
		body := strings.NewReader(`{"reason": "e2e test completion"}`)
		req = httptest.NewRequest("POST", "/api/transactions/e2e-tx-1/force-complete", body)
		req.Header.Set("Content-Type", "application/json")
		w = httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("force complete failed: %d - %s", w.Code, w.Body.String())
		}

		// Step 5: Verify transaction is completed
		tx, _ := store.GetTransaction(context.Background(), "e2e-tx-1")
		if tx.Status != rte.TxStatusCompleted {
			t.Errorf("expected status COMPLETED, got %s", tx.Status)
		}

		// Step 6: Verify stats reflect the change
		req = httptest.NewRequest("GET", "/api/stats", nil)
		w = httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("get stats failed: %d", w.Code)
		}

		var resp APIResponse
		json.NewDecoder(w.Body).Decode(&resp)
		data := resp.Data.(map[string]interface{})

		if completed := data["completed_transactions"].(float64); completed < 1 {
			t.Error("expected at least 1 completed transaction")
		}
	})
}

// ============================================================================
// Error Handling Integration Tests
// ============================================================================

func TestIntegration_ErrorHandling(t *testing.T) {
	server, _, _ := testServerSetup(t)
	handler := server.Handler()

	t.Run("Missing_Transaction_ID", func(t *testing.T) {
		// This tests the path value extraction
		req := httptest.NewRequest("GET", "/api/transactions/", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		// Should return the list, not an error
		if w.Code != http.StatusOK {
			t.Errorf("expected status 200 for list, got %d", w.Code)
		}
	})

	t.Run("Invalid_JSON_Body", func(t *testing.T) {
		body := strings.NewReader(`{invalid}`)
		req := httptest.NewRequest("POST", "/api/transactions/test-tx/force-complete", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Code)
		}
	})

	t.Run("Empty_Service_Name_For_Circuit_Breaker", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/circuit-breakers//reset", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		// Empty service name should be handled
		if w.Code == http.StatusOK {
			// If it succeeds, verify the response
			var resp APIResponse
			json.NewDecoder(w.Body).Decode(&resp)
			if !resp.Success {
				t.Error("expected success for empty service name handling")
			}
		}
	})
}
