// Package admin provides administrative interfaces for the RTE engine.
package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sync"
	"time"

	"rte"
	"rte/circuit"
	"rte/event"
	"rte/recovery"
)

// AdminServer 管理服务器
type AdminServer struct {
	addr        string
	admin       *AdminImpl
	store       rte.TxStore
	coordinator *rte.Coordinator
	breaker     circuit.Breaker
	recovery    *recovery.Worker
	events      event.EventBus
	eventStore  *EventStore
	templates   *template.Template
	mux         *http.ServeMux
	server      *http.Server

	// Handlers
	pageHandler PageHandler
	apiHandler  *APIHandler

	// State
	mu      sync.Mutex
	running bool
}

// AdminServerOption 配置选项
type AdminServerOption func(*AdminServer)

// WithAddr sets the server address.
func WithAddr(addr string) AdminServerOption {
	return func(s *AdminServer) {
		s.addr = addr
	}
}

// WithAdminImpl sets the admin implementation.
func WithAdminImpl(admin *AdminImpl) AdminServerOption {
	return func(s *AdminServer) {
		s.admin = admin
	}
}

// WithServerStore sets the store for the server.
func WithServerStore(store rte.TxStore) AdminServerOption {
	return func(s *AdminServer) {
		s.store = store
	}
}

// WithServerCoordinator sets the coordinator for the server.
func WithServerCoordinator(coord *rte.Coordinator) AdminServerOption {
	return func(s *AdminServer) {
		s.coordinator = coord
	}
}

// WithServerBreaker sets the circuit breaker for the server.
func WithServerBreaker(breaker circuit.Breaker) AdminServerOption {
	return func(s *AdminServer) {
		s.breaker = breaker
	}
}

// WithServerRecovery sets the recovery worker for the server.
func WithServerRecovery(recovery *recovery.Worker) AdminServerOption {
	return func(s *AdminServer) {
		s.recovery = recovery
	}
}

// WithServerEventBus sets the event bus for the server.
func WithServerEventBus(events event.EventBus) AdminServerOption {
	return func(s *AdminServer) {
		s.events = events
	}
}

// WithTemplates sets the HTML templates for the server.
func WithTemplates(templates *template.Template) AdminServerOption {
	return func(s *AdminServer) {
		s.templates = templates
	}
}

// WithEventStore sets the event store for the server.
func WithEventStore(eventStore *EventStore) AdminServerOption {
	return func(s *AdminServer) {
		s.eventStore = eventStore
	}
}

// NewAdminServer 创建管理服务器
func NewAdminServer(opts ...AdminServerOption) *AdminServer {
	s := &AdminServer{
		addr: ":8080",
		mux:  http.NewServeMux(),
	}

	for _, opt := range opts {
		opt(s)
	}

	// Initialize handlers
	s.initHandlers()

	// Setup routes
	s.setupRoutes()

	return s
}

// initHandlers initializes the page and API handlers.
func (s *AdminServer) initHandlers() {
	// Create page handler with templates
	pageHandler, err := NewPageHandler(s.admin, s.recovery, s.breaker, s.eventStore)
	if err != nil {
		// Fallback to a simple handler if templates fail to load
		s.pageHandler = &fallbackPageHandler{}
	} else {
		// If custom templates were provided, use them
		if s.templates != nil {
			pageHandler.SetTemplates(s.templates)
		}
		s.pageHandler = pageHandler
	}

	s.apiHandler = &APIHandler{
		admin:    s.admin,
		recovery: s.recovery,
		breaker:  s.breaker,
		events:   s.eventStore,
	}
}

// fallbackPageHandler provides simple fallback responses when templates fail to load
type fallbackPageHandler struct{}

func (h *fallbackPageHandler) HandleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte("<html><body><h1>RTE 管理控制台</h1><p>模板加载失败</p></body></html>"))
}

func (h *fallbackPageHandler) HandleTransactions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte("<html><body><h1>事务列表</h1><p>模板加载失败</p></body></html>"))
}

func (h *fallbackPageHandler) HandleTransactionDetail(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte("<html><body><h1>事务详情</h1><p>模板加载失败</p></body></html>"))
}

func (h *fallbackPageHandler) HandleRecovery(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte("<html><body><h1>恢复监控</h1><p>模板加载失败</p></body></html>"))
}

func (h *fallbackPageHandler) HandleCircuitBreakers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte("<html><body><h1>熔断器面板</h1><p>模板加载失败</p></body></html>"))
}

func (h *fallbackPageHandler) HandleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte("<html><body><h1>事件日志</h1><p>模板加载失败</p></body></html>"))
}

// setupRoutes configures all HTTP routes.
func (s *AdminServer) setupRoutes() {
	// Page routes
	s.mux.HandleFunc("GET /", s.pageHandler.HandleIndex)
	s.mux.HandleFunc("GET /transactions", s.pageHandler.HandleTransactions)
	s.mux.HandleFunc("GET /transactions/{txID}", s.pageHandler.HandleTransactionDetail)
	s.mux.HandleFunc("GET /recovery", s.pageHandler.HandleRecovery)
	s.mux.HandleFunc("GET /circuit-breakers", s.pageHandler.HandleCircuitBreakers)
	s.mux.HandleFunc("GET /events", s.pageHandler.HandleEvents)

	// API routes - Transaction operations
	s.mux.HandleFunc("GET /api/transactions", s.apiHandler.HandleListTransactions)
	s.mux.HandleFunc("GET /api/transactions/{txID}", s.apiHandler.HandleGetTransaction)
	s.mux.HandleFunc("POST /api/transactions/{txID}/force-complete", s.apiHandler.HandleForceComplete)
	s.mux.HandleFunc("POST /api/transactions/{txID}/force-cancel", s.apiHandler.HandleForceCancel)
	s.mux.HandleFunc("POST /api/transactions/{txID}/retry", s.apiHandler.HandleRetry)

	// API routes - Stats
	s.mux.HandleFunc("GET /api/stats", s.apiHandler.HandleGetStats)
	s.mux.HandleFunc("GET /api/recovery/stats", s.apiHandler.HandleGetRecoveryStats)

	// API routes - Circuit breakers
	s.mux.HandleFunc("GET /api/circuit-breakers", s.apiHandler.HandleGetCircuitBreakers)
	s.mux.HandleFunc("POST /api/circuit-breakers/{service}/reset", s.apiHandler.HandleResetCircuitBreaker)

	// API routes - Events
	s.mux.HandleFunc("GET /api/events", s.apiHandler.HandleListEvents)
}

// Start 启动服务器
func (s *AdminServer) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}
	s.running = true
	s.mu.Unlock()

	s.server = &http.Server{
		Addr:    s.addr,
		Handler: s.mux,
	}

	return s.server.ListenAndServe()
}

// Stop 停止服务器
func (s *AdminServer) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// Handler returns the HTTP handler for testing.
func (s *AdminServer) Handler() http.Handler {
	return s.mux
}

// PageHandler 页面处理器接口
type PageHandler interface {
	HandleIndex(w http.ResponseWriter, r *http.Request)
	HandleTransactions(w http.ResponseWriter, r *http.Request)
	HandleTransactionDetail(w http.ResponseWriter, r *http.Request)
	HandleRecovery(w http.ResponseWriter, r *http.Request)
	HandleCircuitBreakers(w http.ResponseWriter, r *http.Request)
	HandleEvents(w http.ResponseWriter, r *http.Request)
}

// APIHandler API处理器
type APIHandler struct {
	admin    *AdminImpl
	recovery *recovery.Worker
	breaker  circuit.Breaker
	events   *EventStore
}

// APIResponse 统一API响应格式
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   *APIError   `json:"error,omitempty"`
}

// APIError API错误信息
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error codes
const (
	ErrCodeInvalidRequest      = "INVALID_REQUEST"
	ErrCodeTransactionNotFound = "TRANSACTION_NOT_FOUND"
	ErrCodeInvalidOperation    = "INVALID_OPERATION"
	ErrCodeOperationFailed     = "OPERATION_FAILED"
	ErrCodeInternalError       = "INTERNAL_ERROR"
	ErrCodeServiceNotFound     = "SERVICE_NOT_FOUND"
)

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// writeSuccess writes a successful JSON response.
func writeSuccess(w http.ResponseWriter, data interface{}) {
	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    data,
	})
}

// writeError writes an error JSON response.
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, APIResponse{
		Success: false,
		Error: &APIError{
			Code:    code,
			Message: message,
		},
	})
}

// HandleListTransactions GET /api/transactions
func (h *APIHandler) HandleListTransactions(w http.ResponseWriter, r *http.Request) {
	if h.admin == nil {
		writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "admin not configured")
		return
	}

	// Parse query parameters
	filter := parseTransactionFilter(r)

	result, err := h.admin.ListTransactions(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeInternalError, err.Error())
		return
	}

	// Convert to response format
	response := TransactionListResponse{
		Transactions: make([]TransactionSummary, 0, len(result.Transactions)),
		Total:        result.Total,
		Page:         (filter.Offset / filter.Limit) + 1,
		PageSize:     filter.Limit,
		TotalPages:   int((result.Total + int64(filter.Limit) - 1) / int64(filter.Limit)),
	}

	for _, tx := range result.Transactions {
		response.Transactions = append(response.Transactions, TransactionSummary{
			TxID:        tx.TxID,
			TxType:      tx.TxType,
			Status:      string(tx.Status),
			CurrentStep: tx.CurrentStep,
			TotalSteps:  tx.TotalSteps,
			RetryCount:  tx.RetryCount,
			CreatedAt:   tx.CreatedAt,
			UpdatedAt:   tx.UpdatedAt,
		})
	}

	writeSuccess(w, response)
}

// parseTransactionFilter parses query parameters into a StoreTxFilter.
func parseTransactionFilter(r *http.Request) *rte.StoreTxFilter {
	filter := &rte.StoreTxFilter{
		Limit:  20,
		Offset: 0,
	}

	// Parse status
	if statuses := r.URL.Query()["status"]; len(statuses) > 0 {
		for _, s := range statuses {
			filter.Status = append(filter.Status, rte.TxStatus(s))
		}
	}

	// Parse tx_type
	if txType := r.URL.Query().Get("tx_type"); txType != "" {
		filter.TxType = txType
	}

	// Parse time range
	if startTime := r.URL.Query().Get("start_time"); startTime != "" {
		if t, err := time.Parse(time.RFC3339, startTime); err == nil {
			filter.StartTime = t
		}
	}
	if endTime := r.URL.Query().Get("end_time"); endTime != "" {
		if t, err := time.Parse(time.RFC3339, endTime); err == nil {
			filter.EndTime = t
		}
	}

	// Parse pagination
	if page := r.URL.Query().Get("page"); page != "" {
		var p int
		fmt.Sscanf(page, "%d", &p)
		if p > 0 {
			filter.Offset = (p - 1) * filter.Limit
		}
	}
	if pageSize := r.URL.Query().Get("page_size"); pageSize != "" {
		var ps int
		fmt.Sscanf(pageSize, "%d", &ps)
		if ps > 0 && ps <= 100 {
			filter.Limit = ps
		}
	}

	return filter
}

// HandleGetTransaction GET /api/transactions/{txID}
func (h *APIHandler) HandleGetTransaction(w http.ResponseWriter, r *http.Request) {
	if h.admin == nil {
		writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "admin not configured")
		return
	}

	txID := r.PathValue("txID")
	if txID == "" {
		writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "transaction ID is required")
		return
	}

	detail, err := h.admin.GetTransaction(r.Context(), txID)
	if err != nil {
		if err == rte.ErrTransactionNotFound {
			writeError(w, http.StatusNotFound, ErrCodeTransactionNotFound, fmt.Sprintf("事务 %s 不存在", txID))
			return
		}
		writeError(w, http.StatusInternalServerError, ErrCodeInternalError, err.Error())
		return
	}

	// Convert to response format
	response := TransactionDetailResponse{
		Transaction: convertToTransactionDetail(detail.Transaction),
		Steps:       make([]StepDetail, 0, len(detail.Steps)),
	}

	for _, step := range detail.Steps {
		response.Steps = append(response.Steps, convertToStepDetail(step))
	}

	writeSuccess(w, response)
}

// convertToTransactionDetail converts a StoreTx to TransactionDetail.
func convertToTransactionDetail(tx *rte.StoreTx) TransactionDetail {
	detail := TransactionDetail{
		TxID:        tx.TxID,
		TxType:      tx.TxType,
		Status:      string(tx.Status),
		CurrentStep: tx.CurrentStep,
		TotalSteps:  tx.TotalSteps,
		StepNames:   tx.StepNames,
		LockKeys:    tx.LockKeys,
		ErrorMsg:    tx.ErrorMsg,
		RetryCount:  tx.RetryCount,
		MaxRetries:  tx.MaxRetries,
		Version:     tx.Version,
		CreatedAt:   tx.CreatedAt,
		UpdatedAt:   tx.UpdatedAt,
		LockedAt:    tx.LockedAt,
		CompletedAt: tx.CompletedAt,
		TimeoutAt:   tx.TimeoutAt,
	}

	if tx.Context != nil {
		detail.Input = tx.Context.Input
		detail.Output = tx.Context.Output
		detail.Metadata = tx.Context.Metadata
	}

	return detail
}

// convertToStepDetail converts a StoreStepRecord to StepDetail.
func convertToStepDetail(step *rte.StoreStepRecord) StepDetail {
	detail := StepDetail{
		StepIndex:      step.StepIndex,
		StepName:       step.StepName,
		Status:         string(step.Status),
		IdempotencyKey: step.IdempotencyKey,
		ErrorMsg:       step.ErrorMsg,
		RetryCount:     step.RetryCount,
		StartedAt:      step.StartedAt,
		CompletedAt:    step.CompletedAt,
	}

	// Parse input/output from JSON
	if len(step.Input) > 0 {
		json.Unmarshal(step.Input, &detail.Input)
	}
	if len(step.Output) > 0 {
		json.Unmarshal(step.Output, &detail.Output)
	}

	return detail
}

// HandleForceComplete POST /api/transactions/{txID}/force-complete
func (h *APIHandler) HandleForceComplete(w http.ResponseWriter, r *http.Request) {
	if h.admin == nil {
		writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "admin not configured")
		return
	}

	txID := r.PathValue("txID")
	if txID == "" {
		writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "transaction ID is required")
		return
	}

	// Parse request body
	var req OperationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "invalid request body")
		return
	}

	err := h.admin.ForceComplete(r.Context(), txID, req.Reason)
	if err != nil {
		if err == rte.ErrTransactionNotFound {
			writeError(w, http.StatusNotFound, ErrCodeTransactionNotFound, fmt.Sprintf("事务 %s 不存在", txID))
			return
		}
		writeError(w, http.StatusBadRequest, ErrCodeInvalidOperation, err.Error())
		return
	}

	writeSuccess(w, map[string]string{"message": "事务已强制完成"})
}

// HandleForceCancel POST /api/transactions/{txID}/force-cancel
func (h *APIHandler) HandleForceCancel(w http.ResponseWriter, r *http.Request) {
	if h.admin == nil {
		writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "admin not configured")
		return
	}

	txID := r.PathValue("txID")
	if txID == "" {
		writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "transaction ID is required")
		return
	}

	// Parse request body
	var req OperationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "invalid request body")
		return
	}

	err := h.admin.ForceCancel(r.Context(), txID, req.Reason)
	if err != nil {
		if err == rte.ErrTransactionNotFound {
			writeError(w, http.StatusNotFound, ErrCodeTransactionNotFound, fmt.Sprintf("事务 %s 不存在", txID))
			return
		}
		writeError(w, http.StatusBadRequest, ErrCodeInvalidOperation, err.Error())
		return
	}

	writeSuccess(w, map[string]string{"message": "事务已强制取消"})
}

// HandleRetry POST /api/transactions/{txID}/retry
func (h *APIHandler) HandleRetry(w http.ResponseWriter, r *http.Request) {
	if h.admin == nil {
		writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "admin not configured")
		return
	}

	txID := r.PathValue("txID")
	if txID == "" {
		writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "transaction ID is required")
		return
	}

	err := h.admin.RetryTransaction(r.Context(), txID)
	if err != nil {
		if err == rte.ErrTransactionNotFound {
			writeError(w, http.StatusNotFound, ErrCodeTransactionNotFound, fmt.Sprintf("事务 %s 不存在", txID))
			return
		}
		writeError(w, http.StatusBadRequest, ErrCodeInvalidOperation, err.Error())
		return
	}

	writeSuccess(w, map[string]string{"message": "事务重试已触发"})
}

// HandleGetStats GET /api/stats
func (h *APIHandler) HandleGetStats(w http.ResponseWriter, r *http.Request) {
	if h.admin == nil {
		writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "admin not configured")
		return
	}

	stats, err := h.admin.GetStats(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeInternalError, err.Error())
		return
	}

	response := StatsResponse{
		TotalTransactions:       stats.TotalTransactions,
		PendingTransactions:     stats.PendingTransactions,
		FailedTransactions:      stats.FailedTransactions,
		CompletedTransactions:   stats.CompletedTransactions,
		CompensatedTransactions: stats.CompensatedTransactions,
	}

	writeSuccess(w, response)
}

// HandleGetRecoveryStats GET /api/recovery/stats
func (h *APIHandler) HandleGetRecoveryStats(w http.ResponseWriter, r *http.Request) {
	if h.recovery == nil {
		writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "recovery worker not configured")
		return
	}

	stats := h.recovery.Stats()

	response := RecoveryStatsResponse{
		IsRunning:      stats.IsRunning,
		ScannedCount:   stats.ScannedCount,
		ProcessedCount: stats.ProcessedCount,
		FailedCount:    stats.FailedCount,
		Config:         RecoveryConfigInfo{
			// Config info will be populated when recovery worker exposes config
		},
	}

	writeSuccess(w, response)
}

// HandleGetCircuitBreakers GET /api/circuit-breakers
func (h *APIHandler) HandleGetCircuitBreakers(w http.ResponseWriter, r *http.Request) {
	if h.breaker == nil {
		writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "circuit breaker not configured")
		return
	}

	// Note: The current Breaker interface doesn't expose a way to list all breakers.
	// This will need to be enhanced in the circuit breaker implementation.
	// For now, return an empty list.
	response := []CircuitBreakerInfo{}

	writeSuccess(w, response)
}

// HandleResetCircuitBreaker POST /api/circuit-breakers/{service}/reset
func (h *APIHandler) HandleResetCircuitBreaker(w http.ResponseWriter, r *http.Request) {
	if h.breaker == nil {
		writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "circuit breaker not configured")
		return
	}

	service := r.PathValue("service")
	if service == "" {
		writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "service name is required")
		return
	}

	cb := h.breaker.Get(service)
	cb.Reset()

	writeSuccess(w, map[string]string{"message": fmt.Sprintf("熔断器 %s 已重置", service)})
}

// HandleListEvents GET /api/events
func (h *APIHandler) HandleListEvents(w http.ResponseWriter, r *http.Request) {
	if h.events == nil {
		writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "event store not configured")
		return
	}

	// Parse query parameters
	filter := parseEventFilter(r)

	// Get events
	events := h.events.List(filter)
	total := h.events.Count(filter)

	// Build response
	response := EventsListResponse{
		Events:     events,
		Total:      total,
		EventTypes: h.events.GetEventTypes(),
	}

	writeSuccess(w, response)
}

// parseEventFilter parses query parameters into an EventFilter.
func parseEventFilter(r *http.Request) EventFilter {
	filter := EventFilter{
		Limit:  100,
		Offset: 0,
	}

	// Parse type filter
	if eventType := r.URL.Query().Get("type"); eventType != "" {
		filter.Type = eventType
	}

	// Parse tx_id filter
	if txID := r.URL.Query().Get("tx_id"); txID != "" {
		filter.TxID = txID
	}

	// Parse pagination
	if limit := r.URL.Query().Get("limit"); limit != "" {
		var l int
		fmt.Sscanf(limit, "%d", &l)
		if l > 0 && l <= 1000 {
			filter.Limit = l
		}
	}
	if offset := r.URL.Query().Get("offset"); offset != "" {
		var o int
		fmt.Sscanf(offset, "%d", &o)
		if o >= 0 {
			filter.Offset = o
		}
	}

	return filter
}

// ============================================================================
// API Request/Response Models
// ============================================================================

// TransactionListResponse 事务列表响应
type TransactionListResponse struct {
	Transactions []TransactionSummary `json:"transactions"`
	Total        int64                `json:"total"`
	Page         int                  `json:"page"`
	PageSize     int                  `json:"page_size"`
	TotalPages   int                  `json:"total_pages"`
}

// TransactionSummary 事务摘要
type TransactionSummary struct {
	TxID        string    `json:"tx_id"`
	TxType      string    `json:"tx_type"`
	Status      string    `json:"status"`
	CurrentStep int       `json:"current_step"`
	TotalSteps  int       `json:"total_steps"`
	RetryCount  int       `json:"retry_count"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// TransactionDetailResponse 事务详情响应
type TransactionDetailResponse struct {
	Transaction TransactionDetail `json:"transaction"`
	Steps       []StepDetail      `json:"steps"`
}

// TransactionDetail 事务详情
type TransactionDetail struct {
	TxID        string            `json:"tx_id"`
	TxType      string            `json:"tx_type"`
	Status      string            `json:"status"`
	CurrentStep int               `json:"current_step"`
	TotalSteps  int               `json:"total_steps"`
	StepNames   []string          `json:"step_names"`
	LockKeys    []string          `json:"lock_keys"`
	Input       map[string]any    `json:"input"`
	Output      map[string]any    `json:"output"`
	Metadata    map[string]string `json:"metadata"`
	ErrorMsg    string            `json:"error_msg,omitempty"`
	RetryCount  int               `json:"retry_count"`
	MaxRetries  int               `json:"max_retries"`
	Version     int               `json:"version"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	LockedAt    *time.Time        `json:"locked_at,omitempty"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	TimeoutAt   *time.Time        `json:"timeout_at,omitempty"`
}

// StepDetail 步骤详情
type StepDetail struct {
	StepIndex      int            `json:"step_index"`
	StepName       string         `json:"step_name"`
	Status         string         `json:"status"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	Input          map[string]any `json:"input,omitempty"`
	Output         map[string]any `json:"output,omitempty"`
	ErrorMsg       string         `json:"error_msg,omitempty"`
	RetryCount     int            `json:"retry_count"`
	StartedAt      *time.Time     `json:"started_at,omitempty"`
	CompletedAt    *time.Time     `json:"completed_at,omitempty"`
}

// StatsResponse 统计响应
type StatsResponse struct {
	TotalTransactions       int64 `json:"total_transactions"`
	PendingTransactions     int64 `json:"pending_transactions"`
	FailedTransactions      int64 `json:"failed_transactions"`
	CompletedTransactions   int64 `json:"completed_transactions"`
	CompensatedTransactions int64 `json:"compensated_transactions"`
}

// RecoveryStatsResponse 恢复统计响应
type RecoveryStatsResponse struct {
	IsRunning      bool               `json:"is_running"`
	ScannedCount   int64              `json:"scanned_count"`
	ProcessedCount int64              `json:"processed_count"`
	FailedCount    int64              `json:"failed_count"`
	Config         RecoveryConfigInfo `json:"config"`
}

// RecoveryConfigInfo 恢复配置信息
type RecoveryConfigInfo struct {
	RecoveryInterval string `json:"recovery_interval"`
	StuckThreshold   string `json:"stuck_threshold"`
	MaxRetries       int    `json:"max_retries"`
}

// CircuitBreakerInfo 熔断器信息
type CircuitBreakerInfo struct {
	Service              string `json:"service"`
	State                string `json:"state"`
	Requests             int64  `json:"requests"`
	TotalSuccesses       int64  `json:"total_successes"`
	TotalFailures        int64  `json:"total_failures"`
	ConsecutiveSuccesses int64  `json:"consecutive_successes"`
	ConsecutiveFailures  int64  `json:"consecutive_failures"`
}

// OperationRequest 操作请求
type OperationRequest struct {
	Reason string `json:"reason"`
}

// EventsListResponse 事件列表响应
type EventsListResponse struct {
	Events     []StoredEvent `json:"events"`
	Total      int           `json:"total"`
	EventTypes []string      `json:"event_types"`
}
