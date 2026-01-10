// Package admin provides administrative interfaces for the RTE engine.
package admin

import (
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"rte"
	"rte/circuit"
	"rte/recovery"
)

//go:embed templates/*.html
var templatesFS embed.FS

// PageHandlerImpl 页面处理器实现
type PageHandlerImpl struct {
	templates  *template.Template
	admin      *AdminImpl
	recovery   *recovery.Worker
	breaker    circuit.Breaker
	eventStore *EventStore
}

// NewPageHandler 创建页面处理器
func NewPageHandler(admin *AdminImpl, recovery *recovery.Worker, breaker circuit.Breaker, eventStore *EventStore) (*PageHandlerImpl, error) {
	// Load templates
	templates, err := LoadTemplates()
	if err != nil {
		return nil, fmt.Errorf("failed to load templates: %w", err)
	}

	return &PageHandlerImpl{
		templates:  templates,
		admin:      admin,
		recovery:   recovery,
		breaker:    breaker,
		eventStore: eventStore,
	}, nil
}

// LoadTemplates 加载所有HTML模板
func LoadTemplates() (*template.Template, error) {
	// Create template with helper functions
	tmpl := template.New("").Funcs(TemplateFuncs())

	// First, parse the layout template
	layoutContent, err := templatesFS.ReadFile("templates/layout.html")
	if err != nil {
		return nil, fmt.Errorf("failed to read layout template: %w", err)
	}

	// Parse layout
	_, err = tmpl.New("layout.html").Parse(string(layoutContent))
	if err != nil {
		return nil, fmt.Errorf("failed to parse layout template: %w", err)
	}

	// Parse each content template separately
	// Each template will have its own "content" block that can be selected
	contentTemplates := []string{
		"templates/index.html",
		"templates/transactions.html",
		"templates/transaction_detail.html",
		"templates/recovery.html",
		"templates/circuit_breakers.html",
		"templates/events.html",
	}

	for _, pattern := range contentTemplates {
		content, err := templatesFS.ReadFile(pattern)
		if err != nil {
			return nil, fmt.Errorf("failed to read template %s: %w", pattern, err)
		}

		name := filepath.Base(pattern)
		_, err = tmpl.New(name).Parse(string(content))
		if err != nil {
			return nil, fmt.Errorf("failed to parse template %s: %w", pattern, err)
		}
	}

	return tmpl, nil
}

// LoadTemplatesFromDir 从目录加载模板（用于开发模式）
func LoadTemplatesFromDir(dir string) (*template.Template, error) {
	tmpl := template.New("").Funcs(TemplateFuncs())
	pattern := filepath.Join(dir, "*.html")
	return tmpl.ParseGlob(pattern)
}

// renderTemplate 渲染模板的辅助函数
func (h *PageHandlerImpl) renderTemplate(w http.ResponseWriter, layoutName, contentName string, data interface{}) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Clone the template set to avoid conflicts between concurrent requests
	tmpl, err := h.templates.Clone()
	if err != nil {
		http.Error(w, fmt.Sprintf("Template clone error: %v", err), http.StatusInternalServerError)
		return
	}

	// Parse the content template to get its "content" and "scripts" blocks
	contentTmpl := h.templates.Lookup(contentName)
	if contentTmpl == nil {
		http.Error(w, fmt.Sprintf("Template %s not found", contentName), http.StatusInternalServerError)
		return
	}

	// Re-parse the content template into the cloned template set
	// This ensures the "content" block from this specific template is used
	contentBytes, err := templatesFS.ReadFile("templates/" + contentName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read template %s: %v", contentName, err), http.StatusInternalServerError)
		return
	}

	_, err = tmpl.Parse(string(contentBytes))
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to parse template %s: %v", contentName, err), http.StatusInternalServerError)
		return
	}

	// Execute the layout template which includes the content
	err = tmpl.ExecuteTemplate(w, "layout", data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Template execution error: %v", err), http.StatusInternalServerError)
		return
	}
}

// HandleIndex 首页/仪表盘
func (h *PageHandlerImpl) HandleIndex(w http.ResponseWriter, r *http.Request) {
	// Get stats
	var stats StatsResponse
	if h.admin != nil {
		engineStats, err := h.admin.GetStats(r.Context())
		if err == nil {
			stats = StatsResponse{
				TotalTransactions:       engineStats.TotalTransactions,
				PendingTransactions:     engineStats.PendingTransactions,
				FailedTransactions:      engineStats.FailedTransactions,
				CompletedTransactions:   engineStats.CompletedTransactions,
				CompensatedTransactions: engineStats.CompensatedTransactions,
			}
		}
	}

	data := NewIndexPageData(stats)
	h.renderTemplate(w, "layout.html", "index.html", data)
}

// HandleTransactions 事务列表页
func (h *PageHandlerImpl) HandleTransactions(w http.ResponseWriter, r *http.Request) {
	// Parse filter from query parameters
	filter := h.parseTransactionFilterFromQuery(r)

	// Build store filter
	storeFilter := &rte.StoreTxFilter{
		TxType: filter.TxType,
		Limit:  20,
		Offset: 0,
	}

	// Parse status
	for _, s := range filter.Status {
		storeFilter.Status = append(storeFilter.Status, rte.TxStatus(s))
	}

	// Parse time range
	if filter.StartTime != "" {
		if t, err := time.Parse("2006-01-02T15:04", filter.StartTime); err == nil {
			storeFilter.StartTime = t
		}
	}
	if filter.EndTime != "" {
		if t, err := time.Parse("2006-01-02T15:04", filter.EndTime); err == nil {
			storeFilter.EndTime = t
		}
	}

	// Parse pagination
	page := 1
	if p := r.URL.Query().Get("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}
	storeFilter.Offset = (page - 1) * storeFilter.Limit

	// Get transactions
	var transactions []TransactionSummary
	var total int64
	var totalPages int

	if h.admin != nil {
		result, err := h.admin.ListTransactions(r.Context(), storeFilter)
		if err == nil {
			for _, tx := range result.Transactions {
				transactions = append(transactions, TransactionSummary{
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
			total = result.Total
			totalPages = int((total + int64(storeFilter.Limit) - 1) / int64(storeFilter.Limit))
			if totalPages < 1 {
				totalPages = 1
			}
		}
	}

	pagination := NewPaginationData(page, totalPages, storeFilter.Limit, total)
	data := NewTransactionsPageData(transactions, filter, pagination)
	h.renderTemplate(w, "layout.html", "transactions.html", data)
}

// parseTransactionFilterFromQuery 从查询参数解析事务筛选条件
func (h *PageHandlerImpl) parseTransactionFilterFromQuery(r *http.Request) TransactionFilter {
	filter := TransactionFilter{}

	// Parse status (can be multiple)
	filter.Status = r.URL.Query()["status"]

	// Parse tx_type
	filter.TxType = r.URL.Query().Get("tx_type")

	// Parse time range
	filter.StartTime = r.URL.Query().Get("start_time")
	filter.EndTime = r.URL.Query().Get("end_time")

	return filter
}

// HandleTransactionDetail 事务详情页
func (h *PageHandlerImpl) HandleTransactionDetail(w http.ResponseWriter, r *http.Request) {
	txID := r.PathValue("txID")
	if txID == "" {
		http.Error(w, "Transaction ID is required", http.StatusBadRequest)
		return
	}

	if h.admin == nil {
		http.Error(w, "Admin not configured", http.StatusInternalServerError)
		return
	}

	// Get transaction detail
	detail, err := h.admin.GetTransaction(r.Context(), txID)
	if err != nil {
		if err == rte.ErrTransactionNotFound {
			http.Error(w, fmt.Sprintf("事务 %s 不存在", txID), http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("Failed to get transaction: %v", err), http.StatusInternalServerError)
		return
	}

	// Convert to page data
	tx := convertToTransactionDetail(detail.Transaction)
	var steps []StepDetail
	for _, step := range detail.Steps {
		steps = append(steps, convertToStepDetail(step))
	}

	data := NewTransactionDetailPageData(tx, steps)
	h.renderTemplate(w, "layout.html", "transaction_detail.html", data)
}

// HandleRecovery 恢复监控页
func (h *PageHandlerImpl) HandleRecovery(w http.ResponseWriter, r *http.Request) {
	var stats RecoveryStatsResponse

	if h.recovery != nil {
		workerStats := h.recovery.Stats()
		stats = RecoveryStatsResponse{
			IsRunning:      workerStats.IsRunning,
			ScannedCount:   workerStats.ScannedCount,
			ProcessedCount: workerStats.ProcessedCount,
			FailedCount:    workerStats.FailedCount,
			Config:         RecoveryConfigInfo{
				// Config info will be populated when recovery worker exposes config
			},
		}
	}

	data := NewRecoveryPageData(stats)
	h.renderTemplate(w, "layout.html", "recovery.html", data)
}

// HandleCircuitBreakers 熔断器面板页
func (h *PageHandlerImpl) HandleCircuitBreakers(w http.ResponseWriter, r *http.Request) {
	var breakers []CircuitBreakerInfo

	// Note: The current Breaker interface doesn't expose a way to list all breakers.
	// This will need to be enhanced in the circuit breaker implementation.
	// For now, return an empty list.

	data := NewCircuitBreakersPageData(breakers)
	h.renderTemplate(w, "layout.html", "circuit_breakers.html", data)
}

// HandleEvents 事件日志页
func (h *PageHandlerImpl) HandleEvents(w http.ResponseWriter, r *http.Request) {
	// Parse filter from query parameters
	filter := h.parseEventFilterFromQuery(r)

	var events []StoredEvent
	var eventTypes []string

	if h.eventStore != nil {
		events = h.eventStore.List(filter)
		eventTypes = h.eventStore.GetEventTypes()
	}

	// If no event types from store, use default list
	if len(eventTypes) == 0 {
		eventTypes = AllEventTypes
	}

	data := NewEventsPageData(events, eventTypes, filter)
	h.renderTemplate(w, "layout.html", "events.html", data)
}

// parseEventFilterFromQuery 从查询参数解析事件筛选条件
func (h *PageHandlerImpl) parseEventFilterFromQuery(r *http.Request) EventFilter {
	filter := EventFilter{
		Limit:  100,
		Offset: 0,
	}

	// Parse type filter
	filter.Type = r.URL.Query().Get("type")

	// Parse tx_id filter
	filter.TxID = r.URL.Query().Get("tx_id")

	// Parse pagination
	if limit := r.URL.Query().Get("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil && l > 0 && l <= 1000 {
			filter.Limit = l
		}
	}
	if offset := r.URL.Query().Get("offset"); offset != "" {
		if o, err := strconv.Atoi(offset); err == nil && o >= 0 {
			filter.Offset = o
		}
	}

	return filter
}

// SetTemplates 设置模板（用于测试或自定义模板）
func (h *PageHandlerImpl) SetTemplates(templates *template.Template) {
	h.templates = templates
}

// GetTemplates 获取模板
func (h *PageHandlerImpl) GetTemplates() *template.Template {
	return h.templates
}
