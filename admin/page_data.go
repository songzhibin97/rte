// Package admin provides administrative interfaces for the RTE engine.
package admin

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/url"
	"time"
)

// ============================================================================
// 页面模板数据模型 (Page Template Data Models)
// ============================================================================

// PageData 页面基础数据
// 所有页面共享的基础数据结构
type PageData struct {
	// Title 页面标题
	Title string
	// CurrentPage 当前页面标识，用于导航高亮
	CurrentPage string
	// NavItems 导航项列表
	NavItems []NavItem
}

// NavItem 导航项
type NavItem struct {
	// Name 导航项名称（中文）
	Name string
	// URL 导航链接
	URL string
	// Active 是否为当前激活项
	Active bool
}

// DefaultNavItems 返回默认导航项列表
func DefaultNavItems(currentPage string) []NavItem {
	items := []NavItem{
		{Name: "仪表盘", URL: "/", Active: currentPage == "index"},
		{Name: "事务列表", URL: "/transactions", Active: currentPage == "transactions"},
		{Name: "恢复监控", URL: "/recovery", Active: currentPage == "recovery"},
		{Name: "熔断器", URL: "/circuit-breakers", Active: currentPage == "circuit-breakers"},
		{Name: "事件日志", URL: "/events", Active: currentPage == "events"},
	}
	return items
}

// NewPageData 创建页面基础数据
func NewPageData(title, currentPage string) PageData {
	return PageData{
		Title:       title,
		CurrentPage: currentPage,
		NavItems:    DefaultNavItems(currentPage),
	}
}

// IndexPageData 首页/仪表盘数据
type IndexPageData struct {
	PageData
	// Stats 系统统计数据
	Stats StatsResponse
}

// NewIndexPageData 创建首页数据
func NewIndexPageData(stats StatsResponse) IndexPageData {
	return IndexPageData{
		PageData: NewPageData("RTE 管理控制台 - 仪表盘", "index"),
		Stats:    stats,
	}
}

// TransactionsPageData 事务列表页数据
type TransactionsPageData struct {
	PageData
	// Transactions 事务列表
	Transactions []TransactionSummary
	// Filter 当前筛选条件
	Filter TransactionFilter
	// Pagination 分页数据
	Pagination PaginationData
}

// TransactionFilter 事务筛选条件
type TransactionFilter struct {
	// Status 状态筛选（可多选）
	Status []string
	// TxType 事务类型筛选
	TxType string
	// StartTime 开始时间（RFC3339格式）
	StartTime string
	// EndTime 结束时间（RFC3339格式）
	EndTime string
}

// PaginationData 分页数据
type PaginationData struct {
	// CurrentPage 当前页码
	CurrentPage int
	// TotalPages 总页数
	TotalPages int
	// PageSize 每页大小
	PageSize int
	// Total 总记录数
	Total int64
	// HasPrev 是否有上一页
	HasPrev bool
	// HasNext 是否有下一页
	HasNext bool
}

// NewPaginationData 创建分页数据
func NewPaginationData(currentPage, totalPages, pageSize int, total int64) PaginationData {
	return PaginationData{
		CurrentPage: currentPage,
		TotalPages:  totalPages,
		PageSize:    pageSize,
		Total:       total,
		HasPrev:     currentPage > 1,
		HasNext:     currentPage < totalPages,
	}
}

// NewTransactionsPageData 创建事务列表页数据
func NewTransactionsPageData(transactions []TransactionSummary, filter TransactionFilter, pagination PaginationData) TransactionsPageData {
	return TransactionsPageData{
		PageData:     NewPageData("RTE 管理控制台 - 事务列表", "transactions"),
		Transactions: transactions,
		Filter:       filter,
		Pagination:   pagination,
	}
}

// TransactionDetailPageData 事务详情页数据
type TransactionDetailPageData struct {
	PageData
	// Transaction 事务详情
	Transaction TransactionDetail
	// Steps 步骤列表
	Steps []StepDetail
	// CanForceComplete 是否可以强制完成
	CanForceComplete bool
	// CanForceCancel 是否可以强制取消
	CanForceCancel bool
	// CanRetry 是否可以重试
	CanRetry bool
}

// NewTransactionDetailPageData 创建事务详情页数据
func NewTransactionDetailPageData(tx TransactionDetail, steps []StepDetail) TransactionDetailPageData {
	return TransactionDetailPageData{
		PageData:         NewPageData("RTE 管理控制台 - 事务详情", "transactions"),
		Transaction:      tx,
		Steps:            steps,
		CanForceComplete: canForceCompleteStatus(tx.Status),
		CanForceCancel:   canForceCancelStatus(tx.Status),
		CanRetry:         canRetryStatus(tx.Status),
	}
}

// canForceCompleteStatus 检查状态是否可以强制完成
func canForceCompleteStatus(status string) bool {
	switch status {
	case "LOCKED", "EXECUTING", "CONFIRMING":
		return true
	default:
		return false
	}
}

// canForceCancelStatus 检查状态是否可以强制取消
func canForceCancelStatus(status string) bool {
	switch status {
	case "CREATED", "LOCKED", "EXECUTING", "FAILED":
		return true
	default:
		return false
	}
}

// canRetryStatus 检查状态是否可以重试
func canRetryStatus(status string) bool {
	return status == "FAILED"
}

// RecoveryPageData 恢复监控页数据
type RecoveryPageData struct {
	PageData
	// Stats 恢复Worker统计数据
	Stats RecoveryStatsResponse
}

// NewRecoveryPageData 创建恢复监控页数据
func NewRecoveryPageData(stats RecoveryStatsResponse) RecoveryPageData {
	return RecoveryPageData{
		PageData: NewPageData("RTE 管理控制台 - 恢复监控", "recovery"),
		Stats:    stats,
	}
}

// CircuitBreakersPageData 熔断器面板页数据
type CircuitBreakersPageData struct {
	PageData
	// Breakers 熔断器列表
	Breakers []CircuitBreakerInfo
}

// NewCircuitBreakersPageData 创建熔断器面板页数据
func NewCircuitBreakersPageData(breakers []CircuitBreakerInfo) CircuitBreakersPageData {
	return CircuitBreakersPageData{
		PageData: NewPageData("RTE 管理控制台 - 熔断器面板", "circuit-breakers"),
		Breakers: breakers,
	}
}

// EventsPageData 事件日志页数据
type EventsPageData struct {
	PageData
	// Events 事件列表
	Events []StoredEvent
	// EventTypes 可用的事件类型列表（用于筛选下拉框）
	EventTypes []string
	// Filter 当前筛选条件
	Filter EventFilter
}

// NewEventsPageData 创建事件日志页数据
func NewEventsPageData(events []StoredEvent, eventTypes []string, filter EventFilter) EventsPageData {
	return EventsPageData{
		PageData:   NewPageData("RTE 管理控制台 - 事件日志", "events"),
		Events:     events,
		EventTypes: eventTypes,
		Filter:     filter,
	}
}

// ============================================================================
// 辅助类型和常量
// ============================================================================

// StatusDisplayInfo 状态显示信息
type StatusDisplayInfo struct {
	// Label 状态标签（中文）
	Label string
	// CSSClass CSS类名（用于样式）
	CSSClass string
}

// TransactionStatusDisplay 事务状态显示映射
var TransactionStatusDisplay = map[string]StatusDisplayInfo{
	"CREATED":             {Label: "已创建", CSSClass: "status-created"},
	"LOCKED":              {Label: "已锁定", CSSClass: "status-locked"},
	"EXECUTING":           {Label: "执行中", CSSClass: "status-executing"},
	"CONFIRMING":          {Label: "确认中", CSSClass: "status-confirming"},
	"COMPLETED":           {Label: "已完成", CSSClass: "status-completed"},
	"FAILED":              {Label: "失败", CSSClass: "status-failed"},
	"COMPENSATING":        {Label: "补偿中", CSSClass: "status-compensating"},
	"COMPENSATED":         {Label: "已补偿", CSSClass: "status-compensated"},
	"COMPENSATION_FAILED": {Label: "补偿失败", CSSClass: "status-compensation-failed"},
	"CANCELLED":           {Label: "已取消", CSSClass: "status-cancelled"},
	"TIMEOUT":             {Label: "超时", CSSClass: "status-timeout"},
}

// StepStatusDisplay 步骤状态显示映射
var StepStatusDisplay = map[string]StatusDisplayInfo{
	"PENDING":      {Label: "待执行", CSSClass: "step-pending"},
	"EXECUTING":    {Label: "执行中", CSSClass: "step-executing"},
	"COMPLETED":    {Label: "已完成", CSSClass: "step-completed"},
	"FAILED":       {Label: "失败", CSSClass: "step-failed"},
	"COMPENSATING": {Label: "补偿中", CSSClass: "step-compensating"},
	"COMPENSATED":  {Label: "已补偿", CSSClass: "step-compensated"},
	"SKIPPED":      {Label: "已跳过", CSSClass: "step-skipped"},
}

// CircuitBreakerStateDisplay 熔断器状态显示映射
var CircuitBreakerStateDisplay = map[string]StatusDisplayInfo{
	"CLOSED":    {Label: "关闭", CSSClass: "cb-closed"},
	"OPEN":      {Label: "打开", CSSClass: "cb-open"},
	"HALF_OPEN": {Label: "半开", CSSClass: "cb-half-open"},
}

// GetStatusDisplay 获取状态显示信息
func GetStatusDisplay(statusMap map[string]StatusDisplayInfo, status string) StatusDisplayInfo {
	if info, ok := statusMap[status]; ok {
		return info
	}
	return StatusDisplayInfo{Label: status, CSSClass: "status-unknown"}
}

// FormatTime 格式化时间为本地化字符串
func FormatTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format("2006-01-02 15:04:05")
}

// FormatTimePtr 格式化时间指针为本地化字符串
func FormatTimePtr(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return FormatTime(*t)
}

// AllTransactionStatuses 所有事务状态列表（用于筛选下拉框）
var AllTransactionStatuses = []string{
	"CREATED",
	"LOCKED",
	"EXECUTING",
	"CONFIRMING",
	"COMPLETED",
	"FAILED",
	"COMPENSATING",
	"COMPENSATED",
	"COMPENSATION_FAILED",
	"CANCELLED",
	"TIMEOUT",
}

// AllEventTypes 所有事件类型列表（用于筛选下拉框）
var AllEventTypes = []string{
	"tx_created",
	"tx_started",
	"tx_completed",
	"tx_failed",
	"tx_cancelled",
	"tx_compensating",
	"tx_compensated",
	"tx_compensation_failed",
	"step_started",
	"step_completed",
	"step_failed",
	"step_compensating",
	"step_compensated",
}

// ============================================================================
// 模板辅助函数 (Template Helper Functions)
// ============================================================================

// TemplateFuncs 返回模板辅助函数映射
func TemplateFuncs() template.FuncMap {
	return template.FuncMap{
		// 状态相关
		"statusClass":     StatusClass,
		"statusLabel":     StatusLabel,
		"stepStatusClass": StepStatusClass,
		"stepStatusLabel": StepStatusLabel,
		"cbStateClass":    CBStateClass,
		"cbStateLabel":    CBStateLabel,
		"eventTypeClass":  EventTypeClass,
		"eventTypeLabel":  EventTypeLabel,

		// 时间格式化
		"formatTime":    FormatTime,
		"formatTimePtr": FormatTimePtr,

		// JSON格式化
		"toJSON": ToJSON,

		// 分页相关
		"paginationURL":   PaginationURL,
		"paginationRange": PaginationRange,

		// 筛选相关
		"hasStatus": HasStatus,

		// 数学运算
		"add":     Add,
		"sub":     Sub,
		"mul":     Mul,
		"div":     Div,
		"float64": ToFloat64,
	}
}

// StatusClass 返回事务状态的CSS类名
func StatusClass(status string) string {
	if info, ok := TransactionStatusDisplay[status]; ok {
		return info.CSSClass
	}
	return "status-unknown"
}

// StatusLabel 返回事务状态的中文标签
func StatusLabel(status string) string {
	if info, ok := TransactionStatusDisplay[status]; ok {
		return info.Label
	}
	return status
}

// StepStatusClass 返回步骤状态的CSS类名
func StepStatusClass(status string) string {
	if info, ok := StepStatusDisplay[status]; ok {
		return info.CSSClass
	}
	return "status-unknown"
}

// StepStatusLabel 返回步骤状态的中文标签
func StepStatusLabel(status string) string {
	if info, ok := StepStatusDisplay[status]; ok {
		return info.Label
	}
	return status
}

// CBStateClass 返回熔断器状态的CSS类名
func CBStateClass(state string) string {
	if info, ok := CircuitBreakerStateDisplay[state]; ok {
		return info.CSSClass
	}
	return "status-unknown"
}

// CBStateLabel 返回熔断器状态的中文标签
func CBStateLabel(state string) string {
	if info, ok := CircuitBreakerStateDisplay[state]; ok {
		return info.Label
	}
	return state
}

// EventTypeClass 返回事件类型的CSS类名
func EventTypeClass(eventType string) string {
	switch eventType {
	case "tx_created", "tx_started", "step_started":
		return "status-created"
	case "tx_completed", "step_completed":
		return "status-completed"
	case "tx_failed", "step_failed", "tx_compensation_failed":
		return "status-failed"
	case "tx_cancelled":
		return "status-cancelled"
	case "tx_compensating", "step_compensating":
		return "status-compensating"
	case "tx_compensated", "step_compensated":
		return "status-compensated"
	default:
		return "status-unknown"
	}
}

// EventTypeLabel 返回事件类型的中文标签
func EventTypeLabel(eventType string) string {
	labels := map[string]string{
		"tx_created":             "事务创建",
		"tx_started":             "事务开始",
		"tx_completed":           "事务完成",
		"tx_failed":              "事务失败",
		"tx_cancelled":           "事务取消",
		"tx_compensating":        "事务补偿中",
		"tx_compensated":         "事务已补偿",
		"tx_compensation_failed": "补偿失败",
		"step_started":           "步骤开始",
		"step_completed":         "步骤完成",
		"step_failed":            "步骤失败",
		"step_compensating":      "步骤补偿中",
		"step_compensated":       "步骤已补偿",
	}
	if label, ok := labels[eventType]; ok {
		return label
	}
	return eventType
}

// ToJSON 将对象转换为格式化的JSON字符串
func ToJSON(v interface{}) string {
	if v == nil {
		return "{}"
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}
	return string(b)
}

// PaginationURL 生成分页URL
func PaginationURL(filter TransactionFilter, page int) string {
	params := url.Values{}
	for _, s := range filter.Status {
		params.Add("status", s)
	}
	if filter.TxType != "" {
		params.Set("tx_type", filter.TxType)
	}
	if filter.StartTime != "" {
		params.Set("start_time", filter.StartTime)
	}
	if filter.EndTime != "" {
		params.Set("end_time", filter.EndTime)
	}
	params.Set("page", fmt.Sprintf("%d", page))
	return "/transactions?" + params.Encode()
}

// PaginationRange 生成分页范围
func PaginationRange(currentPage, totalPages int) []int {
	var pages []int

	// 显示最多7个页码
	start := currentPage - 3
	end := currentPage + 3

	if start < 1 {
		start = 1
		end = 7
	}
	if end > totalPages {
		end = totalPages
		start = totalPages - 6
	}
	if start < 1 {
		start = 1
	}

	for i := start; i <= end; i++ {
		pages = append(pages, i)
	}

	return pages
}

// HasStatus 检查状态列表中是否包含指定状态
func HasStatus(statuses []string, status string) bool {
	for _, s := range statuses {
		if s == status {
			return true
		}
	}
	return false
}

// Add 加法
func Add(a, b int) int {
	return a + b
}

// Sub 减法
func Sub(a, b int) int {
	return a - b
}

// Mul 乘法
func Mul(a, b float64) float64 {
	return a * b
}

// Div 除法
func Div(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

// ToFloat64 转换为float64
func ToFloat64(v int64) float64 {
	return float64(v)
}
