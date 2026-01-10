// Package admin provides administrative interfaces for the RTE engine.
package admin

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"rte/event"
)

// EventStore 事件存储
// 内存中的事件存储，用于事件日志功能。
// 支持最大事件数限制，超过限制时自动删除最旧的事件。
type EventStore struct {
	events    []StoredEvent
	maxEvents int
	mu        sync.RWMutex
	nextID    int64
}

// StoredEvent 存储的事件
type StoredEvent struct {
	ID        int64          `json:"id"`
	Type      string         `json:"type"`
	TxID      string         `json:"tx_id"`
	TxType    string         `json:"tx_type"`
	StepName  string         `json:"step_name,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
	Data      map[string]any `json:"data,omitempty"`
	Error     string         `json:"error,omitempty"`
}

// EventFilter 事件筛选条件
type EventFilter struct {
	Type   string // 事件类型筛选
	TxID   string // 事务ID筛选
	Limit  int    // 返回数量限制
	Offset int    // 偏移量
}

// NewEventStore 创建事件存储
// maxEvents 指定最大存储事件数，超过时自动删除最旧的事件
func NewEventStore(maxEvents int) *EventStore {
	if maxEvents <= 0 {
		maxEvents = 1000 // 默认最大1000条事件
	}
	return &EventStore{
		events:    make([]StoredEvent, 0, maxEvents),
		maxEvents: maxEvents,
		nextID:    0,
	}
}

// Store 存储事件
// 将 event.Event 转换为 StoredEvent 并存储
func (s *EventStore) Store(e event.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 生成唯一ID
	id := atomic.AddInt64(&s.nextID, 1)

	// 转换错误信息
	var errorMsg string
	if e.Error != nil {
		errorMsg = e.Error.Error()
	}

	// 创建存储事件
	stored := StoredEvent{
		ID:        id,
		Type:      string(e.Type),
		TxID:      e.TxID,
		TxType:    e.TxType,
		StepName:  e.StepName,
		Timestamp: e.Timestamp,
		Data:      e.Data,
		Error:     errorMsg,
	}

	// 添加事件
	s.events = append(s.events, stored)

	// 如果超过最大数量，删除最旧的事件
	if len(s.events) > s.maxEvents {
		// 删除前面多余的事件
		excess := len(s.events) - s.maxEvents
		s.events = s.events[excess:]
	}
}

// List 列出事件
// 根据筛选条件返回事件列表，按时间倒序排列（最新的在前）
func (s *EventStore) List(filter EventFilter) []StoredEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 设置默认值
	if filter.Limit <= 0 {
		filter.Limit = 100
	}

	// 筛选事件
	var filtered []StoredEvent
	for i := len(s.events) - 1; i >= 0; i-- {
		e := s.events[i]

		// 类型筛选
		if filter.Type != "" && e.Type != filter.Type {
			continue
		}

		// 事务ID筛选
		if filter.TxID != "" && e.TxID != filter.TxID {
			continue
		}

		filtered = append(filtered, e)
	}

	// 应用分页
	if filter.Offset >= len(filtered) {
		return []StoredEvent{}
	}

	start := filter.Offset
	end := start + filter.Limit
	if end > len(filtered) {
		end = len(filtered)
	}

	return filtered[start:end]
}

// Count 返回符合筛选条件的事件总数
func (s *EventStore) Count(filter EventFilter) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if filter.Type == "" && filter.TxID == "" {
		return len(s.events)
	}

	count := 0
	for _, e := range s.events {
		if filter.Type != "" && e.Type != filter.Type {
			continue
		}
		if filter.TxID != "" && e.TxID != filter.TxID {
			continue
		}
		count++
	}
	return count
}

// EventHandler 返回事件处理器用于订阅事件总线
// 返回的处理器可以直接传递给 EventBus.SubscribeAll()
func (s *EventStore) EventHandler() event.EventHandler {
	return func(ctx context.Context, e event.Event) error {
		s.Store(e)
		return nil
	}
}

// Clear 清空所有事件
// 主要用于测试
func (s *EventStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = make([]StoredEvent, 0, s.maxEvents)
}

// Len 返回当前存储的事件数量
func (s *EventStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.events)
}

// GetEventTypes 返回所有已存储事件的类型列表（去重）
func (s *EventStore) GetEventTypes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	typeSet := make(map[string]struct{})
	for _, e := range s.events {
		typeSet[e.Type] = struct{}{}
	}

	types := make([]string, 0, len(typeSet))
	for t := range typeSet {
		types = append(types, t)
	}
	return types
}
