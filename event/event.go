// Package event provides event definitions and event bus for the RTE engine.
package event

import (
	"time"
)

// EventType 事件类型
type EventType string

const (
	// Transaction lifecycle events
	EventTxCreated            EventType = "tx.created"
	EventTxCompleted          EventType = "tx.completed"
	EventTxFailed             EventType = "tx.failed"
	EventTxTimeout            EventType = "tx.timeout"
	EventTxCancelled          EventType = "tx.cancelled"
	EventTxCompensationFailed EventType = "tx.compensation_failed"

	// Step lifecycle events
	EventStepStarted   EventType = "step.started"
	EventStepCompleted EventType = "step.completed"
	EventStepFailed    EventType = "step.failed"

	// Circuit breaker events
	EventCircuitOpened EventType = "circuit.opened"
	EventCircuitClosed EventType = "circuit.closed"

	// Recovery events
	EventRecoveryStart EventType = "recovery.start"

	// Alert events
	EventAlertWarning  EventType = "alert.warning"
	EventAlertCritical EventType = "alert.critical"
)

// Event 事件
type Event struct {
	Type      EventType      // 事件类型
	TxID      string         // 事务ID
	TxType    string         // 事务类型
	StepName  string         // 步骤名称（仅步骤相关事件）
	Timestamp time.Time      // 事件时间戳
	Data      map[string]any // 附加数据
	Error     error          // 错误信息（仅失败事件）
}

// NewEvent creates a new event with the given type and automatically sets the timestamp.
func NewEvent(eventType EventType) Event {
	return Event{
		Type:      eventType,
		Timestamp: time.Now(),
		Data:      make(map[string]any),
	}
}

// WithTxID sets the transaction ID on the event.
func (e Event) WithTxID(txID string) Event {
	e.TxID = txID
	return e
}

// WithTxType sets the transaction type on the event.
func (e Event) WithTxType(txType string) Event {
	e.TxType = txType
	return e
}

// WithStepName sets the step name on the event.
func (e Event) WithStepName(stepName string) Event {
	e.StepName = stepName
	return e
}

// WithError sets the error on the event.
func (e Event) WithError(err error) Event {
	e.Error = err
	return e
}

// WithData sets a key-value pair in the event data.
func (e Event) WithData(key string, value any) Event {
	if e.Data == nil {
		e.Data = make(map[string]any)
	}
	e.Data[key] = value
	return e
}

// String returns the string representation of the event type.
func (t EventType) String() string {
	return string(t)
}
