// Package domain 定义领域模型
package domain

import (
	"context"
	"errors"
	"sync"
)

// 外部服务错误
var (
	ErrExternalTimeout     = errors.New("external service timeout")
	ErrExternalUnavailable = errors.New("external service unavailable")
)

// FailureMode 定义模拟服务的失败模式
type FailureMode int

const (
	FailureModeNone      FailureMode = iota // 总是成功
	FailureModeAlways                       // 总是失败
	FailureModeOnce                         // 失败一次后成功
	FailureModeTimeout                      // 超时错误
	FailureModeUncertain                    // 超时但实际成功
)

// ExternalService 外部服务接口
type ExternalService interface {
	Deposit(ctx context.Context, accountID int64, amount float64, ticket string) error
	Withdraw(ctx context.Context, accountID int64, amount float64, ticket string) error
	CheckTransaction(ctx context.Context, accountID int64, ticket string) (bool, error)
}

// FailableExternalService 可配置失败模式的外部服务模拟
type FailableExternalService struct {
	mu           sync.Mutex
	failureMode  FailureMode
	failureCount int
	transactions map[string]bool
}

// NewFailableExternalService 创建可失败的外部服务
func NewFailableExternalService() *FailableExternalService {
	return &FailableExternalService{
		transactions: make(map[string]bool),
	}
}

// SetFailureMode 设置失败模式
func (s *FailableExternalService) SetFailureMode(mode FailureMode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failureMode = mode
	s.failureCount = 0
}

// Deposit 存款操作
func (s *FailableExternalService) Deposit(ctx context.Context, accountID int64, amount float64, ticket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.failureMode {
	case FailureModeAlways:
		return ErrExternalUnavailable
	case FailureModeOnce:
		if s.failureCount == 0 {
			s.failureCount++
			return ErrExternalUnavailable
		}
	case FailureModeTimeout:
		return ErrExternalTimeout
	case FailureModeUncertain:
		// 实际成功但返回超时
		s.transactions[ticket] = true
		return ErrExternalTimeout
	}

	s.transactions[ticket] = true
	return nil
}

// Withdraw 取款操作
func (s *FailableExternalService) Withdraw(ctx context.Context, accountID int64, amount float64, ticket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.failureMode {
	case FailureModeAlways:
		return ErrExternalUnavailable
	case FailureModeOnce:
		if s.failureCount == 0 {
			s.failureCount++
			return ErrExternalUnavailable
		}
	case FailureModeTimeout:
		return ErrExternalTimeout
	case FailureModeUncertain:
		s.transactions[ticket] = true
		return ErrExternalTimeout
	}

	s.transactions[ticket] = true
	return nil
}

// CheckTransaction 检查交易是否存在（用于幂等性验证）
func (s *FailableExternalService) CheckTransaction(ctx context.Context, accountID int64, ticket string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transactions[ticket], nil
}

// Reset 重置状态
func (s *FailableExternalService) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failureMode = FailureModeNone
	s.failureCount = 0
	s.transactions = make(map[string]bool)
}
