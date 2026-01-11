// Package domain 定义领域模型
package domain

import (
	"sync"
	"time"
)

// Account 账户模型
type Account struct {
	ID        int64
	UserID    int64
	Type      string // "saving", "forex", "checking"
	Currency  string
	Balance   float64
	Version   int64
	UpdatedAt time.Time
}

// TransactionLog 交易日志
type TransactionLog struct {
	ID              int64
	Ticket          string
	AccountID       int64
	TransactionType string // "IN", "OUT"
	Amount          float64
	Status          string // "PENDING", "COMPLETE", "FAILED"
	CreatedAt       time.Time
}

// TransferRequest 转账请求
type TransferRequest struct {
	UserID        int64
	FromAccountID int64
	ToAccountID   int64
	Amount        float64
	Currency      string
	Ticket        string
}

// AccountRepository 账户仓储接口
type AccountRepository interface {
	GetAccount(accountID int64) (*Account, error)
	UpdateBalance(accountID int64, delta float64) error
	CreateLog(ticket string, accountID int64, txType string, amount float64) int64
	UpdateLogStatus(logID int64, status string) error
}

// MemoryAccountRepository 内存账户仓储实现
type MemoryAccountRepository struct {
	mu       sync.Mutex
	accounts map[int64]*Account
	logs     map[int64]*TransactionLog
	logSeq   int64
}

// NewMemoryAccountRepository 创建内存账户仓储
func NewMemoryAccountRepository() *MemoryAccountRepository {
	return &MemoryAccountRepository{
		accounts: make(map[int64]*Account),
		logs:     make(map[int64]*TransactionLog),
	}
}

// CreateAccount 创建账户
func (r *MemoryAccountRepository) CreateAccount(acc *Account) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.accounts[acc.ID] = acc
}

// GetAccount 获取账户
func (r *MemoryAccountRepository) GetAccount(accountID int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	acc, ok := r.accounts[accountID]
	if !ok {
		return nil, ErrAccountNotFound
	}
	copy := *acc
	return &copy, nil
}

// UpdateBalance 更新余额
func (r *MemoryAccountRepository) UpdateBalance(accountID int64, delta float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	acc, ok := r.accounts[accountID]
	if !ok {
		return ErrAccountNotFound
	}
	if acc.Balance+delta < 0 {
		return ErrInsufficientBalance
	}
	acc.Balance += delta
	acc.Version++
	acc.UpdatedAt = time.Now()
	return nil
}

// CreateLog 创建交易日志
func (r *MemoryAccountRepository) CreateLog(ticket string, accountID int64, txType string, amount float64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logSeq++
	log := &TransactionLog{
		ID:              r.logSeq,
		Ticket:          ticket,
		AccountID:       accountID,
		TransactionType: txType,
		Amount:          amount,
		Status:          "PENDING",
		CreatedAt:       time.Now(),
	}
	r.logs[log.ID] = log
	return log.ID
}

// UpdateLogStatus 更新日志状态
func (r *MemoryAccountRepository) UpdateLogStatus(logID int64, status string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	log, ok := r.logs[logID]
	if !ok {
		return ErrLogNotFound
	}
	log.Status = status
	return nil
}

// GetLog 获取日志
func (r *MemoryAccountRepository) GetLog(logID int64) (*TransactionLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	log, ok := r.logs[logID]
	if !ok {
		return nil, ErrLogNotFound
	}
	copy := *log
	return &copy, nil
}
