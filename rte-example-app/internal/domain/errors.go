package domain

import "errors"

var (
	// ErrAccountNotFound 账户不存在
	ErrAccountNotFound = errors.New("account not found")

	// ErrInsufficientBalance 余额不足
	ErrInsufficientBalance = errors.New("insufficient balance")

	// ErrLogNotFound 日志不存在
	ErrLogNotFound = errors.New("log not found")

	// ErrInvalidAmount 无效金额
	ErrInvalidAmount = errors.New("invalid amount")

	// ErrSameAccount 相同账户
	ErrSameAccount = errors.New("cannot transfer to same account")
)
