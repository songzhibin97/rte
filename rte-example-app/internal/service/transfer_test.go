package service

import (
	"context"
	"testing"

	"rte"
	"rte-example-app/internal/domain"
)

func TestTransferService_SuccessfulTransfer(t *testing.T) {
	svc := NewTransferService()

	// 创建测试账户
	svc.CreateAccount(1, 100, "saving", "USD", 10000.0)
	svc.CreateAccount(2, 100, "forex", "USD", 5000.0)

	// 执行转账
	req := &domain.TransferRequest{
		UserID:        100,
		FromAccountID: 1,
		ToAccountID:   2,
		Amount:        1000.0,
		Currency:      "USD",
	}

	result, err := svc.Transfer(context.Background(), req)
	if err != nil {
		t.Fatalf("Transfer failed: %v", err)
	}

	if result.Status != rte.TxStatusCompleted {
		t.Errorf("Expected status COMPLETED, got %s", result.Status)
	}

	// 验证余额
	fromAcc, _ := svc.GetAccount(1)
	toAcc, _ := svc.GetAccount(2)

	if fromAcc.Balance != 9000.0 {
		t.Errorf("Expected from account balance 9000.0, got %.2f", fromAcc.Balance)
	}
	if toAcc.Balance != 6000.0 {
		t.Errorf("Expected to account balance 6000.0, got %.2f", toAcc.Balance)
	}
}

func TestTransferService_InsufficientBalance(t *testing.T) {
	svc := NewTransferService()

	// 创建测试账户
	svc.CreateAccount(1, 100, "saving", "USD", 1000.0)
	svc.CreateAccount(2, 100, "forex", "USD", 5000.0)

	// 尝试转账超过余额的金额
	req := &domain.TransferRequest{
		UserID:        100,
		FromAccountID: 1,
		ToAccountID:   2,
		Amount:        2000.0, // 超过余额
		Currency:      "USD",
	}

	result, err := svc.Transfer(context.Background(), req)
	if err == nil && result.Status == rte.TxStatusCompleted {
		t.Fatal("Expected transfer to fail due to insufficient balance")
	}

	// 验证余额未变
	fromAcc, _ := svc.GetAccount(1)
	toAcc, _ := svc.GetAccount(2)

	if fromAcc.Balance != 1000.0 {
		t.Errorf("Expected from account balance unchanged at 1000.0, got %.2f", fromAcc.Balance)
	}
	if toAcc.Balance != 5000.0 {
		t.Errorf("Expected to account balance unchanged at 5000.0, got %.2f", toAcc.Balance)
	}
}

func TestTransferService_SameAccount(t *testing.T) {
	svc := NewTransferService()

	// 创建测试账户
	svc.CreateAccount(1, 100, "saving", "USD", 10000.0)

	// 尝试转账到同一账户
	req := &domain.TransferRequest{
		UserID:        100,
		FromAccountID: 1,
		ToAccountID:   1, // 同一账户
		Amount:        1000.0,
		Currency:      "USD",
	}

	_, err := svc.Transfer(context.Background(), req)
	if err != domain.ErrSameAccount {
		t.Errorf("Expected ErrSameAccount, got %v", err)
	}
}

func TestTransferService_InvalidAmount(t *testing.T) {
	svc := NewTransferService()

	// 创建测试账户
	svc.CreateAccount(1, 100, "saving", "USD", 10000.0)
	svc.CreateAccount(2, 100, "forex", "USD", 5000.0)

	// 尝试转账负数金额
	req := &domain.TransferRequest{
		UserID:        100,
		FromAccountID: 1,
		ToAccountID:   2,
		Amount:        -100.0, // 负数金额
		Currency:      "USD",
	}

	_, err := svc.Transfer(context.Background(), req)
	if err != domain.ErrInvalidAmount {
		t.Errorf("Expected ErrInvalidAmount, got %v", err)
	}
}

func TestTransferService_BalanceConservation(t *testing.T) {
	svc := NewTransferService()

	// 创建测试账户
	svc.CreateAccount(1, 100, "saving", "USD", 10000.0)
	svc.CreateAccount(2, 100, "forex", "USD", 5000.0)
	svc.CreateAccount(3, 200, "checking", "USD", 3000.0)

	initialTotal := 18000.0

	// 执行多次转账
	transfers := []struct {
		from   int64
		to     int64
		amount float64
	}{
		{1, 2, 1000.0},
		{2, 3, 500.0},
		{3, 1, 200.0},
	}

	for _, tr := range transfers {
		req := &domain.TransferRequest{
			UserID:        100,
			FromAccountID: tr.from,
			ToAccountID:   tr.to,
			Amount:        tr.amount,
			Currency:      "USD",
		}
		svc.Transfer(context.Background(), req)
	}

	// 验证总余额守恒
	acc1, _ := svc.GetAccount(1)
	acc2, _ := svc.GetAccount(2)
	acc3, _ := svc.GetAccount(3)

	finalTotal := acc1.Balance + acc2.Balance + acc3.Balance
	if finalTotal != initialTotal {
		t.Errorf("Balance conservation violated: expected %.2f, got %.2f", initialTotal, finalTotal)
	}
}

func TestTransferService_MultipleTransfers(t *testing.T) {
	svc := NewTransferService()

	// 创建测试账户
	svc.CreateAccount(1, 100, "saving", "USD", 10000.0)
	svc.CreateAccount(2, 100, "forex", "USD", 5000.0)

	// 执行多次转账
	for i := 0; i < 5; i++ {
		req := &domain.TransferRequest{
			UserID:        100,
			FromAccountID: 1,
			ToAccountID:   2,
			Amount:        100.0,
			Currency:      "USD",
		}

		result, err := svc.Transfer(context.Background(), req)
		if err != nil {
			t.Fatalf("Transfer %d failed: %v", i+1, err)
		}
		if result.Status != rte.TxStatusCompleted {
			t.Errorf("Transfer %d: expected status COMPLETED, got %s", i+1, result.Status)
		}
	}

	// 验证最终余额
	fromAcc, _ := svc.GetAccount(1)
	toAcc, _ := svc.GetAccount(2)

	if fromAcc.Balance != 9500.0 {
		t.Errorf("Expected from account balance 9500.0, got %.2f", fromAcc.Balance)
	}
	if toAcc.Balance != 5500.0 {
		t.Errorf("Expected to account balance 5500.0, got %.2f", toAcc.Balance)
	}
}
