// Package main 示例应用入口
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"rte"
	"rte-example-app/internal/domain"
	"rte-example-app/internal/service"
)

func main() {
	log.Println("=== RTE Example Application ===")
	log.Println()

	// 检查命令行参数
	if len(os.Args) > 1 && os.Args[1] == "error" {
		// 运行错误处理示例
		service.RunErrorHandlingExamples()
		return
	}

	// 运行基本转账示例
	runTransferExamples()
}

func runTransferExamples() {
	// 创建转账服务
	svc := service.NewTransferService()

	// 创建测试账户
	log.Println("Creating test accounts...")
	svc.CreateAccount(1, 100, "saving", "USD", 10000.0)
	svc.CreateAccount(2, 100, "forex", "USD", 5000.0)
	svc.CreateAccount(3, 200, "checking", "USD", 3000.0)

	printAccountBalances(svc, "Initial balances")

	// 示例1: 成功的转账
	log.Println()
	log.Println("=== Example 1: Successful Transfer ===")
	runSuccessfulTransfer(svc)

	// 示例2: 余额不足的转账
	log.Println()
	log.Println("=== Example 2: Insufficient Balance Transfer ===")
	runInsufficientBalanceTransfer(svc)

	// 示例3: 多次转账
	log.Println()
	log.Println("=== Example 3: Multiple Transfers ===")
	runMultipleTransfers(svc)

	// 最终余额
	printAccountBalances(svc, "Final balances")

	log.Println()
	log.Println("=== All examples completed ===")
}

func runSuccessfulTransfer(svc *service.TransferService) {
	ctx := context.Background()

	req := &domain.TransferRequest{
		UserID:        100,
		FromAccountID: 1,
		ToAccountID:   2,
		Amount:        1000.0,
		Currency:      "USD",
	}

	log.Printf("Transferring %.2f from account %d to account %d...",
		req.Amount, req.FromAccountID, req.ToAccountID)

	result, err := svc.Transfer(ctx, req)
	if err != nil {
		log.Printf("Transfer failed: %v", err)
		return
	}

	log.Printf("Transfer result: status=%s, duration=%v", result.Status, result.Duration)

	if result.Status == rte.TxStatusCompleted {
		log.Println("Transfer completed successfully!")
	} else {
		log.Printf("Transfer ended with status: %s", result.Status)
	}

	printAccountBalances(svc, "After successful transfer")
}

func runInsufficientBalanceTransfer(svc *service.TransferService) {
	ctx := context.Background()

	req := &domain.TransferRequest{
		UserID:        100,
		FromAccountID: 1,
		ToAccountID:   2,
		Amount:        100000.0, // 超过余额
		Currency:      "USD",
	}

	log.Printf("Attempting to transfer %.2f from account %d (insufficient balance)...",
		req.Amount, req.FromAccountID)

	result, err := svc.Transfer(ctx, req)
	if err != nil {
		log.Printf("Transfer failed as expected: %v", err)
		return
	}

	log.Printf("Transfer result: status=%s", result.Status)
	if result.Error != nil {
		log.Printf("Error: %v", result.Error)
	}

	printAccountBalances(svc, "After failed transfer (should be unchanged)")
}

func runMultipleTransfers(svc *service.TransferService) {
	ctx := context.Background()

	transfers := []struct {
		from   int64
		to     int64
		amount float64
	}{
		{1, 2, 500.0},
		{2, 3, 200.0},
		{3, 1, 100.0},
	}

	for i, t := range transfers {
		req := &domain.TransferRequest{
			UserID:        100,
			FromAccountID: t.from,
			ToAccountID:   t.to,
			Amount:        t.amount,
			Currency:      "USD",
		}

		log.Printf("Transfer %d: %.2f from account %d to account %d",
			i+1, t.amount, t.from, t.to)

		result, err := svc.Transfer(ctx, req)
		if err != nil {
			log.Printf("  Failed: %v", err)
			continue
		}

		log.Printf("  Result: status=%s, duration=%v", result.Status, result.Duration)
	}
}

func printAccountBalances(svc *service.TransferService, title string) {
	log.Println()
	log.Printf("--- %s ---", title)

	for _, id := range []int64{1, 2, 3} {
		acc, err := svc.GetAccount(id)
		if err != nil {
			log.Printf("  Account %d: error - %v", id, err)
			continue
		}
		log.Printf("  Account %d (%s): %.2f %s", acc.ID, acc.Type, acc.Balance, acc.Currency)
	}
	fmt.Println()
}
