// Package mysql provides a MySQL implementation of the rte.TxStore interface.
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"rte"
)

// MySQLStore implements the rte.TxStore interface using MySQL.
type MySQLStore struct {
	db *sql.DB
}

// New creates a new MySQLStore with the given database connection.
func New(db *sql.DB) *MySQLStore {
	return &MySQLStore{db: db}
}

// ============================================================================
// Private serialization types
// ============================================================================

// stringSlice is a custom type for storing string slices as JSON in the database.
type stringSlice []string

// Value implements the driver.Valuer interface for database serialization.
func (s stringSlice) Value() (driver.Value, error) {
	if s == nil {
		return "[]", nil
	}
	return json.Marshal(s)
}

// Scan implements the sql.Scanner interface for database deserialization.
func (s *stringSlice) Scan(value interface{}) error {
	if value == nil {
		*s = nil
		return nil
	}

	var b []byte
	switch v := value.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return fmt.Errorf("cannot scan type %T into stringSlice", value)
	}

	return json.Unmarshal(b, s)
}

// contextJSON is a wrapper for StoreTxContext that implements database serialization.
type contextJSON struct {
	TxID      string            `json:"tx_id"`
	TxType    string            `json:"tx_type"`
	StepIndex int               `json:"step_index"`
	Input     map[string]any    `json:"input"`
	Output    map[string]any    `json:"output"`
	Metadata  map[string]string `json:"metadata"`
}

// Value implements the driver.Valuer interface for database serialization.
func (c *contextJSON) Value() (driver.Value, error) {
	if c == nil {
		return "{}", nil
	}
	return json.Marshal(c)
}

// Scan implements the sql.Scanner interface for database deserialization.
func (c *contextJSON) Scan(value interface{}) error {
	if value == nil {
		return nil
	}

	var b []byte
	switch v := value.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return fmt.Errorf("cannot scan type %T into contextJSON", value)
	}

	return json.Unmarshal(b, c)
}

// toStoreTxContext converts contextJSON to *rte.StoreTxContext.
func (c *contextJSON) toStoreTxContext() *rte.StoreTxContext {
	if c == nil {
		return nil
	}
	return &rte.StoreTxContext{
		TxID:      c.TxID,
		TxType:    c.TxType,
		StepIndex: c.StepIndex,
		Input:     c.Input,
		Output:    c.Output,
		Metadata:  c.Metadata,
	}
}

// newContextJSON creates a contextJSON from *rte.StoreTxContext.
func newContextJSON(ctx *rte.StoreTxContext) *contextJSON {
	if ctx == nil {
		return &contextJSON{}
	}
	return &contextJSON{
		TxID:      ctx.TxID,
		TxType:    ctx.TxType,
		StepIndex: ctx.StepIndex,
		Input:     ctx.Input,
		Output:    ctx.Output,
		Metadata:  ctx.Metadata,
	}
}

// ============================================================================
// Transaction Operations
// ============================================================================

// CreateTransaction creates a new transaction record.
func (s *MySQLStore) CreateTransaction(ctx context.Context, tx *rte.StoreTx) error {
	query := `
		INSERT INTO rte_transactions (
			tx_id, tx_type, status, current_step, total_steps, step_names,
			lock_keys, context, error_msg, retry_count, max_retries, version,
			created_at, updated_at, locked_at, completed_at, timeout_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	stepNames, err := json.Marshal(tx.StepNames)
	if err != nil {
		return fmt.Errorf("marshal step_names: %w", err)
	}

	lockKeys, err := json.Marshal(tx.LockKeys)
	if err != nil {
		return fmt.Errorf("marshal lock_keys: %w", err)
	}

	ctxJSON, err := json.Marshal(newContextJSON(tx.Context))
	if err != nil {
		return fmt.Errorf("marshal context: %w", err)
	}

	result, err := s.db.ExecContext(ctx, query,
		tx.TxID, tx.TxType, tx.Status, tx.CurrentStep, tx.TotalSteps, stepNames,
		lockKeys, ctxJSON, tx.ErrorMsg, tx.RetryCount, tx.MaxRetries, tx.Version,
		tx.CreatedAt, tx.UpdatedAt, tx.LockedAt, tx.CompletedAt, tx.TimeoutAt,
	)
	if err != nil {
		if isDuplicateKeyError(err) {
			return rte.ErrTransactionAlreadyExists
		}
		return fmt.Errorf("%w: create transaction: %v", rte.ErrStoreOperationFailed, err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("get last insert id: %w", err)
	}
	tx.ID = id

	return nil
}

// UpdateTransaction updates an existing transaction with optimistic locking.
// The caller is expected to have already incremented the version before calling this method.
func (s *MySQLStore) UpdateTransaction(ctx context.Context, tx *rte.StoreTx) error {
	query := `
		UPDATE rte_transactions SET
			status = ?, current_step = ?, context = ?, error_msg = ?,
			retry_count = ?, version = ?, updated_at = ?,
			locked_at = ?, completed_at = ?, timeout_at = ?
		WHERE tx_id = ? AND version = ?
	`

	ctxJSON, err := json.Marshal(newContextJSON(tx.Context))
	if err != nil {
		return fmt.Errorf("marshal context: %w", err)
	}

	// The caller has already incremented the version, so we use tx.Version for the new value
	// and tx.Version-1 for the WHERE clause to match the existing version
	result, err := s.db.ExecContext(ctx, query,
		tx.Status, tx.CurrentStep, ctxJSON, tx.ErrorMsg,
		tx.RetryCount, tx.Version, time.Now(),
		tx.LockedAt, tx.CompletedAt, tx.TimeoutAt,
		tx.TxID, tx.Version-1,
	)
	if err != nil {
		return fmt.Errorf("%w: update transaction: %v", rte.ErrStoreOperationFailed, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		// Check if transaction exists
		exists, err := s.transactionExists(ctx, tx.TxID)
		if err != nil {
			return err
		}
		if !exists {
			return rte.ErrTransactionNotFound
		}
		return rte.ErrVersionConflict
	}

	tx.UpdatedAt = time.Now()

	return nil
}

// GetTransaction retrieves a transaction by its ID.
func (s *MySQLStore) GetTransaction(ctx context.Context, txID string) (*rte.StoreTx, error) {
	query := `
		SELECT id, tx_id, tx_type, status, current_step, total_steps, step_names,
			lock_keys, context, error_msg, retry_count, max_retries, version,
			created_at, updated_at, locked_at, completed_at, timeout_at
		FROM rte_transactions
		WHERE tx_id = ?
	`

	var stepNames, lockKeys []byte
	var ctxData contextJSON
	tx := &rte.StoreTx{}

	err := s.db.QueryRowContext(ctx, query, txID).Scan(
		&tx.ID, &tx.TxID, &tx.TxType, &tx.Status, &tx.CurrentStep, &tx.TotalSteps, &stepNames,
		&lockKeys, &ctxData, &tx.ErrorMsg, &tx.RetryCount, &tx.MaxRetries, &tx.Version,
		&tx.CreatedAt, &tx.UpdatedAt, &tx.LockedAt, &tx.CompletedAt, &tx.TimeoutAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, rte.ErrTransactionNotFound
		}
		return nil, fmt.Errorf("%w: get transaction: %v", rte.ErrStoreOperationFailed, err)
	}

	if err := json.Unmarshal(stepNames, &tx.StepNames); err != nil {
		return nil, fmt.Errorf("unmarshal step_names: %w", err)
	}
	if err := json.Unmarshal(lockKeys, &tx.LockKeys); err != nil {
		return nil, fmt.Errorf("unmarshal lock_keys: %w", err)
	}
	tx.Context = ctxData.toStoreTxContext()

	return tx, nil
}

// transactionExists checks if a transaction exists.
func (s *MySQLStore) transactionExists(ctx context.Context, txID string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rte_transactions WHERE tx_id = ?", txID).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("%w: check transaction exists: %v", rte.ErrStoreOperationFailed, err)
	}
	return count > 0, nil
}

// ============================================================================
// Step Operations
// ============================================================================

// CreateStep creates a new step record.
func (s *MySQLStore) CreateStep(ctx context.Context, step *rte.StoreStepRecord) error {
	query := `
		INSERT INTO rte_steps (
			tx_id, step_index, step_name, status, idempotency_key,
			input, output, error_msg, retry_count,
			started_at, completed_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	result, err := s.db.ExecContext(ctx, query,
		step.TxID, step.StepIndex, step.StepName, step.Status, step.IdempotencyKey,
		step.Input, step.Output, step.ErrorMsg, step.RetryCount,
		step.StartedAt, step.CompletedAt, step.CreatedAt, step.UpdatedAt,
	)
	if err != nil {
		if isDuplicateKeyError(err) {
			return fmt.Errorf("%w: step already exists", rte.ErrStoreOperationFailed)
		}
		return fmt.Errorf("%w: create step: %v", rte.ErrStoreOperationFailed, err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("get last insert id: %w", err)
	}
	step.ID = id

	return nil
}

// UpdateStep updates an existing step record.
func (s *MySQLStore) UpdateStep(ctx context.Context, step *rte.StoreStepRecord) error {
	query := `
		UPDATE rte_steps SET
			status = ?, idempotency_key = ?, input = ?, output = ?,
			error_msg = ?, retry_count = ?, started_at = ?, completed_at = ?, updated_at = ?
		WHERE tx_id = ? AND step_index = ?
	`

	result, err := s.db.ExecContext(ctx, query,
		step.Status, step.IdempotencyKey, step.Input, step.Output,
		step.ErrorMsg, step.RetryCount, step.StartedAt, step.CompletedAt, time.Now(),
		step.TxID, step.StepIndex,
	)
	if err != nil {
		return fmt.Errorf("%w: update step: %v", rte.ErrStoreOperationFailed, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return rte.ErrStepNotFound
	}

	step.UpdatedAt = time.Now()
	return nil
}

// GetStep retrieves a specific step by transaction ID and step index.
func (s *MySQLStore) GetStep(ctx context.Context, txID string, stepIndex int) (*rte.StoreStepRecord, error) {
	query := `
		SELECT id, tx_id, step_index, step_name, status, idempotency_key,
			input, output, error_msg, retry_count,
			started_at, completed_at, created_at, updated_at
		FROM rte_steps
		WHERE tx_id = ? AND step_index = ?
	`

	step := &rte.StoreStepRecord{}
	err := s.db.QueryRowContext(ctx, query, txID, stepIndex).Scan(
		&step.ID, &step.TxID, &step.StepIndex, &step.StepName, &step.Status, &step.IdempotencyKey,
		&step.Input, &step.Output, &step.ErrorMsg, &step.RetryCount,
		&step.StartedAt, &step.CompletedAt, &step.CreatedAt, &step.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, rte.ErrStepNotFound
		}
		return nil, fmt.Errorf("%w: get step: %v", rte.ErrStoreOperationFailed, err)
	}

	return step, nil
}

// GetSteps retrieves all steps for a transaction.
func (s *MySQLStore) GetSteps(ctx context.Context, txID string) ([]*rte.StoreStepRecord, error) {
	query := `
		SELECT id, tx_id, step_index, step_name, status, idempotency_key,
			input, output, error_msg, retry_count,
			started_at, completed_at, created_at, updated_at
		FROM rte_steps
		WHERE tx_id = ?
		ORDER BY step_index ASC
	`

	rows, err := s.db.QueryContext(ctx, query, txID)
	if err != nil {
		return nil, fmt.Errorf("%w: get steps: %v", rte.ErrStoreOperationFailed, err)
	}
	defer rows.Close()

	var steps []*rte.StoreStepRecord
	for rows.Next() {
		step := &rte.StoreStepRecord{}
		err := rows.Scan(
			&step.ID, &step.TxID, &step.StepIndex, &step.StepName, &step.Status, &step.IdempotencyKey,
			&step.Input, &step.Output, &step.ErrorMsg, &step.RetryCount,
			&step.StartedAt, &step.CompletedAt, &step.CreatedAt, &step.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("%w: scan step: %v", rte.ErrStoreOperationFailed, err)
		}
		steps = append(steps, step)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate steps: %v", rte.ErrStoreOperationFailed, err)
	}

	return steps, nil
}

// ============================================================================
// Recovery Queries
// ============================================================================

// GetPendingTransactions retrieves transactions that are pending (CREATED status)
// and older than the specified duration.
func (s *MySQLStore) GetPendingTransactions(ctx context.Context, olderThan time.Duration) ([]*rte.StoreTx, error) {
	query := `
		SELECT id, tx_id, tx_type, status, current_step, total_steps, step_names,
			lock_keys, context, error_msg, retry_count, max_retries, version,
			created_at, updated_at, locked_at, completed_at, timeout_at
		FROM rte_transactions
		WHERE status = ? AND updated_at < ?
		ORDER BY created_at ASC
	`

	threshold := time.Now().Add(-olderThan)
	return s.queryTransactions(ctx, query, rte.TxStatusCreated, threshold)
}

// GetStuckTransactions retrieves transactions that are stuck
// (LOCKED or EXECUTING status) and older than the specified duration.
func (s *MySQLStore) GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*rte.StoreTx, error) {
	query := `
		SELECT id, tx_id, tx_type, status, current_step, total_steps, step_names,
			lock_keys, context, error_msg, retry_count, max_retries, version,
			created_at, updated_at, locked_at, completed_at, timeout_at
		FROM rte_transactions
		WHERE status IN (?, ?) AND updated_at < ?
		ORDER BY created_at ASC
	`

	threshold := time.Now().Add(-olderThan)
	return s.queryTransactions(ctx, query, rte.TxStatusLocked, rte.TxStatusExecuting, threshold)
}

// GetRetryableTransactions retrieves failed transactions that can be retried.
func (s *MySQLStore) GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*rte.StoreTx, error) {
	query := `
		SELECT id, tx_id, tx_type, status, current_step, total_steps, step_names,
			lock_keys, context, error_msg, retry_count, max_retries, version,
			created_at, updated_at, locked_at, completed_at, timeout_at
		FROM rte_transactions
		WHERE status = ? AND retry_count < ?
		ORDER BY created_at ASC
	`

	return s.queryTransactions(ctx, query, rte.TxStatusFailed, maxRetries)
}

// queryTransactions is a helper function to query transactions.
func (s *MySQLStore) queryTransactions(ctx context.Context, query string, args ...interface{}) ([]*rte.StoreTx, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%w: query transactions: %v", rte.ErrStoreOperationFailed, err)
	}
	defer rows.Close()

	var transactions []*rte.StoreTx
	for rows.Next() {
		var stepNames, lockKeys []byte
		var ctxData contextJSON
		tx := &rte.StoreTx{}

		err := rows.Scan(
			&tx.ID, &tx.TxID, &tx.TxType, &tx.Status, &tx.CurrentStep, &tx.TotalSteps, &stepNames,
			&lockKeys, &ctxData, &tx.ErrorMsg, &tx.RetryCount, &tx.MaxRetries, &tx.Version,
			&tx.CreatedAt, &tx.UpdatedAt, &tx.LockedAt, &tx.CompletedAt, &tx.TimeoutAt,
		)
		if err != nil {
			return nil, fmt.Errorf("%w: scan transaction: %v", rte.ErrStoreOperationFailed, err)
		}

		if err := json.Unmarshal(stepNames, &tx.StepNames); err != nil {
			return nil, fmt.Errorf("unmarshal step_names: %w", err)
		}
		if err := json.Unmarshal(lockKeys, &tx.LockKeys); err != nil {
			return nil, fmt.Errorf("unmarshal lock_keys: %w", err)
		}
		tx.Context = ctxData.toStoreTxContext()

		transactions = append(transactions, tx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate transactions: %v", rte.ErrStoreOperationFailed, err)
	}

	return transactions, nil
}

// ============================================================================
// Admin Queries
// ============================================================================

// ListTransactions lists transactions with optional filters.
func (s *MySQLStore) ListTransactions(ctx context.Context, filter *rte.StoreTxFilter) ([]*rte.StoreTx, int64, error) {
	// Build WHERE clause
	var conditions []string
	var args []interface{}

	if len(filter.Status) > 0 {
		placeholders := make([]string, len(filter.Status))
		for i, status := range filter.Status {
			placeholders[i] = "?"
			args = append(args, status)
		}
		conditions = append(conditions, fmt.Sprintf("status IN (%s)", strings.Join(placeholders, ",")))
	}

	if filter.TxType != "" {
		conditions = append(conditions, "tx_type = ?")
		args = append(args, filter.TxType)
	}

	if !filter.StartTime.IsZero() {
		conditions = append(conditions, "created_at >= ?")
		args = append(args, filter.StartTime)
	}

	if !filter.EndTime.IsZero() {
		conditions = append(conditions, "created_at <= ?")
		args = append(args, filter.EndTime)
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Count total
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM rte_transactions %s", whereClause)
	var total int64
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("%w: count transactions: %v", rte.ErrStoreOperationFailed, err)
	}

	// Query with pagination
	query := fmt.Sprintf(`
		SELECT id, tx_id, tx_type, status, current_step, total_steps, step_names,
			lock_keys, context, error_msg, retry_count, max_retries, version,
			created_at, updated_at, locked_at, completed_at, timeout_at
		FROM rte_transactions
		%s
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`, whereClause)

	args = append(args, filter.Limit, filter.Offset)
	transactions, err := s.queryTransactions(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}

	return transactions, total, nil
}

// CountTransactionsByStatus counts transactions grouped by status.
func (s *MySQLStore) CountTransactionsByStatus(ctx context.Context) (map[rte.TxStatus]int64, error) {
	query := `SELECT status, COUNT(*) FROM rte_transactions GROUP BY status`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("%w: count transactions by status: %v", rte.ErrStoreOperationFailed, err)
	}
	defer rows.Close()

	result := make(map[rte.TxStatus]int64)
	for rows.Next() {
		var status rte.TxStatus
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("%w: scan status count: %v", rte.ErrStoreOperationFailed, err)
		}
		result[status] = count
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate status counts: %v", rte.ErrStoreOperationFailed, err)
	}

	return result, nil
}

// ============================================================================
// Idempotency Operations
// ============================================================================

// CheckIdempotency checks if an operation was already executed.
func (s *MySQLStore) CheckIdempotency(ctx context.Context, key string) (bool, []byte, error) {
	query := `
		SELECT result FROM rte_idempotency
		WHERE idempotency_key = ? AND expires_at > ?
	`

	var result []byte
	err := s.db.QueryRowContext(ctx, query, key, time.Now()).Scan(&result)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil, nil
		}
		return false, nil, fmt.Errorf("%w: check idempotency: %v", rte.ErrIdempotencyCheckFailed, err)
	}

	return true, result, nil
}

// MarkIdempotency marks an operation as executed with its result.
func (s *MySQLStore) MarkIdempotency(ctx context.Context, key string, result []byte, ttl time.Duration) error {
	query := `
		INSERT INTO rte_idempotency (idempotency_key, result, created_at, expires_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE result = VALUES(result), expires_at = VALUES(expires_at)
	`

	now := time.Now()
	expiresAt := now.Add(ttl)

	_, err := s.db.ExecContext(ctx, query, key, result, now, expiresAt)
	if err != nil {
		return fmt.Errorf("%w: mark idempotency: %v", rte.ErrStoreOperationFailed, err)
	}

	return nil
}

// DeleteExpiredIdempotency removes expired idempotency records.
func (s *MySQLStore) DeleteExpiredIdempotency(ctx context.Context) (int64, error) {
	query := `DELETE FROM rte_idempotency WHERE expires_at < ?`

	result, err := s.db.ExecContext(ctx, query, time.Now())
	if err != nil {
		return 0, fmt.Errorf("%w: delete expired idempotency: %v", rte.ErrStoreOperationFailed, err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get rows affected: %w", err)
	}

	return count, nil
}

// ============================================================================
// Helper Functions
// ============================================================================

// isDuplicateKeyError checks if the error is a MySQL duplicate key error.
func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	// MySQL error code 1062 is for duplicate entry
	return strings.Contains(err.Error(), "Duplicate entry") ||
		strings.Contains(err.Error(), "1062")
}

// Ensure MySQLStore implements rte.TxStore interface.
var _ rte.TxStore = (*MySQLStore)(nil)
