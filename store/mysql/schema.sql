-- RTE (Reliable Transaction Engine) Database Schema
-- This schema supports MySQL 5.7+ and MySQL 8.0+

-- ============================================================================
-- Table: rte_transactions
-- Description: Stores transaction records with optimistic locking support
-- ============================================================================
CREATE TABLE IF NOT EXISTS rte_transactions (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT 'Auto-increment primary key',
    tx_id VARCHAR(64) UNIQUE NOT NULL COMMENT 'Unique transaction identifier',
    tx_type VARCHAR(64) NOT NULL COMMENT 'Transaction type (e.g., saving_to_forex)',
    status VARCHAR(32) NOT NULL COMMENT 'Transaction status',
    current_step INT DEFAULT 0 COMMENT 'Current step index being executed',
    total_steps INT NOT NULL COMMENT 'Total number of steps',
    step_names JSON COMMENT 'Ordered list of step names',
    lock_keys JSON COMMENT 'Keys to lock for this transaction',
    context JSON NOT NULL COMMENT 'Transaction context (input, output, metadata)',
    error_msg TEXT COMMENT 'Error message if transaction failed',
    retry_count INT DEFAULT 0 COMMENT 'Number of retry attempts',
    max_retries INT DEFAULT 3 COMMENT 'Maximum retry attempts allowed',
    version INT DEFAULT 0 COMMENT 'Version for optimistic locking',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Creation timestamp',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Last update timestamp',
    locked_at TIMESTAMP NULL COMMENT 'When locks were acquired',
    completed_at TIMESTAMP NULL COMMENT 'When transaction completed',
    timeout_at TIMESTAMP NULL COMMENT 'When transaction should timeout',
    
    -- Indexes for common queries
    INDEX idx_status_created (status, created_at) COMMENT 'For listing by status',
    INDEX idx_status_updated (status, updated_at) COMMENT 'For recovery queries',
    INDEX idx_tx_type (tx_type) COMMENT 'For filtering by type',
    INDEX idx_timeout (timeout_at) COMMENT 'For timeout detection',
    INDEX idx_retry (status, retry_count) COMMENT 'For retryable transactions'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='RTE transaction records';

-- ============================================================================
-- Table: rte_steps
-- Description: Stores step execution records for each transaction
-- ============================================================================
CREATE TABLE IF NOT EXISTS rte_steps (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT 'Auto-increment primary key',
    tx_id VARCHAR(64) NOT NULL COMMENT 'Transaction ID this step belongs to',
    step_index INT NOT NULL COMMENT 'Step index in the transaction',
    step_name VARCHAR(64) NOT NULL COMMENT 'Step name',
    status VARCHAR(32) NOT NULL COMMENT 'Step status',
    idempotency_key VARCHAR(256) COMMENT 'Key for idempotency checking',
    input JSON COMMENT 'Step input data',
    output JSON COMMENT 'Step output data',
    error_msg TEXT COMMENT 'Error message if step failed',
    retry_count INT DEFAULT 0 COMMENT 'Number of retry attempts for this step',
    started_at TIMESTAMP NULL COMMENT 'When step started executing',
    completed_at TIMESTAMP NULL COMMENT 'When step completed',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Creation timestamp',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Last update timestamp',
    
    -- Unique constraint for transaction + step index
    UNIQUE KEY uk_tx_step (tx_id, step_index) COMMENT 'Ensure unique step per transaction',
    
    -- Indexes for common queries
    INDEX idx_tx_id (tx_id) COMMENT 'For fetching all steps of a transaction',
    INDEX idx_status (status) COMMENT 'For filtering by status',
    INDEX idx_idempotency (idempotency_key) COMMENT 'For idempotency lookups',
    
    -- Foreign key constraint (optional, can be disabled for performance)
    CONSTRAINT fk_steps_tx FOREIGN KEY (tx_id) REFERENCES rte_transactions(tx_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='RTE step execution records';

-- ============================================================================
-- Table: rte_idempotency
-- Description: Stores idempotency records for step execution
-- ============================================================================
CREATE TABLE IF NOT EXISTS rte_idempotency (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT 'Auto-increment primary key',
    idempotency_key VARCHAR(256) UNIQUE NOT NULL COMMENT 'Unique idempotency key',
    result BLOB COMMENT 'Operation result (serialized)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Creation timestamp',
    expires_at TIMESTAMP NOT NULL COMMENT 'When this record expires',
    
    -- Index for cleanup queries
    INDEX idx_expires (expires_at) COMMENT 'For expired record cleanup'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='RTE idempotency records';

-- ============================================================================
-- Stored Procedures (Optional)
-- ============================================================================

-- Procedure to clean up expired idempotency records
DELIMITER //
CREATE PROCEDURE IF NOT EXISTS rte_cleanup_expired_idempotency()
BEGIN
    DELETE FROM rte_idempotency WHERE expires_at < NOW();
END //
DELIMITER ;

-- Procedure to get stuck transactions
DELIMITER //
CREATE PROCEDURE IF NOT EXISTS rte_get_stuck_transactions(IN threshold_seconds INT)
BEGIN
    SELECT * FROM rte_transactions 
    WHERE status IN ('LOCKED', 'EXECUTING') 
    AND updated_at < DATE_SUB(NOW(), INTERVAL threshold_seconds SECOND);
END //
DELIMITER ;

-- Procedure to get retryable transactions
DELIMITER //
CREATE PROCEDURE IF NOT EXISTS rte_get_retryable_transactions(IN max_retry_count INT)
BEGIN
    SELECT * FROM rte_transactions 
    WHERE status = 'FAILED' 
    AND retry_count < max_retry_count;
END //
DELIMITER ;
