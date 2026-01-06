// Package testinfra provides test infrastructure for RTE production validation.
// It includes MySQL and Redis connections, cleanup utilities, and test helpers.
package testinfra

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"

	"rte"
	"rte/circuit/memory"
	"rte/event"
	"rte/lock"
	rteredis "rte/lock/redis"
	"rte/store/mysql"
)

// DefaultConfig returns default test configuration
func DefaultConfig() TestConfig {
	return TestConfig{
		MySQLDSN:      getEnvOrDefault("RTE_TEST_MYSQL_DSN", "root:123456@tcp(localhost:3306)/rte_test?parseTime=true"),
		RedisAddr:     getEnvOrDefault("RTE_TEST_REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnvOrDefault("RTE_TEST_REDIS_PASSWORD", ""),
		RedisDB:       0,
		LockTTL:       30 * time.Second,
		StepTimeout:   5 * time.Second,
		TxTimeout:     30 * time.Second,
		MaxRetries:    3,
		RetryInterval: 100 * time.Millisecond,
		PropertyRuns:  100,
	}
}

// TestConfig holds test configuration
type TestConfig struct {
	MySQLDSN      string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	LockTTL       time.Duration
	StepTimeout   time.Duration
	TxTimeout     time.Duration
	MaxRetries    int
	RetryInterval time.Duration
	PropertyRuns  int
}

// TestInfrastructure provides test infrastructure with real MySQL and Redis
type TestInfrastructure struct {
	DB           *sql.DB
	Redis        *redis.Client
	MySQLStore   *mysql.MySQLStore
	StoreAdapter *StoreAdapter
	Locker       lock.Locker
	EventBus     event.EventBus
	Breaker      *memory.MemoryBreaker
	Engine       *rte.Engine
	Config       TestConfig
	testID       string
}

// NewTestInfrastructure creates a new test infrastructure with real MySQL and Redis.
// It skips the test if the infrastructure is not available.
func NewTestInfrastructure(t *testing.T) *TestInfrastructure {
	t.Helper()

	cfg := DefaultConfig()
	testID := fmt.Sprintf("test-%d", time.Now().UnixNano())

	// Connect to MySQL
	db, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		t.Skipf("Skipping test: MySQL connection failed: %v", err)
	}

	// Test MySQL connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Skipf("Skipping test: MySQL ping failed: %v", err)
	}

	// Connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	// Test Redis connection
	if err := redisClient.Ping(ctx).Err(); err != nil {
		db.Close()
		t.Skipf("Skipping test: Redis ping failed: %v", err)
	}

	// Create MySQL store and adapter
	mysqlStore := mysql.New(db)
	storeAdapter := NewStoreAdapter(mysqlStore)

	// Create locker
	locker := rteredis.NewRedisLocker(redisClient)

	// Create event bus
	eventBus := event.NewMemoryEventBus()

	// Create breaker
	breaker := memory.NewMemoryBreaker()

	// Create engine with store adapter
	engine := rte.NewEngine(
		rte.WithEngineStore(storeAdapter),
		rte.WithEngineLocker(locker),
		rte.WithEngineBreaker(breaker),
		rte.WithEngineEventBus(eventBus),
		rte.WithEngineConfig(rte.Config{
			LockTTL:          cfg.LockTTL,
			LockExtendPeriod: cfg.LockTTL / 3,
			StepTimeout:      cfg.StepTimeout,
			TxTimeout:        cfg.TxTimeout,
			MaxRetries:       cfg.MaxRetries,
			RetryInterval:    cfg.RetryInterval,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	return &TestInfrastructure{
		DB:           db,
		Redis:        redisClient,
		MySQLStore:   mysqlStore,
		StoreAdapter: storeAdapter,
		Locker:       locker,
		EventBus:     eventBus,
		Breaker:      breaker,
		Engine:       engine,
		Config:       cfg,
		testID:       testID,
	}
}

// NewTestInfrastructureWithConfig creates test infrastructure with custom config
func NewTestInfrastructureWithConfig(t *testing.T, cfg TestConfig) *TestInfrastructure {
	t.Helper()

	testID := fmt.Sprintf("test-%d", time.Now().UnixNano())

	// Connect to MySQL
	db, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		t.Skipf("Skipping test: MySQL connection failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Skipf("Skipping test: MySQL ping failed: %v", err)
	}

	// Connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	if err := redisClient.Ping(ctx).Err(); err != nil {
		db.Close()
		t.Skipf("Skipping test: Redis ping failed: %v", err)
	}

	mysqlStore := mysql.New(db)
	storeAdapter := NewStoreAdapter(mysqlStore)
	locker := rteredis.NewRedisLocker(redisClient)
	eventBus := event.NewMemoryEventBus()
	breaker := memory.NewMemoryBreaker()

	engine := rte.NewEngine(
		rte.WithEngineStore(storeAdapter),
		rte.WithEngineLocker(locker),
		rte.WithEngineBreaker(breaker),
		rte.WithEngineEventBus(eventBus),
		rte.WithEngineConfig(rte.Config{
			LockTTL:          cfg.LockTTL,
			LockExtendPeriod: cfg.LockTTL / 3,
			StepTimeout:      cfg.StepTimeout,
			TxTimeout:        cfg.TxTimeout,
			MaxRetries:       cfg.MaxRetries,
			RetryInterval:    cfg.RetryInterval,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	return &TestInfrastructure{
		DB:           db,
		Redis:        redisClient,
		MySQLStore:   mysqlStore,
		StoreAdapter: storeAdapter,
		Locker:       locker,
		EventBus:     eventBus,
		Breaker:      breaker,
		Engine:       engine,
		Config:       cfg,
		testID:       testID,
	}
}

// TestID returns the unique test identifier
func (ti *TestInfrastructure) TestID() string {
	return ti.testID
}

// GenerateTxID generates a unique transaction ID for testing
func (ti *TestInfrastructure) GenerateTxID(suffix string) string {
	return fmt.Sprintf("%s-%s", ti.testID, suffix)
}

// Cleanup cleans up test data from MySQL and Redis
func (ti *TestInfrastructure) Cleanup(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	// Clean up transactions with test prefix
	_, err := ti.DB.ExecContext(ctx, "DELETE FROM rte_steps WHERE tx_id LIKE ?", ti.testID+"%")
	if err != nil {
		t.Logf("Warning: failed to cleanup steps: %v", err)
	}

	_, err = ti.DB.ExecContext(ctx, "DELETE FROM rte_transactions WHERE tx_id LIKE ?", ti.testID+"%")
	if err != nil {
		t.Logf("Warning: failed to cleanup transactions: %v", err)
	}

	_, err = ti.DB.ExecContext(ctx, "DELETE FROM rte_idempotency WHERE idempotency_key LIKE ?", ti.testID+"%")
	if err != nil {
		t.Logf("Warning: failed to cleanup idempotency: %v", err)
	}

	// Clean up Redis locks
	keys, err := ti.Redis.Keys(ctx, "rte:lock:"+ti.testID+"*").Result()
	if err == nil && len(keys) > 0 {
		ti.Redis.Del(ctx, keys...)
	}
}

// CleanupAll cleans up all test data (use with caution)
func (ti *TestInfrastructure) CleanupAll(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	// Clean up all test transactions
	_, err := ti.DB.ExecContext(ctx, "DELETE FROM rte_steps WHERE tx_id LIKE 'test-%'")
	if err != nil {
		t.Logf("Warning: failed to cleanup all steps: %v", err)
	}

	_, err = ti.DB.ExecContext(ctx, "DELETE FROM rte_transactions WHERE tx_id LIKE 'test-%'")
	if err != nil {
		t.Logf("Warning: failed to cleanup all transactions: %v", err)
	}

	_, err = ti.DB.ExecContext(ctx, "DELETE FROM rte_idempotency WHERE idempotency_key LIKE 'test-%'")
	if err != nil {
		t.Logf("Warning: failed to cleanup all idempotency: %v", err)
	}

	// Clean up all test Redis locks
	keys, err := ti.Redis.Keys(ctx, "rte:lock:test-*").Result()
	if err == nil && len(keys) > 0 {
		ti.Redis.Del(ctx, keys...)
	}
}

// Close closes all connections
func (ti *TestInfrastructure) Close() {
	if ti.DB != nil {
		ti.DB.Close()
	}
	if ti.Redis != nil {
		ti.Redis.Close()
	}
}

// getEnvOrDefault returns environment variable value or default
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// SkipIfNoInfrastructure skips the test if infrastructure is not available
func SkipIfNoInfrastructure(t *testing.T) {
	t.Helper()
	cfg := DefaultConfig()

	// Check MySQL
	db, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		t.Skipf("Skipping test: MySQL not available: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Skipf("Skipping test: MySQL not available: %v", err)
	}

	// Check Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping test: Redis not available: %v", err)
	}
}
