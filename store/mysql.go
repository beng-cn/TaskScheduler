package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"task-scheduler/models"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

// MySQLStore 是基于 MySQL 的 Store 实现。
type MySQLStore struct {
	db  *sql.DB
	seq atomic.Int64
}

func (m *MySQLStore) nextSeq() int64 {
	return m.seq.Add(1)
}

// NewMySQLStore 创建一个新的 MySQL 存储实例并自动建表。
func NewMySQLStore(dsn string) (*MySQLStore, error) {
	db, err := sql.Open("mysql", dsn+"?parseTime=true&charset=utf8mb4&loc=Local")
	if err != nil {
		return nil, fmt.Errorf("mysql: 连接失败: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("mysql: Ping 失败: %w", err)
	}

	store := &MySQLStore{db: db}
	if err := store.autoMigrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("mysql: 建表失败: %w", err)
	}

	return store, nil
}

func (m *MySQLStore) autoMigrate() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS tasks (
			id             VARCHAR(64)   NOT NULL PRIMARY KEY,
			name           VARCHAR(255)  NOT NULL,
			type           VARCHAR(50)   NOT NULL,
			payload        TEXT,
			status         VARCHAR(20)   NOT NULL DEFAULT 'pending',
			priority       INT           NOT NULL DEFAULT 0,
			retries        INT           NOT NULL DEFAULT 0,
			max_retries    INT           NOT NULL DEFAULT 3,
			timeout        BIGINT        NOT NULL DEFAULT 30,
			max_latency_ms BIGINT        NOT NULL DEFAULT 0,
			repeat_sec     BIGINT        NOT NULL DEFAULT 0,
			scheduled_at   DATETIME      NULL,
			started_at     DATETIME      NULL,
			finished_at    DATETIME      NULL,
			result         TEXT,
			error          TEXT,
			steps          TEXT,
			namespace      VARCHAR(64)   NOT NULL DEFAULT 'default',
			created_at     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			INDEX idx_status (status),
			INDEX idx_type (type),
			INDEX idx_namespace (namespace),
			INDEX idx_scheduled (status, scheduled_at),
			INDEX idx_created (created_at)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

		`CREATE TABLE IF NOT EXISTS locks (
			lock_key   VARCHAR(128) NOT NULL PRIMARY KEY,
			expiry     BIGINT       NOT NULL,
			created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	}
	for _, q := range queries {
		if _, err := m.db.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

// --- 任务 CRUD 实现 ---

func (m *MySQLStore) CreateTask(ctx context.Context, task *models.Task) error {
	task.CreatedAt = time.Now()
	task.UpdatedAt = time.Now()
	if task.Status == "" {
		task.Status = models.StatusPending
	}
	if task.ID == "" {
		task.ID = fmt.Sprintf("task-%d-%d", time.Now().UnixMilli(), m.nextSeq())
	}
	if task.Namespace == "" {
		task.Namespace = "default"
	}

	stepsJSON, err := json.Marshal(task.Steps)
	if err != nil {
		return fmt.Errorf("mysql: 序列化 Steps 失败: %w", err)
	}
	_, err = m.db.ExecContext(ctx,
		`INSERT INTO tasks (id, name, type, payload, status, priority, retries, max_retries,
		 timeout, max_latency_ms, repeat_sec, scheduled_at, started_at, finished_at,
		 result, error, steps, namespace, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		task.ID, task.Name, task.Type, task.Payload, task.Status, task.Priority,
		task.Retries, task.MaxRetries, task.Timeout, task.MaxLatencyMs, task.RepeatSec,
		nullTime(task.ScheduledAt), nullTimePtr(task.StartedAt), nullTimePtr(task.FinishedAt),
		task.Result, task.Error, string(stepsJSON), task.Namespace, task.CreatedAt, task.UpdatedAt,
	)
	return err
}

func (m *MySQLStore) GetTask(ctx context.Context, id string) (*models.Task, error) {
	row := m.db.QueryRowContext(ctx,
		`SELECT id, name, type, payload, status, priority, retries, max_retries,
		 timeout, max_latency_ms, repeat_sec, scheduled_at, started_at, finished_at,
		 result, error, steps, namespace, created_at, updated_at
		 FROM tasks WHERE id = ?`, id)
	return scanTask(row)
}

func (m *MySQLStore) ListTasks(ctx context.Context, namespace string) ([]*models.Task, error) {
	query := `SELECT id, name, type, payload, status, priority, retries, max_retries,
		 timeout, max_latency_ms, repeat_sec, scheduled_at, started_at, finished_at,
		 result, error, steps, namespace, created_at, updated_at FROM tasks`
	args := []interface{}{}
	if namespace != "" {
		query += " WHERE namespace = ? OR namespace = ''"
		args = append(args, namespace)
	}
	query += " ORDER BY priority DESC, created_at ASC"
	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTasks(rows)
}

func (m *MySQLStore) ListPendingTasks(ctx context.Context, namespace string) ([]*models.Task, error) {
	query := `SELECT id, name, type, payload, status, priority, retries, max_retries,
		 timeout, max_latency_ms, repeat_sec, scheduled_at, started_at, finished_at,
		 result, error, steps, namespace, created_at, updated_at
		 FROM tasks WHERE status = ? AND (scheduled_at IS NULL OR scheduled_at <= ?)`
	args := []interface{}{models.StatusPending, time.Now()}
	if namespace != "" {
		query += " AND (namespace = ? OR namespace = '')"
		args = append(args, namespace)
	}
	query += " ORDER BY priority DESC, created_at ASC"
	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTasks(rows)
}

func (m *MySQLStore) UpdateTask(ctx context.Context, task *models.Task) error {
	task.UpdatedAt = time.Now()
	stepsJSON, err := json.Marshal(task.Steps)
	if err != nil {
		return fmt.Errorf("mysql: 序列化 Steps 失败: %w", err)
	}
	_, err = m.db.ExecContext(ctx,
		`UPDATE tasks SET name=?, type=?, payload=?, status=?, priority=?, retries=?, max_retries=?,
		 timeout=?, max_latency_ms=?, repeat_sec=?, scheduled_at=?, started_at=?, finished_at=?,
		 result=?, error=?, steps=?, namespace=?, updated_at=?
		 WHERE id=?`,
		task.Name, task.Type, task.Payload, task.Status, task.Priority,
		task.Retries, task.MaxRetries, task.Timeout, task.MaxLatencyMs, task.RepeatSec,
		nullTime(task.ScheduledAt), nullTimePtr(task.StartedAt), nullTimePtr(task.FinishedAt),
		task.Result, task.Error, string(stepsJSON), task.Namespace, task.UpdatedAt, task.ID,
	)
	return err
}

func (m *MySQLStore) DeleteTask(ctx context.Context, id string) error {
	_, err := m.db.ExecContext(ctx, `DELETE FROM tasks WHERE id = ?`, id)
	return err
}

// --- 分布式锁实现 ---

func (m *MySQLStore) TryLock(ctx context.Context, key string, ttl int64) (bool, error) {
	if ttl <= 0 {
		return false, fmt.Errorf("mysql: ttl 必须为正数，当前值: %d", ttl)
	}
	now := time.Now().UnixMilli()
	_, _ = m.db.ExecContext(ctx, `DELETE FROM locks WHERE lock_key = ? AND expiry < ?`, key, now)
	_, err := m.db.ExecContext(ctx,
		`INSERT INTO locks (lock_key, expiry) VALUES (?, ?)`,
		key, now+ttl*1000)
	if err != nil {
		if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == 1062 {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (m *MySQLStore) Unlock(ctx context.Context, key string) error {
	_, err := m.db.ExecContext(ctx, `DELETE FROM locks WHERE lock_key = ?`, key)
	return err
}

func (m *MySQLStore) Close() error {
	return m.db.Close()
}

// --- 扫描辅助函数 ---

func scanTask(scanner interface {
	Scan(dest ...interface{}) error
}) (*models.Task, error) {
	t := &models.Task{}
	var scheduledAt, startedAt, finishedAt sql.NullTime
	var stepsJSON, namespace sql.NullString
	err := scanner.Scan(
		&t.ID, &t.Name, &t.Type, &t.Payload, &t.Status, &t.Priority,
		&t.Retries, &t.MaxRetries, &t.Timeout, &t.MaxLatencyMs, &t.RepeatSec,
		&scheduledAt, &startedAt, &finishedAt,
		&t.Result, &t.Error, &stepsJSON, &namespace, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	if stepsJSON.Valid && stepsJSON.String != "" {
		if err := json.Unmarshal([]byte(stepsJSON.String), &t.Steps); err != nil {
			t.Steps = nil
		}
	}
	if namespace.Valid {
		t.Namespace = namespace.String
	}
	if scheduledAt.Valid {
		t.ScheduledAt = scheduledAt.Time
	}
	if startedAt.Valid {
		t.StartedAt = &startedAt.Time
	}
	if finishedAt.Valid {
		t.FinishedAt = &finishedAt.Time
	}
	return t, nil
}

func scanTasks(rows *sql.Rows) ([]*models.Task, error) {
	var tasks []*models.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

func nullTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t
}

func nullTimePtr(t *time.Time) interface{} {
	if t == nil {
		return nil
	}
	return *t
}
