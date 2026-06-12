package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"task-scheduler/models"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// MySQLStore 是基于 MySQL 的 Store 实现。
// 任务数据持久化到磁盘，服务重启不丢失。
type MySQLStore struct {
	db  *sql.DB
	seq atomic.Int64
}

// nextSeq 返回一个单调递增的序号，用于生成唯一任务 ID。
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
		return nil, fmt.Errorf("mysql: Ping 失败: %w", err)
	}

	store := &MySQLStore{db: db}
	if err := store.autoMigrate(); err != nil {
		return nil, fmt.Errorf("mysql: 建表失败: %w", err)
	}

	return store, nil
}

// autoMigrate 自动创建表结构（幂等，已存在则跳过）。
func (m *MySQLStore) autoMigrate() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS tasks (
			id           VARCHAR(64)   NOT NULL PRIMARY KEY,
			name         VARCHAR(255)  NOT NULL,
			type         VARCHAR(50)   NOT NULL,
			payload      TEXT,
			status       VARCHAR(20)   NOT NULL DEFAULT 'pending',
			priority     INT           NOT NULL DEFAULT 0,
			retries      INT           NOT NULL DEFAULT 0,
			max_retries  INT           NOT NULL DEFAULT 3,
			timeout      BIGINT        NOT NULL DEFAULT 30,
			repeat_sec   BIGINT        NOT NULL DEFAULT 0,
			scheduled_at DATETIME      NULL,
			started_at   DATETIME      NULL,
			finished_at  DATETIME      NULL,
			result       TEXT,
			error        TEXT,
			created_at   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			INDEX idx_status (status),
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
	// 自动生成 ID（格式: task-{纳秒时间戳}，避免并发冲突）
	if task.ID == "" {
		task.ID = fmt.Sprintf("task-%d-%d", time.Now().UnixMilli(), m.nextSeq())
	}

	stepsJSON, _ := json.Marshal(task.Steps)
	_, err := m.db.ExecContext(ctx,
		`INSERT INTO tasks (id, name, type, payload, status, priority, retries, max_retries, timeout, repeat_sec, scheduled_at, started_at, finished_at, result, error, steps, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		task.ID, task.Name, task.Type, task.Payload, task.Status, task.Priority,
		task.Retries, task.MaxRetries, task.Timeout, task.RepeatSec,
		nullTime(task.ScheduledAt), nullTimePtr(task.StartedAt), nullTimePtr(task.FinishedAt),
		task.Result, task.Error, string(stepsJSON), task.CreatedAt, task.UpdatedAt,
	)
	return err
}

func (m *MySQLStore) GetTask(ctx context.Context, id string) (*models.Task, error) {
	row := m.db.QueryRowContext(ctx,
		`SELECT id, name, type, payload, status, priority, retries, max_retries, timeout, repeat_sec,
		        scheduled_at, started_at, finished_at, result, error, steps, created_at, updated_at
		 FROM tasks WHERE id = ?`, id)
	return scanTask(row)
}

func (m *MySQLStore) ListTasks(ctx context.Context) ([]*models.Task, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT id, name, type, payload, status, priority, retries, max_retries, timeout, repeat_sec,
		        scheduled_at, started_at, finished_at, result, error, steps, created_at, updated_at
		 FROM tasks ORDER BY priority DESC, created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTasks(rows)
}

func (m *MySQLStore) ListPendingTasks(ctx context.Context) ([]*models.Task, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT id, name, type, payload, status, priority, retries, max_retries, timeout, repeat_sec,
		        scheduled_at, started_at, finished_at, result, error, steps, created_at, updated_at
		 FROM tasks WHERE status = ? AND (scheduled_at IS NULL OR scheduled_at <= ?)
		 ORDER BY priority DESC, created_at ASC`,
		models.StatusPending, time.Now())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTasks(rows)
}

func (m *MySQLStore) UpdateTask(ctx context.Context, task *models.Task) error {
	task.UpdatedAt = time.Now()
	stepsJSON, _ := json.Marshal(task.Steps)
	_, err := m.db.ExecContext(ctx,
		`UPDATE tasks SET name=?, type=?, payload=?, status=?, priority=?, retries=?, max_retries=?,
		 timeout=?, repeat_sec=?, scheduled_at=?, started_at=?, finished_at=?, result=?, error=?, steps=?, updated_at=?
		 WHERE id=?`,
		task.Name, task.Type, task.Payload, task.Status, task.Priority,
		task.Retries, task.MaxRetries, task.Timeout, task.RepeatSec,
		nullTime(task.ScheduledAt), nullTimePtr(task.StartedAt), nullTimePtr(task.FinishedAt),
		task.Result, task.Error, string(stepsJSON), task.UpdatedAt, task.ID,
	)
	return err
}

func (m *MySQLStore) DeleteTask(ctx context.Context, id string) error {
	_, err := m.db.ExecContext(ctx, `DELETE FROM tasks WHERE id = ?`, id)
	return err
}

// --- 分布式锁实现 ---

func (m *MySQLStore) TryLock(ctx context.Context, key string, ttl int64) (bool, error) {
	now := time.Now().UnixMilli()
	// 先清理过期锁
	m.db.ExecContext(ctx, `DELETE FROM locks WHERE lock_key = ? AND expiry < ?`, key, now)
	// 尝试插入
	_, err := m.db.ExecContext(ctx,
		`INSERT INTO locks (lock_key, expiry) VALUES (?, ?)`,
		key, now+ttl*1000)
	if err != nil {
		// 插入失败说明锁已存在
		return false, nil
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
	var stepsJSON sql.NullString
	err := scanner.Scan(
		&t.ID, &t.Name, &t.Type, &t.Payload, &t.Status, &t.Priority,
		&t.Retries, &t.MaxRetries, &t.Timeout, &t.RepeatSec,
		&scheduledAt, &startedAt, &finishedAt,
		&t.Result, &t.Error, &stepsJSON, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	if stepsJSON.Valid && stepsJSON.String != "" {
		json.Unmarshal([]byte(stepsJSON.String), &t.Steps)
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

// nullTime 将零值 time.Time 转为 NULL。
func nullTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t
}

// nullTimePtr 将 nil 指针转为 NULL。
func nullTimePtr(t *time.Time) interface{} {
	if t == nil {
		return nil
	}
	return *t
}
