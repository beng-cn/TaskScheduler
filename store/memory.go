// Package store 的内存存储实现。
// 使用 sync.RWMutex 保证并发安全，适合开发调试和单机演示。
package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"task-scheduler/models"
	"time"
)

// MemoryStore 是基于内存的 Store 实现。
// 所有数据存储在进程内存中，进程重启后数据丢失。
type MemoryStore struct {
	mu     sync.RWMutex
	tasks  map[string]*models.Task  // 任务映射表
	locks  map[string]int64            // 分布式锁（key → 过期时间戳）
	seq    int64                       // 自增 ID 计数器
}

// NewMemoryStore 创建一个新的内存存储实例。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		tasks: make(map[string]*models.Task),
		locks: make(map[string]int64),
	}
}

// CreateTask 创建任务，自动生成 ID。
func (m *MemoryStore) CreateTask(ctx context.Context, task *models.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.seq++
	task.ID = fmt.Sprintf("task-%d", m.seq)
	task.CreatedAt = time.Now()
	task.UpdatedAt = time.Now()
	if task.Status == "" {
		task.Status = models.StatusPending
	}

	// 深拷贝避免外部修改影响内部状态
	clone := *task
	m.tasks[task.ID] = &clone
	return nil
}

// GetTask 根据 ID 查询任务。
func (m *MemoryStore) GetTask(ctx context.Context, id string) (*models.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, ok := m.tasks[id]
	if !ok {
		return nil, fmt.Errorf("任务 %s 不存在", id)
	}
	clone := *task
	return &clone, nil
}

// ListTasks 列出全部任务。
func (m *MemoryStore) ListTasks(ctx context.Context) ([]*models.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*models.Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		clone := *t
		result = append(result, &clone)
	}
		// 按优先级降序（越大越优先），同优先级按创建时间升序
		sort.Slice(result, func(i, j int) bool {
			if result[i].Priority != result[j].Priority {
				return result[i].Priority > result[j].Priority
			}
			return result[i].CreatedAt.Before(result[j].CreatedAt)
		})
	return result, nil
}

// ListPendingTasks 获取所有待执行的任务。
func (m *MemoryStore) ListPendingTasks(ctx context.Context) ([]*models.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*models.Task, 0)
	now := time.Now()
	for _, t := range m.tasks {
		if t.Status == models.StatusPending && (t.ScheduledAt.IsZero() || t.ScheduledAt.Before(now)) {
			clone := *t
			result = append(result, &clone)
		}
	}
		// 按优先级降序（越大越优先），同优先级按创建时间升序
		sort.Slice(result, func(i, j int) bool {
			if result[i].Priority != result[j].Priority {
				return result[i].Priority > result[j].Priority
			}
			return result[i].CreatedAt.Before(result[j].CreatedAt)
		})
	return result, nil
}

// UpdateTask 更新任务。
func (m *MemoryStore) UpdateTask(ctx context.Context, task *models.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.tasks[task.ID]
	if !ok {
		return fmt.Errorf("任务 %s 不存在", task.ID)
	}

	task.UpdatedAt = time.Now()
	clone := *task
	m.tasks[task.ID] = &clone
	_ = existing // 保留原引用，后续可扩展通知机制
	return nil
}

// DeleteTask 删除任务。
func (m *MemoryStore) DeleteTask(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tasks[id]; !ok {
		return fmt.Errorf("任务 %s 不存在", id)
	}
	delete(m.tasks, id)
	return nil
}

// TryLock 尝试获取锁。
// 修复：校验 ttl 正值，与 RedisStore 行为一致。
func (m *MemoryStore) TryLock(ctx context.Context, key string, ttl int64) (bool, error) {
	if ttl <= 0 {
		return false, fmt.Errorf("memory: ttl 必须为正数，当前值: %d", ttl)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UnixMilli()
	if expiry, ok := m.locks[key]; ok && expiry > now {
		return false, nil // 锁仍被持有
	}
	m.locks[key] = now + ttl*1000
	return true, nil
}

// Unlock 释放锁。
func (m *MemoryStore) Unlock(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.locks, key)
	return nil
}

// Close 关闭存储（内存实现无需释放资源）。
func (m *MemoryStore) Close() error {
	return nil
}
