package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync/atomic"
	"task-scheduler/models"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore 是基于 Redis 的 Store 实现。
// 任务数据存储在 Redis 中，兼具缓存性能和数据持久化（RDB/AOF）。
type RedisStore struct {
	client *redis.Client
	seq    atomic.Int64
}

// nextSeq 返回一个单调递增的序号，用于生成唯一任务 ID。
func (r *RedisStore) nextSeq() int64 {
	return r.seq.Add(1)
}

// NewRedisStore 创建一个新的 Redis 存储实例。
func NewRedisStore(addr, password string, db int) (*RedisStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close() // 修复：Ping 失败时关闭连接，避免泄漏
		return nil, fmt.Errorf("redis: 连接失败: %w", err)
	}
	return &RedisStore{client: client}, nil
}

// CreateTask 创建任务。
func (r *RedisStore) CreateTask(ctx context.Context, task *models.Task) error {
	task.CreatedAt = time.Now()
	task.UpdatedAt = time.Now()
	if task.Status == "" {
		task.Status = models.StatusPending
	}
	if task.ID == "" {
		task.ID = fmt.Sprintf("task-%d-%d", time.Now().UnixMilli(), r.nextSeq())
	}
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("redis: 序列化失败: %w", err)
	}
	pipe := r.client.Pipeline()
	pipe.Set(ctx, "task:"+task.ID, data, 0)
	pipe.ZAdd(ctx, "tasks:by_created", redis.Z{
		Score:  float64(task.CreatedAt.UnixNano()),
		Member: task.ID,
	})
	if task.Status == models.StatusPending {
		pipe.SAdd(ctx, "tasks:pending", task.ID)
	}
	_, err = pipe.Exec(ctx)
	// 修复：Pipeline 失败时记录日志（部分命令可能成功，属于尽力而为语义）
	if err != nil {
		log.Printf("[RedisStore] CreateTask Pipeline 执行失败: %v", err)
	}
	return err
}

// GetTask 获取单个任务。
func (r *RedisStore) GetTask(ctx context.Context, id string) (*models.Task, error) {
	data, err := r.client.Get(ctx, "task:"+id).Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("任务 %s 不存在", id)
	}
	if err != nil {
		return nil, err
	}
	t := &models.Task{}
	if err := json.Unmarshal(data, t); err != nil {
		return nil, fmt.Errorf("redis: 反序列化失败: %w", err)
	}
	return t, nil
}

// ListTasks 列出任务。namespace 为空时返回所有，否则按 namespace 过滤。
func (r *RedisStore) ListTasks(ctx context.Context, namespace string) ([]*models.Task, error) {
	ids, err := r.client.ZRangeByScore(ctx, "tasks:by_created", &redis.ZRangeBy{
		Min: "-inf", Max: "+inf",
	}).Result()
	if err != nil {
		return nil, err
	}
	tasks, err := r.getTasksByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	// namespace 过滤
	if namespace != "" {
		filtered := make([]*models.Task, 0, len(tasks))
		for _, t := range tasks {
			if t.Namespace == "" || t.Namespace == namespace {
				filtered = append(filtered, t)
			}
		}
		tasks = filtered
	}
	// 排序：priority DESC, created_at ASC
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].Priority != tasks[j].Priority {
			return tasks[i].Priority > tasks[j].Priority
		}
		return tasks[i].CreatedAt.Before(tasks[j].CreatedAt)
	})
	return tasks, nil
}

// ListPendingTasks 获取待执行任务。namespace 为空时返回所有。
func (r *RedisStore) ListPendingTasks(ctx context.Context, namespace string) ([]*models.Task, error) {
	ids, err := r.client.SMembers(ctx, "tasks:pending").Result()
	if err != nil {
		return nil, err
	}
	tasks, err := r.getTasksByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	filtered := make([]*models.Task, 0, len(tasks))
	for _, t := range tasks {
		if namespace != "" && t.Namespace != "" && t.Namespace != namespace {
			continue
		}
		if t.ScheduledAt.IsZero() || t.ScheduledAt.Before(now) {
			filtered = append(filtered, t)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Priority != filtered[j].Priority {
			return filtered[i].Priority > filtered[j].Priority
		}
		return filtered[i].CreatedAt.Before(filtered[j].CreatedAt)
	})
	return filtered, nil
}

// UpdateTask 更新任务。
func (r *RedisStore) UpdateTask(ctx context.Context, task *models.Task) error {
	task.UpdatedAt = time.Now()
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("redis: 序列化失败: %w", err)
	}
	pipe := r.client.Pipeline()
	pipe.Set(ctx, "task:"+task.ID, data, 0)
	if task.Status == models.StatusPending {
		pipe.SAdd(ctx, "tasks:pending", task.ID)
	} else {
		pipe.SRem(ctx, "tasks:pending", task.ID)
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Printf("[RedisStore] UpdateTask Pipeline 执行失败: %v", err)
	}
	return err
}

// DeleteTask 删除任务。
func (r *RedisStore) DeleteTask(ctx context.Context, id string) error {
	pipe := r.client.Pipeline()
	pipe.Del(ctx, "task:"+id)
	pipe.ZRem(ctx, "tasks:by_created", id)
	pipe.SRem(ctx, "tasks:pending", id)
	_, err := pipe.Exec(ctx)
	if err != nil {
		log.Printf("[RedisStore] DeleteTask Pipeline 执行失败: %v", err)
	}
	return err
}

// TryLock 尝试获取分布式锁（SETNX + TTL）。
// 修复：校验 ttl 正值
func (r *RedisStore) TryLock(ctx context.Context, key string, ttl int64) (bool, error) {
	if ttl <= 0 {
		return false, fmt.Errorf("redis: ttl 必须为正数，当前值: %d", ttl)
	}
	ok, err := r.client.SetNX(ctx, "lock:"+key, 1, time.Duration(ttl)*time.Second).Result()
	return ok, err
}

// Unlock 释放锁。
func (r *RedisStore) Unlock(ctx context.Context, key string) error {
	return r.client.Del(ctx, "lock:"+key).Err()
}

// Close 关闭连接。
func (r *RedisStore) Close() error {
	return r.client.Close()
}

// getTasksByIDs 批量获取任务。
func (r *RedisStore) getTasksByIDs(ctx context.Context, ids []string) ([]*models.Task, error) {
	if len(ids) == 0 {
		return []*models.Task{}, nil
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = "task:" + id
	}
	vals, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	tasks := make([]*models.Task, 0, len(vals))
	for i, v := range vals {
		if v == nil {
			continue
		}
		// 修复：使用安全断言，避免非 string 类型导致 panic
		s, ok := v.(string)
		if !ok {
			log.Printf("[RedisStore] key %s 的值类型异常，已跳过", keys[i])
			continue
		}
		t := &models.Task{}
		if err := json.Unmarshal([]byte(s), t); err != nil {
			// 修复：记录日志而非静默跳过
			log.Printf("[RedisStore] 任务 %s 反序列化失败: %v", ids[i], err)
			continue
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}
