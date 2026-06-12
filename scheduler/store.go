// Package scheduler 本文件定义任务存储的抽象接口。
// 将 Store 接口放在 scheduler 包中是为了避免循环导入问题：
// scheduler → store → scheduler。
// 通过接口依赖反转，store 包实现 scheduler.Store 接口。
package scheduler

import "context"

// Store 是任务存储的抽象接口。
// 所有实现必须保证并发安全。
type Store interface {
	// --- 任务 CRUD ---

	// CreateTask 创建一个新任务，返回完整的任务对象（含生成的 ID）。
	CreateTask(ctx context.Context, task *Task) error

	// GetTask 根据 ID 查询任务。
	GetTask(ctx context.Context, id string) (*Task, error)

	// ListTasks 列出全部任务，按创建时间倒序。
	ListTasks(ctx context.Context) ([]*Task, error)

	// ListPendingTasks 获取所有待执行的任务。
	ListPendingTasks(ctx context.Context) ([]*Task, error)

	// UpdateTask 更新任务（状态、结果等）。
	UpdateTask(ctx context.Context, task *Task) error

	// DeleteTask 删除任务。
	DeleteTask(ctx context.Context, id string) error

	// --- 分布式锁（用于多节点部署时防止重复调度） ---

	// TryLock 尝试获取一个带 TTL 的分布式锁。
	// 返回 true 表示获取成功。
	TryLock(ctx context.Context, key string, ttl int64) (bool, error)

	// Unlock 释放锁。
	Unlock(ctx context.Context, key string) error

	// --- 生命周期 ---

	// Close 关闭存储连接，释放资源。
	Close() error
}
