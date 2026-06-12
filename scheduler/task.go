// Package scheduler 任务数据结构源自 models 包。
// 此文件通过类型别名重导出，保持对外 API 的一致性。
package scheduler

import "task-scheduler/models"

// 类型别名 —— 所有引用 scheduler.Task 的代码无需修改
type (
	Task       = models.Task
	TaskStatus = models.TaskStatus
)

// 状态常量的便捷引用
const (
	StatusPending  = models.StatusPending
	StatusRunning  = models.StatusRunning
	StatusDone     = models.StatusDone
	StatusFailed   = models.StatusFailed
	StatusTimeout  = models.StatusTimeout
	StatusRetrying = models.StatusRetrying
)
