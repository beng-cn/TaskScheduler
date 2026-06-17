// Package models 定义任务调度系统的核心数据结构。
// 将 Task 从 scheduler 包中提取出来，是为了解决以下循环导入问题：
//   scheduler → worker → scheduler
//   scheduler → store → scheduler
// 独立的数据模型包被所有包引用，不会产生循环依赖。
package models

import "time"

// TaskStatus 表示任务的生命周期状态。
type TaskStatus string

const (
	StatusPending  TaskStatus = "pending"   // 待执行：任务已创建，等待调度
	StatusRunning  TaskStatus = "running"   // 执行中：Worker 正在处理
	StatusDone     TaskStatus = "done"      // 已完成：任务成功执行
	StatusFailed   TaskStatus = "failed"    // 已失败：重试次数用尽或不可恢复错误
	StatusTimeout  TaskStatus = "timeout"   // 超时：任务执行超过限制时间
	StatusRetrying TaskStatus = "retrying"  // 重试中：失败后等待再次调度
)

// Task 是调度系统中的最小执行单元。
// 每个任务描述一个需要异步执行的操作。
type Task struct {
	ID           string     `json:"id"`            // 唯一标识，创建时自动生成
	Name         string     `json:"name"`          // 任务名称（用于日志和展示）
	Type         string     `json:"type"`          // 任务类型（由业务定义，如 "email"、"report"）
	Payload      string     `json:"payload"`       // 任务负载数据（JSON 字符串）
	Status       TaskStatus `json:"status"`        // 当前状态
	Priority     int        `json:"priority"`      // 优先级（数字越大越优先）
	Retries      int        `json:"retries"`       // 已重试次数
	MaxRetries   int        `json:"max_retries"`   // 最大重试次数
	Timeout      int64      `json:"timeout"`       // 超时时间（秒）
	MaxLatencyMs int64      `json:"max_latency_ms"` // 响应延迟阈值（毫秒），超过告警但不算失败
	RepeatSec    int64      `json:"repeat_sec"`    // 固定间隔循环（秒），0=不循环
	CronExpr     string     `json:"cron_expr"`     // Cron 表达式循环，如 "*/5 * * * *"，优先级高于 RepeatSec
	DependsOn    string     `json:"depends_on"`    // 依赖任务 ID，依赖完成后才执行
	Namespace    string     `json:"namespace"`     // 租户隔离命名空间，默认 "default"
	ScheduledAt  time.Time  `json:"scheduled_at"`  // 计划执行时间
	StartedAt    *time.Time `json:"started_at"`    // 实际开始执行时间
	FinishedAt   *time.Time `json:"finished_at"`   // 完成时间
	Result       string     `json:"result"`        // 执行结果（成功时存储返回值）
	Error        string     `json:"error"`         // 错误信息（失败时记录原因）
	Steps        []TaskStep `json:"steps"`         // 子步骤详情（仅多步runner填充）
	CreatedAt    time.Time  `json:"created_at"`    // 创建时间
	UpdatedAt    time.Time  `json:"updated_at"`    // 最后更新时间
}

// TaskStep 表示任务内部的一个子步骤。
// 用于多步执行流程，每一步独立记录状态和耗时。
type TaskStep struct {
	Name       string `json:"name"`        // 步骤名称
	Status     string `json:"status"`      // done / failed / skipped
	DurationMs int64  `json:"duration_ms"` // 耗时（毫秒）
	Result     string `json:"result"`      // 成功时的摘要
	Error      string `json:"error"`       // 失败时的原因
}

// CanRetry 判断任务是否还有重试配额。
func (t *Task) CanRetry() bool {
	return t.Retries < t.MaxRetries
}

// IsFinished 判断任务是否已经结束（无论成功还是失败）。
func (t *Task) IsFinished() bool {
	return t.Status == StatusDone || t.Status == StatusFailed || t.Status == StatusTimeout
}

// Clone 返回任务的深拷贝，避免并发修改冲突。
// 修复：对切片和指针字段进行深拷贝，确保克隆体与原对象完全独立。
func (t *Task) Clone() *Task {
	clone := *t
	// 深拷贝 Steps 切片
	if t.Steps != nil {
		clone.Steps = make([]TaskStep, len(t.Steps))
		copy(clone.Steps, t.Steps)
	}
	// 深拷贝时间指针
	if t.StartedAt != nil {
		started := *t.StartedAt
		clone.StartedAt = &started
	}
	if t.FinishedAt != nil {
		finished := *t.FinishedAt
		clone.FinishedAt = &finished
	}
	return &clone
}
