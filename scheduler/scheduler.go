package scheduler

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"
	"task-scheduler/config"
	"task-scheduler/worker"
)

// Scheduler 是任务调度引擎的核心。
// 它负责：轮询待执行任务 → 分发给 Worker 池 → 更新任务状态。
//
// 设计要点：
//   - 使用有缓冲 channel 解耦调度逻辑和执行逻辑
//   - 通过 context 实现全链路超时控制和优雅退出
//   - sync.Map 跟踪运行中的任务，用于查询和强制取消
//   - 轮询采用 Ticker，避免高频空转
type Scheduler struct {
	cfg *config.Config

	store    Store         // 任务持久化存储
	pool     *worker.Pool  // Worker 协程池

	taskCh     chan *Task  // 待执行任务队列（由调度 goroutine 写入，Worker 消费）
	runningSet sync.Map    // 正在执行的任务集合 (taskID → struct{})

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	// 统计指标
	dispatched int64 // 累计分发的任务数
}

// New 创建一个新的调度器实例。
func New(cfg *config.Config, s Store) *Scheduler {
	pool := worker.NewPool(cfg.Worker.Count, cfg.Worker.QueueSize)
	ctx, cancel := context.WithCancel(context.Background())

	return &Scheduler{
		cfg:    cfg,
		store:  s,
		pool:   pool,
		taskCh: make(chan *Task, cfg.Worker.QueueSize),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start 启动调度引擎。
// 同时启动 Worker 池和调度循环。
func (s *Scheduler) Start() {
	s.pool.Start()

	s.wg.Add(1)
	go s.dispatchLoop()
	log.Println("[Scheduler] 调度引擎已启动")
}

// Stop 优雅关闭调度引擎。
// 顺序：停止分发 → 等待 Worker 完成 → 关闭存储。
func (s *Scheduler) Stop() {
	log.Println("[Scheduler] 正在优雅关闭...")

	// 1. 通知调度循环退出
	s.cancel()

	// 2. 等待调度循环退出
	s.wg.Wait()

	// 3. 关闭 Worker 池（等待正在执行的任务完成）
	s.pool.Stop()

	// 4. 关闭存储
	if err := s.store.Close(); err != nil {
		log.Printf("[Scheduler] 关闭存储时出错: %v", err)
	}

	log.Printf("[Scheduler] 已安全退出。累计分发: %d 个任务", s.dispatched)
}

// Submit 提交一个任务到调度系统。
func (s *Scheduler) Submit(task *Task) error {
	return s.store.CreateTask(s.ctx, task)
}

// GetTask 查询单个任务。
func (s *Scheduler) GetTask(id string) (*Task, error) {
	return s.store.GetTask(s.ctx, id)
}

// ListTasks 列出所有任务。
func (s *Scheduler) ListTasks() ([]*Task, error) {
	return s.store.ListTasks(s.ctx)
}

// DeleteTask 删除任务。
func (s *Scheduler) DeleteTask(id string) error {
	return s.store.DeleteTask(s.ctx, id)
}

// Stats 返回调度系统的运行统计。
func (s *Scheduler) Stats() SchedulerStats {
	poolStats := s.pool.Stats()
	return SchedulerStats{
		Dispatched: atomic.LoadInt64(&s.dispatched),
		Running:    s.RunningCount(),
		Pool:       poolStats,
	}
}

// RunningCount 返回当前正在执行的任务数。
func (s *Scheduler) RunningCount() int {
	count := 0
	s.runningSet.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// SchedulerStats 调度系统运行统计。
type SchedulerStats struct {
	Dispatched int64            `json:"dispatched"` // 累计分发任务数
	Running    int              `json:"running"`    // 当前运行中任务数
	Pool       worker.PoolStats `json:"pool"`       // Worker 池统计
}

// --- 内部方法 ---

// dispatchLoop 是调度主循环。
// 定时轮询存储中的待执行任务，分发给 Worker 池执行。
func (s *Scheduler) dispatchLoop() {
	defer s.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Scheduler] PANIC 恢复: %v，调度循环即将重启", r)
			// 重启调度循环
			s.wg.Add(1)
			go s.dispatchLoop()
		}
	}()

	ticker := time.NewTicker(s.cfg.Scheduler.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.pollAndDispatch()
			s.checkPoolHealth()
		case <-s.ctx.Done():
			log.Println("[Scheduler] 调度循环收到退出信号")
			return
		}
	}
}

// checkPoolHealth 检查 Worker 池健康度，必要时发出告警。
func (s *Scheduler) checkPoolHealth() {
	stats := s.pool.Stats()
	if stats.QueueCap > 0 {
		usage := float64(stats.QueueLen) / float64(stats.QueueCap)
		if usage >= 0.9 {
			log.Printf("[Scheduler] 队列即将满: %d/%d (%.0f%%)", stats.QueueLen, stats.QueueCap, usage*100)
		}
		if usage >= 0.5 {
			log.Printf("[Scheduler] 队列积压: %d/%d (%.0f%%)", stats.QueueLen, stats.QueueCap, usage*100)
		}
	}
}

// pollAndDispatch 查询待执行任务并分发给 Worker 池。
func (s *Scheduler) pollAndDispatch() {
	tasks, err := s.store.ListPendingTasks(s.ctx)
	if err != nil {
		log.Printf("[Scheduler] 查询待执行任务失败: %v", err)
		return
	}

	for _, task := range tasks {
		// Cron 检查：如果设置了 cron_expr 且当前时间不匹配，跳过
		if task.CronExpr != "" {
			if !s.shouldRunCron(task) {
				continue
			}
		}
		// DAG 依赖检查
		if task.DependsOn != "" {
			dep, err := s.store.GetTask(s.ctx, task.DependsOn)
			if err != nil || dep.Status != StatusDone {
				continue
			}
		}
		// 尝试获取分布式锁，防止多节点重复执行
		locked, err := s.store.TryLock(s.ctx, "task:"+task.ID, 60)
		if err != nil || !locked {
			continue // 锁获取失败，跳过（可能已被其他节点处理）
		}

		// 更新状态为 Running
		task.Status = StatusRunning
		if err := s.store.UpdateTask(s.ctx, task); err != nil {
			s.store.Unlock(s.ctx, "task:"+task.ID)
			log.Printf("[Scheduler] 更新任务 %s 状态失败: %v", task.ID, err)
			continue
		}

		// 放入执行队列
		if err := s.pool.Submit(task.Clone()); err != nil {
			log.Printf("[Scheduler] 提交任务 %s 到 Worker 池失败: %v", task.ID, err)
			s.store.Unlock(s.ctx, "task:"+task.ID)
			continue
		}

		s.runningSet.Store(task.ID, struct{}{})
		atomic.AddInt64(&s.dispatched, 1)
	}

	// 检查重试任务
	s.handleRetries()
}

// shouldRunCron 检查 cron 表达式是否匹配当前时间（每分钟最多触发一次）。
func (s *Scheduler) shouldRunCron(task *Task) bool {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(task.CronExpr)
	if err != nil {
		log.Printf("[Cron] 解析表达式失败: %s", task.CronExpr)
		return false
	}
	// 检查当前时间是否在 cron 表达式的执行窗口内
	now := time.Now()
	next := schedule.Next(now.Add(-1 * time.Minute))
	return next.Before(now) || next.Equal(now)
}

// handleRetries 检查任务执行结果，处理重试逻辑和状态更新。
func (s *Scheduler) handleRetries() {
	tasks, err := s.store.ListTasks(s.ctx)
	if err != nil {
		return
	}

	for _, task := range tasks {
		// Cron 检查：如果设置了 cron_expr 且当前时间不匹配，跳过
		if task.CronExpr != "" {
			if !s.shouldRunCron(task) {
				continue
			}
		}
		// DAG 依赖检查
		if task.DependsOn != "" {
			dep, err := s.store.GetTask(s.ctx, task.DependsOn)
			if err != nil || dep.Status != StatusDone {
				continue
			}
		}
		// 释放已完成任务的锁
		if task.IsFinished() {
			s.runningSet.Delete(task.ID)
			s.store.Unlock(s.ctx, "task:"+task.ID)
			if err := s.store.UpdateTask(s.ctx, task); err != nil {
				log.Printf("[Scheduler] 更新任务 %s 最终状态失败: %v", task.ID, err)
			}
			log.Printf("[Scheduler] 任务 %s 已结束，状态: %s", task.ID, task.Status)
			continue
		}

		// 处理需要重试的任务
		if task.Status == StatusRetrying && task.ScheduledAt.Before(time.Now()) {
			task.Status = StatusPending
			if err := s.store.UpdateTask(s.ctx, task); err != nil {
				log.Printf("[Scheduler] 重试任务 %s 状态更新失败: %v", task.ID, err)
			}
			log.Printf("[Scheduler] 任务 %s 进入重试队列", task.ID)
		}
	}
}
