package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"task-scheduler/models"
	"time"
)

// Pool 管理一组固定数量的 Worker goroutine。
// 每个 Worker 从共享的任务 channel 中领取任务执行。
type Pool struct {
	workers   int              // 当前 Worker 数量
	taskQueue chan *models.Task // 共享任务队列（有缓冲 channel）
	quit      chan struct{}    // 通知所有 Worker 退出
	wg        sync.WaitGroup   // 等待所有 Worker 完成
	stopped   atomic.Bool      // 标记是否已停止（防止 TrySubmit 在 Stop 后提交）
	running   int64            // 当前正在执行的任务数（atomic）
	completed int64            // 已完成任务总数（atomic）
	failed    int64            // 失败任务总数（atomic）
	stopOnce  sync.Once        // 确保 Stop 只执行一次
	rootCtx   context.Context  // Pool 级别的 root context，Stop 时传播取消
	rootCancel context.CancelFunc
}

// PoolStats 是 Worker 池的运行统计信息。
type PoolStats struct {
	Workers   int   `json:"workers"`   // Worker 数量
	QueueLen  int   `json:"queue_len"` // 当前队列长度
	QueueCap  int   `json:"queue_cap"` // 队列容量
	Running   int64 `json:"running"`   // 正在执行的任务数
	Completed int64 `json:"completed"` // 已完成任务数
	Failed    int64 `json:"failed"`    // 失败任务数
}

// NewPool 创建一个新的 Worker 池。
func NewPool(workerCount int, queueSize int) *Pool {
	rootCtx, rootCancel := context.WithCancel(context.Background())
	return &Pool{
		workers:    workerCount,
		taskQueue:  make(chan *models.Task, queueSize),
		quit:       make(chan struct{}),
		rootCtx:    rootCtx,
		rootCancel: rootCancel,
	}
}

// Start 启动所有 Worker goroutine。
func (p *Pool) Start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.workerLoop(i)
	}
	log.Printf("[WorkerPool] 启动了 %d 个 Worker", p.workers)
}

// Submit 向队列提交一个任务。如果队列已满则阻塞。
// 修复：增加 nil 检查和已停止检查
func (p *Pool) Submit(task *models.Task) error {
	if task == nil {
		return fmt.Errorf("Worker 池：不能提交 nil 任务")
	}
	// 修复：Submit 在 Stop 后也能正确拒绝
	select {
	case p.taskQueue <- task:
		return nil
	case <-p.quit:
		return fmt.Errorf("Worker 池已关闭，无法提交任务")
	}
}

// TrySubmit 尝试提交任务，队列满时立即返回 false。
// 修复：增加 Stop 后拒绝和 nil 检查
func (p *Pool) TrySubmit(task *models.Task) bool {
	if task == nil {
		return false
	}
	if p.stopped.Load() {
		return false
	}
	select {
	case p.taskQueue <- task:
		return true
	case <-p.quit:
		return false
	default:
		return false
	}
}

// Stats 返回当前的运行统计。
func (p *Pool) Stats() PoolStats {
	return PoolStats{
		Workers:   p.workers,
		QueueLen:  len(p.taskQueue),
		QueueCap:  cap(p.taskQueue),
		Running:   atomic.LoadInt64(&p.running),
		Completed: atomic.LoadInt64(&p.completed),
		Failed:    atomic.LoadInt64(&p.failed),
	}
}

// Stop 优雅关闭 Worker 池，等待所有正在执行的任务完成。
// 修复：使用 sync.Once 防止重复关闭导致 panic
func (p *Pool) Stop() {
	p.stopOnce.Do(func() {
		log.Println("[WorkerPool] 正在关闭 Worker 池，不再接受新任务...")
		p.stopped.Store(true)
		close(p.quit)
		// 传播取消信号给所有运行中的任务
		p.rootCancel()
		p.wg.Wait()
		log.Println("[WorkerPool] 所有 Worker 已安全退出")
	})
}

// workerLoop 是每个 Worker 的主循环。
// 它从共享任务队列领取任务，在自己的 goroutine 中执行。
func (p *Pool) workerLoop(id int) {
	defer p.wg.Done()
	log.Printf("[Worker-%d] 已启动", id)

	for {
		select {
		case task := <-p.taskQueue:
			if task == nil {
				continue
			}
			p.execute(id, task)
		case <-p.quit:
			// 处理完队列中剩余的任务再退出
			p.drainRemaining(id)
			log.Printf("[Worker-%d] 已退出", id)
			return
		}
	}
}

// drainRemaining 退出前排空队列中剩余的任务。
func (p *Pool) drainRemaining(id int) {
	for {
		select {
		case task := <-p.taskQueue:
			if task != nil {
				p.execute(id, task)
			}
		default:
			return
		}
	}
}

// execute 执行单个任务，处理超时、成功、失败等状态转换。
func (p *Pool) execute(workerID int, task *models.Task) {
	atomic.AddInt64(&p.running, 1)
	defer atomic.AddInt64(&p.running, -1)

	log.Printf("[Worker-%d] 开始执行任务 %s (类型: %s)", workerID, task.ID, task.Type)

	// 修复：Timeout=0 时不创建立即过期的 context，改用 rootCtx（无超时）
	var ctx context.Context
	var cancel context.CancelFunc
	if task.Timeout > 0 {
		timeout := time.Duration(task.Timeout) * time.Second
		ctx, cancel = context.WithTimeout(p.rootCtx, timeout)
	} else {
		ctx, cancel = context.WithCancel(p.rootCtx)
	}
	defer cancel()

	// 获取对应的执行器
	runner := GetRunner(task.Type)
	if runner == nil {
		task.Status = models.StatusFailed
		task.Error = fmt.Sprintf("未注册的任务类型: %s", task.Type)
		now := time.Now()
		task.FinishedAt = &now
		atomic.AddInt64(&p.failed, 1)
		log.Printf("[Worker-%d] 任务 %s 失败: 未知类型 %s", workerID, task.ID, task.Type)
		if cb := getOnTaskComplete(); cb != nil {
			cb(context.Background(), task)
		}
		return
	}

	// 执行任务
	now := time.Now()
	task.StartedAt = &now
	task.Status = models.StatusRunning

	result, err := runner(ctx, task)

	// 处理结果
	finishedAt := time.Now()
	task.FinishedAt = &finishedAt

	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			task.Status = models.StatusTimeout
			task.Error = fmt.Sprintf("任务超时 (限制: %ds)，原始错误: %v", task.Timeout, err)
			log.Printf("[Worker-%d] 任务 %s 超时", workerID, task.ID)
		} else {
			task.Error = err.Error()
			log.Printf("[Worker-%d] 任务 %s 执行失败: %v", workerID, task.ID, err)

			// 修复：重试逻辑完整——将任务重新提交到队列
			if task.CanRetry() {
				task.Status = models.StatusRetrying
				task.Retries++
				task.ScheduledAt = time.Now().Add(5 * time.Second) // 5秒后重试
				log.Printf("[Worker-%d] 任务 %s 将在 5 秒后重试 (%d/%d)",
					workerID, task.ID, task.Retries, task.MaxRetries)
				// 通过回调通知调度器处理重试
				if cb := getOnTaskComplete(); cb != nil {
					cb(context.Background(), task)
				}
				return // 不增加 failed 计数，等待重试
			}
			task.Status = models.StatusFailed
			log.Printf("[Worker-%d] 任务 %s 重试次数用尽，标记为失败", workerID, task.ID)
		}
		atomic.AddInt64(&p.failed, 1)
	} else {
		task.Status = models.StatusDone
		task.Result = result
		atomic.AddInt64(&p.completed, 1)
		log.Printf("[Worker-%d] 任务 %s 执行成功", workerID, task.ID)
	}

	// 回调通知调度器（回写持久化存储）
	if cb := getOnTaskComplete(); cb != nil {
		cb(context.Background(), task)
	}
}
