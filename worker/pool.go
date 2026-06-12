package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
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
	mu        sync.RWMutex     // 保护运行时统计
	running   int64            // 当前正在执行的任务数
	completed int64            // 已完成任务总数
	failed    int64            // 失败任务总数
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
	return &Pool{
		workers:   workerCount,
		taskQueue: make(chan *models.Task, queueSize),
		quit:      make(chan struct{}),
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
func (p *Pool) Submit(task *models.Task) error {
	select {
	case p.taskQueue <- task:
		return nil
	case <-p.quit:
		return fmt.Errorf("Worker 池已关闭，无法提交任务")
	}
}

// TrySubmit 尝试提交任务，队列满时立即返回 false。
func (p *Pool) TrySubmit(task *models.Task) bool {
	select {
	case p.taskQueue <- task:
		return true
	default:
		return false
	}
}

// Stats 返回当前的运行统计。
func (p *Pool) Stats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return PoolStats{
		Workers:   p.workers,
		QueueLen:  len(p.taskQueue),
		QueueCap:  cap(p.taskQueue),
		Running:   p.running,
		Completed: p.completed,
		Failed:    p.failed,
	}
}

// Stop 优雅关闭 Worker 池，等待所有正在执行的任务完成。
func (p *Pool) Stop() {
	log.Println("[WorkerPool] 正在关闭 Worker 池，不再接受新任务...")
	close(p.quit)
	p.wg.Wait()
	log.Println("[WorkerPool] 所有 Worker 已安全退出")
}

// workerLoop 是每个 Worker 的主循环。
// 它从共享任务队列领取任务，在自己的 goroutine 中执行。
func (p *Pool) workerLoop(id int) {
	defer p.wg.Done()
	log.Printf("[Worker-%d] 已启动", id)

	for {
		select {
		case task := <-p.taskQueue:
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
			p.execute(id, task)
		default:
			return
		}
	}
}

// execute 执行单个任务，处理超时、成功、失败等状态转换。
func (p *Pool) execute(workerID int, task *models.Task) {
	p.mu.Lock()
	p.running++
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.running--
		p.mu.Unlock()
	}()

	log.Printf("[Worker-%d] 开始执行任务 %s (类型: %s)", workerID, task.ID, task.Type)

	// 创建带超时的 context
	timeout := time.Duration(task.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// 获取对应的执行器
	runner := GetRunner(task.Type)
	if runner == nil {
		task.Status = models.StatusFailed
		task.Error = fmt.Sprintf("未注册的任务类型: %s", task.Type)
		now := time.Now()
		task.FinishedAt = &now
		p.mu.Lock()
		p.failed++
		p.mu.Unlock()
		log.Printf("[Worker-%d] 任务 %s 失败: 未知类型 %s", workerID, task.ID, task.Type)
		if onTaskComplete != nil {
			onTaskComplete(context.Background(), task)
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
			task.Error = fmt.Sprintf("任务超时 (限制: %s)", timeout)
			log.Printf("[Worker-%d] 任务 %s 超时", workerID, task.ID)
		} else {
			task.Error = err.Error()
			log.Printf("[Worker-%d] 任务 %s 执行失败: %v", workerID, task.ID, err)

			if task.CanRetry() {
				task.Status = models.StatusRetrying
				task.Retries++
				log.Printf("[Worker-%d] 任务 %s 将在 %d 秒后重试 (%d/%d)",
					workerID, task.ID, 5, task.Retries, task.MaxRetries)
				task.ScheduledAt = time.Now().Add(5 * time.Second) // 5秒后重试
			} else {
				task.Status = models.StatusFailed
				log.Printf("[Worker-%d] 任务 %s 重试次数用尽，标记为失败", workerID, task.ID)
			}
		}
		p.mu.Lock()
		p.failed++
		p.mu.Unlock()
	} else {
		task.Status = models.StatusDone
		task.Result = result
		p.mu.Lock()
		p.completed++
		p.mu.Unlock()
		log.Printf("[Worker-%d] 任务 %s 执行成功", workerID, task.ID)
	}

	// 回调通知调度器（回写持久化存储）
	if onTaskComplete != nil {
		onTaskComplete(context.Background(), task)
	}
}
