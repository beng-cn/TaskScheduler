package api

import (
	"net/http"
	"task-scheduler/notify"
	"task-scheduler/scheduler"
	"task-scheduler/worker"
	"time"

	"github.com/gin-gonic/gin"
)

// Handler 是 HTTP 请求处理器。
// 它持有调度器引用，将 HTTP 请求转换为调度器调用。
type Handler struct {
	sched *scheduler.Scheduler
}

// NewHandler 创建新的处理器实例。
func NewHandler(sched *scheduler.Scheduler) *Handler {
	return &Handler{sched: sched}
}

// --- 请求/响应结构体 ---

type CreateTaskRequest struct {
	Name        string `json:"name" binding:"required"`    // 任务名称（必填）
	Type        string `json:"type" binding:"required"`    // 任务类型（必填）
	Payload     string `json:"payload"`                    // 任务负载
	Priority    int    `json:"priority"`                   // 优先级
	MaxRetries  int    `json:"max_retries"`                // 最大重试次数
	Timeout     int64  `json:"timeout"`                    // 超时时间（秒）
	Delay       int64  `json:"delay"`                      // 延迟执行（秒）
}

// CreateTask 创建新任务。
// POST /api/tasks
func (h *Handler) CreateTask(c *gin.Context) {
	var req CreateTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数校验失败: " + err.Error()})
		return
	}

	// 设置默认值
	if req.MaxRetries == 0 {
		req.MaxRetries = 3
	}
	if req.Timeout == 0 {
		req.Timeout = 30
	}

	task := &scheduler.Task{
		Name:        req.Name,
		Type:        req.Type,
		Payload:     req.Payload,
		Priority:    req.Priority,
		MaxRetries:  req.MaxRetries,
		Timeout:     req.Timeout,
		ScheduledAt: time.Now().Add(time.Duration(req.Delay) * time.Second),
		Status:      scheduler.StatusPending,
	}

	// 检查任务类型是否有对应的执行器
	if runner := worker.GetRunner(task.Type); runner == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":          "不支持的任务类型: " + task.Type,
			"supported_types": supportedTypes(),
		})
		return
	}

	if err := h.sched.Submit(task); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "创建任务失败: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message": "任务创建成功",
		"task":    task,
	})
}

// GetTask 获取单个任务详情。
// GET /api/tasks/:id
func (h *Handler) GetTask(c *gin.Context) {
	id := c.Param("id")
	task, err := h.sched.GetTask(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "任务不存在: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, task)
}

// ListTasks 列出全部任务。
// GET /api/tasks
func (h *Handler) ListTasks(c *gin.Context) {
	tasks, err := h.sched.ListTasks()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询任务失败: " + err.Error()})
		return
	}
	if tasks == nil {
		tasks = []*scheduler.Task{}
	}
	c.JSON(http.StatusOK, gin.H{
		"tasks": tasks,
		"total": len(tasks),
	})
}

// DeleteTask 删除任务。
// DELETE /api/tasks/:id
func (h *Handler) DeleteTask(c *gin.Context) {
	id := c.Param("id")
	if err := h.sched.DeleteTask(id); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "删除任务失败: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "任务已删除"})
}

// GetStats 获取调度系统运行统计。
// GET /api/stats
func (h *Handler) GetStats(c *gin.Context) {
	stats := h.sched.Stats()
	c.JSON(http.StatusOK, stats)
}

// GetTaskTypes 返回支持的任务类型列表。
// GET /api/task-types
func (h *Handler) GetTaskTypes(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"types": supportedTypes(),
	})
}

// Health 健康检查接口。
// GET /api/health
func (h *Handler) Health(c *gin.Context) {
	stats := h.sched.Stats()
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"stats":  stats,
	})
}

// ErrorLog 返回错误日志列表。
// GET /api/error-log
func (h *Handler) ErrorLog(c *gin.Context) {
	entries, err := notify.ReadErrorLog()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取错误日志失败: " + err.Error()})
		return
	}
	if entries == nil {
		entries = []notify.ErrorEntry{}
	}
	c.JSON(http.StatusOK, gin.H{"total": len(entries), "entries": entries})
}

// supportedTypes 返回当前注册的所有任务类型。
func supportedTypes() []string {
	return []string{"http_call", "data_clean", "flash_warmup", "cart_flow", "flash_full_check"}
}
