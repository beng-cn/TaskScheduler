package api

import (
	"log"
	"net/http"
	"strings"
	"task-scheduler/notify"
	"task-scheduler/scheduler"
	"task-scheduler/worker"
	"time"

	"github.com/gin-gonic/gin"
)

// Handler 是 HTTP 请求处理器。
type Handler struct {
	sched         *scheduler.Scheduler
	swaggerReload func(path string) (string, int, error) // Swagger 重载回调
}

// NewHandler 创建新的处理器实例。
func NewHandler(sched *scheduler.Scheduler) *Handler {
	return &Handler{sched: sched}
}

// SetSwaggerReload 注入 Swagger 重载回调函数（由 main 包提供）。
func (h *Handler) SetSwaggerReload(fn func(path string) (string, int, error)) {
	h.swaggerReload = fn
}

// --- 请求/响应结构体 ---

type CreateTaskRequest struct {
	Name       string `json:"name" binding:"required"`
	Type       string `json:"type" binding:"required"`
	Payload    string `json:"payload"`
	Priority   int    `json:"priority"`
	MaxRetries int    `json:"max_retries"`
	Timeout    int64  `json:"timeout"`
	Delay      int64  `json:"delay"`
}

// CreateTask 创建新任务。
// POST /api/tasks
func (h *Handler) CreateTask(c *gin.Context) {
	var req CreateTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数校验失败: " + err.Error()})
		return
	}

	if req.Delay < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "delay 不能为负数"})
		return
	}
	if req.Priority < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "priority 不能为负数"})
		return
	}
	if req.MaxRetries < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "max_retries 不能为负数"})
		return
	}
	if req.Timeout < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "timeout 不能为负数"})
		return
	}
	if req.Delay > 31536000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "delay 值过大"})
		return
	}
	if req.Timeout > 86400 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "timeout 值过大"})
		return
	}

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
		Namespace:   GetNamespace(c), // 自动填入当前租户 namespace
	}

	if runner := worker.GetRunner(task.Type); runner == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":          "不支持的任务类型: " + task.Type,
			"supported_types": worker.RegisteredTypes(),
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
	// namespace 隔离校验：租户只能看自己的任务
	ns := GetNamespace(c)
	if task.Namespace != "" && task.Namespace != ns {
		c.JSON(http.StatusForbidden, gin.H{"error": "无权访问该任务"})
		return
	}
	c.JSON(http.StatusOK, task)
}

// ListTasks 列出当前租户的全部任务。
// GET /api/tasks
func (h *Handler) ListTasks(c *gin.Context) {
	ns := GetNamespace(c)
	tasks, err := h.sched.ListTasks(ns)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询任务失败: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"tasks":     tasks,
		"total":     len(tasks),
		"namespace": ns,
	})
}

// DeleteTask 删除任务。
// DELETE /api/tasks/:id
func (h *Handler) DeleteTask(c *gin.Context) {
	id := c.Param("id")
	// 先查出来校验 namespace 归属
	task, err := h.sched.GetTask(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "任务不存在: " + err.Error()})
		return
	}
	ns := GetNamespace(c)
	if task.Namespace != "" && task.Namespace != ns {
		c.JSON(http.StatusForbidden, gin.H{"error": "无权删除该任务"})
		return
	}
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
		"types": worker.RegisteredTypes(),
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

// ListProjects 列出所有已导入的项目（不同 namespace）。
// GET /api/projects
func (h *Handler) ListProjects(c *gin.Context) {
	// 用空 namespace 获取全量任务，提取不同的 namespace
	tasks, err := h.sched.ListTasks("")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询项目列表失败: " + err.Error()})
		return
	}

	seen := make(map[string]bool)
	var projects []string
	for _, t := range tasks {
		ns := t.Namespace
		if ns == "" {
			ns = "default"
		}
		if !seen[ns] {
			seen[ns] = true
			projects = append(projects, ns)
		}
	}
	if len(projects) == 0 {
		projects = append(projects, "default")
	}

	c.JSON(http.StatusOK, gin.H{
		"projects": projects,
		"total":    len(projects),
	})
}

// SwaggerReload 后端扫描固定目录、解析 swagger、创建任务，前端只拿结果。
// POST /api/swagger/reload
// 传 {"scan_dir": true} 扫描 D:\DEMO\api test\ 目录
func (h *Handler) SwaggerReload(c *gin.Context) {
	if h.swaggerReload == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Swagger 重载功能未配置"})
		return
	}

	var req struct {
		Path    string `json:"path"`
		ScanDir bool   `json:"scan_dir"`
	}
	// 允许空 body（默认扫描目录）
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误: " + err.Error()})
			return
		}
	}

	// scan_dir 模式：空 path → 后端扫描目录全部加载
	targetPath := req.Path
	if req.ScanDir || targetPath == "" {
		targetPath = "" // 空 path 触发后端扫描整个目录
	}

	// 调用重载回调（后端处理一切：扫描、解析、创建任务）
	result, count, err := h.swaggerReload(targetPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "重载失败: " + err.Error()})
		return
	}

	// 多项目以逗号分隔返回
	projects := strings.Split(result, ",")
	if result == "" {
		projects = nil
	}

	log.Printf("[API] Swagger 重载完成，%d 个项目: %v", count, projects)
	c.JSON(http.StatusOK, gin.H{
		"message":  "后端解析完成，项目列表已更新",
		"projects": projects,
		"count":    count,
	})
}
