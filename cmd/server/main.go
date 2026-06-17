// Package main 是任务调度系统的服务端入口。
// 它负责：加载配置 → 初始化各组件 → 启动调度器 → 启动 HTTP 服务 → 等待退出信号 → 优雅关闭。
//
// 启动方式：
//
//	go run cmd/server/main.go
//	go run cmd/server/main.go -config config.json
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"task-scheduler/api"
	"task-scheduler/config"
	"task-scheduler/models"
	"task-scheduler/notify"
	"task-scheduler/scheduler"
	"task-scheduler/store"
	"task-scheduler/worker"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Println("═══════════════════════════════════════════")
	log.Println("  TaskScheduler — 轻量级任务调度系统")
	log.Println("═══════════════════════════════════════════")

	// --- 1. 加载配置 ---
	configPath := flag.String("config", "", "配置文件路径 (JSON格式)")
	flag.Parse()

	cfg, err := config.LoadFromFile(*configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	log.Printf("[启动] 存储类型: %s", cfg.Store.Type)
	log.Printf("[启动] Worker数量: %d, 队列大小: %d", cfg.Worker.Count, cfg.Worker.QueueSize)
	log.Printf("[启动] 轮询间隔: %v, 默认超时: %v", cfg.Scheduler.PollInterval, cfg.Scheduler.DefaultTimeout)

	// --- 2. 初始化存储 ---
	var taskStore scheduler.Store
	switch cfg.Store.Type {
	case "mysql":
		mysqlStore, err := store.NewMySQLStore(cfg.Store.DSN)
		if err != nil {
			log.Fatalf("[启动] MySQL 连接失败: %v", err)
		}
		taskStore = mysqlStore
		log.Println("[启动] 使用 MySQL 存储（任务持久化，重启不丢失）")
	case "redis":
		// 修复：使用配置文件中的 DSN 而非硬编码 127.0.0.1:6379
		redisAddr := cfg.Store.DSN
		if redisAddr == "" {
			redisAddr = "127.0.0.1:6379"
		}
		redisStore, err := store.NewRedisStore(redisAddr, "", 0)
		if err != nil {
			log.Fatalf("[启动] Redis 连接失败: %v", err)
		}
		taskStore = redisStore
		log.Println("[启动] 使用 Redis 存储（高性能缓存 + RDB/AOF 持久化）")
	default:
		taskStore = store.NewMemoryStore()
		log.Println("[启动] 使用内存存储（进程重启后数据会丢失）")
	}

	// 注入 MySQL/Redis 连接（供多步验证 runner 使用，通过环境变量配置）
	// 修复：不再硬编码默认密码，未配置时留空
	mysqlConn := os.Getenv("MYSQL_DSN")
	redisConn := os.Getenv("REDIS_ADDR")
	worker.SetDBConnectors(mysqlConn, redisConn)

	// 注入清理函数：data_clean 任务通过此回调访问存储层
	worker.SetCleanupFunc(func(ctx context.Context) (int, error) {
		tasks, err := taskStore.ListTasks(ctx, "") // 空 namespace = 全量清理
		if err != nil {
			return 0, err
		}
		cutoff := time.Now().Add(-10 * time.Minute) // 清理 10 分钟前完成的任务（演示用）
		deleted := 0
		for _, t := range tasks {
			if t.Status == models.StatusDone && t.FinishedAt != nil && t.FinishedAt.Before(cutoff) {
				if err := taskStore.DeleteTask(ctx, t.ID); err != nil {
					log.Printf("[Cleanup] 删除任务 %s 失败: %v", t.ID, err)
					continue
				}
				log.Printf("[Cleanup] 自动清理过期任务: %s (%s)", t.Name, t.ID)
				deleted++
			}
		}
		return deleted, nil
	})

	// --- 3. 初始化调度器 ---
	sched := scheduler.New(cfg, taskStore)

	// 配置飞书告警 Webhook
	feishuHook := os.Getenv("FEISHU_WEBHOOK")
	if feishuHook != "" {
		notify.SetWebhook(feishuHook)
	} else {
		log.Println("[启动] 飞书 Webhook 未配置，告警功能已禁用")
	}
	// 初始化错误日志文件（超过 7 天自动清理）
	notify.InitErrorLog("logs/error.log")

	// 注入任务完成回调：回写存储 + 循环任务 + 失败告警
	worker.SetOnTaskComplete(func(ctx context.Context, task *scheduler.Task) {
		if err := taskStore.UpdateTask(ctx, task); err != nil {
			log.Printf("[回调] 回写任务 %s 失败: %v", task.ID, err)
		}
		// 失败告警：任务失败或超时时写入错误日志 + 推送飞书通知
		if task.Status == scheduler.StatusFailed || task.Status == scheduler.StatusTimeout {
			notify.LogTaskError(task)
			level := notify.RecordFailure(task.Name)
			notify.LogEscalation(task.Name, level) // 修复：传入 AlertLevel 而非 count
			if err := notify.SendTaskAlert(task); err != nil {
				log.Printf("[告警] 飞书推送失败: %v", err)
			} else {
				log.Printf("[告警] 已推送飞书通知: %s", task.Name)
			}
		}
		// 如果设置了循环间隔且任务成功执行，自动重新创建副本
		if task.RepeatSec > 0 && task.Status == scheduler.StatusDone {
			notify.ResetEscalation(task.Name)
			clone := task.Clone()
			clone.ID = ""
			clone.Status = scheduler.StatusPending
			clone.Retries = 0
			clone.ScheduledAt = time.Now().Add(time.Duration(task.RepeatSec) * time.Second)
			clone.StartedAt = nil
			clone.FinishedAt = nil
			clone.Result = ""
			clone.Error = ""
			// 删除旧的循环任务实例，避免无限累积
			if err := taskStore.DeleteTask(ctx, task.ID); err != nil {
				log.Printf("[循环] 删除旧实例 %s 失败: %v", task.ID, err)
			}
			if err := sched.Submit(clone); err != nil {
				log.Printf("[循环] 重新创建任务「%s」失败: %v", task.Name, err)
			} else {
				log.Printf("[循环] 任务「%s」将在 %d 秒后再次执行", task.Name, task.RepeatSec)
			}
		}
	})

	// --- 4. 初始化 HTTP 路由 ---
	router := api.SetupRouter(sched)

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// --- 5. 预先创建几个演示任务 ---
	tasksPath := "tasks.json"
	loadTasksFromFile(sched, tasksPath)

	// --- 6. 启动调度器 ---
	sched.Start()

	// --- 7. 启动 HTTP 服务 ---
	go func() {
		log.Printf("[HTTP] 服务已启动，访问 http://localhost:%d", cfg.Server.Port)
		log.Printf("[HTTP] Dashboard: http://localhost:%d", cfg.Server.Port)
		log.Printf("[HTTP] API示例: curl http://localhost:%d/api/health", cfg.Server.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP 服务启动失败: %v", err)
		}
	}()

	// --- 8. 等待退出信号 ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("[信号] 收到 %v 信号，开始优雅退出...", sig)

	// --- 9. 优雅关闭 ---
	// 9a. 先停止 HTTP 服务（不再接收新请求）
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[HTTP] 强制关闭: %v", err)
	}
	log.Println("[HTTP] 服务已停止")

	// 9b. 停止调度器（等待所有任务完成）
	// 修复：使用带超时的 goroutine，避免 hang
	done := make(chan struct{})
	go func() {
		sched.Stop()
		close(done)
	}()
	select {
	case <-done:
		log.Println("[Scheduler] 正常退出")
	case <-time.After(30 * time.Second):
		log.Println("[Scheduler] 退出超时（30秒），强制终止")
	}

	log.Println("═══════════════════════════════════════════")
	log.Println("  调度系统已安全退出，再见！")
	log.Println("═══════════════════════════════════════════")
}

// taskFileConfig JSON 任务配置文件的格式。
type taskFileConfig struct {
	Tasks []struct {
		Name         string `json:"name"`
		Type         string `json:"type"`
		Payload      string `json:"payload"`
		MaxRetries   int    `json:"max_retries"`
		Timeout      int64  `json:"timeout"`
		MaxLatencyMs int64  `json:"max_latency_ms"`
		RepeatSec    int64  `json:"repeat_sec"`
		Delay        int64  `json:"delay"`
		Priority     int    `json:"priority"`
	} `json:"tasks"`
}

// loadTasksFromFile 从 JSON 文件加载任务定义并批量创建。
// 换项目只需准备一个新的 tasks.json 即可，无需改代码。
func loadTasksFromFile(sched *scheduler.Scheduler, path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("[TaskFile] 任务配置文件 %s 不存在，跳过（可指定 -tasks 参数）", path)
		return
	}

	var cfg taskFileConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Printf("[TaskFile] 解析 %s 失败: %v", path, err)
		return
	}

	for _, t := range cfg.Tasks {
		task := &scheduler.Task{
			Name:         t.Name,
			Type:         t.Type,
			Payload:      t.Payload,
			MaxRetries:   t.MaxRetries,
			Timeout:      t.Timeout,
			RepeatSec:    t.RepeatSec,
			MaxLatencyMs: t.MaxLatencyMs,
			Priority:     t.Priority,
			ScheduledAt:  time.Now().Add(time.Duration(t.Delay) * time.Second),
		}
		if err := sched.Submit(task); err != nil {
			log.Printf("[TaskFile] 创建任务 %s 失败: %v", t.Name, err)
		} else {
			log.Printf("[TaskFile] 已创建: %s (类型: %s, ID: %s)", task.Name, task.Type, task.ID)
		}
	}

	log.Printf("[TaskFile] 从 %s 加载了 %d 个任务", path, len(cfg.Tasks))
}
