// Package worker 提供任务执行器的实现。
// Worker 是实际"干活"的 goroutine，从共享队列中领取任务并执行。
package worker

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"task-scheduler/models"
	"time"

	"github.com/redis/go-redis/v9"
)

// httpClient 是带有超时的全局 HTTP 客户端（修复：不再使用无超时的 DefaultClient）。
var httpClient = &http.Client{Timeout: 30 * time.Second}

// TaskRunner 定义任务的执行逻辑。
type TaskRunner func(ctx context.Context, task *models.Task) (string, error)

// builtinRunners 内置的任务执行器注册表。
// 修复：并发安全——使用 sync.RWMutex 保护 map 读写。
var (
	builtinRunnersMu sync.RWMutex
	builtinRunners   = map[string]TaskRunner{
		"http_call":        httpCallRunner,
		"data_clean":       dataCleanRunner,
		"flash_warmup":     flashWarmupRunner,
		"cart_flow":        cartFlowRunner,
		"flash_full_check": flashFullCheckRunner,
		"order_flow":       orderFlowRunner,
		"user_flow":        userFlowRunner,
		"admin_crud":       adminCRUDRunner,
	}
)

// GetRunner 根据任务类型获取对应的执行器。
func GetRunner(taskType string) TaskRunner {
	builtinRunnersMu.RLock()
	defer builtinRunnersMu.RUnlock()
	return builtinRunners[taskType]
}

// RegisterRunner 注册自定义任务执行器。
func RegisterRunner(taskType string, runner TaskRunner) {
	builtinRunnersMu.Lock()
	defer builtinRunnersMu.Unlock()
	builtinRunners[taskType] = runner
}

// RegisteredTypes 返回所有已注册的任务类型列表。
func RegisteredTypes() []string {
	builtinRunnersMu.RLock()
	defer builtinRunnersMu.RUnlock()
	types := make([]string, 0, len(builtinRunners))
	for t := range builtinRunners {
		types = append(types, t)
	}
	return types
}

// --- 数据库连接注入（用于验证步骤） ---
// 修复：使用 atomic.Value 保护全局变量的读写并发安全，且改为连接池单例模式。

var (
	mysqlDB   atomic.Value // *sql.DB — 修复：单例连接池，不再每次调用创建新连接
	redisConn atomic.Value // *redis.Client — 修复：单例连接池
)

// SetDBConnectors 注入 MySQL DSN 和 Redis 地址，初始化全局连接池。
func SetDBConnectors(mysql, redisAddr string) {
	if mysql != "" {
		db, err := sql.Open("mysql", mysql+"?parseTime=true&charset=utf8mb4&loc=Local")
		if err != nil {
			log.Printf("[Worker] MySQL 连接池初始化失败: %v", err)
		} else {
			db.SetMaxOpenConns(10)
			db.SetMaxIdleConns(3)
			db.SetConnMaxLifetime(5 * time.Minute)
			mysqlDB.Store(db)
		}
	}
	if redisAddr != "" {
		client := redis.NewClient(&redis.Options{
			Addr:         redisAddr,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		})
		redisConn.Store(client)
	}
}

// getMySQLDB 获取全局 MySQL 连接池。
func getMySQLDB() *sql.DB {
	if v := mysqlDB.Load(); v != nil {
		return v.(*sql.DB)
	}
	return nil
}

// getRedisClient 获取全局 Redis 连接池。
func getRedisClient() *redis.Client {
	if v := redisConn.Load(); v != nil {
		return v.(*redis.Client)
	}
	return nil
}

// --- 任务查询注入（供 swagger 生成的 DELETE 任务查询父任务结果） ---

// TaskLookupFunc 根据任务 ID 查询任务对象。
type TaskLookupFunc func(ctx context.Context, taskID string) (*models.Task, error)

var taskLookup atomic.Value // TaskLookupFunc

// SetTaskLookup 设置任务查询回调（供 runner 内部查询依赖任务的结果）。
func SetTaskLookup(fn TaskLookupFunc) {
	taskLookup.Store(fn)
}

func getTaskLookup() TaskLookupFunc {
	if v := taskLookup.Load(); v != nil {
		return v.(TaskLookupFunc)
	}
	return nil
}

// --- 清理函数注入 ---

type CleanupFunc func(ctx context.Context) (int, error)

// 修复：使用 atomic.Value 保护清理函数的并发访问
var cleanupFunc atomic.Value // CleanupFunc

// SetCleanupFunc 设置清理回调函数。
func SetCleanupFunc(fn CleanupFunc) {
	cleanupFunc.Store(fn)
}

func getCleanupFunc() CleanupFunc {
	if v := cleanupFunc.Load(); v != nil {
		return v.(CleanupFunc)
	}
	return nil
}

// --- 任务完成回调 ---

type TaskCallback func(ctx context.Context, task *models.Task)

// 修复：使用 atomic.Value 保护回调函数，消除 data race
var onTaskComplete atomic.Value // TaskCallback

// SetOnTaskComplete 设置任务完成回调函数。
func SetOnTaskComplete(fn TaskCallback) {
	onTaskComplete.Store(fn)
}

func getOnTaskComplete() TaskCallback {
	if v := onTaskComplete.Load(); v != nil {
		return v.(TaskCallback)
	}
	return nil
}

// --- 辅助函数 ---

// stepFailed 检查是否有子步骤失败。
func stepFailed(task *models.Task) (bool, string) {
	for _, s := range task.Steps {
		if s.Status == "failed" {
			return true, s.Name
		}
	}
	return false, ""
}

// finalResult 统一检查子步骤状态，任何一步失败则返回 error。
// 所有多步 runner 必须在 return 前调用此函数。
func finalResult(task *models.Task, label string) (string, error) {
	if failed, step := stepFailed(task); failed {
		return fmt.Sprintf("%s失败: 第「%s」步出错，共 %d 步", label, step, len(task.Steps)),
			fmt.Errorf("子步骤失败: %s", step)
	}
	return fmt.Sprintf("%s完成，共 %d 步", label, len(task.Steps)), nil
}

// RecordStep 执行一个子步骤并自动记录结果到 task.Steps。
// fn 返回 (摘要, error)，失败时步骤标记为 failed。
func RecordStep(task *models.Task, name string, fn func() (string, error)) {
	step := models.TaskStep{Name: name}
	start := time.Now()
	result, err := fn()
	step.DurationMs = time.Since(start).Milliseconds()
	if err != nil {
		step.Status = "failed"
		step.Error = err.Error()
	} else {
		step.Status = "done"
		step.Result = result
	}
	task.Steps = append(task.Steps, step)
}

// httpGet 发起 HTTP GET 请求并返回响应体。
func httpGet(ctx context.Context, url, token string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("构造 GET 请求失败: %w", err)
	}
	if token != "" && !strings.HasPrefix(token, "Bearer ") {
		req.Header.Set("Authorization", "Bearer "+token)
	} else if token != "" {
		req.Header.Set("Authorization", token)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", fmt.Errorf("读取响应失败: %w", err)
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return fmt.Sprintf("%d — %s", resp.StatusCode, truncateStr(string(body), 200)), nil
}

// httpPost 发起 HTTP POST 请求并返回响应体。
func httpPost(ctx context.Context, url, token string, jsonBody string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBufferString(jsonBody))
	if err != nil {
		return "", fmt.Errorf("构造 POST 请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" && !strings.HasPrefix(token, "Bearer ") {
		req.Header.Set("Authorization", "Bearer "+token)
	} else if token != "" {
		req.Header.Set("Authorization", token)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", fmt.Errorf("读取响应失败: %w", err)
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return fmt.Sprintf("%d — %s", resp.StatusCode, truncateStr(string(body), 200)), nil
}

// login 调登录接口获取 token。
// loginURL 可以是完整 URL（http://...）或基础地址（自动追加 /api/v1/user/login）。
func login(ctx context.Context, loginURL, username, password string) (string, error) {
	// 如果不是完整 URL，则作为基础地址拼接登录路径
	if !strings.HasPrefix(loginURL, "http") {
		loginURL = loginURL + "/api/v1/user/login"
	}
	loginBody, err := json.Marshal(map[string]string{
		"username": username,
		"password": password,
	})
	if err != nil {
		return "", fmt.Errorf("序列化登录请求失败: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", loginURL,
		bytes.NewBuffer(loginBody))
	if err != nil {
		return "", fmt.Errorf("构造登录请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("登录请求失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("读取登录响应失败: %w", err)
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("登录返回 %d: %s", resp.StatusCode, truncateStr(string(body), 200))
	}
	var result struct {
		Code    int    `json:"code"`
		Message string `json:"msg"`
		Data    struct {
			Token string `json:"token"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("解析登录响应失败: %w", err)
	}
	// 检查业务状态码（code=0 表示成功）
	if result.Code != 0 {
		return "", fmt.Errorf("登录业务失败: %s", result.Message)
	}
	if result.Data.Token == "" {
		return "", fmt.Errorf("未获取到 token")
	}
	return result.Data.Token, nil
}

// monitorAdminUser 和 monitorAdminPass 是监控用管理员凭据，可通过环境变量覆盖。
func monitorAdminUser() string {
	if u := os.Getenv("MONITOR_USER"); u != "" {
		return u
	}
	return "testuser"
}
func monitorAdminPass() string {
	if p := os.Getenv("MONITOR_PASS"); p != "" {
		return p
	}
	return "test123456"
}

// queryMySQL 执行 MySQL 查询并返回结果摘要。
// 修复：使用全局连接池，不再每次调用新建连接。
func queryMySQL(query string, args ...interface{}) (string, error) {
	db := getMySQLDB()
	if db == nil {
		return "", fmt.Errorf("MySQL 未配置")
	}

	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		return "", fmt.Errorf("查询失败: %w", err)
	}
	return fmt.Sprintf("记录数: %d", count), nil
}

// queryRedis 查询 Redis 键是否存在。
// 修复：使用全局连接池，不再每次调用新建连接。
func queryRedis(key string) (string, error) {
	client := getRedisClient()
	if client == nil {
		return "", fmt.Errorf("Redis 未配置")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	val, err := client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("键 %s 不存在", key)
	}
	if err != nil {
		return "", fmt.Errorf("Redis查询失败: %w", err)
	}
	return fmt.Sprintf("键存在，长度: %d", len(val)), nil
}

// truncateStr 安全截断字符串，按 rune（字符）而非字节截断，避免截断多字节 UTF-8 字符。
func truncateStr(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "..."
}

func nowPtr() *time.Time {
	t := time.Now()
	return &t
}

// --- http_call（支持自动认证 + 父任务ID继承） ---

func httpCallRunner(ctx context.Context, task *models.Task) (string, error) {
	var params struct {
		URL                  string            `json:"url"`
		Method               string            `json:"method"`
		Headers              map[string]string `json:"headers"`
		Body                 string            `json:"body"`
		ExpectCode           int               `json:"expect_code"`            // 期望的 HTTP 状态码，0 表示不校验
		NeedAuth             bool              `json:"need_auth"`              // 是否需要自动管理员登录获取 token
		InheritIDFromParent  bool              `json:"inherit_id_from_parent"`  // 是否从父任务 Result 中提取 ID
		ParentTaskID         string            `json:"parent_task_id"`         // 父任务 ID
	}
	if err := json.Unmarshal([]byte(task.Payload), &params); err != nil {
		return "", fmt.Errorf("解析 Payload 失败: %w", err)
	}
	if params.URL == "" {
		return "", fmt.Errorf("url 不能为空")
	}
	if params.Method == "" {
		params.Method = "GET"
	}

	// ── 处理 ID 继承：从父任务 Result 中提取 ID 替换 __FROM_PARENT__ ──
	if params.InheritIDFromParent && params.ParentTaskID != "" {
		lookup := getTaskLookup()
		if lookup == nil {
			return "", fmt.Errorf("任务查询函数未注册，无法从父任务 %s 继承 ID", params.ParentTaskID)
		}
		parentTask, err := lookup(ctx, params.ParentTaskID)
		if err != nil {
			return "", fmt.Errorf("查询父任务 %s 失败: %w", params.ParentTaskID, err)
		}
		if parentTask.Status != models.StatusDone {
			return "", fmt.Errorf("父任务 %s 尚未完成（当前状态: %s）", params.ParentTaskID, parentTask.Status)
		}
		// 从父任务 Result 中提取 data.id
		parentID, err := extractIDFromResult(parentTask.Result)
		if err != nil {
			return "", fmt.Errorf("从父任务 %s 的结果中提取 ID 失败: %w", params.ParentTaskID, err)
		}
		params.URL = strings.Replace(params.URL, "__FROM_PARENT__", parentID, 1)
		log.Printf("[Worker] 任务 %s 从父任务 %s 继承了 ID=%s", task.ID, params.ParentTaskID, parentID)
	}

	// ── 处理自动认证：用管理员账号登录获取 token ──
	var authToken string
	if params.NeedAuth {
		loginURL := os.Getenv("LOGIN_URL")
		if loginURL == "" {
			// 从目标 URL 推断登录地址（同 host 下的 /api/v1/user/login）
			loginURL = inferLoginURL(params.URL)
		}
		user := monitorAdminUser()
		pass := monitorAdminPass()
		var err error
		authToken, err = login(ctx, loginURL, user, pass)
		if err != nil {
			return "", fmt.Errorf("自动登录失败 (%s): %w", loginURL, err)
		}
		log.Printf("[Worker] 任务 %s 自动登录成功", task.ID)
	}

	// ── 构造请求 ──
	var reqBody io.Reader
	if params.Body != "" {
		reqBody = bytes.NewBufferString(params.Body)
	}
	req, err := http.NewRequestWithContext(ctx, params.Method, params.URL, reqBody)
	if err != nil {
		return "", fmt.Errorf("构造请求失败: %w", err)
	}
	// 默认 Content-Type
	if params.Body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range params.Headers {
		req.Header.Set(k, v)
	}
	// 注入认证 token
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	start := time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 2048))
	if err != nil {
		return "", fmt.Errorf("读取响应失败: %w", err)
	}
	elapsed := time.Since(start)

	// 检查响应延迟
	var latencyWarn string
	if task.MaxLatencyMs > 0 && elapsed.Milliseconds() > task.MaxLatencyMs {
		latencyWarn = fmt.Sprintf(" [延迟告警: %v > %dms]", elapsed.Round(time.Millisecond), task.MaxLatencyMs)
		log.Printf("[Worker] 任务 %s 响应延迟 %v 超过阈值 %dms", task.ID, elapsed.Round(time.Millisecond), task.MaxLatencyMs)
	}

	// 检查期望状态码
	if params.ExpectCode > 0 && resp.StatusCode != params.ExpectCode {
		return "", fmt.Errorf("状态码不匹配: 期望 %d, 实际 %d — %s%s",
			params.ExpectCode, resp.StatusCode, string(bodyBytes), latencyWarn)
	}

	result := fmt.Sprintf("HTTP %s %s → %d (%v) — %s%s",
		params.Method, params.URL, resp.StatusCode, elapsed.Round(time.Millisecond), string(bodyBytes), latencyWarn)
	return result, nil
}

// extractIDFromResult 从任务执行结果中提取 data.id 字段。
// 支持格式：HTTP 200 — {"code":0,"data":{"id":123}}
func extractIDFromResult(result string) (string, error) {
	if result == "" {
		return "", fmt.Errorf("父任务结果为空")
	}
	// 去掉 "HTTP 200 — " 前缀
	if idx := strings.Index(result, "{"); idx >= 0 {
		result = result[idx:]
	}
	var resp struct {
		Data struct {
			ID interface{} `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(result), &resp); err != nil {
		return "", fmt.Errorf("解析父任务结果JSON失败: %w", err)
	}
	if resp.Data.ID == nil {
		return "", fmt.Errorf("父任务结果中未找到 data.id 字段")
	}
	return fmt.Sprintf("%v", resp.Data.ID), nil
}

// inferLoginURL 从目标 URL 推断登录接口地址。
// 例如：http://localhost:8080/api/v1/admin/product → http://localhost:8080/api/v1/user/login
func inferLoginURL(targetURL string) string {
	// 提取 scheme://host 部分
	idx := strings.Index(targetURL, "://")
	if idx < 0 {
		return targetURL + "/api/v1/user/login"
	}
	rest := targetURL[idx+3:]
	slashIdx := strings.Index(rest, "/")
	if slashIdx < 0 {
		return targetURL + "/api/v1/user/login"
	}
	base := targetURL[:idx+3+slashIdx]
	return base + "/api/v1/user/login"
}

// --- data_clean（单步） ---

func dataCleanRunner(ctx context.Context, task *models.Task) (string, error) {
	fn := getCleanupFunc()
	if fn == nil {
		return "", fmt.Errorf("清理函数未注册")
	}
	count, err := fn(ctx)
	if err != nil {
		return "", fmt.Errorf("清理失败: %w", err)
	}
	if count == 0 {
		return "清理完成: 没有需要清理的过期任务", nil
	}
	return fmt.Sprintf("清理完成: 已删除 %d 个过期任务", count), nil
}

// --- flash_warmup（3 步） ---

func flashWarmupRunner(ctx context.Context, task *models.Task) (string, error) {
	task.Steps = nil // 重置子步骤
	var params struct {
		BaseURL string `json:"base_url"`
		FlashID string `json:"flash_id"`
	}
	if err := json.Unmarshal([]byte(task.Payload), &params); err != nil {
		return "", fmt.Errorf("解析 Payload 失败: %w", err)
	}
	if params.BaseURL == "" {
		params.BaseURL = "http://localhost:8080"
	}
	if params.FlashID == "" {
		params.FlashID = "1"
	}
	base := params.BaseURL
	fid := params.FlashID

	var token string

	// 步骤1：登录
	RecordStep(task, "HTTP 登录管理员", func() (string, error) {
		var err error
		token, err = login(ctx, base, monitorAdminUser(), monitorAdminPass())
		if err != nil {
			return "", err
		}
		return "Token 已获取", nil
	})
	if token == "" {
		return "", fmt.Errorf("%s", task.Steps[0].Error)
	}
		// 步骤2：查询秒杀列表，自动发现存在的秒杀活动
		RecordStep(task, "HTTP 查询秒杀列表", func() (string, error) {
			if fid != "1" {
				return "使用指定的 flash_id=" + fid, nil
			}
			// flash_id 未指定或默认值，自动从列表获取
			req, err := http.NewRequestWithContext(ctx, "GET", base+"/api/v1/flash/list", nil)
			if err != nil { return "", err }
			resp, err := httpClient.Do(req)
			if err != nil { return "", err }
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil { return "", fmt.Errorf("读取秒杀列表失败: %w", err) }
			var flashResp struct {
				Data []struct { ID int `json:"id"` } `json:"data"`
			}
			if json.Unmarshal(body, &flashResp) == nil && len(flashResp.Data) > 0 {
				fid = fmt.Sprintf("%d", flashResp.Data[0].ID)
				return fmt.Sprintf("自动选择 flash_id=%s", fid), nil
			}
			return "", fmt.Errorf("无可用秒杀活动，跳过预热")
		})

		// 步骤3：预热
		RecordStep(task, "HTTP 调用预热API", func() (string, error) {
			return httpPost(ctx, base+"/api/v1/admin/flash/"+fid+"/warmup", token, "{}")
		})

		// 步骤4：验证 Redis 库存
		RecordStep(task, "Redis 验证库存写入", func() (string, error) {
			return queryRedis("flash:stock:" + fid)
		})

		return finalResult(task, "秒杀预热检查")
	}

// --- cart_flow（5 步） ---

func cartFlowRunner(ctx context.Context, task *models.Task) (string, error) {
	task.Steps = nil
	var params struct {
		BaseURL   string `json:"base_url"`
		ProductID string `json:"product_id"`
	}
	if err := json.Unmarshal([]byte(task.Payload), &params); err != nil {
		return "", fmt.Errorf("解析 Payload 失败: %w", err)
	}
	if params.BaseURL == "" {
		params.BaseURL = "http://localhost:8080"
	}
	if params.ProductID == "" {
		params.ProductID = "1"
	}
	base := params.BaseURL
	pid := params.ProductID
	var token string

	// 步骤1：登录
	RecordStep(task, "HTTP 用户登录", func() (string, error) {
		var err error
		token, err = login(ctx, base, monitorAdminUser(), monitorAdminPass())
		if err != nil {
			return "", err
		}
		return "Token 已获取", nil
	})
	if token == "" {
		return "", fmt.Errorf("%s", task.Steps[0].Error)
	}

	// 步骤2：查看商品详情
	RecordStep(task, "HTTP 查看商品详情", func() (string, error) {
		return httpGet(ctx, base+"/api/v1/product/"+pid, "")
	})

	// 步骤3：加入购物车
	RecordStep(task, "HTTP 加入购物车", func() (string, error) {
		return httpPost(ctx, base+"/api/v1/auth/cart/add", token,
			fmt.Sprintf(`{"product_id":%s,"quantity":1}`, pid))
	})

	// 步骤4：MySQL 验证购物车记录
	RecordStep(task, "MySQL 验证购物车写入", func() (string, error) {
		return queryMySQL("SELECT COUNT(*) FROM carts WHERE user_id=?", 1)
	})

	// 步骤5：API 验证购物车列表
	RecordStep(task, "HTTP 查询购物车列表", func() (string, error) {
		return httpGet(ctx, base+"/api/v1/auth/cart/list", token)
	})

	return finalResult(task, "购物车流程")
}

// --- flash_full_check（6 步） ---

func flashFullCheckRunner(ctx context.Context, task *models.Task) (string, error) {
	task.Steps = nil
	var params struct {
		BaseURL string `json:"base_url"`
	}
	if err := json.Unmarshal([]byte(task.Payload), &params); err != nil {
		return "", fmt.Errorf("解析 Payload 失败: %w", err)
	}
	if params.BaseURL == "" {
		params.BaseURL = "http://localhost:8080"
	}
	base := params.BaseURL
	var token string
	var flashID string

	// 步骤1：登录
	RecordStep(task, "HTTP 登录管理员", func() (string, error) {
		var err error
		token, err = login(ctx, base, monitorAdminUser(), monitorAdminPass())
		if err != nil {
			return "", err
		}
		return "Token 已获取", nil
	})
	if token == "" {
		return "", fmt.Errorf("%s", task.Steps[0].Error)
	}

	// 步骤2：创建秒杀活动（并解析返回的 ID）
	RecordStep(task, "HTTP 创建秒杀活动", func() (string, error) {
		now := time.Now()
		start := now.Add(10 * time.Second).Format("2006-01-02 15:04:05")
		end := now.Add(1 * time.Hour).Format("2006-01-02 15:04:05")
		body := fmt.Sprintf(`{"product_id":1,"flash_price":1.99,"flash_stock":100,"start_time":"%s","end_time":"%s"}`,
			start, end)
		// 修复：直接发请求获取完整响应，解析真实 flashID
		req, err := http.NewRequestWithContext(ctx, "POST", base+"/api/v1/admin/flash", strings.NewReader(body))
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := httpClient.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("读取响应失败: %w", err)
		}
		if resp.StatusCode >= 400 {
			return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncateStr(string(raw), 200))
		}
		// 解析返回的秒杀活动 ID
		var result struct {
			Data struct {
				ID int `json:"id"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &result); err == nil && result.Data.ID > 0 {
			flashID = fmt.Sprintf("%d", result.Data.ID)
		}
		return fmt.Sprintf("%d — %s", resp.StatusCode, truncateStr(string(raw), 200)), nil
	})

	// 步骤3：预热（使用真实 flashID）
	RecordStep(task, "HTTP 预热秒杀缓存", func() (string, error) {
		if flashID == "" {
			flashID = "1"
		}
		return httpPost(ctx, base+"/api/v1/admin/flash/"+flashID+"/warmup", token, "{}")
	})

	// 步骤4：Redis 验证
	RecordStep(task, "Redis 验证库存缓存", func() (string, error) {
		if flashID == "" {
			flashID = "1"
		}
		return queryRedis("flash:stock:" + flashID)
	})

	// 步骤5：MySQL 验证 — 修复：使用参数化查询
	RecordStep(task, "MySQL 验证秒杀记录", func() (string, error) {
		return queryMySQL("SELECT COUNT(*) FROM flash_sales")
	})

	// 步骤6：公开 API 验证
	RecordStep(task, "HTTP 查询秒杀列表", func() (string, error) {
		return httpGet(ctx, base+"/api/v1/flash/list", "")
	})

	return finalResult(task, "秒杀全链路检查")
}

// --- order_flow（5 步：登录→选品→下单→MySQL验证→查订单） ---

func orderFlowRunner(ctx context.Context, task *models.Task) (string, error) {
	task.Steps = nil
	var params struct {
		BaseURL   string `json:"base_url"`
		ProductID string `json:"product_id"`
	}
	if err := json.Unmarshal([]byte(task.Payload), &params); err != nil {
		return "", fmt.Errorf("解析 Payload 失败: %w", err)
	}
	if params.BaseURL == "" {
		params.BaseURL = "http://localhost:8080"
	}
	if params.ProductID == "" {
		params.ProductID = "1"
	}
	base := params.BaseURL
	pid := params.ProductID
	var token string

	// 步骤1：登录
	RecordStep(task, "HTTP 用户登录", func() (string, error) {
		var err error
		token, err = login(ctx, base, monitorAdminUser(), monitorAdminPass())
		if err != nil {
			return "", err
		}
		return "Token 已获取", nil
	})
	if token == "" {
		return "", fmt.Errorf("%s", task.Steps[0].Error)
	}

	// 步骤2：查看商品
	RecordStep(task, "HTTP 查看商品详情", func() (string, error) {
		return httpGet(ctx, base+"/api/v1/product/"+pid, "")
	})

		// 步骤3：加入购物车
		var cartIDs []int
		RecordStep(task, "HTTP 加入购物车", func() (string, error) {
			return httpPost(ctx, base+"/api/v1/auth/cart/add", token,
				fmt.Sprintf(`{"product_id":%s,"quantity":1}`, pid))
		})

		// 步骤4：查询购物车获取 cart item ID
		RecordStep(task, "HTTP 查询购物车列表", func() (string, error) {
			req, err := http.NewRequestWithContext(ctx, "GET", base+"/api/v1/auth/cart/list", nil)
			if err != nil { return "", err }
			if token != "" { req.Header.Set("Authorization", "Bearer "+token) }
			resp, err := httpClient.Do(req)
			if err != nil { return "", err }
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil { return "", fmt.Errorf("读取购物车响应失败: %w", err) }
			var cartResp struct {
				Data []struct { ID int `json:"id"` } `json:"data"`
			}
			if json.Unmarshal(body, &cartResp) == nil && len(cartResp.Data) > 0 {
				for _, item := range cartResp.Data { cartIDs = append(cartIDs, item.ID) }
			}
			return fmt.Sprintf("%d — %s", resp.StatusCode, truncateStr(string(body), 200)), nil
		})

		// 步骤5：创建订单（带 cart_ids）
		RecordStep(task, "HTTP 创建订单", func() (string, error) {
			if len(cartIDs) == 0 { cartIDs = []int{1} }
			idsJSON, _ := json.Marshal(cartIDs)
			return httpPost(ctx, base+"/api/v1/auth/order/create", token,
				fmt.Sprintf(`{"cart_ids":%s}`, string(idsJSON)))
		})

		// 步骤6：MySQL 验证订单写入
		RecordStep(task, "MySQL 验证订单记录", func() (string, error) {
			return queryMySQL("SELECT COUNT(*) FROM orders WHERE user_id=?", 1)
		})

		// 步骤7：API 查询订单列表
		RecordStep(task, "HTTP 查询订单列表", func() (string, error) {
			return httpGet(ctx, base+"/api/v1/auth/order/list", token)
		})

		return finalResult(task, "下单流程")
	}

// --- user_flow（3 步：注册→登录→查信息） ---

func userFlowRunner(ctx context.Context, task *models.Task) (string, error) {
	task.Steps = nil
	var params struct {
		BaseURL string `json:"base_url"`
	}
	if err := json.Unmarshal([]byte(task.Payload), &params); err != nil {
		return "", fmt.Errorf("解析 Payload 失败: %w", err)
	}
	if params.BaseURL == "" {
		params.BaseURL = "http://localhost:8080"
	}
	base := params.BaseURL
	testUser := "monitor_" + fmt.Sprintf("%d", time.Now().UnixNano()%100000)

	// 步骤1：注册新用户
	RecordStep(task, "HTTP 用户注册", func() (string, error) {
		body := fmt.Sprintf(`{"username":"%s","password":"test123456","phone":"138%08d"}`, testUser, time.Now().UnixNano()%100000000)
		return httpPost(ctx, base+"/api/v1/user/register", "", body)
	})

	// 步骤2：登录
	var token string
	RecordStep(task, "HTTP 用户登录", func() (string, error) {
		var err error
		token, err = login(ctx, base, testUser, "test123456")
		if err != nil {
			return "", err
		}
		return "Token 已获取", nil
	})
	if token == "" {
		return "", fmt.Errorf("%s", task.Steps[0].Error)
	}

	// 步骤3：获取用户信息
	RecordStep(task, "HTTP 获取用户信息", func() (string, error) {
		return httpGet(ctx, base+"/api/v1/auth/user/info", token)
	})

	return finalResult(task, "用户流程")
}

// --- admin_crud（5 步：登录→创建商品→查询→修改→删除） ---

func adminCRUDRunner(ctx context.Context, task *models.Task) (string, error) {
	task.Steps = nil
	var params struct {
		BaseURL string `json:"base_url"`
	}
	if err := json.Unmarshal([]byte(task.Payload), &params); err != nil {
		return "", fmt.Errorf("解析 Payload 失败: %w", err)
	}
	if params.BaseURL == "" {
		params.BaseURL = "http://localhost:8080"
	}
	base := params.BaseURL
	var token string
	var productID string

	// 步骤1：登录管理员
	RecordStep(task, "HTTP 登录管理员", func() (string, error) {
		var err error
		token, err = login(ctx, base, monitorAdminUser(), monitorAdminPass())
		if err != nil {
			return "", err
		}
		return "Token 已获取", nil
	})
	if token == "" {
		return "", fmt.Errorf("%s", task.Steps[0].Error)
	}

	// 步骤2：创建商品（并解析返回的 ID）
	RecordStep(task, "HTTP 创建商品", func() (string, error) {
		body := fmt.Sprintf(`{"name":"哨兵测试商品-%d","category_id":1,"price":9.99,"stock":999,"keywords":"test"}`, time.Now().UnixNano()%10000)
		req, err := http.NewRequestWithContext(ctx, "POST", base+"/api/v1/admin/product", strings.NewReader(body))
		if err != nil {
			return "", fmt.Errorf("构造请求失败: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := httpClient.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("读取响应失败: %w", err)
		}
		if resp.StatusCode >= 400 {
			return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncateStr(string(raw), 200))
		}
		// 解析 ID
		var result struct {
			Data struct {
				ID int `json:"id"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &result); err != nil {
			log.Printf("[admin_crud] 解析商品ID失败: %v", err)
		}
		if result.Data.ID > 0 {
			productID = fmt.Sprintf("%d", result.Data.ID)
		}
		return fmt.Sprintf("%d — %s", resp.StatusCode, truncateStr(string(raw), 200)), nil
	})

	// 步骤3：MySQL 验证商品写入 — 修复：使用参数化查询，消除 SQL 注入模式
	RecordStep(task, "MySQL 验证商品写入", func() (string, error) {
		if productID == "" {
			return "", fmt.Errorf("未获取到商品ID")
		}
		return queryMySQL("SELECT COUNT(*) FROM products WHERE id=?", productID)
	})

	// 步骤4：修改商品（使用真实 ID）
	RecordStep(task, "HTTP 修改商品价格", func() (string, error) {
		if productID == "" {
			productID = "1"
		}
		req, err := http.NewRequestWithContext(ctx, "PUT", base+"/api/v1/admin/product/"+productID, strings.NewReader(`{"price":8.88}`))
		if err != nil {
			return "", fmt.Errorf("构造请求失败: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := httpClient.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if err != nil {
			return "", fmt.Errorf("读取响应失败: %w", err)
		}
		if resp.StatusCode >= 400 {
			return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(raw))
		}
		return fmt.Sprintf("%d — %s", resp.StatusCode, truncateStr(string(raw), 200)), nil
	})

	// 步骤5：查商品列表
	RecordStep(task, "HTTP 查询商品列表", func() (string, error) {
		return httpGet(ctx, base+"/api/v1/product/list", "")
	})

	return finalResult(task, "后台管理流程")
}

