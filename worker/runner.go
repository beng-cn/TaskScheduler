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
	"strings"
	"task-scheduler/models"
	"time"

	"github.com/redis/go-redis/v9"
)

// TaskRunner 定义任务的执行逻辑。
type TaskRunner func(ctx context.Context, task *models.Task) (string, error)

// builtinRunners 内置的任务执行器注册表。
var builtinRunners = map[string]TaskRunner{
	"http_call":        httpCallRunner,
	"data_clean":       dataCleanRunner,
	"flash_warmup":     flashWarmupRunner,
	"cart_flow":        cartFlowRunner,
	"flash_full_check": flashFullCheckRunner,
	"order_flow":       orderFlowRunner,
	"user_flow":        userFlowRunner,
	"admin_crud":       adminCRUDRunner,
}

// GetRunner 根据任务类型获取对应的执行器。
func GetRunner(taskType string) TaskRunner {
	return builtinRunners[taskType]
}

// RegisterRunner 注册自定义任务执行器。
func RegisterRunner(taskType string, runner TaskRunner) {
	builtinRunners[taskType] = runner
}

// --- 数据库连接注入（用于验证步骤） ---

var mysqlDSN string
var redisAddr string

// SetDBConnectors 注入 MySQL DSN 和 Redis 地址，供验证步骤使用。
func SetDBConnectors(mysql, redis string) {
	mysqlDSN = mysql
	redisAddr = redis
}

// --- 清理函数注入 ---

type CleanupFunc func(ctx context.Context) (int, error)

var cleanupFunc CleanupFunc

func SetCleanupFunc(fn CleanupFunc) { cleanupFunc = fn }

// --- 任务完成回调 ---

type TaskCallback func(ctx context.Context, task *models.Task)

var onTaskComplete TaskCallback

func SetOnTaskComplete(fn TaskCallback) { onTaskComplete = fn }

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
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return fmt.Sprintf("%d — %s", resp.StatusCode, truncateStr(string(body), 200)), nil
}

// httpPost 发起 HTTP POST 请求并返回响应体。
func httpPost(ctx context.Context, url, token string, jsonBody string) (string, error) {
	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBufferString(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return fmt.Sprintf("%d — %s", resp.StatusCode, truncateStr(string(body), 200)), nil
}

// login 调登录接口获取 token，不做截断因为 JWT token 可能很长。
func login(ctx context.Context, baseURL, username, password string) (string, error) {
	loginBody, _ := json.Marshal(map[string]string{
		"username": username,
		"password": password,
	})
	req, _ := http.NewRequestWithContext(ctx, "POST", baseURL+"/api/user/login",
		bytes.NewBuffer(loginBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("登录请求失败: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("登录返回 %d: %s", resp.StatusCode, truncateStr(string(body), 200))
	}
	var result struct {
		Data struct {
			Token string `json:"token"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("解析登录响应失败: %w", err)
	}
	if result.Data.Token == "" {
		return "", fmt.Errorf("未获取到 token")
	}
	return result.Data.Token, nil
}

// queryMySQL 执行 MySQL 查询并返回结果摘要。
func queryMySQL(query string, args ...interface{}) (string, error) {
	if mysqlDSN == "" {
		return "", fmt.Errorf("MySQL 未配置")
	}
	db, err := sql.Open("mysql", mysqlDSN+"?parseTime=true&charset=utf8mb4&loc=Local")
	if err != nil {
		return "", err
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		return "", fmt.Errorf("查询失败: %w", err)
	}
	return fmt.Sprintf("记录数: %d", count), nil
}

// queryRedis 查询 Redis 键是否存在。
func queryRedis(key string) (string, error) {
	if redisAddr == "" {
		return "", fmt.Errorf("Redis 未配置")
	}
	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer client.Close()

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

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func nowPtr() *time.Time {
	t := time.Now()
	return &t
}

// --- http_call（单步，无子步骤） ---

func httpCallRunner(ctx context.Context, task *models.Task) (string, error) {
	var params struct {
		URL        string            `json:"url"`
		Method     string            `json:"method"`
		Headers    map[string]string `json:"headers"`
		Body       string            `json:"body"`
		ExpectCode int               `json:"expect_code"` // 期望的 HTTP 状态码，0 表示不校验
	}
	json.Unmarshal([]byte(task.Payload), &params)
	if params.URL == "" {
		params.URL = task.Payload
	}
	if params.Method == "" {
		params.Method = "POST"
	}
	var reqBody io.Reader
	if params.Body != "" {
		reqBody = bytes.NewBufferString(params.Body)
	}
	req, err := http.NewRequestWithContext(ctx, params.Method, params.URL, reqBody)
	if err != nil {
		return "", fmt.Errorf("构造请求失败: %w", err)
	}
	for k, v := range params.Headers {
		req.Header.Set(k, v)
	}
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
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

// --- data_clean（单步） ---

func dataCleanRunner(ctx context.Context, task *models.Task) (string, error) {
	if cleanupFunc == nil {
		return "", fmt.Errorf("清理函数未注册")
	}
	count, err := cleanupFunc(ctx)
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
	json.Unmarshal([]byte(task.Payload), &params)
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
		token, err = login(ctx, base, "admin", "CHANGE_ME")
		if err != nil {
			return "", err
		}
		return "Token 已获取", nil
	})
	if token == "" {
		return "", fmt.Errorf("%s", task.Steps[0].Error)
	}

	// 步骤2：预热
	RecordStep(task, "HTTP 调用预热API", func() (string, error) {
		return httpPost(ctx, base+"/api/admin/flash/"+fid+"/warmup", token, "{}")
	})

	// 步骤3：验证 Redis 库存
	RecordStep(task, "Redis 验证库存写入", func() (string, error) {
		return queryRedis("flash:stock:" + fid)
	})

	if failed, step := stepFailed(task); failed { return fmt.Sprintf("共 %d 步，第「"+step+"」步失败", len(task.Steps)), fmt.Errorf("子步骤失败: %s", step) }
	return finalResult(task, "秒杀预热检查")
}

// --- cart_flow（5 步） ---

func cartFlowRunner(ctx context.Context, task *models.Task) (string, error) {
	task.Steps = nil
	var params struct {
		BaseURL   string `json:"base_url"`
		ProductID string `json:"product_id"`
	}
	json.Unmarshal([]byte(task.Payload), &params)
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
		token, err = login(ctx, base, "admin", "CHANGE_ME")
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
		return httpGet(ctx, base+"/api/product/"+pid, "")
	})

	// 步骤3：加入购物车
	RecordStep(task, "HTTP 加入购物车", func() (string, error) {
		return httpPost(ctx, base+"/api/auth/cart/add", token,
			fmt.Sprintf(`{"product_id":%s,"quantity":1}`, pid))
	})

	// 步骤4：MySQL 验证购物车记录
	RecordStep(task, "MySQL 验证购物车写入", func() (string, error) {
		return queryMySQL("SELECT COUNT(*) FROM carts WHERE user_id=1")
	})

	// 步骤5：API 验证购物车列表
	RecordStep(task, "HTTP 查询购物车列表", func() (string, error) {
		return httpGet(ctx, base+"/api/auth/cart/list", token)
	})

	return finalResult(task, "购物车流程")
}

// --- flash_full_check（6 步） ---

func flashFullCheckRunner(ctx context.Context, task *models.Task) (string, error) {
	task.Steps = nil
	var params struct {
		BaseURL string `json:"base_url"`
	}
	json.Unmarshal([]byte(task.Payload), &params)
	if params.BaseURL == "" {
		params.BaseURL = "http://localhost:8080"
	}
	base := params.BaseURL
	var token string
	var flashID string

	// 步骤1：登录
	RecordStep(task, "HTTP 登录管理员", func() (string, error) {
		var err error
		token, err = login(ctx, base, "admin", "CHANGE_ME")
		if err != nil {
			return "", err
		}
		return "Token 已获取", nil
	})
	if token == "" {
		return "", fmt.Errorf("%s", task.Steps[0].Error)
	}

	// 步骤2：创建秒杀活动
	RecordStep(task, "HTTP 创建秒杀活动", func() (string, error) {
		now := time.Now()
		start := now.Add(10 * time.Second).Format("2006-01-02 15:04:05")
		end := now.Add(1 * time.Hour).Format("2006-01-02 15:04:05")
		body := fmt.Sprintf(`{"product_id":1,"flash_price":1.99,"stock":100,"start_time":"%s","end_time":"%s"}`,
			start, end)
		return httpPost(ctx, base+"/api/admin/flash", token, body)
	})

	// 步骤3：预热
	RecordStep(task, "HTTP 预热秒杀缓存", func() (string, error) {
		// 取最新创建的秒杀 ID（从步骤2结果或默认1）
		flashID = "1" // 简化：默认预热 ID=1
		return httpPost(ctx, base+"/api/admin/flash/"+flashID+"/warmup", token, "{}")
	})

	// 步骤4：Redis 验证
	RecordStep(task, "Redis 验证库存缓存", func() (string, error) {
		return queryRedis("flash:stock:" + flashID)
	})

	// 步骤5：MySQL 验证
	RecordStep(task, "MySQL 验证秒杀记录", func() (string, error) {
		return queryMySQL("SELECT COUNT(*) FROM flash_sales")
	})

	// 步骤6：公开 API 验证
	RecordStep(task, "HTTP 查询秒杀列表", func() (string, error) {
		return httpGet(ctx, base+"/api/flash/list", "")
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
	json.Unmarshal([]byte(task.Payload), &params)
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
		token, err = login(ctx, base, "admin", "CHANGE_ME")
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
		return httpGet(ctx, base+"/api/product/"+pid, "")
	})

	// 步骤3：创建订单
	RecordStep(task, "HTTP 创建订单", func() (string, error) {
		return httpPost(ctx, base+"/api/auth/order/create", token, "{}")
	})

	// 步骤4：MySQL 验证订单写入
	RecordStep(task, "MySQL 验证订单记录", func() (string, error) {
		return queryMySQL("SELECT COUNT(*) FROM orders WHERE user_id=1")
	})

	// 步骤5：API 查询订单列表
	RecordStep(task, "HTTP 查询订单列表", func() (string, error) {
		return httpGet(ctx, base+"/api/auth/order/list", token)
	})

	return finalResult(task, "下单流程")
}

// --- user_flow（3 步：注册→登录→查信息） ---

func userFlowRunner(ctx context.Context, task *models.Task) (string, error) {
	task.Steps = nil
	var params struct {
		BaseURL string `json:"base_url"`
	}
	json.Unmarshal([]byte(task.Payload), &params)
	if params.BaseURL == "" {
		params.BaseURL = "http://localhost:8080"
	}
	base := params.BaseURL
	testUser := "monitor_" + fmt.Sprintf("%d", time.Now().UnixNano()%100000)

	// 步骤1：注册新用户
	RecordStep(task, "HTTP 用户注册", func() (string, error) {
		body := fmt.Sprintf(`{"username":"%s","password":"test123456","phone":"138%08d"}`, testUser, time.Now().UnixNano()%100000000)
		return httpPost(ctx, base+"/api/user/register", "", body)
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
		return httpGet(ctx, base+"/api/auth/user/info", token)
	})

	return finalResult(task, "用户流程")
}

// --- admin_crud（5 步：登录→创建商品→查询→修改→删除） ---

func adminCRUDRunner(ctx context.Context, task *models.Task) (string, error) {
	task.Steps = nil
	var params struct {
		BaseURL string `json:"base_url"`
	}
	json.Unmarshal([]byte(task.Payload), &params)
	if params.BaseURL == "" {
		params.BaseURL = "http://localhost:8080"
	}
	base := params.BaseURL
	var token string
	var productID string

	// 步骤1：登录管理员
	RecordStep(task, "HTTP 登录管理员", func() (string, error) {
		var err error
		token, err = login(ctx, base, "admin", "CHANGE_ME")
		if err != nil {
			return "", err
		}
		return "Token 已获取", nil
	})
	if token == "" {
		return "", fmt.Errorf("%s", task.Steps[0].Error)
	}

	// 步骤2：创建商品（并解析返回的 ID）
	var createResp string
	RecordStep(task, "HTTP 创建商品", func() (string, error) {
		body := fmt.Sprintf(`{"name":"哨兵测试商品-%d","category_id":1,"price":9.99,"stock":999,"keywords":"test"}`, time.Now().UnixNano()%10000)
		var err error
		createResp, err = httpPost(ctx, base+"/api/admin/product", token, body)
		if err != nil {
			return "", err
		}
		// 解析响应中的商品 ID
		var result struct {
			Data struct {
				ID int `json:"id"`
			} `json:"data"`
		}
		// 从响应中提取 JSON（去掉 "200 — " 前缀和尾部 "..."）
		jsonStr := createResp
		for i, c := range jsonStr {
			if c == '{' { jsonStr = jsonStr[i:]; break }
		}
		jsonStr = strings.TrimSuffix(jsonStr, "...")
		if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
			log.Printf("[admin_crud] 解析商品ID失败: %v, raw: %s", err, createResp[:min(len(createResp),100)])
		}
		if result.Data.ID > 0 {
			productID = fmt.Sprintf("%d", result.Data.ID)
		}
		return createResp, nil
	})

	// 步骤3：MySQL 验证商品写入
	RecordStep(task, "MySQL 验证商品写入", func() (string, error) {
		if productID == "" {
			return "", fmt.Errorf("未获取到商品ID")
		}
		return queryMySQL("SELECT COUNT(*) FROM products WHERE id=" + productID)
	})

	// 步骤4：修改商品（使用真实 ID）
	RecordStep(task, "HTTP 修改商品价格", func() (string, error) {
		if productID == "" {
			productID = "1"
		}
		return httpPost(ctx, base+"/api/admin/product/"+productID, token, `{"price":8.88}`)
	})

	// 步骤5：查商品列表
	RecordStep(task, "HTTP 查询商品列表", func() (string, error) {
		return httpGet(ctx, base+"/api/product/list", "")
	})

	return finalResult(task, "后台管理流程")
}
