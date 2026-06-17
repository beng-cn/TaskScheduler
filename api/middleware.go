// Package api 提供 HTTP API 层，包括路由、中间件和请求处理器。
// 采用经典的分层结构：中间件 → 路由 → 处理器 → 调度器。
package api

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// NamespaceKey 是 gin.Context 中存储 namespace 的键。
const NamespaceKey = "namespace"

// LoggerMiddleware 记录每个 HTTP 请求的方法、路径、状态码和耗时。
func LoggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method

		c.Next()

		latency := time.Since(start)
		statusCode := c.Writer.Status()

		log.Printf("[HTTP] %s %s | %d | %v | ns=%s", method, path, statusCode, latency, GetNamespace(c))
	}
}

// RecoveryMiddleware 捕获 panic 并返回 500 错误，避免整个服务崩溃。
func RecoveryMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("[HTTP] panic 恢复: %v", err)
				c.JSON(500, gin.H{
					"error": "服务器内部错误",
				})
				c.Abort()
			}
		}()
		c.Next()
	}
}

// APIKey 是简单的 API 鉴权密钥，通过环境变量 API_KEY 设置。
// 格式：namespace:secret（多租户） 或 纯 secret（单租户，namespace 默认 "default"）。
var APIKey = func() string {
	if key := os.Getenv("API_KEY"); key != "" {
		return key
	}
	log.Println("[安全] API_KEY 环境变量未设置，鉴权已禁用（仅用于开发环境）")
	return ""
}()

// parseAPIKey 解析 API Key，返回 (namespace, secret)。
// 格式：namespace:secret → ("namespace", "secret")
//
//	纯 secret       → ("default", "secret")
func parseAPIKey(key string) (string, string) {
	if idx := strings.LastIndex(key, ":"); idx > 0 {
		return key[:idx], key[idx+1:]
	}
	return "default", key
}

// GetNamespace 从 gin.Context 中获取当前请求的租户命名空间。
func GetNamespace(c *gin.Context) string {
	if ns, exists := c.Get(NamespaceKey); exists {
		return ns.(string)
	}
	return "default"
}

// AuthMiddleware 基于 X-API-Key 请求头的鉴权 + namespace 注入。
// 如果 APIKey 为空则跳过鉴权（开发模式），namespace 从 X-Namespace 头或默认值获取。
func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path
		method := c.Request.Method

		// 只读接口 + 前端页面不鉴权
		if path == "/api/health" || path == "/api/stats" || path == "/api/task-types" || path == "/api/error-log" ||
			path == "/swagger" || path == "/swagger.json" ||
			path == "/" || path == "/index.html" ||
			strings.HasPrefix(path, "/docs/") || path == "/docs" {
			c.Set(NamespaceKey, c.GetHeader("X-Namespace"))
			c.Next()
			return
		}
		if strings.HasPrefix(path, "/static") {
			c.Set(NamespaceKey, c.GetHeader("X-Namespace"))
			c.Next()
			return
		}
		if path == "/ws" {
			c.Set(NamespaceKey, c.GetHeader("X-Namespace"))
			c.Next()
			return
		}
		// GET /api/tasks 允许，POST/DELETE 需要 Key
		if path == "/api/tasks" && method == "GET" {
			c.Set(NamespaceKey, c.GetHeader("X-Namespace"))
			c.Next()
			return
		}
		if strings.HasPrefix(path, "/api/tasks/") && method == "GET" {
			c.Set(NamespaceKey, c.GetHeader("X-Namespace"))
			c.Next()
			return
		}

		// 开发模式：无 Key 时跳过鉴权，namespace 从请求头获取
		if APIKey == "" {
			ns := c.GetHeader("X-Namespace")
			if ns == "" {
				ns = "default"
			}
			c.Set(NamespaceKey, ns)
			c.Next()
			return
		}

		// 生产模式：验证 X-API-Key，并从中解析 namespace
		key := c.GetHeader("X-API-Key")
		if key == "" {
			c.JSON(401, gin.H{"error": "未授权：缺少 X-API-Key 请求头"})
			c.Abort()
			return
		}

		_, expectedSecret := parseAPIKey(APIKey)
		clientNS, clientSecret := parseAPIKey(key)

		if clientSecret != expectedSecret {
			c.JSON(401, gin.H{"error": "未授权：API Key 无效"})
			c.Abort()
			return
		}

		// 注入 namespace 到 context（从 Key 中解析，不可伪造）
		c.Set(NamespaceKey, clientNS)
		c.Next()
	}
}

// CORSMiddleware 处理跨域请求，允许前端 Dashboard 访问 API。
func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, X-API-Key, X-Namespace, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}
