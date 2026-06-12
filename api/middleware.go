// Package api 提供 HTTP API 层，包括路由、中间件和请求处理器。
// 采用经典的分层结构：中间件 → 路由 → 处理器 → 调度器。
package api

import (
	"log"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// LoggerMiddleware 记录每个 HTTP 请求的方法、路径、状态码和耗时。
func LoggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method

		c.Next()

		latency := time.Since(start)
		statusCode := c.Writer.Status()

		log.Printf("[HTTP] %s %s | %d | %v", method, path, statusCode, latency)
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

// APIKey 是简单的 API 鉴权密钥，可通过环境变量覆盖。
var APIKey = "demo-secret-key"

// AuthMiddleware 基于 X-API-Key 请求头的简单鉴权。
// 如果 APIKey 为空则跳过鉴权（开发模式）。
func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 只读接口 + 前端页面不鉴权（Dashboard 需要它们来展示数据）
		path := c.Request.URL.Path
		method := c.Request.Method
		if path == "/api/health" || path == "/api/stats" || path == "/api/task-types" || path == "/api/error-log" ||
			path == "/swagger" || path == "/swagger.json" ||
			path == "/" || path == "/index.html" || len(path) >= 5 && path[:5] == "/docs" {
			c.Next()
			return
		}
		if len(path) >= 7 && path[:7] == "/static" {
			c.Next()
			return
		}
		// GET /api/tasks 允许（Dashboard 实时刷新需要），POST/DELETE 需要 Key
		if path == "/api/tasks" && method == "GET" {
			c.Next()
			return
		}
		if strings.HasPrefix(path, "/api/tasks/") && method == "GET" {
			c.Next()
			return
		}
		if APIKey == "" {
			c.Next()
			return
		}
		key := c.GetHeader("X-API-Key")
		if key == "" {
			key = c.Query("api_key")
		}
		if key != APIKey {
			c.JSON(401, gin.H{"error": "未授权：缺少有效的 API Key"})
			c.Abort()
			return
		}
		c.Next()
	}
}

// CORSMiddleware 处理跨域请求，允许前端 Dashboard 访问 API。
func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}
