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

// APIKey 是简单的 API 鉴权密钥，通过环境变量 API_KEY 设置。
// 修复：不再硬编码默认值，开发环境可设置 API_KEY=demo-secret-key。
var APIKey = func() string {
	if key := os.Getenv("API_KEY"); key != "" {
		return key
	}
	// 默认空密钥 = 跳过鉴权（仅开发环境安全）
	log.Println("[安全] API_KEY 环境变量未设置，鉴权已禁用（仅用于开发环境）")
	return ""
}()

// AuthMiddleware 基于 X-API-Key 请求头的简单鉴权。
// 如果 APIKey 为空则跳过鉴权（开发模式）。
func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 只读接口 + 前端页面不鉴权（Dashboard 需要它们来展示数据）
		path := c.Request.URL.Path
		method := c.Request.Method
		if path == "/api/health" || path == "/api/stats" || path == "/api/task-types" || path == "/api/error-log" ||
			path == "/swagger" || path == "/swagger.json" ||
			path == "/" || path == "/index.html" ||
			strings.HasPrefix(path, "/docs/") || path == "/docs" {
			c.Next()
			return
		}
		if strings.HasPrefix(path, "/static") {
			c.Next()
			return
		}
		// WebSocket 端点单独放行（连接在 Upgrade 时有来源校验）
		if path == "/ws" {
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
		// 修复：仅通过 X-API-Key 请求头鉴权，移除查询参数方式
		key := c.GetHeader("X-API-Key")
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
		// 修复：CORS 允许的头与鉴权头一致
		c.Header("Access-Control-Allow-Headers", "Content-Type, X-API-Key, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}
