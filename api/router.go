package api

import (
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"task-scheduler/scheduler"
)

// SetupRouter 创建并配置 Gin 路由引擎。
// 注册所有中间件和 API 路由，同时托管 Web 前端静态文件。
// swaggerReload 是可选的 Swagger 重载回调（由 main 包提供）。
func SetupRouter(sched *scheduler.Scheduler, swaggerReload func(string) (string, int, error)) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()

	// 全局中间件（按顺序执行）
	router.Use(RecoveryMiddleware())
	router.Use(LoggerMiddleware())
	router.Use(CORSMiddleware())
	router.Use(AuthMiddleware()) // API Key 鉴权

	handler := NewHandler(sched)
	if swaggerReload != nil {
		handler.SetSwaggerReload(swaggerReload)
	}

	// --- API 路由组 ---
	api := router.Group("/api")
	{
		api.GET("/health", handler.Health)
		api.GET("/stats", handler.GetStats)
		api.GET("/task-types", handler.GetTaskTypes)
		api.GET("/error-log", handler.ErrorLog)
		api.GET("/projects", handler.ListProjects)

		tasks := api.Group("/tasks")
		{
			tasks.POST("", handler.CreateTask)
			tasks.GET("", handler.ListTasks)
			tasks.GET("/:id", handler.GetTask)
			tasks.DELETE("/:id", handler.DeleteTask)
		}

		swagger := api.Group("/swagger")
		{
			swagger.POST("/reload", handler.SwaggerReload)
		}
	}

	// --- 前端静态文件 ---
	// 修复：通过 handler 注入 API Key 的 meta 标签，确保前端 getApiKey() 能获取到
	router.GET("/", serveIndexWithAPIKey)
	router.GET("/index.html", serveIndexWithAPIKey)
	router.Static("/static", "./web")

	// --- WebSocket 实时推送 ---
	router.GET("/ws", func(c *gin.Context) { WsHandler(c.Writer, c.Request) })

	// --- Swagger 文档 ---
	router.StaticFile("/swagger", "./web/swagger.html")
	router.StaticFile("/swagger.json", "./docs/swagger.json")
	router.Static("/docs", "./docs")

	return router
}

// serveIndexWithAPIKey 读取 index.html 并在 <head> 中注入 API Key 的 meta 标签。
// 确保前端 getApiKey() 能正确获取密钥。
func serveIndexWithAPIKey(c *gin.Context) {
	data, err := os.ReadFile("./web/index.html")
	if err != nil {
		c.String(500, "无法加载 Dashboard")
		return
	}

	html := string(data)

	// 在 </head> 之前注入 meta 标签，使 getApiKey() 能读取
	metaTag := `<meta name="api-key" content="` + APIKey + `">`
	html = strings.Replace(html, "</head>", metaTag+"\n</head>", 1)

	c.Header("Content-Type", "text/html; charset=utf-8")
	c.String(200, html)
}
