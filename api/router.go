package api

import (
	"github.com/gin-gonic/gin"
	"task-scheduler/scheduler"
)

// SetupRouter 创建并配置 Gin 路由引擎。
// 注册所有中间件和 API 路由，同时托管 Web 前端静态文件。
func SetupRouter(sched *scheduler.Scheduler) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()

	// 全局中间件（按顺序执行）
	router.Use(RecoveryMiddleware())
	router.Use(LoggerMiddleware())
	router.Use(CORSMiddleware())
	router.Use(AuthMiddleware()) // API Key 鉴权

	handler := NewHandler(sched)

	// --- API 路由组 ---
	api := router.Group("/api")
	{
		api.GET("/health", handler.Health)
		api.GET("/stats", handler.GetStats)
		api.GET("/task-types", handler.GetTaskTypes)
		api.GET("/error-log", handler.ErrorLog)

		tasks := api.Group("/tasks")
		{
			tasks.POST("", handler.CreateTask)
			tasks.GET("", handler.ListTasks)
			tasks.GET("/:id", handler.GetTask)
			tasks.DELETE("/:id", handler.DeleteTask)
		}
	}

	// --- 前端静态文件 ---
	router.StaticFile("/", "./web/index.html")
	router.StaticFile("/index.html", "./web/index.html")
	router.Static("/static", "./web")

	// --- Swagger 文档 ---
	router.StaticFile("/swagger", "./web/swagger.html")
	router.StaticFile("/swagger.json", "./docs/swagger.json")
	router.Static("/docs", "./docs")

	return router
}
