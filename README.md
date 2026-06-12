# ⚡ TaskScheduler — 轻量级分布式任务调度系统

> 一个基于 Go 语言开发的任务调度系统，支持异步任务提交、延迟执行、失败重试、超时控制和优雅退出。

---

## 📊 架构概览

```
┌──────────────┐     ┌──────────────┐
│  Web 控制台   │     │  CLI 客户端   │
└──────┬───────┘     └──────┬───────┘
       │ HTTP                │ HTTP
       ▼                     ▼
┌──────────────────────────────────────┐
│           API 层 (Gin)               │
│  任务CRUD / 统计查询 / 健康检查       │
└──────────────┬───────────────────────┘
               │
       ┌───────▼────────┐
       │   Scheduler    │  ◀── 核心调度引擎
       │  (goroutine)   │      轮询 → 分发 → 状态管理
       └───────┬────────┘
               │
    ┌──────────┼──────────┐
    ▼          ▼          ▼
┌────────┐┌────────┐┌────────┐
│Worker 1││Worker 2││Worker N│  ◀── Worker Pool
└────────┘└────────┘└────────┘      (channel 通信)
    │         │         │
    └─────────┼─────────┘
              ▼
    ┌─────────────────┐
    │   Memory Store   │  ◀── 可切换为 MySQL/Redis
    └─────────────────┘
```

---

## 🚀 快速开始

### 前提条件
- Go 1.21+
- (可选) Docker & Docker Compose

### 本地运行

```bash
# 克隆项目
cd task-scheduler

# 安装依赖
go mod tidy

# 一行命令启动
make run
# 或者: go run cmd/server/main.go
```

启动后：
- **Web 控制台**: http://localhost:8080
- **健康检查**: `curl http://localhost:8080/api/health`
- **系统统计**: `curl http://localhost:8080/api/stats`

### Docker 运行

```bash
make docker
# 或者: docker-compose up -d
```

---

## 📖 API 文档

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/health` | 健康检查 |
| GET | `/api/stats` | 系统运行统计 |
| GET | `/api/task-types` | 支持的任务类型 |
| POST | `/api/tasks` | 创建任务 |
| GET | `/api/tasks` | 列出所有任务 |
| GET | `/api/tasks/:id` | 获取任务详情 |
| DELETE | `/api/tasks/:id` | 删除任务 |

### 创建任务示例

```bash
# 真实 HTTP 请求任务（调用 httpbin.org 公开测试 API）
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "获取公网IP",
    "type": "http_call",
    "payload": "{\"url\":\"https://httpbin.org/ip\",\"method\":\"GET\"}",
    "max_retries": 3,
    "timeout": 10
  }'

# 数据清理任务（删除过期任务）
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "清理过期任务",
    "type": "data_clean",
    "payload": "{}"
  }'

# 延迟 10 秒执行的 HTTP 请求
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "延迟回调",
    "type": "http_call",
    "payload": "{\"url\":\"https://httpbin.org/delay/1\",\"method\":\"GET\"}",
    "delay": 10
  }'
```

---

## 🏗️ 项目结构

```
task-scheduler/
├── cmd/
│   ├── server/main.go       # 服务端入口（含优雅退出）
│   └── client/main.go       # 命令行客户端
├── api/
│   ├── router.go            # 路由注册
│   ├── handler.go           # HTTP 处理器
│   └── middleware.go        # 中间件（日志/恢复/CORS）
├── scheduler/
│   ├── scheduler.go         # 核心调度引擎
│   └── task.go              # 任务数据结构 + 状态机
├── worker/
│   ├── pool.go              # Worker 协程池
│   └── runner.go            # 任务执行器（可注册扩展）
├── store/
│   ├── store.go             # 存储接口定义
│   └── memory.go            # 内存实现（开发/测试用）
├── config/
│   └── config.go            # 配置加载
├── web/
│   └── index.html           # Web 控制台（单文件）
├── Makefile
├── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## 🔧 技术选型

| 组件 | 选型 | 理由 |
|---|---|---|
| HTTP 框架 | [Gin](https://github.com/gin-gonic/gin) | Go 生态最流行，性能高 |
| 并发模型 | goroutine + channel | Go 原生并发，CSP 模式 |
| 存储 | 接口抽象 (memory/MySQL/Redis) | 接口隔离，便于切换和测试 |
| 配置 | 内置 JSON 解析 | 零外部依赖，满足需求 |
| 前端 | 原生 HTML/CSS/JS | 单文件，无框架依赖，秒加载 |

---

## 🎯 核心设计要点

### 1. 任务生命周期状态机

```
PENDING → RUNNING → DONE
                ↘ FAILED → RETRYING → PENDING (重试)
                ↘ TIMEOUT
```

### 2. 并发安全

- `sync.Map` 追踪运行中任务（读多写少场景）
- 有缓冲 `channel` 解耦调度与执行
- `sync.RWMutex` 保护统计指标

### 3. 优雅退出

```
收到 SIGTERM
  → 停止接收新请求 (HTTP Shutdown)
  → 停止分发新任务 (cancel context)
  → 排空 Worker 队列中剩余任务
  → 等待所有执行中任务完成
  → 关闭存储连接
  → 安全退出
```

### 4. 接口抽象

`Store` 接口定义了存储层的完整契约。当前提供内存实现（零依赖秒启动），未来只需新增 `mysql.go` 或 `redis.go` 即可切换后端，无需修改业务代码。

---

## 📈 后续扩展方向

| 功能 | 说明 | 难度 |
|---|---|---|
| Cron 定时表达式 | 支持 `0 */6 * * *` 格式的周期性任务 | ⭐⭐ |
| 分布式部署 | 基于 Redis/etcd 的选主和分布式锁 | ⭐⭐⭐⭐ |
| 任务依赖 DAG | 支持 A→B→C 的任务依赖编排 | ⭐⭐⭐ |
| 时间轮调度 | 高精度毫秒级延迟任务（参考 Kafka 设计） | ⭐⭐⭐ |
| gRPC 协议 | 内部服务间高性能通信 | ⭐⭐ |
| 指标监控 | Prometheus + Grafana 接入 | ⭐⭐ |
| 持久化存储 | MySQL/PostgreSQL 存储实现 | ⭐⭐ |

---

## 📝 许可证

MIT License
