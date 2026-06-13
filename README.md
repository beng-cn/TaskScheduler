# ⚡ 哨兵 Sentinel — 电商系统主动式监控调度平台

> 基于 Go 语言的任务调度系统，主动监控电商系统 40 个 API，子步骤追踪、失败告警、数据验证全覆盖。

---

## 📊 架构概览

```
┌──────────┐    ┌──────────┐    ┌──────────┐
│ Dashboard │    │ Swagger  │    │ 飞书推送  │
│ :8888     │    │ :8888/sw │    │ 手机端    │
└─────┬─────┘    └────┬─────┘    └────┬─────┘
      │ 轮询           │ 调用          │ Webhook
      ▼                ▼               ▼
┌──────────────────────────────────────────┐
│              API 层 (Gin)                │
│  CRUD / 统计 / 错误日志 / 鉴权           │
└──────────────────┬───────────────────────┘
                   │
          ┌────────▼────────┐
          │   Scheduler     │  ◀── goroutine + Ticker
          │  轮询 → 分发     │      池满告警 / panic恢复
          └────────┬────────┘
                   │ channel
     ┌─────────────┼─────────────┐
     ▼             ▼             ▼
┌─────────┐  ┌─────────┐  ┌─────────┐
│Worker 1 │  │Worker 2 │  │Worker N │  ◀── 10 Worker 协程池
└────┬────┘  └────┬────┘  └────┬────┘
     │            │            │
     └────────────┼────────────┘
                  ▼
     ┌───────────────────────┐
     │  Store 接口            │
     │  Memory / MySQL / Redis│
     └───────────────────────┘
```

---

## 🚀 快速开始

### 方式一：Docker（推荐，零依赖）

```bash
git clone https://github.com/beng-cn/TaskScheduler.git
cd TaskScheduler
docker-compose up -d
```

镜像约 15MB，Go 依赖、CA 证书全部内置，无需安装任何运行时。

### 方式二：本地 Go 运行

```bash
# 前提：Go 1.21+
git clone https://github.com/beng-cn/TaskScheduler.git
cd TaskScheduler
go run cmd/server/main.go          # 内存模式，秒启动
go run cmd/server/main.go -config config.json  # MySQL 持久化
```

### 方式三：Make

```bash
make run          # 内存模式
make run-mysql    # MySQL 模式
make docker       # Docker Compose
```

启动后：
- **Dashboard**: http://localhost:8888
- **Swagger 文档**: http://localhost:8888/swagger
- **错误日志**: http://localhost:8888/api/error-log

---

## 📖 API 文档

| 方法 | 路径 | 说明 | 鉴权 |
|---|---|---|---|
| GET | `/api/health` | 健康检查 | ❌ |
| GET | `/api/stats` | 系统运行统计 | ❌ |
| GET | `/api/task-types` | 支持的任务类型 | ❌ |
| GET | `/api/error-log` | 错误日志 | ❌ |
| GET | `/api/tasks` | 列出所有任务 | ❌ |
| GET | `/api/tasks/:id` | 任务详情（含子步骤） | ❌ |
| POST | `/api/tasks` | 创建任务 | ✅ `X-API-Key` |
| DELETE | `/api/tasks/:id` | 删除任务 | ✅ `X-API-Key` |

### 创建任务示例

```bash
# HTTP 请求
curl -X POST http://localhost:8888/api/tasks \
  -H "Content-Type: application/json" \
  -H "X-API-Key: demo-secret-key" \
  -d '{"name":"健康检查","type":"http_call","payload":"{\"url\":\"http://localhost:8080/health\",\"method\":\"GET\"}","repeat_sec":60}'

# 购物车全链路（5 步：登录→查商品→加购→MySQL验证→API验证）
curl -X POST http://localhost:8888/api/tasks \
  -H "Content-Type: application/json" \
  -H "X-API-Key: demo-secret-key" \
  -d '{"name":"购物车检查","type":"cart_flow","payload":"{\"base_url\":\"http://localhost:8080\",\"product_id\":\"1\"}","max_retries":1,"timeout":20}'
```

---

## 🏗️ 项目结构

```
TaskScheduler/
├── cmd/server/main.go       ← 入口
├── api/                     ← HTTP 层
│   ├── router.go            │  路由
│   ├── handler.go           │  8 个端点
│   └── middleware.go         │  日志/CORS/鉴权
├── scheduler/               ← 调度引擎
│   ├── scheduler.go         │  轮询/分发/池健康检查
│   ├── store.go             │  接口定义
│   └── task.go              │  类型别名
├── worker/                  ← 执行层
│   ├── pool.go              │  Worker 协程池
│   ├── runner.go            │  8 种内置 runner
│   └── runner_test.go       │  单元测试
├── models/task.go           ← 数据模型
├── store/                   ← 存储实现
│   ├── memory.go            │  内存（200万写/秒）
│   ├── mysql.go             │  MySQL 持久化
│   ├── redis.go             │  Redis 缓存
│   └── memory_test.go       │  测试 + Benchmark
├── notify/                  ← 通知层
│   ├── feishu.go            │  飞书卡片
│   └── errorlog.go          │  错误日志 + 7天清理
├── web/                     ← 前端
│   ├── index.html           │  Dashboard
│   └── swagger.html         │  Swagger 中文 UI
├── docs/swagger.json        ← OpenAPI 规范
├── tasks.json               ← 任务配置（换项目改这个）
├── config/config.go         ← 配置
├── .env.example             ← 环境变量示例
├── git-safe-push.sh         ← 安全提交脚本
├── Dockerfile / docker-compose.yml
└── Makefile
```

---

## 🔧 技术选型

| 组件 | 选型 | 理由 |
|---|---|---|
| 语言 | Go 1.23 | goroutine + channel 原生并发 |
| HTTP 框架 | Gin | 高性能，中间件丰富 |
| 数据库驱动 | go-sql-driver/mysql | 标准 MySQL 驱动 |
| Redis 客户端 | go-redis/v9 | Pipeline/事务/SETNX |
| 存储模式 | 接口抽象 | 三实现一键切换 |
| 前端 | 原生 HTML/CSS/JS | 零框架依赖 |
| API 文档 | Swagger UI 5 (CDN) | 中文汉化 + 自动预授权 |
| 容器化 | Docker 多阶段构建 | Alpine 镜像 ~15MB |

---

## 🎯 核心功能

### 任务生命周期

```
pending → running → done
                  ↘ failed → retrying → pending (重试)
                  ↘ timeout
```

- 延迟执行 / 循环执行 / 优先级调度
- 失败重试（可配次数和间隔）
- 超时控制（context.WithTimeout）
- 优雅退出（signal → HTTP → Worker → Store）

### 8 种内置 Runner

| Runner | 步骤 | 覆盖 API |
|---|---|---|
| `http_call` | 1 | 任意 HTTP 端点 |
| `data_clean` | 1 | 内部清理 |
| `flash_warmup` | 3 | 登录→预热→Redis验证 |
| `cart_flow` | 5 | 登录→商品→加购→MySQL→API |
| `order_flow` | 5 | 登录→商品→下单→MySQL→API |
| `user_flow` | 3 | 注册→登录→查信息 |
| `admin_crud` | 5 | 登录→创建→MySQL→修改→列表 |
| `flash_full_check` | 6 | 登录→创建→预热→Redis→MySQL→API |

### 子步骤追踪

每个 runner 内部步骤独立计时和状态，展开任务一眼定位哪步挂了。

### 告警通知

- 飞书卡片：任务失败推送完整诊断报告
- 错误日志：`logs/error.log` JSON 行格式，7 天自动清理
- Worker 池满告警：50%/90% 两级

### Dashboard ↔ Swagger 联动

Swagger 创建任务 → 浮窗实时同步 → 点击跳转 Dashboard 自动展开

---

## 🧪 测试

```bash
go test -v ./store/ ./worker/     # 8 项全部 PASS
go test -bench=. -benchmem ./store/  # Benchmark
```

Benchmark 数据：MemoryStore 写入 **545 ns/op** (~200 万次/秒)，读取 **62 ns/op** (~1900 万次/秒)

---

## 🔄 复用性

换项目监控只需改 `tasks.json`：

```json
{
  "tasks": [
    {"name": "新项目健康检查", "type": "http_call", "payload": "{\"url\":\"http://新地址/health\",\"method\":\"GET\"}", "repeat_sec": 60}
  ]
}
```

环境变量配置数据库：`MYSQL_DSN` / `REDIS_ADDR` / `FEISHU_WEBHOOK`

---

## 📝 许可证

MIT License
