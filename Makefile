# TaskScheduler Makefile
# 提供开发、构建、测试、运行的常用命令

.PHONY: run build test clean dev help

# 默认目标：显示帮助
help:
	@echo "TaskScheduler — 可用命令:"
	@echo ""
	@echo "  make run      启动服务端 (go run)"
	@echo "  make build    编译可执行文件到 bin/ 目录"
	@echo "  make test     运行所有单元测试"
	@echo "  make clean    清理编译产物"
	@echo "  make dev      开发模式运行（带详细日志）"
	@echo "  make docker   使用 Docker Compose 启动"

# 启动服务端
run:
	go run cmd/server/main.go

# 编译
build:
	@mkdir -p bin
	go build -o bin/scheduler cmd/server/main.go
	go build -o bin/scheduler-cli cmd/client/main.go
	@echo "编译完成: bin/scheduler, bin/scheduler-cli"

# 运行测试
test:
	go test -v -race -cover ./...

# 运行基准测试
bench:
	go test -bench=. -benchmem ./...

# 清理
clean:
	rm -rf bin/

# 开发模式（带竞态检测）
dev:
	go run -race cmd/server/main.go

# Docker Compose 启动
docker:
	docker-compose up --build

# 安装依赖
deps:
	go mod tidy
	go mod download

# 代码检查
lint:
	go vet ./...
