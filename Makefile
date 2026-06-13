# 哨兵 Sentinel Makefile
# https://github.com/beng-cn/TaskScheduler

.PHONY: run build test bench clean dev docker lint help

# 默认目标
help:
	@echo "哨兵 Sentinel — 可用命令:"
	@echo ""
	@echo "  make run       启动服务端 (内存模式)"
	@echo "  make run-mysql 启动服务端 (MySQL 持久化)"
	@echo "  make build     编译到 bin/ 目录"
	@echo "  make test      运行全部单元测试"
	@echo "  make bench     运行基准测试"
	@echo "  make vet       代码检查"
	@echo "  make clean     清理编译产物"
	@echo "  make docker    使用 Docker Compose 启动"
	@echo "  make push      安全提交到 GitHub (自动脱敏)"

# 内存模式启动
run:
	go run cmd/server/main.go

# MySQL 模式启动
run-mysql:
	go run cmd/server/main.go -config config.json

# 编译
build:
	@mkdir -p bin
	go build -ldflags="-s -w" -o bin/scheduler.exe cmd/server/main.go
	go build -ldflags="-s -w" -o bin/scheduler-cli.exe cmd/client/main.go
	@echo "编译完成: bin/scheduler.exe, bin/scheduler-cli.exe"

# 测试
test:
	go test -v -count=1 -race ./...

# 基准测试
bench:
	go test -bench=. -benchmem ./store/

# 代码检查
vet:
	go vet ./...

# 清理
clean:
	rm -rf bin/

# 开发模式（竞态检测）
dev:
	go run -race cmd/server/main.go

# Docker
docker:
	docker-compose up --build

# 安装依赖
deps:
	go mod tidy
	go mod download

# 安全提交到 GitHub
push:
	bash git-safe-push.sh "update"
