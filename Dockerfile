# 多阶段构建：编译阶段使用完整 Go 镜像，运行阶段使用 Alpine 精简镜像
# 最终镜像大小约 15MB

# ——— 编译阶段 ———
FROM golang:1.23-alpine AS builder

# 设置 Go 代理（国内用户可加速）
ENV GOPROXY=https://goproxy.cn,direct

WORKDIR /app

# 先复制依赖文件，利用 Docker 层缓存
COPY go.mod go.sum ./
RUN go mod download

# 复制源码并编译
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o scheduler cmd/server/main.go

# ——— 运行阶段 ———
FROM alpine:3.20

# 安装 ca 证书（HTTPS 请求需要）
RUN apk --no-cache add ca-certificates tzdata
ENV TZ=Asia/Shanghai

WORKDIR /app

# 从编译阶段复制二进制文件
COPY --from=builder /app/scheduler .
COPY --from=builder /app/web ./web
COPY --from=builder /app/tasks.json .

# 暴露 HTTP 端口
EXPOSE 8888

# 健康检查
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD wget -qO- http://localhost:8888/api/health || exit 1

ENTRYPOINT ["./scheduler"]
