// Package config 提供应用配置的加载和管理功能。
// 支持从配置文件和环境变量中读取配置，默认值确保开箱即用。
package config

import (
	"encoding/json"
	"os"
	"time"
)

// Config 是应用的全部配置项。
type Config struct {
	// Server 配置 HTTP 服务相关参数
	Server ServerConfig `json:"server"`
	// Scheduler 配置调度引擎相关参数
	Scheduler SchedulerConfig `json:"scheduler"`
	// Worker 配置 Worker 池相关参数
	Worker WorkerConfig `json:"worker"`
	// Store 配置存储后端
	Store StoreConfig `json:"store"`
	// Log 配置日志级别
	Log LogConfig `json:"log"`
}

// ServerConfig HTTP 服务配置
type ServerConfig struct {
	Host string `json:"host"` // 监听地址
	Port int    `json:"port"` // 监听端口
}

// SchedulerConfig 调度引擎配置
type SchedulerConfig struct {
	PollInterval   time.Duration `json:"poll_interval"`   // 轮询新任务的时间间隔
	MaxRetries     int           `json:"max_retries"`     // 单任务最大重试次数（可由任务级覆盖）
	DefaultTimeout time.Duration `json:"default_timeout"` // 任务默认超时时间
}

// WorkerConfig Worker 池配置
type WorkerConfig struct {
	Count      int `json:"count"`       // Worker 数量
	QueueSize  int `json:"queue_size"`  // 任务队列缓冲大小
	MaxWorkers int `json:"max_workers"` // 最大 Worker 数（动态扩容上限）
}

// StoreConfig 存储配置
type StoreConfig struct {
	Type string `json:"type"` // 存储类型：memory / mysql / redis
	DSN  string `json:"dsn"`  // 数据源名称（MySQL/Redis 连接串）
}

// LogConfig 日志配置
type LogConfig struct {
	Level string `json:"level"` // debug / info / warn / error
}

// DefaultConfig 返回一份可用于开箱即运行的默认配置。
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Host: "0.0.0.0",
			Port: 8888,
		},
		Scheduler: SchedulerConfig{
			PollInterval:   500 * time.Millisecond,
			MaxRetries:     3,
			DefaultTimeout: 30 * time.Second,
		},
		Worker: WorkerConfig{
			Count:      10,
			QueueSize:  1024,
			MaxWorkers: 50,
		},
		Store: StoreConfig{
			Type: "memory",
		},
		Log: LogConfig{
			Level: "info",
		},
	}
}

// LoadFromFile 从 JSON 文件加载配置。文件不存在时返回默认配置。
func LoadFromFile(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil // 配置文件不存在，使用默认值
		}
		return nil, err
	}

	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
