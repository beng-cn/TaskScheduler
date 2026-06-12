// Package notify 提供错误日志文件记录功能。
// 失败任务自动写入日志文件，超过 7 天的记录自动清理。
package notify

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"task-scheduler/models"
	"time"
)

// ErrorEntry 是一条错误日志记录。
type ErrorEntry struct {
	Time      string `json:"time"`       // 发生时间
	TaskID    string `json:"task_id"`    // 任务 ID
	TaskName  string `json:"task_name"`  // 任务名称
	TaskType  string `json:"task_type"`  // 任务类型
	Status    string `json:"status"`     // 最终状态
	Retries   int    `json:"retries"`    // 重试次数
	Error     string `json:"error"`      // 错误信息
}

var (
	errorLogPath string
	logMu        sync.Mutex
)

// InitErrorLog 初始化错误日志文件路径。
func InitErrorLog(path string) {
	errorLogPath = path
	// 确保目录存在
	if dir := filepathDir(path); dir != "" {
		os.MkdirAll(dir, 0755)
	}
	// 启动时清理超过 7 天的记录
	CleanOldEntries()
}

func filepathDir(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' || p[i] == '\\' {
			return p[:i]
		}
	}
	return ""
}

// LogTaskError 记录一个失败任务到错误日志文件。
func LogTaskError(task *models.Task) {
	if errorLogPath == "" {
		return
	}

	entry := ErrorEntry{
		Time:     time.Now().Format("2006-01-02 15:04:05"),
		TaskID:   task.ID,
		TaskName: task.Name,
		TaskType: task.Type,
		Status:   string(task.Status),
		Retries:  task.Retries,
		Error:    task.Error,
	}

	logMu.Lock()
	defer logMu.Unlock()

	f, err := os.OpenFile(errorLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[ErrorLog] 打开日志文件失败: %v", err)
		return
	}
	defer f.Close()

	data, _ := json.Marshal(entry)
	f.Write(append(data, '\n'))
}

// CleanOldEntries 删除超过 7 天的错误日志条目。
// 由于日志是追加写入的（按时间顺序），逐行检查并重写即可。
func CleanOldEntries() {
	if errorLogPath == "" {
		return
	}

	logMu.Lock()
	defer logMu.Unlock()

	data, err := os.ReadFile(errorLogPath)
	if err != nil {
		return // 文件不存在，跳过
	}

	cutoff := time.Now().Add(-7 * 24 * time.Hour)
	lines := 0
	kept := 0

	// 逐行解析，只保留 7 天内的记录
	f, err := os.Create(errorLogPath + ".tmp")
	if err != nil {
		return
	}
	defer f.Close()

	for len(data) > 0 {
		idx := 0
		for i, b := range data {
			if b == '\n' {
				idx = i
				break
			}
		}
		if idx == 0 {
			break
		}
		line := data[:idx]
		data = data[idx+1:]
		lines++

		var entry ErrorEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		t, err := time.Parse("2006-01-02 15:04:05", entry.Time)
		if err != nil || t.Before(cutoff) {
			continue // 过期，跳过
		}
		f.Write(append(line, '\n'))
		kept++
	}

	os.Rename(errorLogPath+".tmp", errorLogPath)
	if lines > kept {
		log.Printf("[ErrorLog] 清理了 %d 条过期错误记录（保留 %d 条）", lines-kept, kept)
	}
}

// GetErrorLogPath 返回错误日志文件路径。
func GetErrorLogPath() string {
	return errorLogPath
}

// ReadErrorLog 读取全部错误日志条目。
func ReadErrorLog() ([]ErrorEntry, error) {
	if errorLogPath == "" {
		return []ErrorEntry{}, nil
	}

	logMu.Lock()
	defer logMu.Unlock()

	data, err := os.ReadFile(errorLogPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []ErrorEntry{}, nil
		}
		return nil, err
	}

	var entries []ErrorEntry
	for len(data) > 0 {
		idx := 0
		for i, b := range data {
			if b == '\n' {
				idx = i
				break
			}
		}
		if idx == 0 {
			break
		}
		var entry ErrorEntry
		if err := json.Unmarshal(data[:idx], &entry); err == nil {
			entries = append(entries, entry)
		}
		data = data[idx+1:]
	}
	return entries, nil
}
