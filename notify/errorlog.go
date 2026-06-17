// Package notify 提供错误日志文件记录功能。
// 失败任务自动写入日志文件，超过 7 天的记录自动清理。
package notify

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"sync"
	"task-scheduler/models"
	"time"
)

// ErrorEntry 是一条错误日志记录。
type ErrorEntry struct {
	Time     string `json:"time"`      // 发生时间
	TaskID   string `json:"task_id"`   // 任务 ID
	TaskName string `json:"task_name"` // 任务名称
	TaskType string `json:"task_type"` // 任务类型
	Status   string `json:"status"`    // 最终状态
	Retries  int    `json:"retries"`   // 重试次数
	Error    string `json:"error"`     // 错误信息
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
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Printf("[ErrorLog] 创建日志目录失败: %v", err)
		}
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

	data, err := json.Marshal(entry)
	if err != nil {
		log.Printf("[ErrorLog] 序列化日志条目失败: %v", err)
		return
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		log.Printf("[ErrorLog] 写入日志失败: %v", err)
	}
}

// CleanOldEntries 删除超过 7 天的错误日志条目。
// 使用 bufio.Scanner 替代手写行解析，避免末行丢失和首行跳过问题。
// 修复：写临时文件失败时不会覆盖原文件。
func CleanOldEntries() {
	if errorLogPath == "" {
		return
	}

	logMu.Lock()
	defer logMu.Unlock()

	// 检查文件是否存在
	info, err := os.Stat(errorLogPath)
	if err != nil {
		return // 文件不存在，跳过
	}
	if info.Size() == 0 {
		return
	}

	cutoff := time.Now().Add(-7 * 24 * time.Hour)

	// 先读原文件
	inFile, err := os.Open(errorLogPath)
	if err != nil {
		return
	}
	defer inFile.Close()

	// 写临时文件
	tmpPath := errorLogPath + ".tmp"
	outFile, err := os.Create(tmpPath)
	if err != nil {
		return
	}

	scanner := bufio.NewScanner(inFile)
	lines := 0
	kept := 0
	writeFailed := false

	for scanner.Scan() {
		line := scanner.Bytes()
		lines++

		var entry ErrorEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			continue // 跳过非法行
		}
		t, err := time.Parse("2006-01-02 15:04:05", entry.Time)
		if err != nil || t.Before(cutoff) {
			continue // 过期，跳过
		}
		if _, err := outFile.Write(append(line, '\n')); err != nil {
			// 修复：写失败时标记失败，放弃临时文件，保留原文件不动
			log.Printf("[ErrorLog] 写入临时文件失败: %v", err)
			writeFailed = true
			break
		}
		kept++
	}

	// 修复：确保 Close 错误也被检查（数据落盘）
	if !writeFailed {
		if err := outFile.Close(); err != nil {
			log.Printf("[ErrorLog] 关闭临时文件失败: %v", err)
			writeFailed = true
		}
	} else {
		outFile.Close()
	}

	if err := scanner.Err(); err != nil {
		log.Printf("[ErrorLog] 扫描日志文件出错: %v", err)
		writeFailed = true
	}

	if writeFailed {
		os.Remove(tmpPath)
		return
	}

	// 修复：检查 Rename 错误
	if err := os.Rename(tmpPath, errorLogPath); err != nil {
		log.Printf("[ErrorLog] 重命名临时文件失败: %v", err)
		os.Remove(tmpPath)
		return
	}

	if lines > kept {
		log.Printf("[ErrorLog] 清理了 %d 条过期错误记录（保留 %d 条）", lines-kept, kept)
	}
}

// GetErrorLogPath 返回错误日志文件路径。
func GetErrorLogPath() string {
	return errorLogPath
}

// ReadErrorLog 读取全部错误日志条目。
// 使用 bufio.Scanner 替代手写行解析。
func ReadErrorLog() ([]ErrorEntry, error) {
	if errorLogPath == "" {
		return []ErrorEntry{}, nil
	}

	logMu.Lock()
	defer logMu.Unlock()

	file, err := os.Open(errorLogPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []ErrorEntry{}, nil
		}
		return nil, err
	}
	defer file.Close()

	var entries []ErrorEntry
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry ErrorEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err == nil {
			entries = append(entries, entry)
		}
	}
	return entries, scanner.Err()
}
