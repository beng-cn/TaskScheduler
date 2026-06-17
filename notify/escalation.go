package notify

import (
	"log"
	"sync"
)

// AlertLevel 告警级别
type AlertLevel int

const (
	LevelWarning  AlertLevel = 1 // 首次失败，飞书通知
	LevelCritical AlertLevel = 2 // 连续失败3次，加急
	LevelUrgent   AlertLevel = 3 // 连续失败5次，紧急
)

var (
	mu         sync.Mutex
	failCounts = make(map[string]int) // taskName → 连续失败次数（互斥锁保护，消除 data race）
)

// RecordFailure 记录一次失败，返回当前告警级别。
func RecordFailure(taskName string) AlertLevel {
	mu.Lock()
	failCounts[taskName]++
	n := failCounts[taskName]
	mu.Unlock()
	switch {
	case n >= 5:
		return LevelUrgent
	case n >= 3:
		return LevelCritical
	default:
		return LevelWarning
	}
}

// ResetEscalation 任务成功后重置告警计数。
func ResetEscalation(taskName string) {
	mu.Lock()
	delete(failCounts, taskName)
	mu.Unlock()
}

// GetFailCount 获取连续失败次数。
func GetFailCount(taskName string) int {
	mu.Lock()
	defer mu.Unlock()
	return failCounts[taskName]
}

// LogEscalation 记录告警升级日志。
func LogEscalation(taskName string, level AlertLevel) {
	switch level {
	case LevelCritical:
		log.Printf("[告警升级] %s 连续失败达到阈值，升级为严重告警", taskName)
	case LevelUrgent:
		log.Printf("[告警升级] %s 连续失败达到阈值，升级为紧急告警！", taskName)
	default:
		// LevelWarning 不单独输出升级日志
	}
}
