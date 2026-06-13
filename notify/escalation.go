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
	failCounts sync.Map                                    // taskName → consecutive fails
	escalationFuncs = map[AlertLevel]func(string){}        // 不同级别的回调
)

// RecordFailure 记录一次失败，返回当前告警级别。
func RecordFailure(taskName string) AlertLevel {
	val, _ := failCounts.LoadOrStore(taskName, new(int))
	count := val.(*int)
	*count++
	n := *count
	switch {
	case n >= 5: return LevelUrgent
	case n >= 3: return LevelCritical
	default: return LevelWarning
	}
}

// ResetEscalation 任务成功后重置告警计数。
func ResetEscalation(taskName string) {
	failCounts.Delete(taskName)
}

// GetFailCount 获取连续失败次数。
func GetFailCount(taskName string) int {
	if v, ok := failCounts.Load(taskName); ok {
		return *(v.(*int))
	}
	return 0
}

// LogEscalation 记录告警升级日志。
func LogEscalation(taskName string, count int) {
	if count == 3 {
		log.Printf("[告警升级] %s 连续失败 %d 次，升级为严重告警", taskName, count)
	} else if count == 5 {
		log.Printf("[告警升级] %s 连续失败 %d 次，升级为紧急告警！", taskName, count)
	}
}
