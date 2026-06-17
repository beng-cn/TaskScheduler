// Package notify 提供飞书机器人推送能力。
// 当任务执行失败时，通过 Webhook 发送卡片消息到飞书群。
package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"task-scheduler/models"
	"time"
)

var webhookURL string

// 修复：使用带超时的 HTTP 客户端
var feishuHTTP = &http.Client{Timeout: 10 * time.Second}

// SetWebhook 设置飞书机器人的 Webhook 地址。
func SetWebhook(url string) {
	webhookURL = url
}

// SendTaskAlert 发送任务失败告警到飞书，包含完整诊断报告。
func SendTaskAlert(task *models.Task) error {
	if webhookURL == "" {
		return fmt.Errorf("feishu: webhook 未配置")
	}

	statusText := map[models.TaskStatus]string{
		models.StatusFailed:   "❌ 失败",
		models.StatusTimeout:  "⏰ 超时",
		models.StatusRetrying: "🔄 重试中",
	}
	status := statusText[task.Status]
	if status == "" {
		status = string(task.Status)
	}

	// 构造诊断报告
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("**任务名称**：%s\n", task.Name))
	sb.WriteString(fmt.Sprintf("**任务类型**：%s\n", task.Type))
	sb.WriteString(fmt.Sprintf("**当前状态**：%s\n", status))
	sb.WriteString(fmt.Sprintf("**重试次数**：%d/%d\n", task.Retries, task.MaxRetries))
	if task.Error != "" {
		sb.WriteString(fmt.Sprintf("**错误信息**：%s\n", task.Error))
	}
	sb.WriteString(fmt.Sprintf("**创建时间**：%s\n", task.CreatedAt.Format("15:04:05")))
	if task.StartedAt != nil {
		sb.WriteString(fmt.Sprintf("**开始执行**：%s\n", task.StartedAt.Format("15:04:05")))
	}
	if task.FinishedAt != nil {
		sb.WriteString(fmt.Sprintf("**完成时间**：%s\n", task.FinishedAt.Format("15:04:05")))
	}

	// 子步骤详情
	if len(task.Steps) > 0 {
		sb.WriteString("\n**子步骤详情：**\n")
		for _, s := range task.Steps {
			icon := "✅"
			if s.Status == "failed" {
				icon = "❌"
			} else if s.Status == "skipped" {
				icon = "⏭"
			}
			detail := s.Result
			if s.Error != "" {
				detail = s.Error
			}
			// 修复：按 rune 截断，避免拆分多字节 UTF-8 字符
			if len([]rune(detail)) > 100 {
				runes := []rune(detail)
				detail = string(runes[:100]) + "..."
			}
			sb.WriteString(fmt.Sprintf("%s %s（%dms）: %s\n", icon, s.Name, s.DurationMs, detail))
		}
	}

	// Payload — 修复：不修改原始 task.Payload（避免副作用），使用局部副本
	payload := task.Payload
	if len([]rune(payload)) > 300 {
		runes := []rune(payload)
		payload = string(runes[:300]) + "..."
	}
	sb.WriteString(fmt.Sprintf("\n**Payload**：%s\n", payload))
	sb.WriteString(fmt.Sprintf("\n📋 完整报告: http://localhost:8888/?task=%s", task.ID))

	// 发送卡片
	card := map[string]interface{}{
		"msg_type": "interactive",
		"card": map[string]interface{}{
			"header": map[string]interface{}{
				"title":    map[string]string{"tag": "plain_text", "content": "⚠️ 告警 — " + task.Name},
				"template": "red",
			},
			"elements": []map[string]interface{}{
				{
					"tag": "div",
					"text": map[string]string{
						"tag":     "lark_md",
						"content": sb.String(),
					},
				},
			},
		},
	}
	body, err := json.Marshal(card)
	if err != nil {
		return fmt.Errorf("feishu: 序列化卡片失败: %w", err)
	}
	// 修复：使用带超时的 HTTP 客户端
	resp, err := feishuHTTP.Post(webhookURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("feishu: 发送失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("feishu: 返回 %d", resp.StatusCode)
	}
	return nil
}
