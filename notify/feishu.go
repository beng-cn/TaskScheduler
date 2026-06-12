// Package notify 提供飞书机器人推送能力。
// 当任务执行失败时，通过 Webhook 发送卡片消息到飞书群。
package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"task-scheduler/models"
	"time"
)

// feishuCard 是飞书消息卡片的 JSON 结构。
type feishuCard struct {
	MsgType string `json:"msg_type"`
	Card    struct {
		Header struct {
			Title    struct {
				Tag     string `json:"tag"`
				Content string `json:"content"`
			} `json:"title"`
			Template string `json:"template"`
		} `json:"header"`
		Elements []struct {
			Tag  string `json:"tag"`
			Text struct {
				Tag     string `json:"tag"`
				Content string `json:"content"`
			} `json:"text"`
		} `json:"elements"`
	} `json:"card"`
}

var webhookURL string

// SetWebhook 设置飞书机器人的 Webhook 地址。
func SetWebhook(url string) {
	webhookURL = url
}

// SendTaskAlert 发送任务失败告警到飞书。
// 内容包含"告警"关键词，满足机器人安全设置要求。
func SendTaskAlert(task *models.Task) error {
	if webhookURL == "" {
		return fmt.Errorf("feishu: webhook 未配置")
	}

	statusText := map[models.TaskStatus]string{
		models.StatusFailed:  "❌ 失败",
		models.StatusTimeout: "⏰ 超时",
		models.StatusRetrying: "🔄 重试中",
	}
	status := statusText[task.Status]
	if status == "" {
		status = string(task.Status)
	}

	// 构造飞书卡片消息
	card := feishuCard{MsgType: "interactive"}
	card.Card.Header.Title.Tag = "plain_text"
	card.Card.Header.Title.Content = "⚠️ TaskScheduler 告警 — " + task.Name
	card.Card.Header.Template = "red"

	content := fmt.Sprintf(
		"**任务名称**：%s\n**任务类型**：%s\n**当前状态**：%s\n**重试次数**：%d/%d\n**错误信息**：%s\n**发生时间**：%s",
		task.Name, task.Type, status,
		task.Retries, task.MaxRetries,
		task.Error,
		time.Now().Format("2006-01-02 15:04:05"),
	)

	elem := struct {
		Tag  string `json:"tag"`
		Text struct {
			Tag     string `json:"tag"`
			Content string `json:"content"`
		} `json:"text"`
	}{}
	elem.Tag = "div"
	elem.Text.Tag = "lark_md"
	elem.Text.Content = content
	card.Card.Elements = []struct {
		Tag  string `json:"tag"`
		Text struct {
			Tag     string `json:"tag"`
			Content string `json:"content"`
		} `json:"text"`
	}{elem}

	body, _ := json.Marshal(card)
	resp, err := http.Post(webhookURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("feishu: 发送失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("feishu: 返回 %d", resp.StatusCode)
	}
	return nil
}
