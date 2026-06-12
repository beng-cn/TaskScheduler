package worker

import (
	"fmt"
	"task-scheduler/models"
	"testing"
)

func TestRecordStep_Done(t *testing.T) {
	task := &models.Task{Name: "测试"}

	RecordStep(task, "步骤1-成功", func() (string, error) {
		return "完成", nil
	})

	if len(task.Steps) != 1 {
		t.Fatalf("步骤数不对: got %d, want 1", len(task.Steps))
	}
	s := task.Steps[0]
	if s.Status != "done" {
		t.Errorf("状态不对: got %s, want done", s.Status)
	}
	if s.Result != "完成" {
		t.Errorf("结果不对: got %s, want 完成", s.Result)
	}
	// DurationMs 可能为 0（执行太快，亚毫秒级）
	_ = s.DurationMs
	if s.Error != "" {
		t.Errorf("错误应为空: got %s", s.Error)
	}
	t.Logf("步骤1: %s | %s | %dms", s.Status, s.Result, s.DurationMs)
}

func TestRecordStep_Fail(t *testing.T) {
	task := &models.Task{Name: "测试"}

	RecordStep(task, "步骤2-失败", func() (string, error) {
		return "", fmt.Errorf("模拟错误")
	})

	s := task.Steps[0]
	if s.Status != "failed" {
		t.Errorf("状态不对: got %s, want failed", s.Status)
	}
	if s.Error == "" {
		t.Error("错误信息不应为空")
	}
	t.Logf("步骤2: %s | %s | %dms", s.Status, s.Error, s.DurationMs)
}

func TestRecordStep_MultipleSteps(t *testing.T) {
	task := &models.Task{Name: "测试"}

	RecordStep(task, "登录", func() (string, error) { return "ok", nil })
	RecordStep(task, "查商品", func() (string, error) { return "", fmt.Errorf("连接超时") })
	RecordStep(task, "加购物车", func() (string, error) { return "skipped", nil })

	if len(task.Steps) != 3 {
		t.Fatalf("步骤数不对: got %d, want 3", len(task.Steps))
	}

	statuses := []string{"done", "failed", "done"}
	for i, s := range task.Steps {
		if s.Status != statuses[i] {
			t.Errorf("步骤%d状态: got %s, want %s", i+1, s.Status, statuses[i])
		}
		t.Logf("  步骤%d: %s | %s | %dms", i+1, s.Status, s.Name, s.DurationMs)
	}
}

func TestGetRunner(t *testing.T) {
	runners := []string{"http_call", "data_clean", "flash_warmup", "cart_flow", "flash_full_check"}
	for _, name := range runners {
		if GetRunner(name) == nil {
			t.Errorf("runner %s 未注册", name)
		}
	}
	if GetRunner("nonexistent") != nil {
		t.Error("不存在的 runner 应返回 nil")
	}
	t.Logf("已注册 %d 个 runner", len(builtinRunners))
}

func TestRecordStep_EmptyStepsOnNewTask(t *testing.T) {
	task := &models.Task{Name: "空步骤测试"}
	if task.Steps != nil {
		t.Error("新任务的 Steps 应为 nil")
	}
	t.Log("新任务 Steps 初始化为 nil ✅")
}
