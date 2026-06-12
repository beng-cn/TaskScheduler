package store

import (
	"context"
	"sync"
	"task-scheduler/models"
	"testing"
	"time"
)

func TestMemoryStore_CRUD(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	// 创建
	task := &models.Task{
		Name:   "测试任务",
		Type:   "http_call",
		Status: models.StatusPending,
	}
	if err := s.CreateTask(ctx, task); err != nil {
		t.Fatalf("创建任务失败: %v", err)
	}
	if task.ID == "" {
		t.Fatal("ID 未生成")
	}
	t.Logf("创建成功: %s", task.ID)

	// 读取
	got, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("读取任务失败: %v", err)
	}
	if got.Name != task.Name {
		t.Errorf("名称不匹配: got %s, want %s", got.Name, task.Name)
	}

	// 更新
	got.Status = models.StatusDone
	got.Result = "成功"
	if err := s.UpdateTask(ctx, got); err != nil {
		t.Fatalf("更新任务失败: %v", err)
	}
	updated, _ := s.GetTask(ctx, task.ID)
	if updated.Status != models.StatusDone {
		t.Errorf("状态更新失败: got %s, want %s", updated.Status, models.StatusDone)
	}

	// 列表
	tasks, err := s.ListTasks(ctx)
	if err != nil {
		t.Fatalf("列表失败: %v", err)
	}
	if len(tasks) != 1 {
		t.Errorf("列表数量不对: got %d, want 1", len(tasks))
	}

	// 删除
	if err := s.DeleteTask(ctx, task.ID); err != nil {
		t.Fatalf("删除失败: %v", err)
	}
	tasks, _ = s.ListTasks(ctx)
	if len(tasks) != 0 {
		t.Errorf("删除后列表应为空: got %d", len(tasks))
	}
}

func TestMemoryStore_ListPendingTasks(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	// 创建待执行任务
	t1 := &models.Task{Name: "立即执行", Type: "echo", Status: models.StatusPending}
	t2 := &models.Task{Name: "延迟执行", Type: "echo", Status: models.StatusPending,
		ScheduledAt: time.Now().Add(1 * time.Hour)} // 一小时后
	t3 := &models.Task{Name: "已完成", Type: "echo", Status: models.StatusDone}

	s.CreateTask(ctx, t1)
	s.CreateTask(ctx, t2)
	s.CreateTask(ctx, t3)

	pending, err := s.ListPendingTasks(ctx)
	if err != nil {
		t.Fatalf("查询待执行失败: %v", err)
	}
	// t1 应该立即返回，t2 计划时间未到，t3 已完成
	if len(pending) != 1 {
		t.Errorf("待执行任务数不对: got %d, want 1", len(pending))
	}
	if pending[0].Name != "立即执行" {
		t.Errorf("待执行的任务不对: got %s, want 立即执行", pending[0].Name)
	}
}

func TestMemoryStore_ConcurrentSafety(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	var wg sync.WaitGroup

	// 并发写入 100 个任务
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			task := &models.Task{
				Name:   "并发任务",
				Type:   "echo",
				Status: models.StatusPending,
			}
			if err := s.CreateTask(ctx, task); err != nil {
				t.Errorf("并发创建失败: %v", err)
			}
		}(i)
	}
	wg.Wait()

	tasks, _ := s.ListTasks(ctx)
	if len(tasks) != 100 {
		t.Errorf("并发写入后任务数不对: got %d, want 100", len(tasks))
	}

	// 验证 ID 无重复
	ids := make(map[string]bool)
	for _, task := range tasks {
		if ids[task.ID] {
			t.Errorf("发现重复 ID: %s", task.ID)
		}
		ids[task.ID] = true
	}
	t.Logf("并发安全测试通过: 100 个任务, ID 全部唯一")
}

func TestMemoryStore_Lock(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	// 获取锁
	ok, err := s.TryLock(ctx, "test-lock", 10)
	if err != nil || !ok {
		t.Fatalf("获取锁失败")
	}

	// 重复获取应失败
	ok, _ = s.TryLock(ctx, "test-lock", 10)
	if ok {
		t.Fatal("同一锁不应被重复获取")
	}

	// 释放后可以重新获取
	s.Unlock(ctx, "test-lock")
	ok, _ = s.TryLock(ctx, "test-lock", 10)
	if !ok {
		t.Fatal("释放后应能重新获取")
	}
}

// 基准测试
func BenchmarkMemoryStore_CreateTask(b *testing.B) {
	s := NewMemoryStore()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		task := &models.Task{Name: "bench", Type: "echo", Status: models.StatusPending}
		s.CreateTask(ctx, task)
	}
}

func BenchmarkMemoryStore_GetTask(b *testing.B) {
	s := NewMemoryStore()
	ctx := context.Background()
	task := &models.Task{Name: "bench", Type: "echo", Status: models.StatusPending}
	s.CreateTask(ctx, task)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.GetTask(ctx, task.ID)
	}
}
