package batch

import (
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// TestRestartRecovery 模拟进程异常退出：第一个 Runtime 在不调用 Close 的
// 情况下被放弃（状态文件保留崩溃瞬间的快照），随后在同一状态文件上创建
// 第二个 Runtime，验证：
//  1. 已完成作业不被重复执行；
//  2. 未完成（pending/running）作业被恢复并继续执行；
//  3. 重复提交已存在的作业 ID 被拒绝。
func TestRestartRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	block := make(chan struct{})
	var doneCalls atomic.Int32

	// 第一次启动：done 迅速完成；blocker 永远阻塞占住唯一并发槽；
	// stuck 因此一直停留在 pending 队列中。
	rt1, err := New(Config{MaxConcurrency: 1, MaxQueue: 8, StatePath: path},
		func(ctx Context, job Job) error {
			switch job.ID {
			case "done":
				doneCalls.Add(1)
				return nil
			case "blocker":
				<-block
			}
			return nil
		})
	if err != nil {
		t.Fatalf("第一次 New 失败: %v", err)
	}
	t.Logf("输入: 提交 done(可完成), blocker(永久阻塞), stuck(排队)")
	rt1.Submit(Job{ID: "done"})
	rt1.Submit(Job{ID: "blocker"})
	rt1.Submit(Job{ID: "stuck"})
	waitFor(t, "done 完成且 blocker 运行中", func() bool {
		d, _ := rt1.Status("done")
		b, _ := rt1.Status("blocker")
		return d.Status == StatusSucceeded && b.Status == StatusRunning
	})
	t.Logf("状态转换: done pending->running->succeeded 已落盘; blocker=running; stuck=pending")
	t.Logf("模拟进程异常退出: 放弃 rt1 (不 Close), 状态文件保留崩溃快照")

	// 模拟重启：新的 Runtime 从同一状态文件恢复。
	var stuckCalls, blockerCalls atomic.Int32
	rt2, err := New(Config{MaxConcurrency: 2, MaxQueue: 8, StatePath: path},
		func(ctx Context, job Job) error {
			switch job.ID {
			case "done":
				doneCalls.Add(1)
			case "stuck":
				stuckCalls.Add(1)
			case "blocker":
				blockerCalls.Add(1)
			}
			return nil
		})
	if err != nil {
		t.Fatalf("恢复 New 失败: %v", err)
	}

	waitFor(t, "stuck 与 blocker 恢复后执行完成", func() bool {
		s, _ := rt2.Status("stuck")
		b, _ := rt2.Status("blocker")
		return s.Status == StatusSucceeded && b.Status == StatusSucceeded
	})
	rec, _ := rt2.Status("done")
	if rec.Status != StatusSucceeded {
		t.Fatalf("done 恢复后状态=%s, 期望保持 succeeded", rec.Status)
	}
	if got := doneCalls.Load(); got != 1 {
		t.Fatalf("done 共执行 %d 次, 期望恰好 1 次 (恢复不重复执行已完成工作)", got)
	}
	if got := stuckCalls.Load(); got != 1 {
		t.Fatalf("stuck 恢复后执行 %d 次, 期望恰好 1 次", got)
	}
	t.Logf("判定依据: done 执行总次数=%d (不重复), stuck 恢复重放次数=%d (不丢失)",
		doneCalls.Load(), stuckCalls.Load())

	t.Logf("输入: 重启后重复提交已完成作业 ID done")
	if err := rt2.Submit(Job{ID: "done"}); !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("期望 ErrDuplicateJob, 实际 %v", err)
	}
	t.Logf("判定依据: 重复触发被拒绝, 不会重复执行已完成工作")

	results := rt2.Results()
	if len(results) != 3 {
		t.Fatalf("终态汇总=%d 条, 期望 3 条", len(results))
	}
	for _, r := range results {
		if r.Status != StatusSucceeded {
			t.Fatalf("作业 %s 最终状态=%s, 期望 succeeded", r.JobID, r.Status)
		}
	}
	t.Logf("状态转换: stuck/blocker pending(恢复重排)->running->succeeded; 汇总与实际执行一致")

	// 收尾：先关闭 rt2 落盘最终状态，再放行并关闭 rt1，避免泄漏 goroutine。
	rt2.Close()
	close(block)
	rt1.Close()
}

// TestRecoveryWithRunningRecord 验证状态文件中 running 记录被安全重置为 pending。
func TestRecoveryWithRunningRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := NewStore(path)
	err := s.Save(map[string]Record{
		"mid": {Job: Job{ID: "mid"}, Status: StatusRunning, Attempts: 1},
	})
	if err != nil {
		t.Fatalf("构造状态失败: %v", err)
	}
	t.Logf("输入: 状态文件中存在 running 记录 (模拟崩溃瞬间)")

	var calls atomic.Int32
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 4, StatePath: path},
		func(ctx Context, job Job) error {
			calls.Add(1)
			return nil
		})
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	defer rt.Close()
	waitFor(t, "mid 恢复执行完成", func() bool {
		rec, _ := rt.Status("mid")
		return rec.Status == StatusSucceeded
	})
	if got := calls.Load(); got != 1 {
		t.Fatalf("mid 执行 %d 次, 期望恰好 1 次", got)
	}
	t.Logf("判定依据: running 记录重置为 pending 并恰好重放一次, 不丢失已确认工作")
}
