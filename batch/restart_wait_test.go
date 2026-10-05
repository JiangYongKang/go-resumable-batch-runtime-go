package batch

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestShutdownRestartWithWaitingJobs 验证带着等待中的作业关停再重启：
//   - 关停时正在等依赖、等时间、等退避的作业不丢失、不取消，状态落盘；
//   - 关停本身不挂起、不崩溃；
//   - 重启后按原安排接续：等依赖的继续等（前置补交成功后自动接上）、
//     到点再跑的仍等到点、退避的按剩余间隔重试，已完成的绝不重复执行。
func TestShutdownRestartWithWaitingJobs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	const backoff = 600 * time.Millisecond
	const futureDelay = 900 * time.Millisecond
	phase1Start := time.Now()
	var phase1BackoffFailAt atomic.Int64 // 退避作业首次失败时刻（相对 phase1Start）

	// ---- 阶段 1：提交各类等待中的作业，然后关停 ----
	rt1, err := New(Config{MaxConcurrency: 1, MaxQueue: 8, StatePath: path})
	if err != nil {
		t.Fatalf("New rt1: %v", err)
	}
	ok := func(ctx context.Context, j *Job) error { return nil }
	if err := rt1.Submit(Job{ID: "done", Work: ok}); err != nil {
		t.Fatal(err)
	}
	if err := rt1.Submit(Job{ID: "dep-waiter", DependsOn: []string{"late-dep"}, Work: ok}); err != nil {
		t.Fatal(err)
	}
	if err := rt1.Submit(Job{ID: "future", NotBefore: phase1Start.Add(futureDelay), Work: ok}); err != nil {
		t.Fatal(err)
	}
	if err := rt1.Submit(Job{ID: "backoff", MaxRetries: 3, RetryBackoff: backoff, Work: func(ctx context.Context, j *Job) error {
		phase1BackoffFailAt.Store(time.Since(phase1Start).Milliseconds())
		return Transient(errors.New("第一次失败，进入退避"))
	}}); err != nil {
		t.Fatal(err)
	}
	if res, _ := rt1.Wait("done"); res.Status != StatusSucceeded {
		t.Fatalf("done 期望 succeeded, 实际 %s", res.Status)
	}
	// 等退避作业的首次失败落盘（进入退避等待）。
	deadline := time.Now().Add(2 * time.Second)
	for {
		sf, err := NewFileStore(path).Load()
		if err == nil {
			if rec, ok := sf.Jobs["backoff"]; ok && rec.Attempts >= 1 && rec.Status == StatusPending {
				t.Logf("关停前落盘: backoff attempts=%d not_before=首次失败+%v", rec.Attempts, backoff)
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("backoff 首次失败未落盘")
		}
		time.Sleep(5 * time.Millisecond)
	}

	closeStart := time.Now()
	if err := rt1.Close(); err != nil {
		t.Fatalf("Close rt1: %v", err)
	}
	t.Logf("关停耗时=%v（等待中的作业不阻塞关停）", time.Since(closeStart))

	// 关停后状态文件应保留等待中的作业及其调度安排。
	sf, err := NewFileStore(path).Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, id := range []string{"dep-waiter", "future", "backoff"} {
		rec, ok := sf.Jobs[id]
		if !ok || rec.Status != StatusPending {
			t.Fatalf("关停后 %s 应保持 pending, 实际 %s found=%v", id, rec.Status, ok)
		}
	}
	if got := sf.Jobs["dep-waiter"].DependsOn; len(got) != 1 || got[0] != "late-dep" {
		t.Fatalf("依赖声明未持久化: %v", got)
	}
	if sf.Jobs["future"].NotBefore.IsZero() || sf.Jobs["backoff"].NotBefore.IsZero() {
		t.Fatal("等待安排（not_before）未持久化")
	}
	t.Log("关停后落盘: dep-waiter/future/backoff 均为 pending, 依赖与等待安排已持久化")

	// ---- 阶段 2：重启，验证接续 ----
	rt2, err := New(Config{MaxConcurrency: 1, MaxQueue: 8, StatePath: path})
	if err != nil {
		t.Fatalf("New rt2: %v", err)
	}
	defer rt2.Close()

	// 已完成的工作绝不重复执行。
	if err := rt2.Submit(Job{ID: "done", Work: ok}); !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("已完成作业重复提交期望 ErrDuplicateJob, 实际 %v", err)
	}

	// 等依赖的作业：恢复提交（不重复声明依赖，沿用持久化记录），
	// 前置未补交前继续等待。
	var waiterRuns atomic.Int32
	if err := rt2.Submit(Job{ID: "dep-waiter", Work: func(ctx context.Context, j *Job) error {
		waiterRuns.Add(1)
		return nil
	}}); err != nil {
		t.Fatalf("恢复 dep-waiter: %v", err)
	}
	time.Sleep(80 * time.Millisecond)
	if waiterRuns.Load() != 0 {
		t.Fatal("前置未补交, dep-waiter 不应执行")
	}
	t.Log("重启后: dep-waiter 继续等待未提交的前置")
	if err := rt2.Submit(Job{ID: "late-dep", Work: ok}); err != nil {
		t.Fatal(err)
	}
	resW, _ := rt2.Wait("dep-waiter")
	t.Logf("前置补交成功后: dep-waiter=%s", resW.Status)
	if resW.Status != StatusSucceeded || waiterRuns.Load() != 1 {
		t.Fatalf("dep-waiter 应自动接上, 实际 status=%s runs=%d", resW.Status, waiterRuns.Load())
	}

	// 退避与到点作业：恢复提交（均在各自等待到期前），验证按原安排接续。
	var backoffSecondStart, futureStart atomic.Int64 // 相对 phase1Start
	if err := rt2.Submit(Job{ID: "backoff", MaxRetries: 3, RetryBackoff: backoff, Work: func(ctx context.Context, j *Job) error {
		backoffSecondStart.Store(time.Since(phase1Start).Milliseconds())
		return nil
	}}); err != nil {
		t.Fatalf("恢复 backoff: %v", err)
	}
	if err := rt2.Submit(Job{ID: "future", Work: func(ctx context.Context, j *Job) error {
		futureStart.Store(time.Since(phase1Start).Milliseconds())
		return nil
	}}); err != nil {
		t.Fatalf("恢复 future: %v", err)
	}

	resB, _ := rt2.Wait("backoff")
	gap := backoffSecondStart.Load() - phase1BackoffFailAt.Load()
	t.Logf("backoff: status=%s attempts=%d, 两次尝试间隔=%dms (退避=%v, 跨越重启)",
		resB.Status, resB.Attempts, gap, backoff)
	if resB.Status != StatusSucceeded {
		t.Fatalf("backoff 期望 succeeded, 实际 %s", resB.Status)
	}
	if resB.Attempts != 2 {
		t.Fatalf("attempts 应含关停前记录(=2), 实际 %d", resB.Attempts)
	}
	if gap < int64(backoff/time.Millisecond)-20 { // 20ms 判定余量
		t.Fatalf("退避安排未跨重启保持: 间隔=%dms < %v", gap, backoff)
	}

	resF, _ := rt2.Wait("future")
	t.Logf("future: status=%s 启动于 %+dms (持久化 NotBefore=+%v)", resF.Status, futureStart.Load(), futureDelay)
	if resF.Status != StatusSucceeded {
		t.Fatalf("future 期望 succeeded, 实际 %s", resF.Status)
	}
	if futureStart.Load() < int64(futureDelay/time.Millisecond)-20 { // 20ms 判定余量
		t.Fatalf("future 未遵守持久化的 NotBefore: 启动于 %+dms", futureStart.Load())
	}
}
