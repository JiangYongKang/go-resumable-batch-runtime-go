package batch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrencyLimit 验证同时执行的作业数不超过 MaxConcurrency，
// 且全部作业最终成功（无丢单、无 goroutine 泄漏）。
func TestConcurrencyLimit(t *testing.T) {
	const maxConc = 2
	const total = 10
	t.Logf("输入: MaxConcurrency=%d, 作业数=%d", maxConc, total)

	rt, err := New(Config{MaxConcurrency: maxConc, MaxQueue: total})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	var cur, peak atomic.Int32
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("job-%d", i)
		if err := rt.Submit(Job{ID: id, Work: func(ctx context.Context, j *Job) error {
			n := cur.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			cur.Add(-1)
			return nil
		}}); err != nil {
			t.Fatalf("Submit %s: %v", id, err)
		}
	}
	for i := 0; i < total; i++ {
		res, err := rt.Wait(fmt.Sprintf("job-%d", i))
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if res.Status != StatusSucceeded {
			t.Fatalf("job-%d 状态=%s, 期望 succeeded", i, res.Status)
		}
	}
	t.Logf("判定依据: 并发峰值=%d, 上限=%d", peak.Load(), maxConc)
	if peak.Load() > maxConc {
		t.Fatalf("并发峰值 %d 超过上限 %d", peak.Load(), maxConc)
	}
}

// TestQueueFull 验证超出 MaxQueue 的提交被拒绝且错误可区分。
func TestQueueFull(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	block := make(chan struct{})
	release := make(chan struct{})
	go func() { <-release; close(block) }()
	defer close(release)

	t.Log("输入: MaxQueue=2, 连续提交 3 个阻塞作业")
	for i := 0; i < 2; i++ {
		if err := rt.Submit(Job{ID: fmt.Sprintf("j%d", i), Work: func(ctx context.Context, j *Job) error {
			<-block
			return nil
		}}); err != nil {
			t.Fatalf("第 %d 个提交应成功: %v", i, err)
		}
	}
	err = rt.Submit(Job{ID: "j2", Work: func(ctx context.Context, j *Job) error { return nil }})
	t.Logf("第 3 个提交结果: err=%v", err)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("期望 ErrQueueFull, 实际 %v", err)
	}
}

// TestDuplicateSubmit 验证重复提交同 ID 被拒绝。
func TestDuplicateSubmit(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	var runs atomic.Int32
	work := func(ctx context.Context, j *Job) error {
		runs.Add(1)
		return nil
	}
	if err := rt.Submit(Job{ID: "dup", Work: work}); err != nil {
		t.Fatalf("首次提交: %v", err)
	}
	err = rt.Submit(Job{ID: "dup", Work: work})
	t.Logf("运行中重复提交: err=%v", err)
	if !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("期望 ErrDuplicateJob, 实际 %v", err)
	}
	res, _ := rt.Wait("dup")
	t.Logf("首次执行完成: status=%s, 再次提交已完成作业", res.Status)
	err = rt.Submit(Job{ID: "dup", Work: work})
	if !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("完成后重复提交期望 ErrDuplicateJob, 实际 %v", err)
	}
	t.Logf("判定依据: 实际执行次数=%d, 期望=1（不重复执行）", runs.Load())
	if runs.Load() != 1 {
		t.Fatalf("执行次数 %d != 1", runs.Load())
	}
}

// TestCancel 验证取消的三种可区分结果：取消等待中作业、取消执行中作业、
// 取消不存在/已终态作业。
func TestCancel(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	started := make(chan struct{})
	unblock := make(chan struct{})
	// 占住唯一 worker，使后续作业处于 pending。
	if err := rt.Submit(Job{ID: "running", Work: func(ctx context.Context, j *Job) error {
		close(started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-unblock:
			return nil
		}
	}}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Submit(Job{ID: "pending", Work: func(ctx context.Context, j *Job) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	<-started

	t.Log("状态转换: pending --Cancel--> canceled")
	if err := rt.Cancel("pending"); err != nil {
		t.Fatalf("取消等待中作业: %v", err)
	}
	if st, _ := rt.Status("pending"); st != StatusCanceled {
		t.Fatalf("pending 作业状态=%s, 期望 canceled", st)
	}

	t.Log("状态转换: running --Cancel--> (Work 返回后) canceled")
	if err := rt.Cancel("running"); err != nil {
		t.Fatalf("取消执行中作业: %v", err)
	}
	res, err := rt.Wait("running")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("执行中作业最终状态: %s", res.Status)
	if res.Status != StatusCanceled {
		t.Fatalf("期望 canceled, 实际 %s", res.Status)
	}

	if err := rt.Cancel("missing"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("取消不存在作业期望 ErrJobNotFound, 实际 %v", err)
	}
	if err := rt.Cancel("running"); !errors.Is(err, ErrJobTerminal) {
		t.Fatalf("取消已终态作业期望 ErrJobTerminal, 实际 %v", err)
	}
	close(unblock)
}

// TestRetryClassification 验证瞬时失败重试、永久失败与依赖失败不重试，
// 且部分成功时汇总与实际执行一致。
func TestRetryClassification(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 2, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	var transientRuns, permanentRuns, depRuns atomic.Int32
	// 瞬时失败：前 2 次失败，第 3 次成功。
	if err := rt.Submit(Job{ID: "transient", MaxRetries: 3, Work: func(ctx context.Context, j *Job) error {
		n := transientRuns.Add(1)
		if n < 3 {
			return Transient(fmt.Errorf("临时错误 第%d次", n))
		}
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	// 永久失败：不应重试。
	if err := rt.Submit(Job{ID: "permanent", MaxRetries: 3, Work: func(ctx context.Context, j *Job) error {
		permanentRuns.Add(1)
		return Permanent(errors.New("不可恢复"))
	}}); err != nil {
		t.Fatal(err)
	}
	// 依赖失败：不应重试，且与永久失败可区分。
	if err := rt.Submit(Job{ID: "dependency", MaxRetries: 3, Work: func(ctx context.Context, j *Job) error {
		depRuns.Add(1)
		return Dependency(errors.New("上游未就绪"))
	}}); err != nil {
		t.Fatal(err)
	}

	resT, _ := rt.Wait("transient")
	resP, _ := rt.Wait("permanent")
	resD, _ := rt.Wait("dependency")
	t.Logf("transient: status=%s attempts=%d (输入 MaxRetries=3, 前2次失败)",
		resT.Status, resT.Attempts)
	t.Logf("permanent: status=%s attempts=%d kind=%s", resP.Status, resP.Attempts, Classify(resP.Err))
	t.Logf("dependency: status=%s attempts=%d kind=%s", resD.Status, resD.Attempts, Classify(resD.Err))

	if resT.Status != StatusSucceeded || resT.Attempts != 3 {
		t.Fatalf("瞬时应重试后成功 attempts=3, 实际 status=%s attempts=%d", resT.Status, resT.Attempts)
	}
	if resP.Status != StatusFailed || permanentRuns.Load() != 1 {
		t.Fatalf("永久失败不应重试, 实际执行 %d 次", permanentRuns.Load())
	}
	if resD.Status != StatusFailed || depRuns.Load() != 1 {
		t.Fatalf("依赖失败不应重试, 实际执行 %d 次", depRuns.Load())
	}
	if Classify(resD.Err) != KindDependency {
		t.Fatalf("依赖失败分类丢失: %v", Classify(resD.Err))
	}
}

// TestRetryExhausted 验证重试耗尽后进入 failed 终态。
func TestRetryExhausted(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	var runs atomic.Int32
	if err := rt.Submit(Job{ID: "always-fail", MaxRetries: 2, Work: func(ctx context.Context, j *Job) error {
		runs.Add(1)
		return Transient(errors.New("一直失败"))
	}}); err != nil {
		t.Fatal(err)
	}
	res, _ := rt.Wait("always-fail")
	t.Logf("输入 MaxRetries=2, 判定: attempts=%d (1 次原始 + 2 次重试), status=%s",
		res.Attempts, res.Status)
	if res.Status != StatusFailed || res.Attempts != 3 || runs.Load() != 3 {
		t.Fatalf("期望 failed/3 次尝试, 实际 status=%s attempts=%d runs=%d",
			res.Status, res.Attempts, runs.Load())
	}
}

// TestTimeout 验证单次执行超时向 Work 传播并计为可重试失败。
func TestTimeout(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	var runs atomic.Int32
	if err := rt.Submit(Job{ID: "slow", Timeout: 30 * time.Millisecond, MaxRetries: 1,
		Work: func(ctx context.Context, j *Job) error {
			runs.Add(1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
				return nil
			}
		}}); err != nil {
		t.Fatal(err)
	}
	res, _ := rt.Wait("slow")
	t.Logf("输入 Timeout=30ms MaxRetries=1, 判定: status=%s attempts=%d err=%v",
		res.Status, res.Attempts, res.Err)
	if res.Status != StatusFailed || res.Attempts != 2 {
		t.Fatalf("期望超时重试 1 次后失败, 实际 status=%s attempts=%d", res.Status, res.Attempts)
	}
}

// TestPartialSuccessAggregation 验证部分成功时各作业结果与实际执行一致。
func TestPartialSuccessAggregation(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 2, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	okWork := func(ctx context.Context, j *Job) error { return nil }
	failWork := func(ctx context.Context, j *Job) error { return Permanent(errors.New("坏数据")) }
	jobs := []struct {
		id   string
		work WorkFunc
		want Status
	}{
		{"ok-1", okWork, StatusSucceeded},
		{"bad-1", failWork, StatusFailed},
		{"ok-2", okWork, StatusSucceeded},
	}
	for _, j := range jobs {
		if err := rt.Submit(Job{ID: j.id, Work: j.work}); err != nil {
			t.Fatal(err)
		}
	}
	succeeded, failed := 0, 0
	for _, j := range jobs {
		res, _ := rt.Wait(j.id)
		t.Logf("作业 %s: 期望=%s 实际=%s", j.id, j.want, res.Status)
		if res.Status != j.want {
			t.Fatalf("作业 %s 汇总 %s 与实际期望 %s 不一致", j.id, res.Status, j.want)
		}
		if res.Status == StatusSucceeded {
			succeeded++
		} else {
			failed++
		}
	}
	t.Logf("汇总: 成功=%d 失败=%d (期望 2/1)", succeeded, failed)
	if succeeded != 2 || failed != 1 {
		t.Fatalf("汇总不一致: 成功=%d 失败=%d", succeeded, failed)
	}
}

// TestCloseRejectsSubmit 验证关闭后提交被拒绝。
func TestCloseRejectsSubmit(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := rt.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	err = rt.Submit(Job{ID: "x", Work: func(ctx context.Context, j *Job) error { return nil }})
	t.Logf("关闭后提交: err=%v", err)
	if !errors.Is(err, ErrRuntimeClosed) {
		t.Fatalf("期望 ErrRuntimeClosed, 实际 %v", err)
	}
}

// TestNoGoroutineLeak 粗略验证大量作业完成后 runtime 正常关闭。
func TestNoGoroutineLeak(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 4, MaxQueue: 16})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		id := fmt.Sprintf("g-%d", i)
		if err := rt.Submit(Job{ID: id, Work: func(ctx context.Context, j *Job) error { return nil }}); err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			rt.Wait(id)
		}(id)
	}
	wg.Wait()
	if err := rt.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	t.Log("16 个作业全部完成, Close 正常返回, worker 全部退出")
}
