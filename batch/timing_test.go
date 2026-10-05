package batch

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestNotBefore 验证到点再跑：NotBefore 未到时不执行、不占用执行资源
// （唯一 worker 可被其他就绪作业使用），到点后才执行。
func TestNotBefore(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	const delay = 200 * time.Millisecond
	start := time.Now()
	var delayedStart, otherStart atomic.Int64 // 相对 start 的毫秒偏移
	stamp := func(p *atomic.Int64) WorkFunc {
		return func(ctx context.Context, j *Job) error {
			p.Store(time.Since(start).Milliseconds())
			return nil
		}
	}

	t.Logf("输入: MaxConcurrency=1, 先提交 delayed(NotBefore=+%v), 再提交 other(立即)", delay)
	if err := rt.Submit(Job{ID: "delayed", NotBefore: start.Add(delay), Work: stamp(&delayedStart)}); err != nil {
		t.Fatal(err)
	}
	// delayed 挂起期间 other 应能立即执行——证明等待中的作业不占执行资源。
	if err := rt.Submit(Job{ID: "other", Work: stamp(&otherStart)}); err != nil {
		t.Fatal(err)
	}

	resD, _ := rt.Wait("delayed")
	resO, _ := rt.Wait("other")
	t.Logf("结果: other 启动于 %+dms, delayed 启动于 %+dms (NotBefore=%v)",
		otherStart.Load(), delayedStart.Load(), delay)
	if resO.Status != StatusSucceeded || resD.Status != StatusSucceeded {
		t.Fatalf("均期望 succeeded, 实际 other=%s delayed=%s", resO.Status, resD.Status)
	}
	if otherStart.Load() >= int64(delay/time.Millisecond) {
		t.Fatalf("other 应在 delayed 挂起期间立即执行, 实际启动于 %+dms", otherStart.Load())
	}
	if delayedStart.Load() < int64(delay/time.Millisecond) {
		t.Fatalf("delayed 提前执行: 启动于 %+dms < %v", delayedStart.Load(), delay)
	}
}

// TestRetryBackoff 验证失败退避：两次尝试间隔不小于 RetryBackoff，
// 且退避期间不占用执行资源（其他作业可插空执行）。
func TestRetryBackoff(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	const backoff = 200 * time.Millisecond
	var attempts atomic.Int32
	var firstFailAt, secondStartAt atomic.Int64
	start := time.Now()

	t.Logf("输入: MaxConcurrency=1, flaky 第 1 次瞬时失败, RetryBackoff=%v", backoff)
	if err := rt.Submit(Job{ID: "flaky", MaxRetries: 3, RetryBackoff: backoff, Work: func(ctx context.Context, j *Job) error {
		n := attempts.Add(1)
		if n == 1 {
			firstFailAt.Store(time.Since(start).Milliseconds())
			return Transient(errRetry)
		}
		secondStartAt.Store(time.Since(start).Milliseconds())
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	// 等第一次失败落账后，提交插空作业：若退避占着 worker，它将被延迟到退避结束。
	deadline := time.Now().Add(2 * time.Second)
	for attempts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if attempts.Load() == 0 {
		t.Fatal("第一次尝试未发生")
	}
	var fillerStart atomic.Int64
	if err := rt.Submit(Job{ID: "filler", Work: func(ctx context.Context, j *Job) error {
		fillerStart.Store(time.Since(start).Milliseconds())
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	res, _ := rt.Wait("flaky")
	resF, _ := rt.Wait("filler")
	gap := secondStartAt.Load() - firstFailAt.Load()
	t.Logf("结果: flaky status=%s attempts=%d, 两次尝试间隔=%dms (退避=%v), filler 启动于 %+dms",
		res.Status, res.Attempts, gap, backoff, fillerStart.Load())

	if res.Status != StatusSucceeded || res.Attempts != 2 {
		t.Fatalf("期望第 2 次成功, 实际 status=%s attempts=%d", res.Status, res.Attempts)
	}
	if gap < int64(backoff/time.Millisecond) {
		t.Fatalf("重试间隔 %dms 小于退避 %v", gap, backoff)
	}
	if resF.Status != StatusSucceeded {
		t.Fatalf("filler 期望 succeeded, 实际 %s", resF.Status)
	}
	// filler 在退避窗口内提交，若退避不占执行资源，它应早于退避结束点启动。
	if fillerStart.Load() >= firstFailAt.Load()+int64(backoff/time.Millisecond) {
		t.Fatalf("退避期间 worker 被占用: filler 启动于 %+dms, 退避结束于 %+dms",
			fillerStart.Load(), firstFailAt.Load()+int64(backoff/time.Millisecond))
	}
}

// TestImmediateRetryWithoutBackoff 验证未设置退避时保持原有立即重试行为。
func TestImmediateRetryWithoutBackoff(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	var attempts atomic.Int32
	start := time.Now()
	if err := rt.Submit(Job{ID: "quick-retry", MaxRetries: 2, Work: func(ctx context.Context, j *Job) error {
		if attempts.Add(1) < 3 {
			return Transient(errRetry)
		}
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	res, _ := rt.Wait("quick-retry")
	elapsed := time.Since(start)
	t.Logf("结果: status=%s attempts=%d 耗时=%v (无退避应快速完成)", res.Status, res.Attempts, elapsed)
	if res.Status != StatusSucceeded || res.Attempts != 3 {
		t.Fatalf("期望 3 次尝试后成功, 实际 status=%s attempts=%d", res.Status, res.Attempts)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("无退避重试耗时异常: %v", elapsed)
	}
}

var errRetry = &retryError{}

type retryError struct{}

func (e *retryError) Error() string { return "瞬时错误" }
