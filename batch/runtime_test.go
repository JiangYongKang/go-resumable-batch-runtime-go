package batch

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor 轮询等待条件成立，超时则失败。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			t.Logf("条件达成: %s", what)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

func TestConcurrencyLimit(t *testing.T) {
	var cur, max atomic.Int32
	release := make(chan struct{})
	h := func(ctx Context, job Job) error {
		c := cur.Add(1)
		for {
			m := max.Load()
			if c <= m || max.CompareAndSwap(m, c) {
				break
			}
		}
		<-release // 阻塞直到测试放行，制造并发窗口
		cur.Add(-1)
		return nil
	}
	rt, err := New(Config{MaxConcurrency: 2, MaxQueue: 8}, h)
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	defer rt.Close()

	t.Logf("输入: 提交 6 个阻塞作业, MaxConcurrency=2")
	for i := 0; i < 6; i++ {
		if err := rt.Submit(Job{ID: string(rune('a' + i))}); err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
	}
	waitFor(t, "并发数达到上限 2", func() bool { return cur.Load() == 2 })
	time.Sleep(50 * time.Millisecond)
	if got := max.Load(); got > 2 {
		t.Fatalf("并发上限被突破: max=%d, 期望 <=2", got)
	}
	t.Logf("判定依据: 观测到的最大并发=%d, 上限=2", max.Load())
	close(release)
	waitFor(t, "全部作业完成", func() bool { return len(rt.Results()) == 6 })
	for _, r := range rt.Results() {
		if r.Status != StatusSucceeded {
			t.Fatalf("作业 %s 状态=%s, 期望 succeeded", r.JobID, r.Status)
		}
	}
	t.Logf("状态转换: 6 个作业均为 pending->running->succeeded")
}

func TestQueueFullAndDuplicate(t *testing.T) {
	release := make(chan struct{})
	h := func(ctx Context, job Job) error { <-release; return nil }
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 2}, h)
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	defer func() { close(release); rt.Close() }()

	t.Logf("输入: MaxQueue=2, 连续提交 3 个不同作业 + 1 个重复作业")
	if err := rt.Submit(Job{ID: "j1"}); err != nil {
		t.Fatalf("j1 提交失败: %v", err)
	}
	if err := rt.Submit(Job{ID: "j2"}); err != nil {
		t.Fatalf("j2 提交失败: %v", err)
	}
	if err := rt.Submit(Job{ID: "j3"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("j3 期望 ErrQueueFull, 实际 %v", err)
	}
	t.Logf("判定依据: 第 3 个作业被拒, 错误=ErrQueueFull (超限请求可区分)")
	if err := rt.Submit(Job{ID: "j1"}); !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("重复 j1 期望 ErrDuplicateJob, 实际 %v", err)
	}
	t.Logf("判定依据: 重复提交 j1 被拒, 错误=ErrDuplicateJob (重复提交可区分)")
}

func TestCancelPendingAndRunning(t *testing.T) {
	started := make(chan string, 4)
	h := func(ctx Context, job Job) error {
		started <- job.ID
		<-ctx.Done() // 响应取消
		return ctx.Err()
	}
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8}, h)
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	defer rt.Close()

	t.Logf("输入: 提交 run(将占住唯一并发槽) 与 wait(排队中)")
	if err := rt.Submit(Job{ID: "run"}); err != nil {
		t.Fatalf("提交 run 失败: %v", err)
	}
	if err := rt.Submit(Job{ID: "wait"}); err != nil {
		t.Fatalf("提交 wait 失败: %v", err)
	}
	waitFor(t, "run 进入 running", func() bool {
		rec, _ := rt.Status("run")
		return rec.Status == StatusRunning
	})

	t.Logf("状态转换: wait pending->cancelled (队列中取消)")
	if err := rt.Cancel("wait"); err != nil {
		t.Fatalf("取消 wait 失败: %v", err)
	}
	rec, _ := rt.Status("wait")
	if rec.Status != StatusCancelled {
		t.Fatalf("wait 状态=%s, 期望 cancelled", rec.Status)
	}

	t.Logf("状态转换: run running->cancelled (context 传播取消)")
	if err := rt.Cancel("run"); err != nil {
		t.Fatalf("取消 run 失败: %v", err)
	}
	waitFor(t, "run 变为 cancelled", func() bool {
		rec, _ := rt.Status("run")
		return rec.Status == StatusCancelled
	})

	t.Logf("输入: 对已终态作业再次取消")
	if err := rt.Cancel("run"); !errors.Is(err, ErrNotCancellable) {
		t.Fatalf("期望 ErrNotCancellable, 实际 %v", err)
	}
	t.Logf("判定依据: 取消后状态为终态, 再取消返回 ErrNotCancellable (可区分)")
}

func TestRetryTransientOnly(t *testing.T) {
	var transientCalls, permanentCalls atomic.Int32
	h := func(ctx Context, job Job) error {
		switch job.ID {
		case "flaky":
			n := transientCalls.Add(1)
			t.Logf("flaky 第 %d 次尝试", n)
			if n < 3 {
				return Transient(errors.New("临时抖动"))
			}
			return nil
		case "broken":
			permanentCalls.Add(1)
			return Permanent(errors.New("不可修复"))
		}
		return nil
	}
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8, MaxRetries: 3,
		RetryBackoff: time.Millisecond}, h)
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	defer rt.Close()

	t.Logf("输入: flaky(前2次瞬时失败后成功), broken(永久失败), MaxRetries=3")
	rt.Submit(Job{ID: "flaky"})
	rt.Submit(Job{ID: "broken"})
	waitFor(t, "两个作业到达终态", func() bool { return len(rt.Results()) == 2 })

	results := map[string]Result{}
	for _, r := range rt.Results() {
		results[r.JobID] = r
	}
	f := results["flaky"]
	if f.Status != StatusSucceeded || f.Attempts != 3 {
		t.Fatalf("flaky 结果=%+v, 期望 succeeded 且 attempts=3", f)
	}
	t.Logf("判定依据: flaky 瞬时失败重试后成功, attempts=%d (重试未放大并发, MaxConcurrency=1)", f.Attempts)
	b := results["broken"]
	if b.Status != StatusFailed || b.Failure != FailurePermanent || b.Attempts != 1 {
		t.Fatalf("broken 结果=%+v, 期望 failed/permanent 且 attempts=1", b)
	}
	if permanentCalls.Load() != 1 {
		t.Fatalf("永久失败被执行 %d 次, 期望 1 次 (不重试)", permanentCalls.Load())
	}
	t.Logf("判定依据: broken 永久失败不重试, 汇总结果与实际执行一致")
}

func TestTimeoutPropagatesAndRetries(t *testing.T) {
	var calls atomic.Int32
	h := func(ctx Context, job Job) error {
		calls.Add(1)
		select {
		case <-ctx.Done():
			t.Logf("job %s 收到取消/超时信号: %v", job.ID, ctx.Err())
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return nil
		}
	}
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 4, MaxRetries: 1,
		RetryBackoff: time.Millisecond}, h)
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	defer rt.Close()

	t.Logf("输入: 作业 Timeout=50ms, handler 永不自行返回, MaxRetries=1")
	rt.Submit(Job{ID: "slow", Timeout: 50 * time.Millisecond})
	waitFor(t, "slow 到达终态", func() bool { return len(rt.Results()) == 1 })
	r := rt.Results()[0]
	if r.Status != StatusFailed || r.Failure != FailureTransient || r.Attempts != 2 {
		t.Fatalf("结果=%+v, 期望 failed/transient 且 attempts=2 (1次+1次重试)", r)
	}
	t.Logf("判定依据: 超时按瞬时失败传播并重试, attempts=%d", r.Attempts)
}
