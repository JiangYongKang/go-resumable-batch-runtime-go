package batch

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// execOrder 记录作业执行先后顺序（线程安全）。
type execOrder struct {
	mu    sync.Mutex
	order []string
}

func (e *execOrder) add(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.order = append(e.order, id)
}

func (e *execOrder) index(id string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, v := range e.order {
		if v == id {
			return i
		}
	}
	return -1
}

// TestDependencySuccess 验证前置全部成功后依赖作业才执行，且顺序正确。
func TestDependencySuccess(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	var order execOrder
	work := func(ctx context.Context, j *Job) error {
		order.add(j.ID)
		return nil
	}
	t.Log("输入: MaxConcurrency=1, 先提交 b(dep a) 再提交 a")
	if err := rt.Submit(Job{ID: "b", DependsOn: []string{"a"}, Work: work}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Submit(Job{ID: "a", Work: work}); err != nil {
		t.Fatal(err)
	}
	resB, _ := rt.Wait("b")
	resA, _ := rt.Wait("a")
	t.Logf("结果: a=%s b=%s, 执行顺序=%v", resA.Status, resB.Status, order.order)
	if resA.Status != StatusSucceeded || resB.Status != StatusSucceeded {
		t.Fatalf("期望两者均 succeeded, 实际 a=%s b=%s", resA.Status, resB.Status)
	}
	if order.index("a") > order.index("b") {
		t.Fatalf("b 在前置 a 之前执行: %v", order.order)
	}
}

// TestDependencyFailurePropagation 验证前置失败时后继以 skipped 收尾：
// 不执行 Work、终态与普通失败可区分、错误分类为 dependency。
func TestDependencyFailurePropagation(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	var depRuns atomic.Int32
	if err := rt.Submit(Job{ID: "bad", Work: func(ctx context.Context, j *Job) error {
		return Permanent(errors.New("数据损坏"))
	}}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Submit(Job{ID: "child", DependsOn: []string{"bad"}, Work: func(ctx context.Context, j *Job) error {
		depRuns.Add(1)
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	res, _ := rt.Wait("child")
	t.Logf("前置永久失败, 后继结果: status=%s attempts=%d err=%v kind=%s",
		res.Status, res.Attempts, res.Err, Classify(res.Err))
	if res.Status != StatusSkipped {
		t.Fatalf("期望 skipped, 实际 %s", res.Status)
	}
	if depRuns.Load() != 0 {
		t.Fatalf("skipped 作业的 Work 不应执行, 实际执行 %d 次", depRuns.Load())
	}
	if !errors.Is(res.Err, ErrDependencyFailed) {
		t.Fatalf("错误应可判定为 ErrDependencyFailed, 实际 %v", res.Err)
	}
	if Classify(res.Err) != KindDependency {
		t.Fatalf("错误分类应为 dependency, 实际 %s", Classify(res.Err))
	}
	if res.Status == StatusFailed {
		t.Fatal("skipped 与普通执行失败不可区分")
	}
}

// TestDependencyCancelPropagation 验证前置被取消时后继以 skipped 收尾，
// 且跳过沿依赖链传递（a 取消 -> b 跳过 -> c 跳过）。
func TestDependencyCancelPropagation(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	noop := func(ctx context.Context, j *Job) error { return nil }
	for _, j := range []Job{
		{ID: "a", Work: noop},
		{ID: "b", DependsOn: []string{"a"}, Work: noop},
		{ID: "c", DependsOn: []string{"b"}, Work: noop},
	} {
		if err := rt.Submit(j); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("操作: 取消链首 a（pending 状态）")
	if err := rt.Cancel("a"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	resB, _ := rt.Wait("b")
	resC, _ := rt.Wait("c")
	t.Logf("传播结果: a=canceled b=%s c=%s", resB.Status, resC.Status)
	if resB.Status != StatusSkipped || resC.Status != StatusSkipped {
		t.Fatalf("期望 b/c 均 skipped, 实际 b=%s c=%s", resB.Status, resC.Status)
	}
}

// TestDependencyLateArrival 验证前置后到：先提交依赖作业，再补交前置，
// 前置成功后等待中的作业自动接续，无需人工干预。
func TestDependencyLateArrival(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	var order execOrder
	work := func(ctx context.Context, j *Job) error {
		order.add(j.ID)
		return nil
	}
	t.Log("输入: 先提交 w(dep missing)，此时 missing 尚未提交")
	if err := rt.Submit(Job{ID: "w", DependsOn: []string{"missing"}, Work: work}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	st, _ := rt.Status("w")
	t.Logf("前置缺失 50ms 后: w 状态=%s (期望 pending, 未执行)", st)
	if st != StatusPending {
		t.Fatalf("前置未提交时应挂起等待, 实际 %s", st)
	}
	if order.index("w") != -1 {
		t.Fatal("前置未就绪时 w 不应执行")
	}
	t.Log("操作: 补交前置 missing")
	if err := rt.Submit(Job{ID: "missing", Work: work}); err != nil {
		t.Fatal(err)
	}
	res, _ := rt.Wait("w")
	t.Logf("前置成功后: w=%s, 执行顺序=%v", res.Status, order.order)
	if res.Status != StatusSucceeded {
		t.Fatalf("期望 succeeded, 实际 %s", res.Status)
	}
	if order.index("missing") > order.index("w") {
		t.Fatalf("w 在前置之前执行: %v", order.order)
	}
}

// TestDependencyCycle 验证自依赖与相互依赖被显式拒绝。
func TestDependencyCycle(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	noop := func(ctx context.Context, j *Job) error { return nil }
	err = rt.Submit(Job{ID: "self", DependsOn: []string{"self"}, Work: noop})
	t.Logf("自依赖提交: err=%v", err)
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("自依赖期望 ErrDependencyCycle, 实际 %v", err)
	}
	if err := rt.Submit(Job{ID: "a", DependsOn: []string{"b"}, Work: noop}); err != nil {
		t.Fatal(err)
	}
	err = rt.Submit(Job{ID: "b", DependsOn: []string{"a"}, Work: noop})
	t.Logf("相互依赖提交: err=%v", err)
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("相互依赖期望 ErrDependencyCycle, 实际 %v", err)
	}
}

// TestPriorityWithinCapacity 验证并发额度占满时高优先级先调度，
// 且并发上限不被优先级顶破。
func TestPriorityWithinCapacity(t *testing.T) {
	const maxConc = 1
	rt, err := New(Config{MaxConcurrency: maxConc, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	block := make(chan struct{})
	started := make(chan struct{})
	var cur, peak atomic.Int32
	var order execOrder
	track := func(ctx context.Context, j *Job) error {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		order.add(j.ID)
		time.Sleep(10 * time.Millisecond)
		cur.Add(-1)
		return nil
	}
	// 占住唯一并发额度。
	if err := rt.Submit(Job{ID: "blocker", Work: func(ctx context.Context, j *Job) error {
		close(started)
		<-block
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	<-started
	t.Log("输入: 额度占满后依次提交 low(priority=1)、high(priority=9)")
	if err := rt.Submit(Job{ID: "low", Priority: 1, Work: track}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Submit(Job{ID: "high", Priority: 9, Work: track}); err != nil {
		t.Fatal(err)
	}
	close(block)
	resH, _ := rt.Wait("high")
	resL, _ := rt.Wait("low")
	t.Logf("结果: high=%s low=%s, 执行顺序=%v, 并发峰值=%d",
		resH.Status, resL.Status, order.order, peak.Load())
	if resH.Status != StatusSucceeded || resL.Status != StatusSucceeded {
		t.Fatalf("期望均 succeeded, 实际 high=%s low=%s", resH.Status, resL.Status)
	}
	if order.index("high") > order.index("low") {
		t.Fatalf("高优先级应先调度: %v", order.order)
	}
	if peak.Load() > maxConc {
		t.Fatalf("并发峰值 %d 超过上限 %d（优先级顶破硬上限）", peak.Load(), maxConc)
	}
}

// TestPriorityAging 验证老化机制：低优先级作业等待足够久后有效优先级
// 超过后来的高优先级作业，不会被无限压制。
func TestPriorityAging(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8, PriorityAging: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	block := make(chan struct{})
	started := make(chan struct{})
	var order execOrder
	track := func(ctx context.Context, j *Job) error {
		order.add(j.ID)
		return nil
	}
	if err := rt.Submit(Job{ID: "blocker", Work: func(ctx context.Context, j *Job) error {
		close(started)
		<-block
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	<-started
	t.Log("输入: aging=10ms, 先提交 low(priority=0)，等待 50ms 后提交 high(priority=1)")
	if err := rt.Submit(Job{ID: "low", Priority: 0, Work: track}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // low 有效优先级老化至约 5
	if err := rt.Submit(Job{ID: "high", Priority: 1, Work: track}); err != nil {
		t.Fatal(err)
	}
	close(block)
	resL, _ := rt.Wait("low")
	resH, _ := rt.Wait("high")
	t.Logf("结果: low=%s high=%s, 执行顺序=%v", resL.Status, resH.Status, order.order)
	if order.index("low") > order.index("high") {
		t.Fatalf("老化后低优先级应先获得调度（防饿死）: %v", order.order)
	}
}

// TestNotBefore 验证最早开始时间：时间未到期不执行、不占用并发额度
// （其他作业照常执行），到期后自动调度。
func TestNotBefore(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	notBefore := time.Now().Add(250 * time.Millisecond)
	var delayedStart, normalDone atomic.Int64 // unixnano
	t.Logf("输入: MaxConcurrency=1, delayed.NotBefore=now+250ms, 同时提交 normal")
	if err := rt.Submit(Job{ID: "delayed", NotBefore: notBefore, Work: func(ctx context.Context, j *Job) error {
		delayedStart.Store(time.Now().UnixNano())
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Submit(Job{ID: "normal", Work: func(ctx context.Context, j *Job) error {
		normalDone.Store(time.Now().UnixNano())
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	resN, _ := rt.Wait("normal")
	resD, _ := rt.Wait("delayed")
	t.Logf("结果: normal=%s delayed=%s, normal完成时间=%v, delayed开始时间=%v, 门控=%v",
		resN.Status, resD.Status,
		time.Unix(0, normalDone.Load()).Format("15:04:05.000"),
		time.Unix(0, delayedStart.Load()).Format("15:04:05.000"),
		notBefore.Format("15:04:05.000"))
	if resN.Status != StatusSucceeded || resD.Status != StatusSucceeded {
		t.Fatalf("期望均 succeeded, 实际 normal=%s delayed=%s", resN.Status, resD.Status)
	}
	if normalDone.Load() > delayedStart.Load() {
		t.Fatal("等待中的 delayed 占用了并发额度（normal 应在其开始前完成）")
	}
	if time.Unix(0, delayedStart.Load()).Before(notBefore.Add(-20 * time.Millisecond)) {
		t.Fatalf("delayed 在门控时间前执行: start=%v notBefore=%v",
			time.Unix(0, delayedStart.Load()), notBefore)
	}
}

// TestRetryBackoff 验证重试退避：两次尝试之间间隔不小于 RetryBackoff，
// 且退避期间不占用并发额度（其他作业可插入执行）。
func TestRetryBackoff(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	const backoff = 120 * time.Millisecond
	var mu sync.Mutex
	var attempts []time.Time
	var otherStart time.Time
	t.Logf("输入: MaxConcurrency=1, flaky 前 2 次瞬时失败, RetryBackoff=%v", backoff)
	if err := rt.Submit(Job{ID: "flaky", MaxRetries: 3, RetryBackoff: backoff,
		Work: func(ctx context.Context, j *Job) error {
			mu.Lock()
			attempts = append(attempts, time.Now())
			n := len(attempts)
			mu.Unlock()
			if n < 3 {
				return Transient(fmt.Errorf("第 %d 次失败", n))
			}
			return nil
		}}); err != nil {
		t.Fatal(err)
	}
	// 等第一次失败进入退避后提交 other，验证退避期间额度被释放。
	time.Sleep(30 * time.Millisecond)
	if err := rt.Submit(Job{ID: "other", Work: func(ctx context.Context, j *Job) error {
		mu.Lock()
		otherStart = time.Now()
		mu.Unlock()
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	res, _ := rt.Wait("flaky")
	resO, _ := rt.Wait("other")
	mu.Lock()
	defer mu.Unlock()
	t.Logf("结果: flaky=%s attempts=%d, 尝试时刻间隔=%v/%v, other=%s 开始于第二次尝试前=%v",
		res.Status, res.Attempts,
		attempts[1].Sub(attempts[0]).Round(time.Millisecond),
		attempts[2].Sub(attempts[1]).Round(time.Millisecond),
		resO.Status, otherStart.Before(attempts[1]))
	if res.Status != StatusSucceeded || res.Attempts != 3 {
		t.Fatalf("期望退避重试后成功 attempts=3, 实际 status=%s attempts=%d", res.Status, res.Attempts)
	}
	for i := 1; i < len(attempts); i++ {
		if gap := attempts[i].Sub(attempts[i-1]); gap < backoff-20*time.Millisecond {
			t.Fatalf("第 %d 次重试间隔 %v 小于退避 %v", i, gap, backoff)
		}
	}
	if !otherStart.Before(attempts[1]) {
		t.Fatal("退避期间 other 未插入执行（退避占用了并发额度）")
	}
}

// TestQueueFullCountsWaitingJobs 验证等依赖的作业同样计入在途上限，
// 超限提交被明确拒绝。
func TestQueueFullCountsWaitingJobs(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	noop := func(ctx context.Context, j *Job) error { return nil }
	t.Log("输入: MaxQueue=2, 提交 2 个等依赖（前置不存在）的作业")
	if err := rt.Submit(Job{ID: "w1", DependsOn: []string{"x"}, Work: noop}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Submit(Job{ID: "w2", DependsOn: []string{"x"}, Work: noop}); err != nil {
		t.Fatal(err)
	}
	err = rt.Submit(Job{ID: "w3", Work: noop})
	t.Logf("第 3 个提交: err=%v", err)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("等待中的作业应计入在途上限, 期望 ErrQueueFull, 实际 %v", err)
	}
}

// TestShutdownRestartWithWaitingJobs 验证带着等依赖/等时间/等退避的作业
// 关停再重启：关停不挂起不崩溃，已受理工作不丢失，重启后原样接续。
func TestShutdownRestartWithWaitingJobs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	rt1, err := New(Config{MaxConcurrency: 2, MaxQueue: 8, StatePath: path})
	if err != nil {
		t.Fatalf("New rt1: %v", err)
	}
	noop := func(ctx context.Context, j *Job) error { return nil }
	// 等依赖：前置 dep 尚未提交。
	if err := rt1.Submit(Job{ID: "wait-dep", DependsOn: []string{"dep"}, Work: noop}); err != nil {
		t.Fatal(err)
	}
	// 等时间：400ms 后才可调度。
	notBefore := time.Now().Add(400 * time.Millisecond)
	if err := rt1.Submit(Job{ID: "wait-time", NotBefore: notBefore, Work: noop}); err != nil {
		t.Fatal(err)
	}
	// 等退避：第一次尝试瞬时失败，退避 1s（关停时处于退避中）。
	var flakyRuns atomic.Int32
	if err := rt1.Submit(Job{ID: "wait-backoff", MaxRetries: 3, RetryBackoff: 1 * time.Second,
		Work: func(ctx context.Context, j *Job) error {
			flakyRuns.Add(1)
			return Transient(errors.New("瞬时错误"))
		}}); err != nil {
		t.Fatal(err)
	}
	// 等退避生效（第一次尝试完成并转入退避等待）。
	deadline := 200
	for i := 0; i < deadline; i++ {
		sf, err := NewFileStore(path).Load()
		if err == nil {
			if rec, ok := sf.Jobs["wait-backoff"]; ok && rec.Attempts >= 1 && !rec.ReadyAt.IsZero() {
				t.Logf("退避已生效: attempts=%d ready_at=%v", rec.Attempts, rec.ReadyAt)
				break
			}
		}
		if i == deadline-1 {
			t.Fatal("状态文件未记录退避等待")
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Log("操作: 带着 3 个等待中的作业执行 Close")
	closeStart := time.Now()
	if err := rt1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	t.Logf("Close 耗时=%v（不应挂起等待 1s 退避或 400ms 门控）", time.Since(closeStart).Round(time.Millisecond))
	if d := time.Since(closeStart); d > 3*time.Second {
		t.Fatalf("Close 挂起 %v，等待中的作业阻塞了关停", d)
	}

	// 关停后状态文件应保留 pending 记录（已受理工作不丢失）。
	sf, err := NewFileStore(path).Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, id := range []string{"wait-dep", "wait-time", "wait-backoff"} {
		rec, ok := sf.Jobs[id]
		t.Logf("关停后状态文件: %s status=%s attempts=%d ready_at=%v", id, rec.Status, rec.Attempts, rec.ReadyAt)
		if !ok || rec.Status != StatusPending {
			t.Fatalf("%s 应保留 pending 供重启接续, 实际 %s found=%v", id, rec.Status, ok)
		}
	}

	// 模拟进程重启。
	rt2, err := New(Config{MaxConcurrency: 2, MaxQueue: 8, StatePath: path})
	if err != nil {
		t.Fatalf("New rt2: %v", err)
	}
	defer rt2.Close()

	var order execOrder
	resume := func(ctx context.Context, j *Job) error {
		order.add(j.ID)
		return nil
	}
	// 同 ID 重新提交 = 接续执行。
	for _, id := range []string{"wait-dep", "wait-time", "wait-backoff"} {
		if err := rt2.Submit(Job{ID: id, DependsOn: map[string][]string{
			"wait-dep": {"dep"},
		}[id], Work: resume}); err != nil {
			t.Fatalf("恢复提交 %s: %v", id, err)
		}
	}
	// 补交前置，等待中的依赖作业应自动接上。
	if err := rt2.Submit(Job{ID: "dep", Work: resume}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"wait-dep", "wait-time", "wait-backoff"} {
		res, err := rt2.Wait(id)
		if err != nil {
			t.Fatalf("Wait %s: %v", id, err)
		}
		t.Logf("重启接续: %s status=%s attempts=%d", id, res.Status, res.Attempts)
		if res.Status != StatusSucceeded {
			t.Fatalf("%s 期望 succeeded, 实际 %s", id, res.Status)
		}
	}
	resB, _ := rt2.Wait("wait-backoff")
	t.Logf("判定依据: wait-backoff attempts=%d（含关停前已确认的 1 次尝试）", resB.Attempts)
	if resB.Attempts < 2 {
		t.Fatalf("重启后尝试次数应包含关停前记录, 实际 %d", resB.Attempts)
	}
	if order.index("dep") > order.index("wait-dep") {
		t.Fatalf("wait-dep 在前置 dep 之前执行: %v", order.order)
	}
	if flakyRuns.Load() != 1 {
		t.Fatalf("关停前已执行的尝试不应重放, 实际 %d 次", flakyRuns.Load())
	}
}

// TestDependencyConfirmedSurvivesRestart 验证重启前已确认（成功）的依赖不丢：
// 重启后提交依赖作业，前置直接视为已满足且不重复执行。
func TestDependencyConfirmedSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	var depRuns atomic.Int32
	rt1, err := New(Config{MaxConcurrency: 1, MaxQueue: 8, StatePath: path})
	if err != nil {
		t.Fatalf("New rt1: %v", err)
	}
	if err := rt1.Submit(Job{ID: "base", Work: func(ctx context.Context, j *Job) error {
		depRuns.Add(1)
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if res, _ := rt1.Wait("base"); res.Status != StatusSucceeded {
		t.Fatalf("base 期望 succeeded, 实际 %s", res.Status)
	}
	if err := rt1.Close(); err != nil {
		t.Fatalf("Close rt1: %v", err)
	}
	t.Logf("重启前: base=succeeded (执行 %d 次)", depRuns.Load())

	rt2, err := New(Config{MaxConcurrency: 1, MaxQueue: 8, StatePath: path})
	if err != nil {
		t.Fatalf("New rt2: %v", err)
	}
	defer rt2.Close()
	if err := rt2.Submit(Job{ID: "follow", DependsOn: []string{"base"}, Work: func(ctx context.Context, j *Job) error {
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	res, _ := rt2.Wait("follow")
	t.Logf("重启后: follow(dep base)=%s, base 总执行次数=%d", res.Status, depRuns.Load())
	if res.Status != StatusSucceeded {
		t.Fatalf("已确认的依赖应直接满足, follow 期望 succeeded, 实际 %s", res.Status)
	}
	if depRuns.Load() != 1 {
		t.Fatalf("已成功的前置不应重复执行, 实际 %d 次", depRuns.Load())
	}
}
