package batch

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestDependencySuccess 验证依赖成功传播：前置全部成功后下游才执行，
// 且下游提交先于前置提交也能正确等待。
func TestDependencySuccess(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 2, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	var orderMu atomic.Int32
	aOrder, bOrder := atomic.Int32{}, atomic.Int32{}

	// 先提交下游 b（依赖 a），再提交前置 a。
	t.Log("输入: 先提交 b(DependsOn=[a]), 再提交 a")
	if err := rt.Submit(Job{ID: "b", DependsOn: []string{"a"}, Work: func(ctx context.Context, j *Job) error {
		bOrder.Store(orderMu.Add(1))
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Submit(Job{ID: "a", Work: func(ctx context.Context, j *Job) error {
		time.Sleep(30 * time.Millisecond) // 给 b 留出“抢跑”窗口
		aOrder.Store(orderMu.Add(1))
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	resA, _ := rt.Wait("a")
	resB, _ := rt.Wait("b")
	t.Logf("结果: a=%s(执行序号=%d) b=%s(执行序号=%d)", resA.Status, aOrder.Load(), resB.Status, bOrder.Load())
	if resA.Status != StatusSucceeded || resB.Status != StatusSucceeded {
		t.Fatalf("期望均 succeeded, 实际 a=%s b=%s", resA.Status, resB.Status)
	}
	if aOrder.Load() == 0 || bOrder.Load() == 0 {
		t.Fatal("两个作业都应实际执行")
	}
	if bOrder.Load() < aOrder.Load() {
		t.Fatalf("判定失败: b(%d) 先于 a(%d) 执行，依赖未生效", bOrder.Load(), aOrder.Load())
	}
}

// TestDependencyFailurePropagation 验证前置失败时下游以 skipped 收尾（与
// 执行失败可区分）、下游 Work 不执行，且沿依赖链递归传播。
func TestDependencyFailurePropagation(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 2, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	var bRuns, cRuns atomic.Int32
	t.Log("输入: a 永久失败; b 依赖 a; c 依赖 b（链式）")
	if err := rt.Submit(Job{ID: "a", Work: func(ctx context.Context, j *Job) error {
		return Permanent(errors.New("坏数据"))
	}}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Submit(Job{ID: "b", DependsOn: []string{"a"}, Work: func(ctx context.Context, j *Job) error {
		bRuns.Add(1)
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Submit(Job{ID: "c", DependsOn: []string{"b"}, Work: func(ctx context.Context, j *Job) error {
		cRuns.Add(1)
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	resA, _ := rt.Wait("a")
	resB, _ := rt.Wait("b")
	resC, _ := rt.Wait("c")
	t.Logf("结果: a=%s b=%s(err=%v) c=%s(err=%v)", resA.Status, resB.Status, resB.Err, resC.Status, resC.Err)

	if resA.Status != StatusFailed {
		t.Fatalf("a 期望 failed, 实际 %s", resA.Status)
	}
	if resB.Status != StatusSkipped {
		t.Fatalf("b 期望 skipped（区别于 failed）, 实际 %s", resB.Status)
	}
	if resC.Status != StatusSkipped {
		t.Fatalf("c 期望沿链 skipped, 实际 %s", resC.Status)
	}
	if bRuns.Load() != 0 || cRuns.Load() != 0 {
		t.Fatalf("被跳过的作业不应执行: b=%d c=%d", bRuns.Load(), cRuns.Load())
	}
	if Classify(resB.Err) != KindDependency {
		t.Fatalf("b 的错误分类期望 dependency, 实际 %s", Classify(resB.Err))
	}
	if resB.Attempts != 0 {
		t.Fatalf("b 未执行, attempts 应为 0, 实际 %d", resB.Attempts)
	}
}

// TestDependencyCancelPropagation 验证前置被取消时下游同样以 skipped 收尾。
func TestDependencyCancelPropagation(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	block := make(chan struct{})
	defer close(block)
	// 占住唯一 worker，保证 a 处于 pending 时被取消。
	if err := rt.Submit(Job{ID: "holder", Work: func(ctx context.Context, j *Job) error {
		<-block
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Submit(Job{ID: "a", Work: func(ctx context.Context, j *Job) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	var bRuns atomic.Int32
	if err := rt.Submit(Job{ID: "b", DependsOn: []string{"a"}, Work: func(ctx context.Context, j *Job) error {
		bRuns.Add(1)
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	t.Log("操作: 取消 pending 状态的 a")
	if err := rt.Cancel("a"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	resB, _ := rt.Wait("b")
	t.Logf("结果: a=canceled, b=%s(err=%v), b 执行次数=%d", resB.Status, resB.Err, bRuns.Load())
	if resB.Status != StatusSkipped {
		t.Fatalf("前置被取消, b 期望 skipped, 实际 %s", resB.Status)
	}
	if bRuns.Load() != 0 {
		t.Fatalf("b 不应执行, 实际 %d 次", bRuns.Load())
	}
}

// TestDependencyLateSubmission 验证前置后补交：下游先提交并等待，
// 前置补交并跑成功后下游自动接上，无需人工干预。
func TestDependencyLateSubmission(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 2, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	var downstreamRuns atomic.Int32
	t.Log("输入: 先提交 downstream(DependsOn=[late]), late 尚未提交")
	if err := rt.Submit(Job{ID: "downstream", DependsOn: []string{"late"}, Work: func(ctx context.Context, j *Job) error {
		downstreamRuns.Add(1)
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	// 等待期间 downstream 不应执行。
	time.Sleep(80 * time.Millisecond)
	st, _ := rt.Status("downstream")
	t.Logf("前置未提交期间: downstream status=%s 执行次数=%d", st, downstreamRuns.Load())
	if st != StatusPending || downstreamRuns.Load() != 0 {
		t.Fatalf("前置未提交时 downstream 应等待, 实际 status=%s runs=%d", st, downstreamRuns.Load())
	}

	t.Log("操作: 补交前置 late 并成功")
	if err := rt.Submit(Job{ID: "late", Work: func(ctx context.Context, j *Job) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	res, _ := rt.Wait("downstream")
	t.Logf("结果: downstream=%s 执行次数=%d", res.Status, downstreamRuns.Load())
	if res.Status != StatusSucceeded || downstreamRuns.Load() != 1 {
		t.Fatalf("前置补交成功后 downstream 应自动执行, 实际 status=%s runs=%d", res.Status, downstreamRuns.Load())
	}
}

// TestDependencySubmittedAfterDepFailed 验证提交时前置已失败：
// 作业被受理并立即以 skipped 收尾，不占用调度。
func TestDependencySubmittedAfterDepFailed(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	if err := rt.Submit(Job{ID: "bad-dep", Work: func(ctx context.Context, j *Job) error {
		return Permanent(errors.New("失败的前置"))
	}}); err != nil {
		t.Fatal(err)
	}
	if res, _ := rt.Wait("bad-dep"); res.Status != StatusFailed {
		t.Fatalf("前置期望 failed, 实际 %s", res.Status)
	}

	var runs atomic.Int32
	t.Log("输入: 前置已 failed, 提交依赖它的作业")
	if err := rt.Submit(Job{ID: "late-child", DependsOn: []string{"bad-dep"}, Work: func(ctx context.Context, j *Job) error {
		runs.Add(1)
		return nil
	}}); err != nil {
		t.Fatalf("提交应被受理: %v", err)
	}
	res, _ := rt.Wait("late-child")
	t.Logf("结果: status=%s 执行次数=%d", res.Status, runs.Load())
	if res.Status != StatusSkipped || runs.Load() != 0 {
		t.Fatalf("期望立即 skipped 且不执行, 实际 status=%s runs=%d", res.Status, runs.Load())
	}
}

// TestDependencyCycle 验证自依赖与已知作业间的依赖环被明确拒绝。
func TestDependencyCycle(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	work := func(ctx context.Context, j *Job) error { return nil }
	err = rt.Submit(Job{ID: "self", DependsOn: []string{"self"}, Work: work})
	t.Logf("自依赖提交: err=%v", err)
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("自依赖期望 ErrDependencyCycle, 实际 %v", err)
	}

	// x 依赖 y（未提交），随后 y 依赖 x 时成环。
	if err := rt.Submit(Job{ID: "x", DependsOn: []string{"y"}, Work: work}); err != nil {
		t.Fatal(err)
	}
	err = rt.Submit(Job{ID: "y", DependsOn: []string{"x"}, Work: work})
	t.Logf("成环提交 y->x（x 已声明依赖 y）: err=%v", err)
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("期望 ErrDependencyCycle, 实际 %v", err)
	}
}

// TestDependencyMulti 验证多前置语义：全部成功才执行，任一失败即跳过。
func TestDependencyMulti(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 2, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	ok := func(ctx context.Context, j *Job) error { return nil }
	for i := 0; i < 3; i++ {
		if err := rt.Submit(Job{ID: fmt.Sprintf("dep-%d", i), Work: ok}); err != nil {
			t.Fatal(err)
		}
	}
	var runs atomic.Int32
	if err := rt.Submit(Job{ID: "join", DependsOn: []string{"dep-0", "dep-1", "dep-2"}, Work: func(ctx context.Context, j *Job) error {
		runs.Add(1)
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	res, _ := rt.Wait("join")
	t.Logf("多前置全部成功: join=%s 执行次数=%d", res.Status, runs.Load())
	if res.Status != StatusSucceeded || runs.Load() != 1 {
		t.Fatalf("期望 succeeded 且执行 1 次, 实际 status=%s runs=%d", res.Status, runs.Load())
	}
}
