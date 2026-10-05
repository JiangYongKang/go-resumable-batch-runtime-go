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

// TestPriorityScheduling 验证并发额度被占满时高优先级作业先被调度，
// 且并发峰值不超过上限（优先级不顶破硬上限）。
func TestPriorityScheduling(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	block := make(chan struct{})
	// 占住唯一 worker，让后续作业排队。
	if err := rt.Submit(Job{ID: "blocker", Work: func(ctx context.Context, j *Job) error {
		<-block
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var order []string
	record := func(id string) WorkFunc {
		return func(ctx context.Context, j *Job) error {
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			return nil
		}
	}
	// 先低后高提交，调度顺序应反转（高优先级先行）。
	t.Log("输入: MaxConcurrency=1, 依次提交 low(p=0) mid(p=5) high(p=10)")
	if err := rt.Submit(Job{ID: "low", Priority: 0, Work: record("low")}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Submit(Job{ID: "mid", Priority: 5, Work: record("mid")}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Submit(Job{ID: "high", Priority: 10, Work: record("high")}); err != nil {
		t.Fatal(err)
	}
	close(block)

	for _, id := range []string{"low", "mid", "high"} {
		res, _ := rt.Wait(id)
		if res.Status != StatusSucceeded {
			t.Fatalf("%s 期望 succeeded, 实际 %s", id, res.Status)
		}
	}
	mu.Lock()
	t.Logf("判定依据: 实际执行顺序=%v, 期望 [high mid low]", order)
	mu.Unlock()
	if len(order) != 3 || order[0] != "high" || order[1] != "mid" || order[2] != "low" {
		t.Fatalf("优先级调度顺序错误: %v", order)
	}
}

// TestPriorityAging 验证老化机制：低优先级作业等待足够久后，
// 有效优先级反超晚到的高优先级作业，不会被永远压制。
func TestPriorityAging(t *testing.T) {
	const aging = 40 * time.Millisecond
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 8, PriorityAging: aging})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	block := make(chan struct{})
	if err := rt.Submit(Job{ID: "blocker", Work: func(ctx context.Context, j *Job) error {
		<-block
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var order []string
	record := func(id string) WorkFunc {
		return func(ctx context.Context, j *Job) error {
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			return nil
		}
	}
	t.Logf("输入: aging=%v, 先提交 low(p=0), 等待 3 个老化间隔后提交 high(p=1)", aging)
	if err := rt.Submit(Job{ID: "low", Priority: 0, Work: record("low")}); err != nil {
		t.Fatal(err)
	}
	// low 等待 3 个间隔后有效优先级 = 0+3 = 3 > high 的 1。
	time.Sleep(3*aging + 30*time.Millisecond)
	if err := rt.Submit(Job{ID: "high", Priority: 1, Work: record("high")}); err != nil {
		t.Fatal(err)
	}
	close(block)

	rt.Wait("low")
	rt.Wait("high")
	mu.Lock()
	t.Logf("判定依据: 实际执行顺序=%v, 期望 low 因老化先于 high", order)
	mu.Unlock()
	if len(order) != 2 || order[0] != "low" {
		t.Fatalf("老化未生效, low 被压制: 顺序=%v", order)
	}
}

// TestPriorityRespectsQueueLimit 验证优先级不改变在途容量硬上限：
// 高优先级提交在额度占满时同样被明确拒绝。
func TestPriorityRespectsQueueLimit(t *testing.T) {
	rt, err := New(Config{MaxConcurrency: 1, MaxQueue: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	block := make(chan struct{})
	release := make(chan struct{})
	go func() { <-release; close(block) }()
	defer close(release)

	for i := 0; i < 2; i++ {
		if err := rt.Submit(Job{ID: fmt.Sprintf("j%d", i), Work: func(ctx context.Context, j *Job) error {
			<-block
			return nil
		}}); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("输入: MaxQueue=2 已占满, 提交最高优先级作业")
	err = rt.Submit(Job{ID: "vip", Priority: 100, Work: func(ctx context.Context, j *Job) error { return nil }})
	t.Logf("判定: err=%v", err)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("高优先级也不能顶破容量上限, 期望 ErrQueueFull, 实际 %v", err)
	}
}

// TestPriorityConcurrencyHardCap 验证大量不同优先级作业并发提交时，
// 并发峰值仍不超过 MaxConcurrency。
func TestPriorityConcurrencyHardCap(t *testing.T) {
	const maxConc = 2
	const total = 12
	rt, err := New(Config{MaxConcurrency: maxConc, MaxQueue: total})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer rt.Close()

	var cur, peak atomic.Int32
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("p-%d", i)
		prio := i % 3 // 混合优先级
		if err := rt.Submit(Job{ID: id, Priority: prio, Work: func(ctx context.Context, j *Job) error {
			n := cur.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			cur.Add(-1)
			return nil
		}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < total; i++ {
		res, _ := rt.Wait(fmt.Sprintf("p-%d", i))
		if res.Status != StatusSucceeded {
			t.Fatalf("p-%d 期望 succeeded, 实际 %s", i, res.Status)
		}
	}
	t.Logf("判定依据: 并发峰值=%d, 上限=%d", peak.Load(), maxConc)
	if peak.Load() > maxConc {
		t.Fatalf("并发峰值 %d 超过上限 %d", peak.Load(), maxConc)
	}
}
