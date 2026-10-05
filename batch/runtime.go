package batch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Runtime 是批处理执行运行时。
//
// 并发模型：固定数量（MaxConcurrency）的 worker 经无缓冲 dispatch 通道从
// 调度器接收作业，不存在无界 goroutine；重试与延迟通过调度器重新安排，
// 不放大并发、不阻塞 worker。调度器是唯一的调度决策者：按
// “依赖就绪 → 时间就绪 → 有效优先级（含老化）→ 提交先后”挑选作业，
// 只有 worker 空闲时交接才完成，因此并发上限是硬约束。
// 所有状态转换在锁内完成并同步持久化，保证重启后已确认的工作不丢失、
// 已完成的工作不重复执行。
type Runtime struct {
	cfg   Config
	store Store

	mu         sync.Mutex
	jobs       map[string]*jobState
	active     int   // 非终态且已受理（占用在途额度）的作业数
	idle       int   // 正阻塞在交接通道上的 worker 数；>0 时调度器才做选择
	closed     bool  //
	seq        int64 // 提交序号，同优先级时按先到先服务
	persistErr error // 首次持久化失败错误，由 Close 返回，不静默丢失

	dispatch  chan *jobState // 调度器 → worker 的交接通道（无缓冲，天然限流）
	notify    chan struct{}  // 容量 1 的状态变化信号，唤醒调度器重新评估
	quit      chan struct{}  // Close 时关闭，通知调度器退出
	schedDone chan struct{}  // 调度器退出后关闭
	wg        sync.WaitGroup // worker 退出屏障
}

// jobState 是运行时的作业内部状态。
type jobState struct {
	job             Job
	status          Status
	attempts        int
	lastErr         error
	resumed         bool // 是否为崩溃恢复后等待重新提交的作业（此时 job.Work 为空）
	cancelRequested bool // Cancel 已请求取消（执行中作业由 worker 转终态）
	cancel          context.CancelFunc
	done            chan struct{} // 到达终态时关闭

	notBefore  time.Time // 生效中的最早可调度时间（NotBefore 与退避的合并结果）
	readySince time.Time // 进入“可调度等待”的时刻，优先级老化计时基准
	seq        int64     // 提交序号，调度平局时先到先服务
	dispatched bool      // 已交接给 worker，避免调度器重复选中
}

// New 创建运行时并加载持久化状态。
//
// 恢复规则：终态记录原样恢复（重复提交同 ID 会被拒绝，不会重复执行）；
// 非终态记录（崩溃时 pending/running，含等依赖/等时间/等退避的作业）
// 恢复为待恢复状态，保留已确认的依赖声明、优先级与生效中的最早调度时间，
// 调用方以相同 ID 重新 Submit 后按原安排继续执行。
func New(cfg Config) (*Runtime, error) {
	cfg = cfg.withDefaults()
	r := &Runtime{
		cfg:       cfg,
		jobs:      map[string]*jobState{},
		dispatch:  make(chan *jobState),
		notify:    make(chan struct{}, 1),
		quit:      make(chan struct{}),
		schedDone: make(chan struct{}),
	}
	if cfg.StatePath != "" {
		r.store = NewFileStore(cfg.StatePath)
		sf, err := r.store.Load()
		if err != nil {
			return nil, err
		}
		for id, rec := range sf.Jobs {
			js := &jobState{
				job: Job{
					ID:        rec.ID,
					Priority:  rec.Priority,
					DependsOn: rec.DependsOn,
				},
				status:    rec.Status,
				attempts:  rec.Attempts,
				notBefore: rec.NotBefore,
				done:      make(chan struct{}),
			}
			if rec.LastError != "" {
				js.lastErr = errors.New(rec.LastError)
			}
			if rec.Status.Terminal() {
				close(js.done)
			} else {
				// 崩溃时未完成的作业：恢复为 pending，等待同 ID 重新提交。
				js.status = StatusPending
				js.resumed = true
			}
			r.jobs[id] = js
		}
	}
	for i := 0; i < cfg.MaxConcurrency; i++ {
		r.wg.Add(1)
		go r.worker()
	}
	go r.scheduler()
	return r, nil
}

// Submit 提交作业。
//
// 返回值可区分：nil 受理；ErrQueueFull 超限；ErrDuplicateJob 重复提交
// （含已完成的同 ID 作业，绝不重复执行）；ErrDependencyCycle 依赖成环；
// ErrRuntimeClosed 运行时已关闭。
// 对崩溃恢复的同 ID 作业，Submit 视为恢复执行，保留已记录的尝试次数、
// 依赖声明与生效中的最早调度时间。
// 若提交时前置已确定失败/被取消，作业被受理并立即以 skipped 收尾。
func (r *Runtime) Submit(job Job) error {
	if job.ID == "" {
		return errors.New("batch: job id is empty")
	}
	if job.Work == nil {
		return errors.New("batch: job work func is nil")
	}
	for _, d := range job.DependsOn {
		if d == "" {
			return errors.New("batch: dependency id is empty")
		}
		if d == job.ID {
			return ErrDependencyCycle
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRuntimeClosed
	}
	if js, ok := r.jobs[job.ID]; ok {
		if js.resumed && !js.status.Terminal() {
			// 恢复崩溃前未完成的作业：补全执行体；调用方未重复声明的
			// 依赖沿用持久化记录，生效中的等待安排不丢。
			if len(job.DependsOn) == 0 {
				job.DependsOn = js.job.DependsOn
			}
			if r.hasCycleLocked(job.ID, job.DependsOn) {
				return ErrDependencyCycle
			}
			js.job = job
			if js.notBefore.Before(job.NotBefore) {
				js.notBefore = job.NotBefore
			}
			js.resumed = false
			js.readySince = time.Now()
			js.seq = r.nextSeqLocked()
			r.active++
			r.persistLocked()
			r.signal()
			return nil
		}
		return ErrDuplicateJob
	}
	if r.active >= r.cfg.MaxQueue {
		return ErrQueueFull
	}
	if r.hasCycleLocked(job.ID, job.DependsOn) {
		return ErrDependencyCycle
	}
	now := time.Now()
	js := &jobState{
		job:        job,
		status:     StatusPending,
		notBefore:  job.NotBefore,
		readySince: now,
		seq:        r.nextSeqLocked(),
		done:       make(chan struct{}),
	}
	r.jobs[job.ID] = js
	r.active++
	// 提交时前置已处于非成功终态：立即以 skipped 收尾（可区分于执行失败）。
	if dep, st := r.unsatisfiableDepLocked(js); dep != "" {
		r.finishLocked(js, StatusSkipped, depSkippedErr(dep, st), false)
		return nil
	}
	r.persistLocked()
	r.signal()
	return nil
}

// Cancel 请求取消作业。
//
// 返回值可区分：nil 已接受取消；ErrJobNotFound 不存在；
// ErrJobTerminal 已到终态（状态不再变化）。
// 等待依赖/时间/退避的作业同为 pending，取消立即生效并向下游传播 skipped。
func (r *Runtime) Cancel(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	js, ok := r.jobs[id]
	if !ok {
		return ErrJobNotFound
	}
	if js.status.Terminal() {
		return ErrJobTerminal
	}
	switch js.status {
	case StatusPending:
		// 未开始执行：直接转终态。若已被调度器选中，worker 取出时会跳过。
		r.finishLocked(js, StatusCanceled, nil, false)
	case StatusRunning:
		// 执行中：记录取消意图并传播取消信号，由 worker 在 Work 返回后转终态。
		js.cancelRequested = true
		if js.cancel != nil {
			js.cancel()
		}
	}
	return nil
}

// Status 查询作业当前状态；第二个返回值表示作业是否已知。
func (r *Runtime) Status(id string) (Status, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	js, ok := r.jobs[id]
	if !ok {
		return "", false
	}
	return js.status, true
}

// Wait 阻塞等待作业到达终态并返回与实际执行一致的结果。
func (r *Runtime) Wait(id string) (Result, error) {
	r.mu.Lock()
	js, ok := r.jobs[id]
	r.mu.Unlock()
	if !ok {
		return Result{}, ErrJobNotFound
	}
	<-js.done
	r.mu.Lock()
	defer r.mu.Unlock()
	return Result{
		JobID:    id,
		Status:   js.status,
		Attempts: js.attempts,
		Err:      js.lastErr,
	}, nil
}

// Close 停止接收新作业与新的调度，等待正在执行的作业结束后退出全部
// goroutine。等待依赖/时间/退避的作业保持已受理状态并持久化，
// 不取消、不丢弃，下次启动时按原安排接续处理。
func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRuntimeClosed
	}
	r.closed = true
	r.mu.Unlock()
	close(r.quit) // 调度器退出（可能正阻塞在交接上，quit 保证可唤醒）
	<-r.schedDone // 确认调度器已退出，之后关闭 dispatch 是安全的
	close(r.dispatch)
	r.wg.Wait() // 等待执行中的作业完成
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.persistErr
}

// scheduler 是唯一的调度决策者。每次唤醒重新评估全部作业，
// 仅在有 worker 空闲等待交接时，才从“依赖就绪且到点”的作业中选出
// 有效优先级最高者（平局按提交先后）立即交接——选择在交接前一刻做出，
// 不会提前锁定某个作业而挡住后来的更高优先级作业。
func (r *Runtime) scheduler() {
	defer close(r.schedDone)
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return
		}
		now := time.Now()
		var best *jobState
		var nextWake time.Time
		for _, js := range r.jobs {
			if js.status != StatusPending || js.resumed || js.dispatched {
				continue
			}
			if !r.depsSatisfiedLocked(js) {
				continue
			}
			if js.notBefore.After(now) {
				// 到点再跑/退避中的作业不占用执行资源，仅登记最近唤醒点。
				if nextWake.IsZero() || js.notBefore.Before(nextWake) {
					nextWake = js.notBefore
				}
				continue
			}
			if best == nil || r.betterLocked(js, best, now) {
				best = js
			}
		}
		if best != nil && r.idle > 0 {
			best.dispatched = true
			r.mu.Unlock()
			select {
			case r.dispatch <- best: // 有空闲 worker 等待接收，交接即刻完成
			case <-r.quit:
				return
			}
			continue
		}
		r.mu.Unlock()
		var timer *time.Timer
		var timerC <-chan time.Time
		if !nextWake.IsZero() {
			timer = time.NewTimer(time.Until(nextWake))
			timerC = timer.C
		}
		select {
		case <-r.notify:
		case <-timerC:
		case <-r.quit:
			if timer != nil {
				timer.Stop()
			}
			return
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// worker 从交接通道消费作业，worker 数量即并发上限。
// 取作业前先登记空闲并唤醒调度器，保证调度选择在交接前一刻做出。
func (r *Runtime) worker() {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		r.idle++
		r.mu.Unlock()
		r.signal()
		js, ok := <-r.dispatch
		r.mu.Lock()
		r.idle--
		r.mu.Unlock()
		if !ok {
			return
		}
		r.execute(js)
	}
}

// execute 执行单个作业的一次尝试。可重试的瞬时失败不原地循环，
// 而是按 RetryBackoff 设定生效时间后交还调度器，退避期间不占用 worker。
func (r *Runtime) execute(js *jobState) {
	r.mu.Lock()
	if js.status != StatusPending {
		// 交接后被取消：跳过执行。
		r.mu.Unlock()
		return
	}
	js.status = StatusRunning
	js.attempts++
	ctx := context.Background()
	if js.job.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, js.job.Timeout)
		js.cancel = cancel
	} else {
		ctx, js.cancel = context.WithCancel(ctx)
	}
	r.persistLocked()
	r.mu.Unlock()

	err := js.job.Work(ctx, &js.job)

	r.mu.Lock()
	cancel := js.cancel
	js.cancel = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel() // 释放 timer/goroutine 资源
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if js.cancelRequested {
		// 执行期间被取消：以取消为最终状态，不重试。
		r.finishLocked(js, StatusCanceled, nil, true)
		return
	}
	if err == nil {
		r.finishLocked(js, StatusSucceeded, nil, true)
		return
	}
	kind := Classify(err)
	retryable := kind == KindTransient && js.attempts <= js.job.MaxRetries
	if retryable {
		// 退避重试：设定生效时间后交还调度器，期间不占用执行资源。
		js.lastErr = err
		js.status = StatusPending
		js.dispatched = false
		if js.job.RetryBackoff > 0 {
			js.notBefore = time.Now().Add(js.job.RetryBackoff)
			js.readySince = js.notBefore
		} else {
			js.notBefore = time.Time{}
			js.readySince = time.Now()
		}
		r.persistLocked()
		r.signal()
		return
	}
	r.finishLocked(js, StatusFailed, err, true)
}

// finishLocked 将作业转为终态、释放在途额度并向下游传播。调用方须持有锁。
func (r *Runtime) finishLocked(js *jobState, st Status, err error, countActive bool) {
	js.status = st
	js.lastErr = err
	if countActive || !js.resumed {
		r.active--
	}
	close(js.done)
	r.propagateDepsLocked(js)
	r.persistLocked()
	r.signal()
}

// propagateDepsLocked 在 js 到达终态后处理等待它的作业：js 非成功终态时，
// 直接下游以 skipped 收尾（递归传播，覆盖依赖链）；成功时无需动作，
// 等待中的下游由调度器在下一次评估时自动接上。调用方须持有锁。
func (r *Runtime) propagateDepsLocked(js *jobState) {
	if js.status == StatusSucceeded {
		return
	}
	for _, w := range r.jobs {
		if w.status != StatusPending {
			continue
		}
		if !dependsOn(w, js.job.ID) {
			continue
		}
		r.finishLocked(w, StatusSkipped, depSkippedErr(js.job.ID, js.status), false)
	}
}

// depsSatisfiedLocked 报告作业的全部前置是否均已成功。
// 前置未提交或未完成时继续等待。调用方须持有锁。
func (r *Runtime) depsSatisfiedLocked(js *jobState) bool {
	for _, d := range js.job.DependsOn {
		dep, ok := r.jobs[d]
		if !ok || dep.status != StatusSucceeded {
			return false
		}
	}
	return true
}

// unsatisfiableDepLocked 返回第一个已处于非成功终态的前置；
// 全部前置成功/进行中/未提交时返回空串。调用方须持有锁。
func (r *Runtime) unsatisfiableDepLocked(js *jobState) (string, Status) {
	for _, d := range js.job.DependsOn {
		if dep, ok := r.jobs[d]; ok && dep.status.Terminal() && dep.status != StatusSucceeded {
			return d, dep.status
		}
	}
	return "", ""
}

// hasCycleLocked 检测“jobID 依赖 deps”是否会在已知作业的依赖关系上成环。
// 尚未提交的前置没有已声明的下游，不参与判定。调用方须持有锁。
func (r *Runtime) hasCycleLocked(jobID string, deps []string) bool {
	visited := map[string]bool{}
	stack := append([]string(nil), deps...)
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if cur == jobID {
			return true
		}
		if visited[cur] {
			continue
		}
		visited[cur] = true
		if js, ok := r.jobs[cur]; ok {
			stack = append(stack, js.job.DependsOn...)
		}
	}
	return false
}

// betterLocked 报告 a 是否应比 b 先调度：有效优先级高者优先，平局先到先服务。
func (r *Runtime) betterLocked(a, b *jobState, now time.Time) bool {
	ea, eb := r.effectivePriorityLocked(a, now), r.effectivePriorityLocked(b, now)
	if ea != eb {
		return ea > eb
	}
	return a.seq < b.seq
}

// effectivePriorityLocked 计算含老化加成的有效优先级：
// 等待每满一个 PriorityAging 间隔 +1，保证低优先级作业终会被调度。
func (r *Runtime) effectivePriorityLocked(js *jobState, now time.Time) int {
	p := js.job.Priority
	if r.cfg.PriorityAging > 0 {
		if wait := now.Sub(js.readySince); wait > 0 {
			p += int(wait / r.cfg.PriorityAging)
		}
	}
	return p
}

// nextSeqLocked 分配单调递增的提交序号。调用方须持有锁。
func (r *Runtime) nextSeqLocked() int64 {
	r.seq++
	return r.seq
}

// signal 非阻塞地通知调度器重新评估。
func (r *Runtime) signal() {
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

// dependsOn 报告 js 是否声明了对 id 的依赖。
func dependsOn(js *jobState, id string) bool {
	for _, d := range js.job.DependsOn {
		if d == id {
			return true
		}
	}
	return false
}

// depSkippedErr 构造下游作业被跳过的可区分错误（分类为 KindDependency）。
func depSkippedErr(depID string, st Status) error {
	return Dependency(fmt.Errorf("dependency %q finished with status %s", depID, st))
}

// persistLocked 将当前全部作业状态写入存储。调用方须持有锁。
func (r *Runtime) persistLocked() {
	if r.store == nil {
		return
	}
	sf := &stateFile{Version: stateVersion, Jobs: make(map[string]jobRecord, len(r.jobs))}
	for id, js := range r.jobs {
		rec := jobRecord{
			ID:        id,
			Status:    js.status,
			Attempts:  js.attempts,
			Priority:  js.job.Priority,
			DependsOn: js.job.DependsOn,
			NotBefore: js.notBefore,
			UpdatedAt: time.Now(),
		}
		if js.lastErr != nil {
			rec.LastError = js.lastErr.Error()
		}
		sf.Jobs[id] = rec
	}
	// 持久化失败不静默：记录到作业错误中会导致状态不一致，
	// 这里选择暴露给调用方——但 persist 在锁内被多处调用，
	// 简单起见保存首次错误供 Close 返回。
	if err := r.store.Save(sf); err != nil && r.persistErr == nil {
		r.persistErr = err
	}
}
