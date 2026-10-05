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
// 并发模型：一个调度器 goroutine 维护就绪集合（按有效优先级 = 静态优先级 +
// 老化提升 调度）与时间门控（NotBefore / 重试退避），在并发额度空闲时把作业
// 派发给固定数量（MaxConcurrency）的 worker 执行；等待依赖、等待时间或退避中
// 的作业只占用在途额度（MaxQueue），不占用并发额度与 worker。
// 所有状态转换在锁内完成并同步持久化，保证重启后已确认的工作不丢失、
// 已完成的工作不重复执行、已确认的依赖不丢。
type Runtime struct {
	cfg   Config
	store Store

	mu         sync.Mutex
	jobs       map[string]*jobState
	dependents map[string][]*jobState // 前置作业 ID -> 正在等它的作业
	active     int                    // 非终态且已受理（占用在途额度）的作业数
	running    int                    // 正在执行（占用并发额度）的作业数
	closed     bool                   //
	persistErr error                  // 首次持久化失败错误，由 Close 返回，不静默丢失
	seq        uint64                 // 就绪入队序号，同有效优先级时先到先服务

	ready    []*jobState    // 已就绪、等待并发额度的作业
	wakeCh   chan struct{}  // 通知调度器重新评估（容量 1，合并信号）
	stopCh   chan struct{}  // 关闭时停止调度器
	dispatch chan *jobState // 调度器 -> worker 的派发通道（容量 = MaxConcurrency）

	schedWg sync.WaitGroup
	wg      sync.WaitGroup // worker
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

	pendingDeps map[string]struct{} // 尚未成功的前置作业 ID
	readyAt     time.Time           // 时间门控（NotBefore / 退避到期），零值表示无
	inReady     bool                // 是否在就绪集合中
	enqueuedAt  time.Time           // 进入就绪集合的时刻（用于优先级老化）
	seq         uint64              // 就绪序号
}

// New 创建运行时并加载持久化状态。
//
// 恢复规则：终态记录原样恢复（重复提交同 ID 会被拒绝，不会重复执行）；
// 非终态记录（崩溃时 pending/running，含等依赖/等时间/等退避的作业）恢复为
// 待恢复状态，调用方以相同 ID 重新 Submit 后从已记录的状态继续执行：
// 已确认的依赖不丢（前置的终态记录仍在），时间门控按原样继续等待。
// 恢复时若发现前置已失败/取消/跳过，该作业直接以 skipped 收尾。
func New(cfg Config) (*Runtime, error) {
	cfg = cfg.withDefaults()
	r := &Runtime{
		cfg:        cfg,
		jobs:       map[string]*jobState{},
		dependents: map[string][]*jobState{},
		wakeCh:     make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
		dispatch:   make(chan *jobState, cfg.MaxConcurrency),
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
					ID:           rec.ID,
					MaxRetries:   rec.MaxRetries,
					Timeout:      rec.Timeout,
					Priority:     rec.Priority,
					DependsOn:    rec.DependsOn,
					NotBefore:    rec.NotBefore,
					RetryBackoff: rec.RetryBackoff,
				},
				status:      rec.Status,
				attempts:    rec.Attempts,
				readyAt:     rec.ReadyAt,
				pendingDeps: map[string]struct{}{},
				done:        make(chan struct{}),
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
		// 第二遍：重建依赖边。前置已终结（非成功）的等待作业直接以
		// skipped 收尾；前置未完成的继续等（含前置尚未提交的情况）。
		for _, js := range r.jobs {
			if js.status.Terminal() {
				continue
			}
			r.linkDepsLocked(js)
		}
		r.persistLocked()
	}
	r.schedWg.Add(1)
	go r.scheduler()
	for i := 0; i < cfg.MaxConcurrency; i++ {
		r.wg.Add(1)
		go r.worker()
	}
	return r, nil
}

// Submit 提交作业。
//
// 返回值可区分：nil 受理；ErrQueueFull 超限；ErrDuplicateJob 重复提交
// （含已完成的同 ID 作业，绝不重复执行）；ErrDependencyCycle 依赖成环
// （含自依赖）；ErrRuntimeClosed 运行时已关闭。
// 对崩溃恢复的同 ID 作业，Submit 视为恢复执行，保留已记录的尝试次数、
// 已确认的依赖与未到期的时间门控。
//
// 依赖语义：DependsOn 中全部前置成功后才调度执行；任一前置失败/被取消/
// 被跳过，本作业立即以 skipped 终态收尾（不执行 Work）；前置尚未提交的，
// 挂起等待其补交并执行。提交时前置已终结（非成功）的，直接以 skipped 收尾。
func (r *Runtime) Submit(job Job) error {
	if job.ID == "" {
		return errors.New("batch: job id is empty")
	}
	if job.Work == nil {
		return errors.New("batch: job work func is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRuntimeClosed
	}
	if js, ok := r.jobs[job.ID]; ok {
		if js.resumed && !js.status.Terminal() {
			// 恢复崩溃前未完成的作业：补全执行体，保留已确认状态。
			js.job = job
			js.resumed = false
			r.active++
			now := time.Now()
			if js.readyAt.Before(now) { // 含零值：持久化的时间门已到期
				js.readyAt = time.Time{}
				if job.NotBefore.After(now) {
					js.readyAt = job.NotBefore
				}
			}
			r.linkDepsLocked(js)
			if !js.status.Terminal() {
				r.persistLocked()
				r.considerLocked(js)
			}
			return nil
		}
		return ErrDuplicateJob
	}
	if r.wouldCycleLocked(job.ID, job.DependsOn) {
		return ErrDependencyCycle
	}
	if r.active >= r.cfg.MaxQueue {
		return ErrQueueFull
	}
	js := &jobState{
		job:         job,
		status:      StatusPending,
		pendingDeps: map[string]struct{}{},
		done:        make(chan struct{}),
	}
	if job.NotBefore.After(time.Now()) {
		js.readyAt = job.NotBefore
	}
	r.jobs[job.ID] = js
	r.active++
	r.linkDepsLocked(js)
	if !js.status.Terminal() {
		r.persistLocked()
		r.considerLocked(js)
	}
	return nil
}

// Cancel 请求取消作业。
//
// 返回值可区分：nil 已接受取消；ErrJobNotFound 不存在；
// ErrJobTerminal 已到终态（状态不再变化）。
// 等待中（含等依赖/等时间/等退避）的作业立即转 canceled；
// 执行中的作业由 Work 的 ctx 收到信号，返回后转 canceled。
// 被取消作业的后继（DependsOn 含它的作业）会以 skipped 收尾。
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
		// 未开始执行：直接转终态。若仍在就绪集合中，调度器取出时会跳过。
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
// 若作业因 Close 而未能在本次进程内到达终态，Wait 返回 canceled
// （持久化记录仍为 pending，下次启动可接续）。
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

// Close 停止接收新作业与新的派发，等待执行中的作业结束后退出全部
// goroutine。等待依赖/时间/退避以及尚未派发的作业不会被静默丢弃：
// 持久化记录保持 pending（含依赖与 ready_at），下次启动可原样接续；
// 进程内它们被标记为 canceled 以唤醒 Wait（该收尾不落盘）。
// 返回首个持久化错误（如有）。
func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRuntimeClosed
	}
	r.closed = true
	close(r.stopCh)
	r.mu.Unlock()

	r.schedWg.Wait()  // 调度器退出，不再派发
	close(r.dispatch) // worker 处理完已派发作业后退出
	r.wg.Wait()       // 执行中作业完成，其状态转换已正常持久化

	r.mu.Lock()
	defer r.mu.Unlock()
	// 未开始的非终态作业：状态文件保留 pending 供下次接续（上面的
	// persistLocked 已落盘），进程内标记 canceled 并唤醒 Wait。
	for _, js := range r.jobs {
		if js.status.Terminal() {
			continue
		}
		js.status = StatusCanceled
		close(js.done)
	}
	return r.persistErr
}

// linkDepsLocked 建立作业的依赖边：已成功的前置直接确认；已终结（非成功）
// 的前置使本作业立即以 skipped 收尾；其余（含前置尚未提交）登记等待。
// 调用方须持有锁。
func (r *Runtime) linkDepsLocked(js *jobState) {
	js.pendingDeps = map[string]struct{}{}
	for _, depID := range js.job.DependsOn {
		dep, ok := r.jobs[depID]
		switch {
		case ok && dep.status == StatusSucceeded:
			// 前置已成功（含重启前已确认的），直接满足。
		case ok && dep.status.Terminal():
			r.skipLocked(js, depID, dep.status)
			return
		default:
			js.pendingDeps[depID] = struct{}{}
			r.dependents[depID] = append(r.dependents[depID], js)
		}
	}
}

// skipLocked 以前置未成功为由将作业转为 skipped 终态。调用方须持有锁。
func (r *Runtime) skipLocked(js *jobState, depID string, depStatus Status) {
	err := Dependency(fmt.Errorf("dependency %q finished as %s: %w", depID, depStatus, ErrDependencyFailed))
	r.finishLocked(js, StatusSkipped, err, false)
}

// considerLocked 在作业可运行（pending、依赖齐备、时间门到期）时将其
// 放入就绪集合并唤醒调度器。调用方须持有锁。
func (r *Runtime) considerLocked(js *jobState) {
	if js.status != StatusPending || js.resumed || js.inReady {
		return
	}
	if len(js.pendingDeps) > 0 {
		return
	}
	now := time.Now()
	if !js.readyAt.IsZero() && js.readyAt.After(now) {
		return // 时间门未到，由调度器定时器接管
	}
	js.readyAt = time.Time{}
	js.inReady = true
	js.enqueuedAt = now
	js.seq = r.seq
	r.seq++
	r.ready = append(r.ready, js)
	r.signalWake()
}

// finishLocked 将作业转为终态、释放在途额度并向后继传播。调用方须持有锁。
// 传播规则：本作业成功 -> 后继确认一个前置，齐备后进入就绪评估；
// 本作业失败/取消/跳过 -> 后继递归地以 skipped 收尾。
func (r *Runtime) finishLocked(js *jobState, st Status, err error, countActive bool) {
	js.status = st
	js.lastErr = err
	if countActive || !js.resumed {
		r.active--
	}
	close(js.done)
	id := js.job.ID
	waiting := r.dependents[id]
	delete(r.dependents, id)
	for _, d := range waiting {
		if d.status.Terminal() {
			continue
		}
		if st == StatusSucceeded {
			delete(d.pendingDeps, id)
			r.considerLocked(d)
		} else {
			r.skipLocked(d, id, st)
		}
	}
	r.persistLocked()
}

// wouldCycleLocked 检查为 id 添加 deps 依赖是否成环：沿“依赖于”边
// （作业 -> 其未决前置）从各前置出发，若能到达 id 则成环。调用方须持有锁。
func (r *Runtime) wouldCycleLocked(id string, deps []string) bool {
	visited := map[string]bool{}
	stack := make([]string, 0, len(deps))
	for _, d := range deps {
		if d == id {
			return true // 自依赖
		}
		stack = append(stack, d)
	}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if cur == id {
			return true
		}
		if visited[cur] {
			continue
		}
		visited[cur] = true
		js, ok := r.jobs[cur]
		if !ok {
			continue // 前置尚未提交，无出边
		}
		for d := range js.pendingDeps {
			stack = append(stack, d)
		}
	}
	return false
}

// scheduler 是唯一的调度 goroutine：把时间门到期的作业转入就绪，
// 在并发额度空闲时按有效优先级派发，并用定时器对齐下一个到期时刻。
func (r *Runtime) scheduler() {
	defer r.schedWg.Done()
	var timer *time.Timer
	var timerC <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		r.mu.Lock()
		now := time.Now()
		// 1. 时间门到期的挂起作业转入就绪，并找出下一个到期时刻。
		var next time.Time
		for _, js := range r.jobs {
			if js.status != StatusPending || js.resumed || js.inReady || len(js.pendingDeps) > 0 {
				continue
			}
			if js.readyAt.IsZero() {
				continue
			}
			if js.readyAt.After(now) {
				if next.IsZero() || js.readyAt.Before(next) {
					next = js.readyAt
				}
				continue
			}
			r.considerLocked(js)
		}
		// 2. 有空闲并发额度时按有效优先级派发。dispatch 容量 =
		// MaxConcurrency 且发送前已预约槽位，发送永不阻塞。
		for !r.closed && r.running < r.cfg.MaxConcurrency {
			js := r.popReadyLocked(now)
			if js == nil {
				break
			}
			js.inReady = false
			js.status = StatusRunning
			r.running++
			r.persistLocked()
			r.dispatch <- js
		}
		// 3. 定时器对齐下一个到期时刻。
		if next.IsZero() {
			if timer != nil {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
			timerC = nil
		} else {
			d := time.Until(next)
			if d < 0 {
				d = 0
			}
			if timer == nil {
				timer = time.NewTimer(d)
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(d)
			}
			timerC = timer.C
		}
		r.mu.Unlock()
		select {
		case <-r.wakeCh:
		case <-timerC:
		case <-r.stopCh:
			return
		}
	}
}

// popReadyLocked 从就绪集合取出有效优先级最高的作业（同优先级按就绪序号
// 先到先服务），并惰性清理已非 pending 的条目。空集合返回 nil。
func (r *Runtime) popReadyLocked(now time.Time) *jobState {
	out := r.ready[:0]
	for _, js := range r.ready {
		if js.inReady && js.status == StatusPending {
			out = append(out, js)
		}
	}
	r.ready = out
	if len(r.ready) == 0 {
		return nil
	}
	best := 0
	bestEff := r.effectivePriority(r.ready[0], now)
	for i := 1; i < len(r.ready); i++ {
		eff := r.effectivePriority(r.ready[i], now)
		if eff > bestEff || (eff == bestEff && r.ready[i].seq < r.ready[best].seq) {
			best, bestEff = i, eff
		}
	}
	js := r.ready[best]
	r.ready = append(r.ready[:best], r.ready[best+1:]...)
	return js
}

// effectivePriority 计算有效优先级：静态优先级 + 就绪等待时长/老化间隔。
// 等待越久提升越多，保证低优先级作业不会被无限压制。
func (r *Runtime) effectivePriority(js *jobState, now time.Time) int {
	eff := js.job.Priority
	if age := now.Sub(js.enqueuedAt); age > 0 {
		eff += int(age / r.cfg.PriorityAging)
	}
	return eff
}

// worker 从派发通道消费作业执行，worker 数量即并发上限。
func (r *Runtime) worker() {
	defer r.wg.Done()
	for js := range r.dispatch {
		r.execute(js)
		r.mu.Lock()
		r.running--
		r.mu.Unlock()
		r.signalWake()
	}
}

// execute 执行单个作业的一次尝试循环：可重试的失败把作业转回 pending
// （按 RetryBackoff 设置时间门控）并重新进入就绪评估，不占用并发额度、
// 不阻塞 worker。
func (r *Runtime) execute(js *jobState) {
	r.mu.Lock()
	if js.cancelRequested {
		// 派发后、执行前收到取消。
		r.finishLocked(js, StatusCanceled, nil, true)
		r.mu.Unlock()
		return
	}
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
		js.lastErr = err
		js.status = StatusPending
		if js.job.RetryBackoff > 0 {
			js.readyAt = time.Now().Add(js.job.RetryBackoff)
		}
		r.persistLocked()
		r.considerLocked(js) // 无退避时立即回到就绪集合
		return
	}
	r.finishLocked(js, StatusFailed, err, true)
}

// signalWake 非阻塞地通知调度器重新评估。
func (r *Runtime) signalWake() {
	select {
	case r.wakeCh <- struct{}{}:
	default:
	}
}

// persistLocked 将当前全部作业状态写入存储。调用方须持有锁。
func (r *Runtime) persistLocked() {
	if r.store == nil {
		return
	}
	sf := &stateFile{Version: stateVersion, Jobs: make(map[string]jobRecord, len(r.jobs))}
	for id, js := range r.jobs {
		rec := jobRecord{
			ID:           id,
			Status:       js.status,
			Attempts:     js.attempts,
			UpdatedAt:    time.Now(),
			MaxRetries:   js.job.MaxRetries,
			Timeout:      js.job.Timeout,
			Priority:     js.job.Priority,
			DependsOn:    js.job.DependsOn,
			NotBefore:    js.job.NotBefore,
			RetryBackoff: js.job.RetryBackoff,
			ReadyAt:      js.readyAt,
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
