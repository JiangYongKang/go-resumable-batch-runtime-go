package batch

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// Runtime 是批处理执行子系统：提交、并发执行、取消、持久化与恢复。
type Runtime struct {
	cfg     Config
	handler Handler
	store   *Store

	mu      sync.Mutex
	records map[string]Record
	order   []string // 待调度队列（FIFO），保存 job ID

	sem       chan struct{} // 并发许可，容量 = MaxConcurrency
	cancels   map[string]context.CancelFunc
	cancelReq map[string]bool // 标记由 Cancel 主动发起的取消

	closed bool
	wg     sync.WaitGroup
	wake   chan struct{} // 调度唤醒信号（容量 1，非阻塞发送）
}

// New 创建 Runtime 并从状态文件恢复未完成的作业。
// 恢复时：终态记录保持不变（绝不重复执行）；
// pending/running 记录重置为 pending 并按更新时间顺序重新入队。
func New(cfg Config, h Handler) (*Runtime, error) {
	cfg = cfg.withDefaults()
	r := &Runtime{
		cfg:       cfg,
		handler:   h,
		records:   map[string]Record{},
		sem:       make(chan struct{}, cfg.MaxConcurrency),
		cancels:   map[string]context.CancelFunc{},
		cancelReq: map[string]bool{},
		wake:      make(chan struct{}, 1),
	}
	if cfg.StatePath != "" {
		r.store = NewStore(cfg.StatePath)
		records, err := r.store.Load()
		if err != nil {
			return nil, err
		}
		r.records = records
	}

	// 恢复：非终态作业重置为 pending 重新入队，按更新时间排序保证 FIFO。
	var pending []Record
	for id, rec := range r.records {
		if rec.Status.Terminal() {
			continue
		}
		rec.Status = StatusPending
		r.records[id] = rec
		pending = append(pending, rec)
	}
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].UpdatedAt.Before(pending[j].UpdatedAt)
	})
	for _, rec := range pending {
		r.order = append(r.order, rec.Job.ID)
	}
	if r.store != nil && len(pending) > 0 {
		if err := r.store.Save(r.records); err != nil {
			return nil, err
		}
	}

	r.wg.Add(1)
	go r.scheduler()
	return r, nil
}

// Submit 提交作业。重复 ID 返回 ErrDuplicateJob；
// 队列满返回 ErrQueueFull；已关闭返回 ErrRuntimeClosed。
func (r *Runtime) Submit(job Job) error {
	if job.ID == "" {
		return errors.New("batch: 作业 ID 不能为空")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRuntimeClosed
	}
	if _, ok := r.records[job.ID]; ok {
		return ErrDuplicateJob
	}
	// 资源上限：非终态作业数不得超过 MaxQueue。
	inflight := 0
	for _, rec := range r.records {
		if !rec.Status.Terminal() {
			inflight++
		}
	}
	if inflight >= r.cfg.MaxQueue {
		return ErrQueueFull
	}
	r.records[job.ID] = Record{
		Job:       job,
		Status:    StatusPending,
		UpdatedAt: time.Now(),
	}
	r.order = append(r.order, job.ID)
	r.persistLocked()
	r.signal()
	return nil
}

// Cancel 取消作业。等待中的作业直接转为 cancelled；
// 运行中的作业通过 context 传播取消。终态作业返回 ErrNotCancellable。
func (r *Runtime) Cancel(id string) error {
	r.mu.Lock()
	rec, ok := r.records[id]
	if !ok {
		r.mu.Unlock()
		return ErrJobNotFound
	}
	if rec.Status.Terminal() {
		r.mu.Unlock()
		return ErrNotCancellable
	}
	r.cancelReq[id] = true
	if rec.Status == StatusPending {
		// 从待调度队列移除，直接置为终态。
		for i, qid := range r.order {
			if qid == id {
				r.order = append(r.order[:i], r.order[i+1:]...)
				break
			}
		}
		rec.Status = StatusCancelled
		rec.UpdatedAt = time.Now()
		r.records[id] = rec
		r.persistLocked()
		r.mu.Unlock()
		return nil
	}
	// 运行中：通过 cancel func 传播，由 worker 完成状态转换。
	cancel := r.cancels[id]
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

// Status 查询作业当前状态。
func (r *Runtime) Status(id string) (Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.records[id]
	if !ok {
		return Record{}, ErrJobNotFound
	}
	return rec, nil
}

// Results 返回所有已到达终态作业的结果汇总，与实际执行结果一致。
func (r *Runtime) Results() []Result {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Result
	for id, rec := range r.records {
		if !rec.Status.Terminal() {
			continue
		}
		out = append(out, Result{
			JobID:    id,
			Status:   rec.Status,
			Attempts: rec.Attempts,
			Failure:  rec.Failure,
			Err:      rec.LastError,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].JobID < out[j].JobID })
	return out
}

// Close 停止接收新作业，取消运行中的作业，等待退出并落盘最终状态。
// 未开始的 pending 作业保持 pending 并持久化，供下次启动恢复。
func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	for id, cancel := range r.cancels {
		r.cancelReq[id] = true
		cancel()
	}
	r.mu.Unlock()
	r.signal()
	r.wg.Wait()

	r.mu.Lock()
	defer r.mu.Unlock()
	r.persistLocked()
	return nil
}

// scheduler 是唯一的调度 goroutine：在并发许可可用时按 FIFO 派发作业。
func (r *Runtime) scheduler() {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return
		}
		semFull := false
		for len(r.order) > 0 && !semFull {
			select {
			case r.sem <- struct{}{}:
			default:
				// 并发许可耗尽，等待 worker 释放后唤醒。
				semFull = true
			}
			if semFull {
				break
			}
			id := r.order[0]
			r.order = r.order[1:]
			rec := r.records[id]
			rec.Status = StatusRunning
			rec.Attempts++
			rec.UpdatedAt = time.Now()
			r.records[id] = rec
			r.persistLocked()
			r.wg.Add(1)
			go r.run(rec.Job)
		}
		r.mu.Unlock()
		r.wait()
	}
}

// wait 阻塞直到被唤醒（提交、worker 完成或关闭）。
func (r *Runtime) wait() { <-r.wake }

// signal 非阻塞地唤醒调度器。
func (r *Runtime) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// run 执行单个作业的一次尝试，并根据结果做状态转换与重试。
func (r *Runtime) run(job Job) {
	defer r.wg.Done()
	defer func() {
		<-r.sem // 释放并发许可
		r.signal()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	if job.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, job.Timeout)
	}
	r.mu.Lock()
	r.cancels[job.ID] = cancel
	r.mu.Unlock()

	err := r.handler(jobContext{ctx}, job)

	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.cancels, job.ID)
	cancel()

	rec := r.records[job.ID]
	rec.UpdatedAt = time.Now()

	// 主动取消优先于一切执行结果。
	if r.cancelReq[job.ID] {
		delete(r.cancelReq, job.ID)
		rec.Status = StatusCancelled
		rec.Failure = FailureNone
		rec.LastError = ""
		r.records[job.ID] = rec
		r.persistLocked()
		return
	}

	if err == nil {
		rec.Status = StatusSucceeded
		rec.Failure = FailureNone
		rec.LastError = ""
		r.records[job.ID] = rec
		r.persistLocked()
		return
	}

	kind := classify(err)
	rec.Failure = kind
	rec.LastError = err.Error()

	// 仅瞬时失败可重试；重试重新入队尾，不放大并发、不破坏 FIFO 顺序。
	if kind == FailureTransient && rec.Attempts <= r.cfg.MaxRetries {
		rec.Status = StatusPending
		r.records[job.ID] = rec
		r.order = append(r.order, job.ID)
		r.persistLocked()
		r.mu.Unlock()
		// 退避在锁外进行，避免阻塞调度。
		time.Sleep(r.cfg.RetryBackoff * time.Duration(rec.Attempts))
		r.signal()
		r.mu.Lock()
		return
	}

	rec.Status = StatusFailed
	r.records[job.ID] = rec
	r.persistLocked()
}

// classify 将 handler 返回的错误归类为失败类型。
func classify(err error) FailureKind {
	var f *Failure
	if errors.As(err, &f) {
		return f.Kind
	}
	// 超时视为瞬时失败，可重试。
	if errors.Is(err, context.DeadlineExceeded) {
		return FailureTransient
	}
	// 未分类错误按永久失败处理，避免无限重试。
	return FailurePermanent
}

// persistLocked 在持有 r.mu 时持久化当前状态；未配置存储时为空操作。
// 持久化失败不阻断执行，但状态已保留在内存中。
func (r *Runtime) persistLocked() {
	if r.store != nil {
		_ = r.store.Save(r.records)
	}
}

// jobContext 把 context.Context 适配为 Handler 使用的 Context 接口。
type jobContext struct{ ctx context.Context }

func (c jobContext) Done() <-chan struct{} { return c.ctx.Done() }
func (c jobContext) Err() error            { return c.ctx.Err() }
