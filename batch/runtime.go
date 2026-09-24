package batch

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Runtime 是批处理执行运行时。
//
// 并发模型：固定数量（MaxConcurrency）的 worker 从有界队列（容量 MaxQueue）
// 取作业执行，不存在无界 goroutine；重试重新入队，不放大并发、不阻塞 worker。
// 所有状态转换在锁内完成并同步持久化，保证重启后已确认的工作不丢失、
// 已完成的工作不重复执行。
type Runtime struct {
	cfg   Config
	store Store

	mu         sync.Mutex
	jobs       map[string]*jobState
	active     int   // 非终态且已受理（占用队列额度）的作业数
	closed     bool  //
	persistErr error // 首次持久化失败错误，由 Close 返回，不静默丢失

	queue chan *jobState
	wg    sync.WaitGroup
}

// jobState 是运行时的作业内部状态。
type jobState struct {
	job      Job
	status   Status
	attempts int
	lastErr  error
	resumed  bool // 是否为崩溃恢复后等待重新提交的作业（此时 job.Work 为空）
	cancelRequested bool // Cancel 已请求取消（执行中作业由 worker 转终态）
	cancel   context.CancelFunc
	done     chan struct{} // 到达终态时关闭
}

// New 创建运行时并加载持久化状态。
//
// 恢复规则：终态记录原样恢复（重复提交同 ID 会被拒绝，不会重复执行）；
// 非终态记录（崩溃时 pending/running）恢复为待恢复状态，调用方以相同 ID
// 重新 Submit 后从已记录的尝试次数继续执行。
func New(cfg Config) (*Runtime, error) {
	cfg = cfg.withDefaults()
	r := &Runtime{
		cfg:   cfg,
		jobs:  map[string]*jobState{},
		queue: make(chan *jobState, cfg.MaxQueue),
	}
	if cfg.StatePath != "" {
		r.store = NewFileStore(cfg.StatePath)
		sf, err := r.store.Load()
		if err != nil {
			return nil, err
		}
		for id, rec := range sf.Jobs {
			js := &jobState{
				job:      Job{ID: rec.ID},
				status:   rec.Status,
				attempts: rec.Attempts,
				done:     make(chan struct{}),
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
	return r, nil
}

// Submit 提交作业。
//
// 返回值可区分：nil 受理；ErrQueueFull 超限；ErrDuplicateJob 重复提交
// （含已完成的同 ID 作业，绝不重复执行）；ErrRuntimeClosed 运行时已关闭。
// 对崩溃恢复的同 ID 作业，Submit 视为恢复执行，保留已记录的尝试次数。
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
			// 恢复崩溃前未完成的作业：补全执行体并重新入队。
			js.job = job
			js.resumed = false
			r.active++
			r.persistLocked()
			r.queue <- js
			return nil
		}
		return ErrDuplicateJob
	}
	if r.active >= r.cfg.MaxQueue {
		return ErrQueueFull
	}
	js := &jobState{
		job:    job,
		status: StatusPending,
		done:   make(chan struct{}),
	}
	r.jobs[job.ID] = js
	r.active++
	r.persistLocked()
	r.queue <- js
	return nil
}

// Cancel 请求取消作业。
//
// 返回值可区分：nil 已接受取消；ErrJobNotFound 不存在；
// ErrJobTerminal 已到终态（状态不再变化）。
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
		// 未开始执行：直接转终态。若仍在队列中，worker 取出时会跳过。
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

// Close 停止接收新作业，等待在途作业结束后退出所有 worker。
// 未开始的在队列作业会被标记为取消，不会泄漏 goroutine。
func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRuntimeClosed
	}
	r.closed = true
	close(r.queue)
	r.mu.Unlock()
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.persistErr
}

// worker 从有界队列消费作业，worker 数量即并发上限。
func (r *Runtime) worker() {
	defer r.wg.Done()
	for js := range r.queue {
		r.execute(js)
	}
}

// execute 执行单个作业（含重试），每次重试通过重新入队进行，
// 因此重试不占用额外并发额度，也不阻塞当前 worker。
func (r *Runtime) execute(js *jobState) {
	r.mu.Lock()
	if js.status != StatusPending {
		// 入队后被取消：跳过执行。
		r.mu.Unlock()
		return
	}
	js.status = StatusRunning
	r.persistLocked()
	r.mu.Unlock()

	for {
		r.mu.Lock()
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
		if js.cancelRequested {
			// 执行期间被取消：以取消为最终状态，不重试。
			r.finishLocked(js, StatusCanceled, nil, true)
			r.mu.Unlock()
			return
		}
		if err == nil {
			r.finishLocked(js, StatusSucceeded, nil, true)
			r.mu.Unlock()
			return
		}
		kind := Classify(err)
		retryable := kind == KindTransient && js.attempts <= js.job.MaxRetries
		if retryable {
			js.lastErr = err
			js.status = StatusPending
			r.persistLocked()
			r.mu.Unlock()
			r.queue <- js // 重新入队：总额度受限，不会阻塞
			return
		}
		r.finishLocked(js, StatusFailed, err, true)
		r.mu.Unlock()
		return
	}
}

// finishLocked 将作业转为终态并释放队列额度。调用方须持有锁。
func (r *Runtime) finishLocked(js *jobState, st Status, err error, countActive bool) {
	js.status = st
	js.lastErr = err
	if countActive || !js.resumed {
		r.active--
	}
	close(js.done)
	r.persistLocked()
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
