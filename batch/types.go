// Package batch 提供可在进程重启后继续运行的本地批处理执行子系统。
package batch

import (
	"errors"
	"time"
)

// Status 表示作业的生命周期状态。
type Status string

const (
	StatusPending   Status = "pending"   // 已入队，等待调度
	StatusRunning   Status = "running"   // 正在执行
	StatusSucceeded Status = "succeeded" // 执行成功（终态）
	StatusFailed    Status = "failed"    // 执行失败且不可重试（终态）
	StatusCancelled Status = "cancelled" // 已被取消（终态）
)

// Terminal 报告状态是否为终态。
func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	}
	return false
}

// FailureKind 失败分类。
type FailureKind string

const (
	FailureNone       FailureKind = ""           // 无失败
	FailureTransient  FailureKind = "transient"  // 瞬时失败，可重试
	FailurePermanent  FailureKind = "permanent"  // 永久失败，不重试
	FailureDependency FailureKind = "dependency" // 依赖失败，不重试
)

// Failure 是 Handler 返回的带分类错误。
type Failure struct {
	Kind FailureKind
	Err  error
}

func (f *Failure) Error() string { return f.Err.Error() }
func (f *Failure) Unwrap() error { return f.Err }

// Transient 构造瞬时失败。
func Transient(err error) *Failure { return &Failure{Kind: FailureTransient, Err: err} }

// Permanent 构造永久失败。
func Permanent(err error) *Failure { return &Failure{Kind: FailurePermanent, Err: err} }

// Dependency 构造依赖失败。
func Dependency(err error) *Failure { return &Failure{Kind: FailureDependency, Err: err} }

// Job 是提交给运行时的作业描述。
type Job struct {
	ID      string        // 业务唯一标识，重复提交将被拒绝
	Payload string        //  opaque 负载，由 Handler 解释
	Timeout time.Duration // 单次尝试的超时，0 表示不限制
}

// Record 是作业的持久化状态记录。
type Record struct {
	Job       Job         `json:"job"`
	Status    Status      `json:"status"`
	Attempts  int         `json:"attempts"`
	Failure   FailureKind `json:"failure,omitempty"`
	LastError string      `json:"last_error,omitempty"`
	UpdatedAt time.Time   `json:"updated_at"`
}

// Result 是作业终态的对外结果视图。
type Result struct {
	JobID    string
	Status   Status
	Attempts int
	Failure  FailureKind
	Err      string
}

// 对外可区分的错误。
var (
	ErrQueueFull         = errors.New("batch: 队列已满，拒绝接收新作业")
	ErrDuplicateJob      = errors.New("batch: 重复提交的作业 ID")
	ErrJobNotFound       = errors.New("batch: 作业不存在")
	ErrNotCancellable    = errors.New("batch: 作业已处于终态，无法取消")
	ErrRuntimeClosed     = errors.New("batch: 运行时已关闭")
	ErrIncompatibleState = errors.New("batch: 状态数据版本不兼容，拒绝加载")
	ErrCorruptState      = errors.New("batch: 状态数据损坏，无法安全解析")
)

// Config 运行时配置。
type Config struct {
	MaxConcurrency int           // 并发执行上限，<=0 时取 1
	MaxQueue       int           // 待处理作业上限，<=0 时取 64
	MaxRetries     int           // 瞬时失败的最大重试次数
	RetryBackoff   time.Duration // 重试基础退避
	StatePath      string        // 状态文件路径，空表示不持久化
}

func (c Config) withDefaults() Config {
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = 1
	}
	if c.MaxQueue <= 0 {
		c.MaxQueue = 64
	}
	if c.MaxRetries < 0 {
		c.MaxRetries = 0
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 10 * time.Millisecond
	}
	return c
}

// Handler 是作业执行函数。返回 nil 表示成功；
// 返回 *Failure 按其 Kind 分类处理；返回其他 error 视为永久失败。
type Handler func(ctx Context, job Job) error

// Context 是传给 Handler 的上下文接口（取消与超时传播）。
type Context interface {
	Done() <-chan struct{}
	Err() error
}
