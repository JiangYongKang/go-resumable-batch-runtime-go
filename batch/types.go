// Package batch 提供可在进程重启后继续运行的本地批处理执行子系统。
package batch

import (
	"context"
	"time"
)

// Status 表示作业的生命周期状态。
type Status string

const (
	StatusPending   Status = "pending"   // 已受理，等待调度（含等依赖/等时间/等退避）
	StatusRunning   Status = "running"   // 正在执行
	StatusSucceeded Status = "succeeded" // 执行成功（终态）
	StatusFailed    Status = "failed"    // 永久失败或重试耗尽（终态）
	StatusCanceled  Status = "canceled"  // 被取消（终态）
	StatusSkipped   Status = "skipped"   // 前置失败/被取消，未执行即收尾（终态，与执行失败可区分）
)

// Terminal 报告状态是否为终态。
func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCanceled, StatusSkipped:
		return true
	}
	return false
}

// WorkFunc 是作业的执行体。ctx 携带取消与超时信号。
type WorkFunc func(ctx context.Context, job *Job) error

// Job 描述一个待执行的作业。
type Job struct {
	ID           string        // 业务唯一标识，重复提交同一 ID 会被拒绝
	MaxRetries   int           // 瞬时失败允许的最大重试次数
	Timeout      time.Duration // 单次执行（含每次重试）的超时，0 表示不限制
	Priority     int           // 调度优先级，数值越大越优先；同优先级按提交先后，0 为默认
	DependsOn    []string      // 前置作业 ID 列表，全部成功后才可执行；任一前置非成功终态则本作业 skipped
	NotBefore    time.Time     // 最早开始时间，零值表示立即可调度；到点前不占用执行资源
	RetryBackoff time.Duration // 瞬时失败重试之间的间隔，0 表示立即重试；退避期间不占用执行资源
	Work         WorkFunc      `json:"-"`
}

// Result 是作业的对外汇总结果，与实际执行结果一致。
type Result struct {
	JobID    string
	Status   Status
	Attempts int
	Err      error
}

// Config 是运行时的资源与行为约束。
type Config struct {
	MaxConcurrency int    // 同时执行的作业数上限，<=0 时取默认值
	MaxQueue       int    // 等待队列上限（含运行中），<=0 时取默认值
	StatePath      string // 状态持久化文件路径，空表示不持久化
	// PriorityAging 是优先级老化间隔：等待每满一个间隔，有效优先级 +1，
	// 防止低优先级作业被无限压制；<=0 表示不做老化（纯优先级+FIFO）。
	PriorityAging time.Duration
}

// 默认资源上限。
const (
	DefaultMaxConcurrency = 4
	DefaultMaxQueue       = 64
)

func (c Config) withDefaults() Config {
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = DefaultMaxConcurrency
	}
	if c.MaxQueue <= 0 {
		c.MaxQueue = DefaultMaxQueue
	}
	return c
}
