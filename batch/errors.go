package batch

import "errors"

// Kind 是失败分类。
type Kind int

const (
	KindTransient  Kind = iota // 瞬时失败：可重试
	KindPermanent              // 永久失败：不重试，直接终态
	KindDependency             // 依赖失败：上游未就绪，不重试且明确区分
)

func (k Kind) String() string {
	switch k {
	case KindTransient:
		return "transient"
	case KindPermanent:
		return "permanent"
	case KindDependency:
		return "dependency"
	}
	return "unknown"
}

// KindError 携带失败分类的错误。
type KindError struct {
	Kind Kind
	Err  error
}

func (e *KindError) Error() string { return e.Err.Error() }
func (e *KindError) Unwrap() error { return e.Err }

// Transient 标记一个瞬时失败错误。
func Transient(err error) error { return &KindError{Kind: KindTransient, Err: err} }

// Permanent 标记一个永久失败错误。
func Permanent(err error) error { return &KindError{Kind: KindPermanent, Err: err} }

// Dependency 标记一个依赖失败错误。
func Dependency(err error) error { return &KindError{Kind: KindDependency, Err: err} }

// Classify 判定错误的失败分类；未标记的错误默认按瞬时失败处理。
func Classify(err error) Kind {
	var ke *KindError
	if errors.As(err, &ke) {
		return ke.Kind
	}
	return KindTransient
}

// 提交与取消阶段的可区分结果错误。
var (
	ErrQueueFull       = errors.New("batch: queue is full")        // 超出资源上限
	ErrDuplicateJob    = errors.New("batch: duplicate job id")     // 重复提交
	ErrJobNotFound     = errors.New("batch: job not found")        // 取消不存在的作业
	ErrJobTerminal     = errors.New("batch: job already terminal") // 取消已终态的作业
	ErrRuntimeClosed   = errors.New("batch: runtime is closed")    // 运行时已关闭
	ErrIncompatibleVer = errors.New("batch: incompatible state version")
)
