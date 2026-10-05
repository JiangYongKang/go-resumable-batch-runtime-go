# go-resumable-batch-runtime-go

可在进程重启后继续运行的本地批处理执行子系统（`batch` 包），统一承载需要
并发处理、资源约束和失败恢复的后台作业。支持作业依赖、优先级调度、
延迟执行与重试退避，全部状态可持久化并在重启后接续。

## 快速开始

```go
rt, err := batch.New(batch.Config{
    MaxConcurrency: 4,                 // 并发上限
    MaxQueue:       64,                // 在途作业（运行中+等待中）上限
    StatePath:      "/var/lib/app/batch-state.json", // 状态持久化路径，空则不持久化
    PriorityAging:  time.Second,       // 优先级老化间隔（防低优先级饿死），<=0 用默认值
})
if err != nil { /* 状态文件不兼容或损坏时会在此显式失败 */ }

err = rt.Submit(batch.Job{
    ID:           "import-2026-09-24", // 业务唯一 ID
    MaxRetries:   3,                   // 瞬时失败最多重试 3 次
    RetryBackoff: 5 * time.Second,     // 重试间隔，退避期间不占执行资源
    Timeout:      30 * time.Second,    // 单次尝试超时，向 Work 的 ctx 传播
    Priority:     10,                  // 优先级，越大越先调度
    DependsOn:    []string{"download-2026-09-24"}, // 前置全部成功后才执行
    NotBefore:    time.Now().Add(time.Hour),       // 最早开始时间，0 值立即调度
    Work: func(ctx context.Context, j *batch.Job) error {
        // 失败分类：batch.Transient(err) 可重试（默认）；
        // batch.Permanent(err) / batch.Dependency(err) 不重试且可区分。
        return nil
    },
})
// err 可区分：nil / ErrQueueFull / ErrDuplicateJob / ErrDependencyCycle / ErrRuntimeClosed

res, _ := rt.Wait("import-2026-09-24") // 阻塞到终态，结果与实际执行一致
rt.Close()                             // 等待在途作业结束，返回首个持久化错误
```

## 状态模型

```
pending ──调度器派发──> running ──成功──> succeeded (终态)
   │  │                   │
   │  │                   ├──瞬时失败且未耗尽重试──> pending (退避后重新就绪)
   │  │                   ├──永久/依赖失败或重试耗尽──> failed (终态)
   │  │                   └──Cancel──> canceled (终态)
   │  ├──任一前置 failed/canceled/skipped──> skipped (终态，未执行)
   ▼  ▼
canceled (终态，Cancel)
```

- `pending` 涵盖一切等待形态：等并发额度、等依赖、等 `NotBefore`、等退避。
- 终态（`succeeded` / `failed` / `canceled` / `skipped`）不可逆；对终态作业
  `Cancel` 返回 `ErrJobTerminal`，`Submit` 返回 `ErrDuplicateJob`。
- `skipped` 是专门的收尾方式：前置未成功导致作业从未执行，与普通执行失败
  （`failed`）明确区分；其 `Result.Err` 满足 `errors.Is(err,
  ErrDependencyFailed)` 且 `Classify(err) == KindDependency`。
- 每次状态转换都在锁内完成并同步持久化，已确认的状态不会因崩溃丢失。

## 依赖语义

- `Job.DependsOn` 列出前置作业 ID；**全部成功**后才进入调度，任一前置
  `failed`/`canceled`/`skipped`，本作业立即以 `skipped` 收尾（不执行
  `Work`），并沿依赖链递归传播。
- **前置后到**：前置尚未提交时作业挂起等待；之后补交前置并跑成功，等待中
  的作业自动接续，无需人工干预。
- 提交时前置已终结（非成功）的，直接以 `skipped` 收尾。
- 自依赖与依赖环在提交时被拒绝（`ErrDependencyCycle`）。
- 依赖关系持久化：重启前已确认的前置不丢（前置终态记录仍在）；重启后
  该等的继续等、该跑的照常跑，已成功的前置绝不重复执行。

## 优先级语义

- `Job.Priority` 越大越优先（默认 0）。并发额度占满时就绪作业排队，
  额度空出时有效优先级最高者先调度；同有效优先级按就绪先后 FIFO。
- **有效优先级 = 静态优先级 + 就绪等待时长 / `PriorityAging`**：等待越久
  提升越多，低优先级作业不会被无限压制。
- 优先级只影响调度顺序：`MaxConcurrency` 与 `MaxQueue` 仍是硬上限，
  不会被优先级或依赖关系顶破；在途（含各种等待中的）作业数超
  `MaxQueue` 时 `Submit` 立即返回 `ErrQueueFull`，不阻塞、不静默丢弃。

## 时间语义

- `Job.NotBefore`：最早开始时间。未到期作业挂起等待，不占并发额度、
  不占 worker，到期由调度器自动唤醒。
- `Job.RetryBackoff`：瞬时失败重试之间的间隔。退避期间作业同样不占
  执行资源；为 0 时重试立即重新就绪。
- 两者的时间门控（`ready_at`）都会持久化，重启后按原样继续等待剩余时间。

## 恢复与兼容规则

- 状态文件为带 `version` 的 JSON，写入采用临时文件 + fsync + rename，
  任何时刻落盘的要么是旧完整状态、要么是新完整状态。
- **重启恢复**：`New` 加载状态文件。终态记录原样恢复——同 ID 再提交会被
  `ErrDuplicateJob` 拒绝，已完成的工作绝不重复执行；崩溃时处于
  `pending`/`running` 的记录（含等依赖/等时间/等退避）恢复为待恢复状态，
  调用方以相同 ID 重新 `Submit` 即继续执行，保留崩溃前已确认的尝试次数
  （`attempts`）、依赖确认与时间门控。
- **关停语义**：`Close` 停止接收新作业与新的派发，等执行中的作业完成后
  退出全部 goroutine；等待中的作业不丢失——状态文件保留 `pending`
  （含依赖与 `ready_at`），下次启动原样接续。进程内这些作业的 `Wait`
  以 `canceled` 返回（该收尾不落盘）。关停不会挂起等待退避/门控到期，
  也不会崩溃。
- **版本兼容**：当前状态版本为 **2**。
  - 新版可读取 v1/v2 文件：v1 缺失的新字段（`depends_on`、`priority`、
    `not_before`、`retry_backoff`、`ready_at` 等）按零值处理——无依赖、
    立即就绪，语义与旧版一致。
  - 新版写出的 v2 文件**不能**被旧版（支持 v1）读取：旧版 `New` 会返回
    包装 `ErrIncompatibleVer` 的错误。需要回滚时先迁移或清除状态文件。
  - `version > 2` 的文件同样被显式拒绝；文件损坏显式报错，不静默丢弃。

## 资源限制语义

- `MaxConcurrency`：固定数量的 worker，即同时执行的最大作业数；不存在
  无界 goroutine。重试、延迟、依赖等待都通过调度器重新就绪进行，
  不放大并发、不阻塞 worker。
- `MaxQueue`：在途作业（运行中 + 各种等待中）总数上限，超限时 `Submit`
  立即返回 `ErrQueueFull`。
- 取消：等待中的作业立即转 `canceled`；执行中的作业通过 `context`
  收到取消信号，`Work` 返回后转为 `canceled`；后继作业收到 `skipped`。
- 超时：`Job.Timeout` 作用于每一次尝试，超时按瞬时失败处理（可重试）。

## 本地验证

```bash
go build ./...        # 编译
go vet ./...          # 静态检查
go test ./... -v      # 全部测试（日志含关键输入、状态转换与判定依据）
go test ./... -race   # 竞态检测
```

测试覆盖：并发上限、队列超限（含等待中作业计入额度）、重复提交、取消
（等待中/执行中/不存在/已终态）、失败分类与重试、重试耗尽、超时传播、
部分成功汇总、依赖成功/失败/取消传播、依赖链跳过、前置后到、依赖环拒绝、
优先级在容量上限内调度、优先级老化防饿死、NotBefore 门控不占用执行资源、
重试退避间隔与不占用执行资源、带等待中作业关停再重启的接续、重启后已确认
依赖不丢、已完成工作不重复执行、崩溃作业恢复续跑、版本不兼容拒绝、损坏
文件报错、旧版本兼容读取、原子保存完整性。
