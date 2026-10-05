# go-resumable-batch-runtime-go

可在进程重启后继续运行的本地批处理执行子系统（`batch` 包），统一承载需要
并发处理、资源约束和失败恢复的后台作业。支持作业依赖、优先级调度、
延迟执行与失败退避，且这些等待安排都能跨重启恢复。

## 快速开始

```go
rt, err := batch.New(batch.Config{
    MaxConcurrency: 4,                 // 并发上限
    MaxQueue:       64,                // 在途作业（运行中+等待中）上限
    StatePath:      "/var/lib/app/batch-state.json", // 状态持久化路径，空则不持久化
    PriorityAging:  time.Minute,       // 优先级老化间隔，<=0 不做老化
})
if err != nil { /* 状态文件不兼容或损坏时会在此显式失败 */ }

err = rt.Submit(batch.Job{
    ID:           "import-2026-09-24",  // 业务唯一 ID
    MaxRetries:   3,                    // 瞬时失败最多重试 3 次
    RetryBackoff: 5 * time.Second,      // 重试间隔（退避期间不占执行资源）
    Timeout:      30 * time.Second,     // 单次尝试超时，向 Work 的 ctx 传播
    Priority:     10,                   // 优先级，越大越先调度
    DependsOn:    []string{"fetch-2026-09-24"}, // 前置全部成功后才执行
    NotBefore:    time.Now().Add(time.Hour),    // 最早开始时间，零值立即
    Work: func(ctx context.Context, j *batch.Job) error {
        // 失败分类：batch.Transient(err) 可重试（默认）；
        // batch.Permanent(err) / batch.Dependency(err) 不重试且可区分。
        return nil
    },
})
// err 可区分：nil / ErrQueueFull / ErrDuplicateJob / ErrDependencyCycle / ErrRuntimeClosed

res, _ := rt.Wait("import-2026-09-24") // 阻塞到终态，结果与实际执行一致
rt.Close()                             // 等待执行中作业结束，返回首个持久化错误
```

## 状态模型

```
pending ──调度器选中──> running ──成功──> succeeded (终态)
   │ ▲                    │
   │ │退避到期/到点        ├──瞬时失败且未耗尽重试──> pending (按 RetryBackoff 重新等待)
   │ │                    ├──永久/依赖失败或重试耗尽──> failed (终态)
   │ Cancel               └──Cancel──> canceled (终态)
   ▼
canceled (终态)

pending ──任一前置非成功终态──> skipped (终态，未执行，与 failed 可区分)
```

- `pending` 涵盖一切等待：等并发额度、等依赖、等到点、等退避。
- 终态（`succeeded` / `failed` / `canceled` / `skipped`）不可逆；对终态作业
  `Cancel` 返回 `ErrJobTerminal`，`Submit` 返回 `ErrDuplicateJob`。
- 每次状态转换都在锁内完成并同步持久化，已确认的状态不会因崩溃丢失。
- `Result{Status, Attempts, Err}` 由内部状态直接生成，部分成功时各作业
  汇总与实际执行结果一一对应。

## 依赖语义

- `Job.DependsOn` 声明前置作业 ID 列表。**全部前置成功**后作业才可被调度。
- 任一前置以非成功终态结束（`failed` / `canceled` / `skipped`）时，作业
  **不会执行**，直接以 `skipped` 收尾：`Result.Status == StatusSkipped`
  （与普通执行失败 `failed` 可区分），`Result.Err` 的分类为
  `KindDependency` 并指明是哪个前置以何种状态结束；沿依赖链递归传播。
- 前置**尚未提交**时作业保持等待；之后前置补交并跑成功，等待中的作业
  自动接上，无需人工干预。提交时前置已确定失败的，作业被受理并立即
  `skipped`。
- 自依赖与已知作业间的依赖环在 `Submit` 时被 `ErrDependencyCycle` 拒绝
  （尚未提交的前置没有已声明的下游，不参与判定）。
- 等待依赖的作业计入在途额度（`MaxQueue`），但不占用并发额度；`Cancel`
  对等待中的作业立即生效。

## 优先级语义

- `Job.Priority` 数值越大越先被调度，默认 0；同优先级按提交先后（FIFO）。
- 调度选择发生在 worker 空闲、交接前一刻：并发额度被占满时新到的高
  优先级作业会在下一个空出额度上插队。
- `Config.PriorityAging > 0` 时，作业等待每满一个间隔有效优先级 +1，
  低优先级作业不会被无限压制；`<=0` 时为纯优先级 + FIFO。
- 优先级不改变硬上限：并发峰值不超过 `MaxConcurrency`，在途总数不超过
  `MaxQueue`——额度占满时连最高优先级的提交也会被 `ErrQueueFull` 拒绝。

## 时间语义

- `Job.NotBefore`：最早开始时间。到点前作业保持 `pending`，不占用执行
  资源（其他就绪作业可正常使用 worker），到点后按优先级参与调度。
- `Job.RetryBackoff`：瞬时失败重试之间的间隔。退避期间作业保持
  `pending`、不占用执行资源；为 0 时保持立即重试的原有行为。
- 等待安排以生效中的 `not_before` 时间戳持久化：重启后按原安排接续，
  退避不会因重启而重新计时或丢失。

## 恢复与兼容规则

- 状态文件为带 `version` 的 JSON，写入采用临时文件 + fsync + rename，
  任何时刻落盘的要么是旧完整状态、要么是新完整状态。
- **重启恢复**：`New` 加载状态文件。终态记录原样恢复——同 ID 再提交会被
  `ErrDuplicateJob` 拒绝，已完成的工作绝不重复执行；崩溃时处于
  `pending`/`running` 的记录（含等依赖/等时间/等退避的作业）恢复为
  待恢复状态，保留已确认的尝试次数、依赖声明、优先级与生效中的
  `not_before`。调用方以相同 ID 重新 `Submit` 即按原安排继续执行；
  重新提交时未重复声明的依赖沿用持久化记录。
- **关停语义**：`Close` 停止接收新作业与新的调度，等待正在执行的作业
  结束后退出全部 goroutine；等待依赖/时间/退避的作业保持已受理状态
  并持久化，不取消、不丢弃，下次启动时接续处理。
- **版本兼容**：
  - 当前状态版本为 **v2**（新增 `priority` / `depends_on` / `not_before`）。
  - 新运行时读取 **v1、v2** 文件：v1 文件缺失的新字段按零值处理
    （无依赖、默认优先级、立即可调度），即**老文件可以直接读**。
  - 旧运行时（支持到 v1）打开 v2 文件会按既有规则收到
    `ErrIncompatibleVer` 而**显式拒绝**——v2 文件包含旧运行时不理解的
    调度语义，显式拒绝优于静默忽略新字段。
  - `version > 2` 的文件同样被 `ErrIncompatibleVer` 拒绝；文件损坏
    显式报错，不静默丢弃。

## 资源限制语义

- `MaxConcurrency`：固定数量的 worker，即同时执行的最大作业数；不存在
  无界 goroutine。重试与延迟都通过调度器重新安排，不放大并发、
  不阻塞 worker。
- `MaxQueue`：在途作业（运行中 + 各类等待中）总数上限，超限时 `Submit`
  立即返回 `ErrQueueFull`，不阻塞等待、不静默丢单。
- 取消：等待中的作业立即转 `canceled`；执行中的作业通过 `context`
  收到取消信号，`Work` 返回后转为 `canceled`。
- 超时：`Job.Timeout` 作用于每一次尝试，超时按瞬时失败处理（可重试）。

## 本地验证

```bash
go build ./...        # 编译
go vet ./...          # 静态检查
go test ./... -v      # 全部测试（日志含关键输入、状态转换与判定依据）
go test ./... -race   # 竞态检测
```

测试覆盖：并发上限、队列超限、重复提交、取消（等待中/执行中/不存在/
已终态）、失败分类与重试、重试耗尽、超时传播、部分成功汇总、
依赖成功/失败/取消传播、前置后补交自动接续、提交时前置已失败、
依赖环拒绝、多前置汇合、优先级调度顺序、优先级老化防饿死、
优先级不顶破容量上限、延迟执行与退避不占执行资源、无退避立即重试、
带等待中作业关停再重启的接续、重启后已完成工作不重复执行、
崩溃作业恢复续跑、版本不兼容拒绝、损坏文件报错、旧版本兼容读取、
原子保存完整性。
