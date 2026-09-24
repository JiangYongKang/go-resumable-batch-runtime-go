# go-resumable-batch-runtime-go

可在进程重启后继续运行的本地批处理执行子系统（`batch` 包），统一承载需要
并发处理、资源约束和失败恢复的后台作业。

## 状态模型

每个作业有唯一的业务 ID，生命周期状态机如下：

```
                ┌──────────────┐
   Submit       │              │   调度(获得并发许可)     ┌─────────┐
  ────────────▶ │   pending    │ ──────────────────────▶ │ running │
                │              │                         │         │
                └──────┬───────┘                         └────┬────┘
                       │ Cancel                               │ Cancel(经 context 传播)
                       ▼                                      ▼
                ┌──────────────┐   成功        ┌───────────────┐
                │  cancelled   │ ◀──────────── │   (终态集合)   │
                └──────────────┘               │  succeeded    │
                       ▲        永久/依赖失败   │  failed       │
                       └────────────────────── │  cancelled    │
                                  重试用尽      └───────────────┘
```

- 终态：`succeeded` / `failed` / `cancelled`，进入后不再变化。
- 瞬时失败且重试次数未达 `MaxRetries` 时，作业从 `running` 回到 `pending`
  并排到队尾（FIFO 顺序不被破坏，重试不占用额外并发许可）。
- 每次状态转换都会原子落盘（临时文件 + rename），崩溃不会留下半截文件。

## 提交、取消与资源限制语义

| 场景 | 结果 |
| --- | --- |
| 提交新作业 | 入队为 `pending`，返回 `nil` |
| 重复提交相同 ID（含已终态） | 返回 `ErrDuplicateJob` |
| 非终态作业数达到 `MaxQueue` | 返回 `ErrQueueFull`，不静默丢单 |
| 运行时已 `Close` 后提交 | 返回 `ErrRuntimeClosed` |
| 取消 `pending` 作业 | 立即转为 `cancelled` |
| 取消 `running` 作业 | 通过 context 向 Handler 传播，由 worker 完成状态转换 |
| 取消终态/不存在的作业 | 返回 `ErrNotCancellable` / `ErrJobNotFound` |

- `MaxConcurrency` 通过固定容量的信号量强制执行，任何时刻运行中的作业数
  不超过该上限；调度器为单一 goroutine，worker 数量有界，不存在无界
  goroutine。
- `Job.Timeout` 作用于单次尝试，超时信号经 context 传给 Handler。

## 失败分类与重试

Handler 返回 `*batch.Failure`（用 `Transient` / `Permanent` / `Dependency`
构造）以声明失败类别：

- **瞬时失败（transient）**：按 `RetryBackoff * 已尝试次数` 退避后重试，
  最多 `MaxRetries` 次；超时（`context.DeadlineExceeded`）也归入此类。
- **永久失败（permanent）**：不重试，直接 `failed`。未分类的普通 error
  按永久失败处理，避免无限重试。
- **依赖失败（dependency）**：不重试，直接 `failed`，与永久失败可区分。

`Results()` 只汇总已到达终态的作业，其 `Status/Attempts/Failure/Err`
直接来自实际执行记录，保证部分成功时对外汇总与实际执行一致。

## 持久化、恢复与兼容规则

- 配置 `StatePath` 后，状态以 JSON 文件持久化，格式带 `version` 字段
  （当前版本 2）。
- **重启恢复**：`New` 加载状态文件；终态记录原样保留、绝不重复执行；
  `pending`/`running` 记录重置为 `pending` 并按更新时间顺序重新入队，
  保证已确认的工作不丢失、已完成的工作不重放。
- **兼容读取**：版本低于当前版本时自动迁移（v1 → v2 补全 failure 字段）；
  版本高于当前版本、未知历史版本、JSON 损坏、记录键与作业 ID 不一致、
  状态值未知等情况，分别返回 `ErrIncompatibleState` 或
  `ErrCorruptState` 并附具体原因，显式拒绝加载。
- `Close` 停止接收新作业、取消运行中的作业并等待退出；未开始的
  `pending` 作业保持原状落盘，供下次启动恢复。

## 本地验证方法

```bash
# 构建与静态检查
go build ./... && go vet ./...

# 运行全部测试（含竞态检测）
go test ./... -race -count=1

# 查看关键输入、状态转换与判定依据日志
go test ./batch/ -v
```

测试覆盖：并发上限（`TestConcurrencyLimit`）、队列满与重复提交
（`TestQueueFullAndDuplicate`）、取消（`TestCancelPendingAndRunning`）、
失败分类与重试（`TestRetryTransientOnly`）、超时传播
（`TestTimeoutPropagatesAndRetries`）、重启恢复与重复触发
（`TestRestartRecovery`、`TestRecoveryWithRunningRecord`）、状态数据
不兼容/损坏/迁移（`TestStore*`）。
