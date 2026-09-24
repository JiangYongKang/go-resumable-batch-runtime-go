# go-resumable-batch-runtime-go

可在进程重启后继续运行的本地批处理执行子系统（`batch` 包），统一承载需要
并发处理、资源约束和失败恢复的后台作业。

## 快速开始

```go
rt, err := batch.New(batch.Config{
    MaxConcurrency: 4,                 // 并发上限
    MaxQueue:       64,                // 在途作业（运行中+等待中）上限
    StatePath:      "/var/lib/app/batch-state.json", // 状态持久化路径，空则不持久化
})
if err != nil { /* 状态文件不兼容或损坏时会在此显式失败 */ }

err = rt.Submit(batch.Job{
    ID:         "import-2026-09-24",   // 业务唯一 ID
    MaxRetries: 3,                     // 瞬时失败最多重试 3 次
    Timeout:    30 * time.Second,      // 单次尝试超时，向 Work 的 ctx 传播
    Work: func(ctx context.Context, j *batch.Job) error {
        // 失败分类：batch.Transient(err) 可重试（默认）；
        // batch.Permanent(err) / batch.Dependency(err) 不重试且可区分。
        return nil
    },
})
// err 可区分：nil / ErrQueueFull / ErrDuplicateJob / ErrRuntimeClosed

res, _ := rt.Wait("import-2026-09-24") // 阻塞到终态，结果与实际执行一致
rt.Close()                             // 等待在途作业结束，返回首个持久化错误
```

## 状态模型

```
pending ──worker 取出──> running ──成功──> succeeded (终态)
   │                       │
   │ Cancel                ├──瞬时失败且未耗尽重试──> pending (重新入队)
   ▼                       ├──永久/依赖失败或重试耗尽──> failed (终态)
canceled (终态)            └──Cancel──> canceled (终态)
```

- 终态（`succeeded` / `failed` / `canceled`）不可逆；对终态作业 `Cancel`
  返回 `ErrJobTerminal`，`Submit` 返回 `ErrDuplicateJob`。
- 每次状态转换都在锁内完成并同步持久化，已确认的状态不会因崩溃丢失。
- `Result{Status, Attempts, Err}` 由内部状态直接生成，部分成功时各作业
  汇总与实际执行结果一一对应。

## 恢复与兼容规则

- 状态文件为带 `version` 的 JSON，写入采用临时文件 + fsync + rename，
  任何时刻落盘的要么是旧完整状态、要么是新完整状态。
- **重启恢复**：`New` 加载状态文件。终态记录原样恢复——同 ID 再提交会被
  `ErrDuplicateJob` 拒绝，已完成的工作绝不重复执行；崩溃时处于
  `pending`/`running` 的记录恢复为待恢复状态，调用方以相同 ID 重新
  `Submit` 即继续执行，且保留崩溃前已确认的尝试次数（`attempts`）。
- **兼容读取**：`version <= 当前版本` 的文件可读取，缺失字段按零值处理；
  `version > 当前版本` 的文件无法安全处理，`New` 返回包装
  `ErrIncompatibleVer` 的错误并说明版本号；文件损坏同样显式报错，
  不静默丢弃。

## 资源限制语义

- `MaxConcurrency`：固定数量的 worker，即同时执行的最大作业数；不存在
  无界 goroutine，重试通过重新入队进行，不放大并发、不阻塞 worker。
- `MaxQueue`：在途作业（运行中 + 等待中）总数上限，超限时 `Submit`
  立即返回 `ErrQueueFull`，不会静默丢单或无限排队。
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
已终态）、失败分类与重试、重试耗尽、超时传播、部分成功汇总、重启后
已完成工作不重复执行、崩溃作业恢复续跑、版本不兼容拒绝、损坏文件
报错、旧版本兼容读取、原子保存完整性。
