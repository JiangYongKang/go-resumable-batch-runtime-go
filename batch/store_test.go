package batch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestRestartCompletedNotReexecuted 验证进程重启后已完成的工作不会重复执行，
// 且重复提交同 ID 被明确拒绝。
func TestRestartCompletedNotReexecuted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	var runs atomic.Int32
	work := func(ctx context.Context, j *Job) error {
		runs.Add(1)
		return nil
	}

	rt1, err := New(Config{MaxConcurrency: 2, MaxQueue: 8, StatePath: path})
	if err != nil {
		t.Fatalf("New rt1: %v", err)
	}
	if err := rt1.Submit(Job{ID: "done-job", Work: work}); err != nil {
		t.Fatal(err)
	}
	res, _ := rt1.Wait("done-job")
	t.Logf("重启前: status=%s attempts=%d", res.Status, res.Attempts)
	if res.Status != StatusSucceeded {
		t.Fatalf("期望 succeeded, 实际 %s", res.Status)
	}
	if err := rt1.Close(); err != nil {
		t.Fatalf("Close rt1: %v", err)
	}

	// 模拟进程重启：同一状态文件重新创建运行时。
	rt2, err := New(Config{MaxConcurrency: 2, MaxQueue: 8, StatePath: path})
	if err != nil {
		t.Fatalf("New rt2: %v", err)
	}
	defer rt2.Close()

	st, ok := rt2.Status("done-job")
	t.Logf("重启后状态恢复: status=%s found=%v", st, ok)
	if !ok || st != StatusSucceeded {
		t.Fatalf("重启后期望 succeeded, 实际 %s found=%v", st, ok)
	}
	err = rt2.Submit(Job{ID: "done-job", Work: work})
	t.Logf("重启后重复提交: err=%v", err)
	if !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("期望 ErrDuplicateJob, 实际 %v", err)
	}
	t.Logf("判定依据: 总执行次数=%d, 期望=1（已完成工作不重复执行）", runs.Load())
	if runs.Load() != 1 {
		t.Fatalf("已完成作业被重复执行: runs=%d", runs.Load())
	}
}

// TestRestartResumeInterrupted 验证崩溃时未完成的作业在重启后可通过同 ID
// 重新提交恢复执行，且已记录的尝试次数被保留（不丢已确认的工作）。
func TestRestartResumeInterrupted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	rt1, err := New(Config{MaxConcurrency: 1, MaxQueue: 4, StatePath: path})
	if err != nil {
		t.Fatalf("New rt1: %v", err)
	}
	// 第一次运行总是瞬时失败，重试耗尽进入 failed 之前人为制造
	// “崩溃”：直接丢弃 rt1（不 Close），状态文件中留有 pending 记录。
	if err := rt1.Submit(Job{ID: "fragile", MaxRetries: 5, Work: func(ctx context.Context, j *Job) error {
		return Transient(errors.New("第一次进程内总是失败"))
	}}); err != nil {
		t.Fatal(err)
	}
	// 等待至少一次尝试被持久化。
	deadline := 200
	for i := 0; i < deadline; i++ {
		sf, err := NewFileStore(path).Load()
		if err == nil {
			if rec, ok := sf.Jobs["fragile"]; ok && rec.Attempts >= 1 {
				t.Logf("崩溃前状态: status=%s attempts=%d", rec.Status, rec.Attempts)
				break
			}
		}
		if i == deadline-1 {
			t.Fatal("状态文件未记录任何尝试")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// 不调用 Close，模拟进程异常退出（worker goroutine 随测试进程模型丢弃）。
	_ = rt1

	var phase2Runs atomic.Int32
	rt2, err := New(Config{MaxConcurrency: 1, MaxQueue: 4, StatePath: path})
	if err != nil {
		t.Fatalf("New rt2: %v", err)
	}
	defer rt2.Close()
	st, ok := rt2.Status("fragile")
	t.Logf("重启后恢复状态: status=%s found=%v", st, ok)
	if !ok || st != StatusPending {
		t.Fatalf("崩溃时未完成的作业应恢复为 pending, 实际 %s found=%v", st, ok)
	}
	// 同 ID 重新提交 = 恢复执行，这次成功。
	if err := rt2.Submit(Job{ID: "fragile", MaxRetries: 5, Work: func(ctx context.Context, j *Job) error {
		phase2Runs.Add(1)
		return nil
	}}); err != nil {
		t.Fatalf("恢复提交: %v", err)
	}
	res, _ := rt2.Wait("fragile")
	t.Logf("恢复执行结果: status=%s attempts=%d (含崩溃前已确认的尝试)", res.Status, res.Attempts)
	if res.Status != StatusSucceeded {
		t.Fatalf("期望 succeeded, 实际 %s", res.Status)
	}
	if res.Attempts < 2 {
		t.Fatalf("尝试次数应包含崩溃前记录, 实际 %d", res.Attempts)
	}
}

// TestIncompatibleVersion 验证高于当前支持版本的状态文件被显式拒绝。
func TestIncompatibleVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	content := `{"version": 99, "jobs": {}}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 状态文件 version=99, 当前支持 version=%d", stateVersion)
	_, err := New(Config{StatePath: path})
	t.Logf("判定: err=%v", err)
	if !errors.Is(err, ErrIncompatibleVer) {
		t.Fatalf("期望 ErrIncompatibleVer, 实际 %v", err)
	}
}

// TestCorruptedStateFile 验证损坏的状态文件返回错误而非静默丢弃。
func TestCorruptedStateFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte("{not-json"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := New(Config{StatePath: path})
	t.Logf("输入: 损坏的状态文件, 判定: err=%v", err)
	if err == nil {
		t.Fatal("损坏的状态文件应返回错误")
	}
}

// TestOldVersionCompatible 验证旧版本（缺失字段）状态数据可兼容读取。
func TestOldVersionCompatible(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	// 模拟旧格式：无 updated_at / last_error 字段。
	content := `{"version": 1, "jobs": {"old-job": {"id": "old-job", "status": "succeeded", "attempts": 1}}}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	rt, err := New(Config{StatePath: path})
	if err != nil {
		t.Fatalf("旧版本状态应可兼容读取: %v", err)
	}
	defer rt.Close()
	st, ok := rt.Status("old-job")
	t.Logf("旧版本记录恢复: status=%s found=%v", st, ok)
	if !ok || st != StatusSucceeded {
		t.Fatalf("期望 succeeded, 实际 %s found=%v", st, ok)
	}
}

// TestAtomicSaveNoPartialFile 验证保存后的状态文件始终是完整 JSON。
func TestAtomicSaveNoPartialFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	fs := NewFileStore(path)
	for i := 0; i < 50; i++ {
		sf := &stateFile{Jobs: map[string]jobRecord{
			fmt.Sprintf("j-%d", i): {ID: fmt.Sprintf("j-%d", i), Status: StatusRunning, Attempts: i},
		}}
		if err := fs.Save(sf); err != nil {
			t.Fatalf("Save: %v", err)
		}
		if _, err := fs.Load(); err != nil {
			t.Fatalf("第 %d 次保存后 Load 失败（文件不完整）: %v", i, err)
		}
	}
	// 临时文件不应残留。
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("临时文件残留: %s", e.Name())
		}
	}
	t.Log("50 次原子保存均可完整读取, 无临时文件残留")
}
