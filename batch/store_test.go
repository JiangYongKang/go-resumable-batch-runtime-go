package batch

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStoreRejectsNewerVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	data := `{"version": 99, "records": {}}`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 状态文件版本=99, 当前支持版本=%d", stateFileVersion)
	_, err := NewStore(path).Load()
	if !errors.Is(err, ErrIncompatibleState) {
		t.Fatalf("期望 ErrIncompatibleState, 实际 %v", err)
	}
	t.Logf("判定依据: 拒绝加载并说明原因: %v", err)
}

func TestStoreRejectsCorruptData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("{not-json"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 非 JSON 的损坏状态文件")
	_, err := NewStore(path).Load()
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("期望 ErrCorruptState, 实际 %v", err)
	}
	t.Logf("判定依据: 显式拒绝并说明原因: %v", err)
}

func TestStoreRejectsInconsistentRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	data := `{"version": 2, "records": {"a": {"job": {"id": "b"}, "status": "pending"}}}`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 记录键 a 与作业 ID b 不一致")
	_, err := NewStore(path).Load()
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("期望 ErrCorruptState, 实际 %v", err)
	}
	t.Logf("判定依据: 数据完整性校验失败, 显式拒绝: %v", err)
}

func TestStoreMigratesV1(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	data := `{"version": 1, "records": {"j1": {"job": {"id": "j1", "payload": "p"}, "status": "succeeded", "attempts": 1}}}`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: v1 格式状态文件 (无 failure 字段)")
	records, err := NewStore(path).Load()
	if err != nil {
		t.Fatalf("v1 数据应可兼容读取, 实际错误 %v", err)
	}
	rec := records["j1"]
	if rec.Status != StatusSucceeded || rec.Attempts != 1 {
		t.Fatalf("迁移后记录=%+v, 期望保留原状态与尝试次数", rec)
	}
	t.Logf("判定依据: v1->v%d 迁移成功, 记录状态=%s attempts=%d", stateFileVersion, rec.Status, rec.Attempts)
}

func TestStoreSaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := NewStore(path)
	in := map[string]Record{
		"j1": {Job: Job{ID: "j1", Payload: "x"}, Status: StatusSucceeded, Attempts: 2},
	}
	if err := s.Save(in); err != nil {
		t.Fatalf("Save 失败: %v", err)
	}
	out, err := NewStore(path).Load()
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if out["j1"].Status != StatusSucceeded || out["j1"].Attempts != 2 {
		t.Fatalf("往返不一致: %+v", out["j1"])
	}
	t.Logf("判定依据: 原子写入后重新读取, 状态与尝试次数一致")
}
