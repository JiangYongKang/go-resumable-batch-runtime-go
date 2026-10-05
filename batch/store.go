package batch

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// stateVersion 是当前状态文件格式版本。
// 兼容规则：可读取 version <= stateVersion 的文件（旧版本按当前结构解析，
// 缺失字段取零值）；version > stateVersion 的文件无法安全处理，显式拒绝。
//
// 版本历史：
//   - v1：id/status/attempts/last_error/updated_at。
//   - v2：新增 priority/depends_on/not_before（依赖、优先级、时间语义）。
//     v2 文件包含 v1 运行时不理解的调度语义，故旧运行时（支持到 v1）会
//     按既有规则显式拒绝 v2 文件，而不是静默忽略新字段。
const stateVersion = 2

// jobRecord 是持久化的作业状态记录。
type jobRecord struct {
	ID        string    `json:"id"`
	Status    Status    `json:"status"`
	Attempts  int       `json:"attempts"`
	LastError string    `json:"last_error,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
	// v2 新增字段；读取 v1 文件时取零值（无依赖、默认优先级、立即可调度）。
	Priority  int       `json:"priority,omitempty"`
	DependsOn []string  `json:"depends_on,omitempty"`
	NotBefore time.Time `json:"not_before,omitempty"` // 生效中的最早可调度时间（含退避截止）
}

// stateFile 是状态文件的顶层结构，带版本号以支持兼容读取。
type stateFile struct {
	Version int                  `json:"version"`
	Jobs    map[string]jobRecord `json:"jobs"`
}

// Store 是执行状态持久化接口。
type Store interface {
	// Save 原子写入全部状态。
	Save(s *stateFile) error
	// Load 读取状态；版本不兼容时返回包装了 ErrIncompatibleVer 的错误。
	Load() (*stateFile, error)
}

// FileStore 基于本地 JSON 文件实现 Store，写入采用临时文件+rename 保证原子性。
type FileStore struct {
	path string
}

// NewFileStore 创建基于 path 的文件存储。
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

// Save 将状态原子写入文件：先写临时文件并 fsync，再 rename 覆盖。
func (f *FileStore) Save(s *stateFile) error {
	if s.Jobs == nil {
		s.Jobs = map[string]jobRecord{}
	}
	s.Version = stateVersion
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("batch: marshal state: %w", err)
	}
	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("batch: create temp state file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // rename 成功后为 no-op
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("batch: write temp state file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("batch: fsync temp state file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("batch: close temp state file: %w", err)
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		return fmt.Errorf("batch: rename state file: %w", err)
	}
	return nil
}

// Load 读取状态文件。
//   - 文件不存在：返回空状态（首次启动）。
//   - 版本高于当前支持版本：返回包装 ErrIncompatibleVer 的错误，显式拒绝。
//   - 内容损坏：返回错误，不静默丢弃。
func (f *FileStore) Load() (*stateFile, error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return &stateFile{Version: stateVersion, Jobs: map[string]jobRecord{}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("batch: read state file: %w", err)
	}
	var s stateFile
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("batch: parse state file %q: %w", f.path, err)
	}
	if s.Version > stateVersion {
		return nil, fmt.Errorf("%w: file version %d > supported %d",
			ErrIncompatibleVer, s.Version, stateVersion)
	}
	if s.Jobs == nil {
		s.Jobs = map[string]jobRecord{}
	}
	return &s, nil
}
