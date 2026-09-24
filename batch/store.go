package batch

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// stateFileVersion 是当前状态文件格式版本。
// 读取规则：大于该版本 -> 拒绝（ErrIncompatibleState）；小于该版本 -> 迁移。
const stateFileVersion = 2

// stateFile 是状态文件的磁盘格式。
type stateFile struct {
	Version int               `json:"version"`
	Records map[string]Record `json:"records"`
}

// Store 负责作业状态的持久化与兼容读取。
type Store struct {
	mu   sync.Mutex
	path string
}

// NewStore 创建基于指定文件路径的 Store。
func NewStore(path string) *Store {
	return &Store{path: path}
}

// Load 读取状态文件。文件不存在时返回空状态；
// 版本过新或数据损坏时返回错误并说明原因。
func (s *Store) Load() (map[string]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return map[string]Record{}, nil
		}
		return nil, fmt.Errorf("%w: 读取状态文件失败: %v", ErrCorruptState, err)
	}

	var sf stateFile
	if err := json.Unmarshal(data, &sf); err != nil {
		return nil, fmt.Errorf("%w: JSON 解析失败: %v", ErrCorruptState, err)
	}

	switch {
	case sf.Version > stateFileVersion:
		return nil, fmt.Errorf("%w: 状态文件版本 %d 高于当前支持的版本 %d",
			ErrIncompatibleState, sf.Version, stateFileVersion)
	case sf.Version < stateFileVersion:
		if err := migrate(&sf); err != nil {
			return nil, err
		}
	}

	if sf.Records == nil {
		sf.Records = map[string]Record{}
	}
	// 数据完整性校验：记录必须携带作业 ID 与合法状态。
	for id, rec := range sf.Records {
		if rec.Job.ID == "" || rec.Job.ID != id {
			return nil, fmt.Errorf("%w: 记录 %q 的作业 ID 缺失或与键不一致", ErrCorruptState, id)
		}
		switch rec.Status {
		case StatusPending, StatusRunning, StatusSucceeded, StatusFailed, StatusCancelled:
		default:
			return nil, fmt.Errorf("%w: 记录 %q 含有未知状态 %q", ErrCorruptState, id, rec.Status)
		}
	}
	return sf.Records, nil
}

// Save 将状态原子写入磁盘（临时文件 + rename）。
func (s *Store) Save(records map[string]Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sf := stateFile{Version: stateFileVersion, Records: records}
	data, err := json.MarshalIndent(&sf, "", "  ")
	if err != nil {
		return fmt.Errorf("batch: 序列化状态失败: %w", err)
	}

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("batch: 创建临时状态文件失败: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("batch: 写入临时状态文件失败: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("batch: 落盘临时状态文件失败: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("batch: 关闭临时状态文件失败: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("batch: 替换状态文件失败: %w", err)
	}
	return nil
}

// migrate 将旧版本状态迁移到当前版本。
// v1 -> v2：v1 没有 failure 字段，补零值即可；未知旧版本显式拒绝。
func migrate(sf *stateFile) error {
	switch sf.Version {
	case 1:
		for id, rec := range sf.Records {
			if rec.Failure == "" {
				rec.Failure = FailureNone
			}
			sf.Records[id] = rec
		}
		sf.Version = stateFileVersion
		return nil
	default:
		return fmt.Errorf("%w: 未知的历史版本 %d，无法安全迁移",
			ErrIncompatibleState, sf.Version)
	}
}
