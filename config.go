package mybot

import (
	"log/slog"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config 系统总配置
type Config struct {
	System   SystemConfig    `yaml:"system"`
	Adapters []AdapterConfig `yaml:"adapters"`
}

// SystemConfig 系统全局配置
type SystemConfig struct {
	TraceEnabled   bool             `yaml:"trace_enabled"`
	WorkDir        string           `yaml:"work_dir"`        // 主程序工作目录，空则用进程 cwd；各 adapter 默认目录为 work_dir/adapters/{adapter_id}
	DefaultAdapter string           `yaml:"default_adapter"` // 路由兜底：无 Target 且无 Tags 或标签无匹配时投递的 adapter id
	StateStore     StateStoreConfig `yaml:"state_store"`     // 状态存储配置
	Admin          AdminConfig      `yaml:"admin"`           // 管理服务配置
}

// AdminConfig 管理服务配置
type AdminConfig struct {
	Enabled bool   `yaml:"enabled"` // 是否启用管理服务
	Port    string `yaml:"port"`    // 管理服务端口
}

// StateStoreConfig 状态存储配置
type StateStoreConfig struct {
	Enabled     bool   `yaml:"enabled"`      // 是否启用状态存储
	DBPath      string `yaml:"db_path"`      // 数据库文件路径
	AutoCleanup bool   `yaml:"auto_cleanup"` // 是否自动清理旧数据
	CleanupDays int    `yaml:"cleanup_days"` // 自动清理多少天前的数据
}

// AdapterConfig 适配器实例配置
// 使用 map[string]interface{} 以支持灵活的配置结构
type AdapterConfig map[string]interface{}

// LoadConfig 从指定路径加载 YAML 配置文件
func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var cfg Config
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}
	slog.Debug("Loaded config", "config", cfg)

	return &cfg, nil
}

// AdapterConfigWithDir 复制 config 并注入 adapter_dir = workDir/adapters/{id}。
// 各 adapter 可直接使用 config["adapter_dir"] 作为自己的工作目录，不存在时可 os.MkdirAll 创建。
// workDir 为空时仅复制 config，不注入 adapter_dir。
func AdapterConfigWithDir(config map[string]interface{}, workDir, id string) map[string]interface{} {
	out := make(map[string]interface{}, len(config)+1)
	for k, v := range config {
		out[k] = v
	}
	if workDir != "" {
		out["adapter_dir"] = filepath.Join(workDir, "adapters", id)
	}
	return out
}
