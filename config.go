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

type SystemConfig struct {
	TraceEnabled   bool             `yaml:"trace_enabled"`
	WorkDir        string           `yaml:"work_dir"`         // 主程序工作目录，空则使用「配置文件所在目录/workdir」；各 adapter 默认目录为 work_dir/adapters/{adapter_id}
	DefaultAdapter string           `yaml:"default_adapter"`  // 路由兜底：无 Target 且无 Tags 或标签无匹配时投递的 adapter id
	LogLevel       string           `yaml:"log_level"`        // 日志级别：debug/info/warn/error，空则 info
	LogFile        string           `yaml:"log_file"`         // 日志文件路径，空则输出到 stdout
	StateStore     StateStoreConfig `yaml:"state_store"`      // 状态存储配置
	Admin          AdminConfig      `yaml:"admin"`            // 管理服务配置
	MaxHops        int              `yaml:"max_hops"`          // 消息最大转发跳数，防循环；默认 20，0 表示不限制
	Heartbeat      HeartbeatConfig `yaml:"heartbeat"`        // 心跳配置：周期触发自主轮次
}

// HeartbeatConfig 心跳配置（内置于 Dispatcher）
type HeartbeatConfig struct {
	Enabled        bool   `yaml:"enabled"`           // 是否启用心跳
	IntervalSec    int    `yaml:"interval_sec"`       // 触发间隔（秒），如 300 表示每 5 分钟判断一次
	MinIntervalSec int    `yaml:"min_interval_sec"`    // 最小间隔（秒），Constitution：两次触发间隔不得小于此值，0 表示不限制
	Content        string `yaml:"content"`            // 心跳消息的指令内容，空则使用内置默认文案
}

// AdminConfig 管理服务配置
type AdminConfig struct {
	Enabled     bool     `yaml:"enabled"`      // 是否启用管理服务
	Port        string   `yaml:"port"`         // 管理服务端口
	ConfigPath  string   `yaml:"config_path"` // 配置文件路径，供管理页只读展示
	CronAPIURLs []string `yaml:"cron_api_urls"` // Cron 适配器 API 地址列表，供管理页展示任务
	LogPath     string   `yaml:"log_path"`     // 日志文件路径，供管理页查看
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
	baseDir := filepath.Dir(path)
	cfg.System.WorkDir = filepath.Join(baseDir, cfg.System.WorkDir)
	if cfg.System.WorkDir != "" {
		if abs, err := filepath.Abs(cfg.System.WorkDir); err == nil {
			cfg.System.WorkDir = abs
		}
	}
	if cfg.System.StateStore.Enabled && cfg.System.StateStore.DBPath != "" && !filepath.IsAbs(cfg.System.StateStore.DBPath) && cfg.System.WorkDir != "" {
		cfg.System.StateStore.DBPath = filepath.Join(cfg.System.WorkDir, cfg.System.StateStore.DBPath)
	}

	// 若 admin 未配置 config_path，则使用当前加载的配置文件路径，供管理页只读展示
	if cfg.System.Admin.ConfigPath == "" && path != "" {
		if abs, err := filepath.Abs(path); err == nil {
			cfg.System.Admin.ConfigPath = abs
		} else {
			cfg.System.Admin.ConfigPath = path
		}
	}

	// 为每个 adapter 默认设置 adapter_dir = WorkDir/adapters/{id}，若未显式配置
	workDir := cfg.System.WorkDir
	for i := range cfg.Adapters {
		ac := cfg.Adapters[i]
		if ac == nil {
			continue
		}
		if _, has := ac["adapter_dir"]; has {
			continue
		}
		if workDir == "" {
			continue
		}
		id, _ := ac["id"].(string)
		if id == "" {
			continue
		}
		ac["adapter_dir"] = filepath.Join(workDir, "adapters", id)
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
