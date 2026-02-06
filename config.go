package mybot

import (
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
	TraceEnabled   bool   `yaml:"trace_enabled"`
	WorkDir        string `yaml:"work_dir"`         // 主程序工作目录，空则用进程 cwd；各 adapter 默认目录为 work_dir/adapters/{adapter_id}
	DefaultAdapter string `yaml:"default_adapter"` // 路由兜底：无 Target 且无 Tags 或标签无匹配时投递的 adapter id
}

// AdapterConfig 适配器实例配置
type AdapterConfig struct {
	ID      string                 `yaml:"id"`
	Type    string                 `yaml:"type"`
	Enabled bool                   `yaml:"enabled"`
	Config  map[string]interface{} `yaml:"config"`
}

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
