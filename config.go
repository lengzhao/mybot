package mybot

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Config 系统总配置
type Config struct {
	System   SystemConfig    `yaml:"system"`
	Adapters []AdapterConfig `yaml:"adapters"`
}

// SystemConfig 系统全局配置
type SystemConfig struct {
	TraceEnabled bool `yaml:"trace_enabled"`
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
