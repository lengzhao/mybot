package mybot

import (
	"fmt"
	"sync"
)

// Factory 适配器创建工厂函数
type Factory func(id string, config map[string]interface{}) (Adapter, error)

var (
	registryMu sync.RWMutex
	registry   = make(map[string]Factory)
)

// RegisterAdapterType 注册适配器类型到全局注册表
func RegisterAdapterType(typeName string, factory Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if factory == nil {
		panic("adapter factory is nil")
	}
	if _, dup := registry[typeName]; dup {
		panic(fmt.Sprintf("adapter type %s registered twice", typeName))
	}
	registry[typeName] = factory
}

// CreateAdapter 根据类型名称创建适配器实例
func CreateAdapter(typeName string, id string, config map[string]interface{}) (Adapter, error) {
	registryMu.RLock()
	factory, ok := registry[typeName]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown adapter type: %s", typeName)
	}
	return factory(id, config)
}

// GetRegisteredTypes 返回所有已注册的适配器类型
func GetRegisteredTypes() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	types := make([]string, 0, len(registry))
	for k := range registry {
		types = append(types, k)
	}
	return types
}
