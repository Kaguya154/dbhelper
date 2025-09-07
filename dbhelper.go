package dbhelper

import (
	"dbhelper/types"
	"fmt"
	"sync"
)

// 驱动注册表
var (
	registeredDriversMu sync.RWMutex
	registeredDrivers   = make(map[string]types.Driver)
)

// RegisterDriver 注册数据库驱动
func RegisterDriver(name string, drv types.Driver) error {
	registeredDriversMu.Lock()
	defer registeredDriversMu.Unlock()

	if name == "" {
		return fmt.Errorf("driver name cannot be empty")
	}
	if drv == nil {
		return fmt.Errorf("driver cannot be nil")
	}
	if _, exists := registeredDrivers[name]; exists {
		return fmt.Errorf("driver %s already registered", name)
	}

	registeredDrivers[name] = drv
	return nil
}

// getDriver 获取注册的驱动
func getDriver(name string) (types.Driver, error) {
	registeredDriversMu.RLock()
	defer registeredDriversMu.RUnlock()

	drv, ok := registeredDrivers[name]
	if !ok {
		return nil, fmt.Errorf("driver %s not registered", name)
	}
	return drv, nil
}

// Open 通过注册的驱动创建数据库连接
func Open(cfg types.DBConfig) (types.Conn, error) {
	drv, err := getDriver(cfg.Driver)
	if err != nil {
		return nil, err
	}
	return drv.Open(cfg)
}

func Condition() *types.CondBuilder {
	return types.NewCondition()
}
