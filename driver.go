// driver.go 定义数据库驱动插件化接口与注册机制。
//
// 设计原则:
//   - 根包不静态依赖任何具体驱动（SQLite / MySQL / ClickHouse 等），保持纯净与极速编译；
//   - 各驱动在子包（sqlite / mysql / clickhouse）中实现并通过 init() 向根包注册；
//   - 调用方按需引入驱动子包（如 `_ "github.com/golang-io/db/sqlite"`），无用驱动绝不进入依赖树。

package db

import (
	"fmt"
	"strings"
	"sync"

	"gorm.io/gorm"
)

// Driver 数据库驱动接口与生命周期钩子，由各驱动子包提供。
type Driver struct {
	// Dialector 根据 DSN 返回 GORM 方言 Dialector
	Dialector func(dsn string) gorm.Dialector

	// Config 方言定制 GORM Config（可选，如 ClickHouse 关闭事务跳过）
	Config func(cfg *gorm.Config)

	// PostOpen 建立连接并 Ping 通过后触发的钩子（可选，如 SQLite 开启 WAL）
	PostOpen func(gdb *gorm.DB, dsn string) error
}

var (
	driverMu sync.RWMutex
	drivers  = make(map[string]Driver)
)

// Register 注册指定 scheme 的数据库驱动（大小写无关）。通常在子包 init() 中调用。
func Register(scheme string, d Driver) {
	driverMu.Lock()
	defer driverMu.Unlock()
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	if d.Dialector == nil {
		panic(fmt.Sprintf("db: Register driver %q has nil Dialector", scheme))
	}
	drivers[scheme] = d
}

// GetDriver 获取已注册的驱动。若未注册则返回明确引导错误。
func GetDriver(scheme string) (Driver, error) {
	driverMu.RLock()
	defer driverMu.RUnlock()
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	d, ok := drivers[scheme]
	if !ok {
		return Driver{}, fmt.Errorf("unknown database schema: %q (did you import _ \"github.com/golang-io/db/%s\"?)", scheme, scheme)
	}
	return d, nil
}
