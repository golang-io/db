// driver.go 定义数据库驱动插件化接口与注册机制。
//
// 设计原则:
//   - 根包不静态 import 任何具体驱动（SQLite / MySQL / ClickHouse 等）；
//   - mysql / sqlite / duckdb 子包按需导入并自动注册；其他方言在 init() 中调用 Register；
//   - 子包和根包测试依赖具体驱动，因此 go.mod 列出这些依赖；未被业务导入的驱动不会编译进业务二进制。

package db

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// drivers 驱动注册表 scheme->方言工厂，不加锁：只在 init() 阶段注册，之后只读。
var drivers = make(map[string]func(string, *Options) (gorm.Dialector, error))

// Register 注册指定 scheme 的方言工厂（大小写无关）。仅限在 init() 中调用。
// scheme 建议与 Dialector.Name() 一致。
// 工厂接收去掉 "scheme://" 的 DSN 和已补齐默认值的 *Options，负责 DSN 校验及超时合并，也可按 DSN 调整 Options（如 sqlite 内存库的连接池参数）。
// 工厂返回配置错误时 CreateDB / OpenDB 直接返回，不注册、不进入建连重试。
// 用于 CreateDB 的工厂不得绑定自带 Conn：失败重试复用同一方言，不能恢复被 GORM 关闭的自带池。
// 以下为不导入 sqlite / mysql 子包时的注册示例，按需保留所用驱动及导入；每个 scheme 只能注册一次，已导入子包的 scheme 不要再注册。
//
//	import (
//		"github.com/libtnb/sqlite"
//		gomysql "github.com/go-sql-driver/mysql"
//		"github.com/golang-io/db"
//		"gorm.io/driver/clickhouse"
//		gormmysql "gorm.io/driver/mysql"
//		"gorm.io/gorm"
//	)
//
//	func init() {
//		db.Register("sqlite", func(dsn string, _ *db.Options) (gorm.Dialector, error) {
//			return sqlite.Open(dsn), nil
//		})
//		db.Register("mysql", func(dsn string, opts *db.Options) (gorm.Dialector, error) {
//			cfg, err := gomysql.ParseDSN(dsn)
//			if err != nil {
//				return nil, err
//			}
//			if cfg.Timeout == 0 {
//				cfg.Timeout = max(opts.DialTimeout, 0)
//			}
//			if cfg.ReadTimeout == 0 {
//				cfg.ReadTimeout = max(opts.ReadTimeout, 0)
//			}
//			if cfg.WriteTimeout == 0 {
//				cfg.WriteTimeout = max(opts.WriteTimeout, 0)
//			}
//			return gormmysql.New(gormmysql.Config{DSNConfig: cfg}), nil
//		})
//		db.Register("clickhouse", func(dsn string, _ *db.Options) (gorm.Dialector, error) {
//			return clickhouse.Open("clickhouse://" + dsn), nil
//		})
//	}
//
// ClickHouse DSN 需补回 scheme；其默认事务由本包按 Dialector.Name() 自动保留。
// MySQL 示例仅在解析后的超时为零时回填 Options；DSN 中显式的 0s 也会被回填。
// Open 使用调用方传入的方言，不调用工厂，因此不会执行工厂中的校验或超时合并。
func Register(scheme string, d func(string, *Options) (gorm.Dialector, error)) {
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	if d == nil {
		panic(fmt.Sprintf("db: Register driver %q has nil Dialector", scheme))
	}
	if _, dup := drivers[scheme]; dup {
		panic(fmt.Sprintf("db: Register called twice for driver %q", scheme))
	}
	drivers[scheme] = d
}

// GetDriver 获取已注册的方言工厂。若未注册则返回明确引导错误。
func GetDriver(scheme string) (func(string, *Options) (gorm.Dialector, error), error) {
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	d, ok := drivers[scheme]
	if !ok {
		return nil, fmt.Errorf("unknown database schema: %q (call db.Register in init with a GORM dialector factory)", scheme)
	}
	return d, nil
}
