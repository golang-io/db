// conn.go 提供统一的数据库创建入口，根据 Schema 自动分发到对应的驱动工厂。
// conn.go provides a unified database creation entry that dispatches to the
// appropriate driver factory based on the Schema field.

package db

import (
	"fmt"
	"strings"
)

// CreateDB 根据 URL 中的 scheme 自动创建对应类型的数据库连接并注册到全局 dbm。
// 这是一个统一入口函数，内部使用 strings.Cut 以 "://" 为分隔符将 Options.URL 拆分为
// Schema（驱动类型）和 DSN（数据源名称），然后按 Schema（大小写无关，通过 strings.ToLower）
// 分别调用 NewMySQL、NewSQLite 或 NewClickHouse。
//
// CreateDB automatically creates and registers a database connection based on
// the scheme parsed from Options.URL. It uses strings.Cut to split URL at "://"
// into Schema (driver type) and DSN (data source name), then dispatches
// (case-insensitive via strings.ToLower) to NewMySQL, NewSQLite, or NewClickHouse.
//
// 支持的 Schema / Supported Schema values:
//   - "mysql"      → NewMySQL
//   - "sqlite"     → NewSQLite
//   - "clickhouse" → NewClickHouse
//
// 返回值 / Returns:
//   - error: 若 Schema 不在支持列表中，返回 "unknown database schema: ..." 错误；
//     若底层 NewXxx 注册失败也会返回相应 error。
//     Returns "unknown database schema: ..." if Schema is not supported;
//     also propagates errors from underlying NewXxx calls.
//
// 参数 / Parameters:
//   - options: Options 结构体，必须设置 URL（格式 scheme://dsn）和 Name。
//     Schema 由函数内部通过 strings.Cut 从 URL 自动解析，无需外部设置。
//     Options struct; URL (scheme://dsn) and Name are required.
//     Schema is auto-parsed from URL via strings.Cut; no need to set externally.
//
// 示例 / Example:
//
//	db.CreateDB(db.Options{Name: "default", URL: "mysql://user:pass@tcp(host:3306)/mydb?charset=utf8mb4"})
//	db.CreateDB(db.Options{Name: "local", URL: "sqlite://:memory:"})
//	db.CreateDB(db.Options{Name: "analytics", URL: "clickhouse://user:pass@localhost:9000/mydb"})
func CreateDB(options Options) error {
	schema, _, _ := strings.Cut(options.URL, "://")
	switch strings.ToLower(schema) {
	case "mysql":
		return NewMySQL(options)
	case "sqlite":
		return NewSQLite(options)
	case "clickhouse":
		return NewClickHouse(options)
	default:
		return fmt.Errorf("unknown database schema: %s", schema)
	}
}
