// compat.go 提供向后兼容的特定驱动快捷入口函数。
// 内部统一委托给 OpenURL / CreateDB 经驱动注册中心分发，根包无需 import 任何具体驱动。

package db

import (
	"context"
	"strings"

	"gorm.io/gorm"
)

// OpenSQLite 立即打开 SQLite 连接并返回 *gorm.DB，不向全局 dbm 注册。
// 若 URL 不含 scheme，自动补齐 "sqlite://"。需导入驱动：_ "github.com/golang-io/db/sqlite"
func OpenSQLite(ctx context.Context, options Options) (*gorm.DB, error) {
	opts := options
	if !strings.Contains(opts.URL, "://") {
		opts.URL = "sqlite://" + opts.URL
	}
	return OpenURL(ctx, opts)
}

// NewSQLite 创建 SQLite 数据库连接并注册到全局 dbm。
// 若 URL 不含 scheme，自动补齐 "sqlite://"。需导入驱动：_ "github.com/golang-io/db/sqlite"
func NewSQLite(options Options) error {
	opts := options
	if !strings.Contains(opts.URL, "://") {
		opts.URL = "sqlite://" + opts.URL
	}
	return CreateDB(opts)
}

// OpenMySQL 立即打开 MySQL 连接并返回 *gorm.DB，不向全局 dbm 注册。
// 若 URL 不含 scheme，自动补齐 "mysql://"。需导入驱动：_ "github.com/golang-io/db/mysql"
func OpenMySQL(ctx context.Context, options Options) (*gorm.DB, error) {
	opts := options
	if !strings.Contains(opts.URL, "://") {
		opts.URL = "mysql://" + opts.URL
	}
	return OpenURL(ctx, opts)
}

// NewMySQL 创建 MySQL 数据库连接并注册到全局 dbm。
// 若 URL 不含 scheme，自动补齐 "mysql://"。需导入驱动：_ "github.com/golang-io/db/mysql"
func NewMySQL(options Options) error {
	opts := options
	if !strings.Contains(opts.URL, "://") {
		opts.URL = "mysql://" + opts.URL
	}
	return CreateDB(opts)
}

// OpenClickHouse 立即打开 ClickHouse 连接并返回 *gorm.DB，不向全局 dbm 注册。
// 若 URL 不含 scheme，自动补齐 "clickhouse://"。需导入驱动：_ "github.com/golang-io/db/clickhouse"
func OpenClickHouse(ctx context.Context, options Options) (*gorm.DB, error) {
	opts := options
	if !strings.Contains(opts.URL, "://") {
		opts.URL = "clickhouse://" + opts.URL
	}
	return OpenURL(ctx, opts)
}

// NewClickHouse 创建 ClickHouse 数据库连接并注册到全局 dbm。
// 若 URL 不含 scheme，自动补齐 "clickhouse://"。需导入驱动：_ "github.com/golang-io/db/clickhouse"
func NewClickHouse(options Options) error {
	opts := options
	if !strings.Contains(opts.URL, "://") {
		opts.URL = "clickhouse://" + opts.URL
	}
	return CreateDB(opts)
}
