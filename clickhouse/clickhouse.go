// Package clickhouse 提供 ClickHouse 驱动接入与事务默认行为定制。
//
// 只要引入该包（如 `_ "github.com/golang-io/db/clickhouse"`），就会自动向 db 注册 "clickhouse" scheme。
package clickhouse

import (
	"context"
	"strings"

	"github.com/golang-io/db"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

func init() {
	db.Register("clickhouse", db.Driver{
		Dialector: clickhouse.Open,
		Config: func(cfg *gorm.Config) {
			// ClickHouse 不能跳过默认事务，否则无法插入数据
			cfg.SkipDefaultTransaction = false
		},
	})
}

// Dialector 返回 ClickHouse 方言驱动。
func Dialector(dsn string) gorm.Dialector {
	return clickhouse.Open(dsn)
}

// Open 立即打开 ClickHouse 连接并返回 *gorm.DB，不向全局 dbm 注册。
// 适用于纯函数式、显式持有 *gorm.DB 实例的场景。
func Open(ctx context.Context, options db.Options) (*gorm.DB, error) {
	opts := options
	if !strings.Contains(opts.URL, "://") {
		opts.URL = "clickhouse://" + opts.URL
	}
	return db.OpenURL(ctx, opts)
}

// New 创建 ClickHouse 连接并注册到全局 dbm。
func New(options db.Options) error {
	opts := options
	if !strings.Contains(opts.URL, "://") {
		opts.URL = "clickhouse://" + opts.URL
	}
	return db.CreateDB(opts)
}
