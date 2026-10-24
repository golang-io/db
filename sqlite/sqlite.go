// Package sqlite 提供 SQLite 驱动接入与自动 WAL 配置。
//
// 只要引入该包（如 `_ "github.com/golang-io/db/sqlite"`），就会自动向 db 注册 "sqlite" scheme。
package sqlite

import (
	"context"
	"fmt"
	"strings"

	"github.com/glebarez/sqlite"
	"github.com/golang-io/db"
	"gorm.io/gorm"
)

func init() {
	db.Register("sqlite", db.Driver{
		Dialector: sqlite.Open,
		PostOpen: func(gdb *gorm.DB, dsn string) error {
			if dsn != ":memory:" {
				sqlDB, err := gdb.DB()
				if err != nil {
					return err
				}
				if _, err := sqlDB.Exec("PRAGMA journal_mode=WAL;"); err != nil {
					return fmt.Errorf("set journal mode error: %w", err)
				}
			}
			return nil
		},
	})
}

// Dialector 返回 SQLite 方言驱动。
func Dialector(dsn string) gorm.Dialector {
	return sqlite.Open(dsn)
}

// Open 立即打开 SQLite 连接并返回 *gorm.DB，不向全局 dbm 注册。
// 适用于纯函数式、显式持有 *gorm.DB 实例的场景。
func Open(ctx context.Context, options db.Options) (*gorm.DB, error) {
	opts := options
	if !strings.Contains(opts.URL, "://") {
		opts.URL = "sqlite://" + opts.URL
	}
	return db.OpenURL(ctx, opts)
}

// New 创建 SQLite 连接并注册到全局 dbm。
func New(options db.Options) error {
	opts := options
	if !strings.Contains(opts.URL, "://") {
		opts.URL = "sqlite://" + opts.URL
	}
	return db.CreateDB(opts)
}
