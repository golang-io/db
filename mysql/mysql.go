// Package mysql 提供 MySQL 驱动接入。
//
// 只要引入该包（如 `_ "github.com/golang-io/db/mysql"`），就会自动向 db 注册 "mysql" scheme。
package mysql

import (
	"context"
	"strings"

	"github.com/golang-io/db"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func init() {
	db.Register("mysql", db.Driver{
		Dialector: mysql.Open,
	})
}

// Dialector 返回 MySQL 方言驱动。
func Dialector(dsn string) gorm.Dialector {
	return mysql.Open(dsn)
}

// Open 立即打开 MySQL 连接并返回 *gorm.DB，不向全局 dbm 注册。
// 适用于纯函数式、显式持有 *gorm.DB 实例的场景。
func Open(ctx context.Context, options db.Options) (*gorm.DB, error) {
	opts := options
	if !strings.Contains(opts.URL, "://") {
		opts.URL = "mysql://" + opts.URL
	}
	return db.OpenURL(ctx, opts)
}

// New 创建 MySQL 连接并注册到全局 dbm。
func New(options db.Options) error {
	opts := options
	if !strings.Contains(opts.URL, "://") {
		opts.URL = "mysql://" + opts.URL
	}
	return db.CreateDB(opts)
}
