// Package mysql 提供 MySQL 驱动接入。
//
// 引入该包（如 `_ "github.com/golang-io/db/mysql"`）会自动向 db 注册 "mysql" scheme。
package mysql

import (
	"context"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/golang-io/db"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func init() {
	db.Register("mysql", func(dsn string, opts *db.Options) (gorm.Dialector, error) {
		cfg, err := gomysql.ParseDSN(dsn)
		if err != nil {
			return nil, err
		}
		if cfg.Timeout == 0 {
			cfg.Timeout = max(opts.DialTimeout, 0)
		}
		if cfg.ReadTimeout == 0 {
			cfg.ReadTimeout = max(opts.ReadTimeout, 0)
		}
		if cfg.WriteTimeout == 0 {
			cfg.WriteTimeout = max(opts.WriteTimeout, 0)
		}
		return mysql.New(mysql.Config{DSNConfig: cfg}), nil
	})
}

// Dialector 返回 MySQL 方言驱动，超时需在 DSN 中配置。
func Dialector(dsn string) gorm.Dialector {
	return mysql.Open(dsn)
}

// Open 立即打开 MySQL 连接，不注册全局实例；调用方负责关闭。
func Open(ctx context.Context, options db.Options) (*gorm.DB, error) {
	return db.OpenDB(ctx, options.WithScheme("mysql"))
}

// New 登记 MySQL 托管实例。首次 GetDB 时开始拨号，还没连上就等待。
func New(options db.Options) error {
	return db.CreateDB(options.WithScheme("mysql"))
}
