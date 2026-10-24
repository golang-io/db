// conn.go 提供统一的数据库创建与打开入口，根据 URL Schema 自动分发到已注册的驱动。

package db

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// CreateDB 根据 URL 中的 scheme 查找已注册驱动，创建连接并注册到全局 dbm。
// 格式为 scheme://dsn，如 "sqlite:///path/to/db" 或 "mysql://user:pass@tcp(...)/db"。
func CreateDB(options Options) error {
	schema, _, ok := strings.Cut(options.URL, "://")
	if !ok {
		return fmt.Errorf("invalid database url: %q (must be in format scheme://dsn)", options.URL)
	}

	driver, err := GetDriver(schema)
	if err != nil {
		return err
	}

	my := setup(options)
	my.driver = driver
	my.dialector = driver.Dialector(my.opts.URL)

	return Load(my).open(driver.Dialector)
}

// OpenURL 立即打开数据库连接并返回 *gorm.DB，不向全局 dbm 注册。
// 适用于单次任务、迁移脚本或由业务层显式管理 *gorm.DB 生命周期的场景。
func OpenURL(ctx context.Context, options Options) (*gorm.DB, error) {
	schema, _, ok := strings.Cut(options.URL, "://")
	if !ok {
		return nil, fmt.Errorf("invalid database url: %q (must be in format scheme://dsn)", options.URL)
	}

	driver, err := GetDriver(schema)
	if err != nil {
		return nil, err
	}

	opts := options
	if opts.Name == "" {
		opts.Name = "default"
	}
	my := setup(opts)
	my.driver = driver
	my.dialector = driver.Dialector(my.opts.URL)

	return my.OpenDB(ctx)
}

// Open 根据传入的 gorm.Dialector 立即打开连接并返回 *gorm.DB，不向全局 dbm 注册。
// 适用于直接使用已有 Dialector 并应用 db 统一连接池/日志配置的纯函数式场景。
func Open(ctx context.Context, dialector gorm.Dialector, opts ...Option) (*gorm.DB, error) {
	my := &db{opts: newOptions(opts...), dialector: dialector}
	if my.opts.Name == "" {
		my.opts.Name = "default"
	}
	return my.OpenDB(ctx)
}
