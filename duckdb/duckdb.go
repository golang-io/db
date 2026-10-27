// Package duckdb 提供 DuckDB 驱动接入（底层 github.com/duckdb/duckdb-go/v2，需要 CGO）。
//
// 引入该包（如 `_ "github.com/golang-io/db/duckdb"`）会自动向 db 注册 "duckdb" scheme。
// DSN 为文件路径；空字符串或 ":memory:" 为内存库。同一个 *sql.DB 的所有连接共享同一个库，
// 内存库在连接池关闭前一直存在，不需要像 SQLite 那样限制单连接或禁止回收。
package duckdb

import (
	"context"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/golang-io/db"
	"gorm.io/gorm"
)

// ErrNotSupport 表示不支持的操作，可用 errors.Is 判断。
var ErrNotSupport = fmt.Errorf("duckdb: notsupport: %w", gorm.ErrUnsupportedDriver)

func init() {
	db.Register("duckdb", func(dsn string, _ *db.Options) (gorm.Dialector, error) {
		return Dialector(dsn), nil
	})
}

// Dialector 返回 DuckDB 方言驱动。
func Dialector(dsn string) gorm.Dialector {
	return dialect{dsn: dsn}
}

// Open 立即打开 DuckDB 连接，不注册全局实例；调用方负责关闭。
func Open(ctx context.Context, options db.Options) (*gorm.DB, error) {
	return db.OpenDB(ctx, options.WithScheme("duckdb"))
}

// New 登记 DuckDB 托管实例。首次 GetDB 时开始拨号，还没连上就等待。
func New(options db.Options) error {
	return db.CreateDB(options.WithScheme("duckdb"))
}
