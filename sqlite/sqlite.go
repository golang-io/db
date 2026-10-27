// Package sqlite 提供 SQLite 驱动接入。
//
// 引入该包（如 `_ "github.com/golang-io/db/sqlite"`）会自动向 db 注册 "sqlite" scheme。
// WAL 等 PRAGMA 通过 DSN 的 _pragma 参数配置。
// 多连接共享的内存库用 memdb：URL 写 "file:/cache?vfs=memdb"，名字须以 / 开头。
package sqlite

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/golang-io/db"
	"github.com/libtnb/sqlite"
	"gorm.io/gorm"
)

func init() {
	db.Register("sqlite", func(dsn string, opts *db.Options) (gorm.Dialector, error) {
		if dsn == "" {
			return nil, errors.New("empty dsn: use a file path, :memory: or file: URI")
		}
		return dialectorOf(dsn, opts), nil
	})
}

// Dialector 返回 SQLite 方言驱动，供 db.Open 使用；内存库的连接池参数不会自动设置，需在 Options 中自行配置。
func Dialector(dsn string) gorm.Dialector {
	return dialectorOf(dsn, &db.Options{})
}

// dialectorOf 解析一次 DSN，生成方言并按库的类型设置连接池参数：
//   - 事务：DSN 未指定 _txlock 时追加 _txlock=immediate，事务开始即拿写锁，避免多连接下"先读后写"的事务
//     在升级写锁时直接报 database is locked（此时 busy_timeout 不生效）。事务外的读仍可并发。
//   - 内存库：数据挂在连接上，最后一个连接关闭即销毁，所以连接永不回收。
//   - 私有内存库（:memory:、未以 / 开头的 memdb 等）：每个连接各自一个库，所以只能单连接。
func dialectorOf(dsn string, opts *db.Options) gorm.Dialector {
	name, query, hasQuery := strings.Cut(dsn, "?")
	params, _ := url.ParseQuery(query)
	d := dialector{Dialector: sqlite.Dialector{DSN: dsn}}
	if dsn != "" && !params.Has("_txlock") {
		if hasQuery {
			d.DSN += "&_txlock=immediate"
		} else {
			d.DSN += "?_txlock=immediate"
		}
	}

	fileURI := strings.HasPrefix(dsn, "file:")
	if fileURI {
		if u, err := url.Parse(dsn); err == nil {
			name = u.Path
			if u.Opaque != "" {
				name, _ = url.PathUnescape(u.Opaque)
			}
		}
	}
	var private bool
	switch {
	case params.Get("vfs") == "memdb": // memdb：名字以 / 开头才在连接间共享
		private = len(name) < 2 || name[0] != '/' && name[0] != '\\'
	case name == ":memory:" || fileURI && params.Get("mode") == "memory":
		private = !(fileURI && params.Get("cache") == "shared")
	default: // 文件库
		return d
	}
	opts.ConnMaxLifeTime, opts.ConnMaxIdleTime = -1, -1
	if private {
		opts.MaxOpenConns = 1
	}
	return d
}

// dialector 修正上游保存点操作未返回执行错误的问题。
type dialector struct {
	sqlite.Dialector
}

func (dialector) SavePoint(tx *gorm.DB, name string) error {
	return tx.Exec(`SAVEPOINT "` + strings.ReplaceAll(name, `"`, `""`) + `"`).Error
}

func (dialector) RollbackTo(tx *gorm.DB, name string) error {
	return tx.Exec(`ROLLBACK TO SAVEPOINT "` + strings.ReplaceAll(name, `"`, `""`) + `"`).Error
}

// Open 立即打开 SQLite 连接，不注册全局实例；调用方负责关闭。
func Open(ctx context.Context, options db.Options) (*gorm.DB, error) {
	return db.OpenDB(ctx, withScheme(options))
}

// New 登记 SQLite 托管实例。首次 GetDB 时开始拨号，还没连上就等待。
func New(options db.Options) error {
	return db.CreateDB(withScheme(options))
}

// withScheme 补齐 sqlite://，保留原始 DSN。
func withScheme(o db.Options) db.Options {
	if strings.HasPrefix(o.URL, "file:") {
		o.URL = "sqlite://" + o.URL
		return o
	}
	return o.WithScheme("sqlite")
}
