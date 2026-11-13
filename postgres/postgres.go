// Package postgres 提供 PostgreSQL 驱动接入。
//
// 引入该包（如 `_ "github.com/golang-io/db/postgres"`）会自动向 db 注册 "postgres" 与 "postgresql" scheme。
// DSN 使用 URL 格式；根包去掉外层 scheme 后，工厂会补回 "postgres://" 并由 pgx.ParseConfig 预校验配置错误。
package postgres

import (
	"context"
	"slices"
	"strings"

	"github.com/golang-io/db"
	"github.com/jackc/pgx/v5"
	gormpg "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func init() {
	factory := func(dsn string, _ *db.Options) (gorm.Dialector, error) {
		fullDSN := withScheme(dsn)
		// 预先通过 pgx.ParseConfig 校验 URL 格式与连接参数，使语法/参数错误在 CreateDB / OpenDB 阶段立即返回，
		// 避免留到首次 GetDB 才暴露并陷入 10s 无限重试。
		if _, err := pgx.ParseConfig(fullDSN); err != nil {
			return nil, err
		}
		return newDialector(fullDSN), nil
	}
	db.Register("postgres", factory)
	db.Register("postgresql", factory)
}

// Dialector 返回 PostgreSQL 方言驱动，dsn 可省略 postgres:// 前缀（已有前缀忽略大小写规范化为小写）。
// 只支持 URL 格式 DSN，key=value 格式请自行注册方言工厂。
func Dialector(dsn string) gorm.Dialector {
	return newDialector(withScheme(dsn))
}

func newDialector(fullDSN string) gorm.Dialector {
	return dialector{Dialector: gormpg.Dialector{Config: &gormpg.Config{DSN: fullDSN}}}
}

// withScheme 补回并规范化小写 URL scheme：pgx 仅把小写的 postgres:// 或 postgresql:// 开头的 DSN 当作 URL 解析。
func withScheme(dsn string) string {
	for _, prefix := range []string{"postgresql://", "postgres://"} {
		if len(dsn) >= len(prefix) && strings.EqualFold(dsn[:len(prefix)], prefix) {
			return prefix + dsn[len(prefix):]
		}
	}
	return "postgres://" + dsn
}

// dialector 包装上游 gorm.io/driver/postgres.Dialector，修正两处上游行为：
//  1. 隔离 Config 副本，防止含 TimeZone 参数的 DSN 在托管后台重试 Initialize 时向共享切片 OptionOpenDB 无限追加回调；
//  2. 修正 SavePoint / RollbackTo 丢弃 tx.Exec 返回值导致错误被吞掉且未转义标识符的问题（与 sqlite 子包一致）。
type dialector struct {
	gormpg.Dialector
}

func (d dialector) Initialize(gdb *gorm.DB) error {
	if d.Config != nil {
		cfg := *d.Config
		cfg.OptionOpenDB = slices.Clone(d.Config.OptionOpenDB)
		d.Config = &cfg
	}
	return d.Dialector.Initialize(gdb)
}

func (dialector) SavePoint(tx *gorm.DB, name string) error {
	return tx.Exec(`SAVEPOINT "` + strings.ReplaceAll(name, `"`, `""`) + `"`).Error
}

func (dialector) RollbackTo(tx *gorm.DB, name string) error {
	return tx.Exec(`ROLLBACK TO SAVEPOINT "` + strings.ReplaceAll(name, `"`, `""`) + `"`).Error
}

// Open 立即打开 PostgreSQL 连接，不注册全局实例；调用方负责关闭。
func Open(ctx context.Context, options db.Options, opts ...db.Option) (*gorm.DB, error) {
	return db.OpenDB(ctx, options.WithScheme("postgres"), opts...)
}

// New 登记 PostgreSQL 托管实例。首次 GetDB 时开始拨号，还没连上就等待。
func New(options db.Options, opts ...db.Option) error {
	return db.CreateDB(options.WithScheme("postgres"), opts...)
}
