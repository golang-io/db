package db

import (
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

// NewClickHouse 创建 ClickHouse 数据库连接并注册到全局 dbm。
// Creates ClickHouse connection and registers it to global dbm.
//
// 参数 / Parameters:
//   - options: Options 结构体，需设置 Name 和 URL 字段。
//     若通过 CreateDB 调用，URL 为 DSN 部分（不含 scheme "clickhouse://"），Schema 已被拆分。
//     若直接调用，URL 应为纯 ClickHouse DSN。
//     Options struct; Name and URL are required.
//     When called via CreateDB, URL is the DSN part (without "clickhouse://" scheme).
//     When called directly, URL should be a raw ClickHouse DSN.
//
// 返回值 / Returns:
//   - error: Name 为空或注册失败时返回错误 / Error if Name is empty or registration fails
//
// 行为 / Behavior:
//   - 内部调用 setup(options).open(clickhouse.Open)；setup 通过 LoadOptions 与 newOptions 合并非零字段与默认值
//     Calls setup(options).open(clickhouse.Open); setup merges non-zero fields with defaults via LoadOptions and newOptions
//   - 连接为懒加载：首次调用 db.GetDB(ctx, name) 时才真正建连
//     Lazy connect: connects on first db.GetDB(ctx, name) call
//   - ClickHouse 不能跳过默认事务，否则无法插入数据（在 OpenDB 中处理）
//     ClickHouse requires default transaction for inserts (handled in OpenDB)
//
// 示例 / Example:
//
//	// 通过 CreateDB 统一入口（推荐）/ Via CreateDB (recommended)
//	db.CreateDB(db.Options{Name: "analytics", URL: "clickhouse://user:pass@localhost:9000/mydb"})
//
//	// 直接调用（URL 为纯 DSN，不含 scheme）/ Direct call (URL is raw DSN, no scheme)
//	db.NewClickHouse(db.Options{Name: "clickhouse", URL: "user:pass@localhost:9000/mydb"})
//	gormDB := db.GetDB(ctx, "clickhouse")
func NewClickHouse(options Options) error {
	return Load(setup(options)).open(clickhouse.Open)
}

// OpenClickHouse 立即打开 ClickHouse 连接并返回 *gorm.DB，不向全局 dbm 注册。
// 适用于单次任务、迁移脚本等不需要 GetDB 按名取实例的场景。
//
// Opens ClickHouse immediately and returns *gorm.DB without registering to global dbm.
// Use for one-off jobs, migrations, etc., when name-based GetDB is not needed.
func OpenClickHouse(options Options) (*gorm.DB, error) {
	my := setup(options)
	if err := my.open(clickhouse.Open); err != nil {
		return nil, err
	}
	return my.OpenDB()
}
