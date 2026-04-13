package db

import (
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// NewSQLite 创建 SQLite 数据库连接并注册到全局 dbm。
// Creates SQLite connection and registers it to global dbm.
//
// 参数 / Parameters:
//   - options: Options 结构体，需设置 Name 和 URL 字段。
//     若通过 CreateDB 调用，URL 为 DSN 部分（不含 scheme "sqlite://"），Schema 已被拆分。
//     若直接调用，URL 应为纯 SQLite DSN。
//     Options struct; Name and URL are required.
//     When called via CreateDB, URL is the DSN part (without "sqlite://" scheme).
//     When called directly, URL should be a raw SQLite DSN.
//
// URL（DSN）格式 / URL (DSN) format:
//   - ":memory:" - 内存数据库，单测常用
//     In-memory database, common for unit tests
//   - "/path/to/file.db" - 文件数据库，开发/轻量部署
//     File-based database, for dev or light deployment
//
// 返回值 / Returns:
//   - error: Name 为空或注册失败时返回错误 / Error if Name is empty or registration fails
//
// 行为 / Behavior:
//   - 内部调用 setup(options).open(sqlite.Open)；setup 通过 LoadOptions 与 newOptions 合并非零字段与默认值
//     Calls setup(options).open(sqlite.Open); setup merges non-zero fields with defaults via LoadOptions and newOptions
//   - 连接为懒加载：首次 db.GetDB(ctx, name) 时才建连
//     Lazy connect: connects on first db.GetDB(ctx, name) call
//
// 示例 / Example:
//
//	// 通过 CreateDB 统一入口（推荐）/ Via CreateDB (recommended)
//	db.CreateDB(db.Options{Name: "default", URL: "sqlite://:memory:"})
//
//	// 直接调用（URL 为纯 DSN，不含 scheme）/ Direct call (URL is raw DSN, no scheme)
//	db.NewSQLite(db.Options{Name: "default", URL: ":memory:"})
//	db.NewSQLite(db.Options{Name: "local", URL: "/data/iot-mesh.db"})
func NewSQLite(options Options) error {
	return Load(setup(options)).open(sqlite.Open)
}

// OpenSQLite 立即打开 SQLite 连接并返回 *gorm.DB，不向全局 dbm 注册。
// 适用于单次任务、迁移脚本、本地工具等不需要 GetDB 按名取实例的场景。
//
// Opens SQLite immediately and returns *gorm.DB without registering to global dbm.
// Use for one-off jobs, migrations, local tools, etc., when name-based GetDB is not needed.
func OpenSQLite(options Options) (*gorm.DB, error) {
	my := setup(options)
	if err := my.open(sqlite.Open); err != nil {
		return nil, err
	}
	return my.OpenDB()
}
