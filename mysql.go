package db

import (
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// NewMySQL 创建 MySQL 数据库连接并注册到全局 dbm。
// Creates MySQL connection and registers it to global dbm.
//
// 参数 / Parameters:
//   - options: Options 结构体，需设置 Name 和 URL 字段。
//     若通过 CreateDB 调用，URL 为 DSN 部分（不含 scheme "mysql://"），Schema 已被拆分。
//     若直接调用，URL 应为纯 MySQL DSN（如 "user:pass@tcp(host:3306)/db?charset=utf8mb4"）。
//     Options struct; Name and URL are required.
//     When called via CreateDB, URL is the DSN part (without "mysql://" scheme).
//     When called directly, URL should be a raw MySQL DSN.
//
// 返回值 / Returns:
//   - error: Name 为空或注册失败时返回错误 / Error if Name is empty or registration fails
//
// 行为 / Behavior:
//   - 内部调用 setup(options).open(mysql.Open)；setup 通过 LoadOptions 与 newOptions 合并非零字段与默认值
//     Calls setup(options).open(mysql.Open); setup merges non-zero fields with defaults via LoadOptions and newOptions
//   - 连接为懒加载：首次调用 db.GetDB(ctx, name) 时才真正建连
//     Lazy connect: connects on first db.GetDB(ctx, name) call
//   - 连接失败时每 10 秒自动重试，Logger 输出重试日志
//     Retries every 10s on connect failure; Logger logs retries
//
// 示例 / Example:
//
//	// 通过 CreateDB 统一入口（推荐）/ Via CreateDB (recommended)
//	db.CreateDB(db.Options{Name: "default", URL: "mysql://user:pass@tcp(127.0.0.1:3306)/mydb?charset=utf8mb4&parseTime=True&loc=Local"})
//
//	// 直接调用（URL 为纯 DSN，不含 scheme）/ Direct call (URL is raw DSN, no scheme)
//	db.NewMySQL(db.Options{Name: "default", URL: "user:pass@tcp(127.0.0.1:3306)/mydb?charset=utf8mb4&parseTime=True&loc=Local"})
//	gormDB := db.GetDB(ctx, "default")
func NewMySQL(options Options) error {
	return Load(setup(options)).open(mysql.Open)
}

// OpenMySQL 立即打开 MySQL 连接并返回 *gorm.DB，不向全局 dbm 注册。
// 适用于单次任务、迁移脚本等不需要 GetDB 按名取实例的场景。
//
// Opens MySQL immediately and returns *gorm.DB without registering to global dbm.
// Use for one-off jobs, migrations, etc., when name-based GetDB is not needed.
func OpenMySQL(options Options) (*gorm.DB, error) {
	my := setup(options)
	if err := my.open(mysql.Open); err != nil {
		return nil, err
	}
	return my.OpenDB()
}
