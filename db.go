// Package db 提供数据库连接与管理的核心能力。
//
// Package db provides core capabilities for database connection and management.
//
// 主要特性 / Features:
//   - 多驱动支持：MySQL、SQLite、ClickHouse
//     Multi-driver support: MySQL, SQLite, ClickHouse
//   - Options 结构体配置连接参数，支持 YAML 反序列化
//     Options struct for connection configuration, supports YAML deserialization
//   - 连接池：MaxIdleConns、MaxOpenConns、ConnMaxLifetime、ConnMaxIdleTime
//     Connection pool: MaxIdleConns, MaxOpenConns, ConnMaxLifetime, ConnMaxIdleTime
//   - 懒连接：首次 GetDB 时建立连接，连接失败每 10 秒重试
//     Lazy connection: connect on first GetDB call; retry every 10s on failure
//   - 双通道日志：SQL 走标准 log（Output），业务（连接/重试）走 Logger（对接 slog）
//     Dual-channel logging: SQL via Output (std log), business (connect/retry) via Logger (slog)
//   - 全局多实例：按 Name 注册，按名获取
//     Global multi-instance: register by Name, fetch by name
//
// 典型用法 / Typical usage:
//
//	// 通过 CreateDB 统一入口，自动从 URL 解析驱动类型
//	// Unified entry via CreateDB, auto-parses driver type from URL
//	db.CreateDB(db.Options{Name: "default", URL: "mysql://user:pass@tcp(host:3306)/db?charset=utf8mb4&parseTime=True&loc=Local"})
//
//	// 或直接调用具体驱动的构造函数 / Or call driver-specific constructors directly
//	db.NewMySQL(db.Options{Name: "default", URL: "user:pass@tcp(host:3306)/db?charset=utf8mb4"})
//
//	// 业务层按名获取 GORM 实例（自动懒连接 + 带 context）
//	// Get GORM instance by name (lazy connect + context-aware)
//	gormDB := db.GetDB(ctx, "default")
package db

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Options 数据库连接配置，支持 YAML 反序列化与函数式选项。
// Database connection options, supports YAML deserialization and functional options.
type Options struct {
	schema          string          `yaml:"-"`               // 驱动类型："mysql" | "sqlite" | "clickhouse"，由 CreateDB 内部从 URL 解析，无需手动设置 / Driver type, auto-parsed from URL by CreateDB
	Name            string          `yaml:"Name"`            // 实例名，用于 GetDB(ctx, name) / Instance name for lookup
	URL             string          `yaml:"URL"`             // 连接串 scheme://dsn，如 mysql://... 或 sqlite://... / Connection URL
	TimeZone        string          `yaml:"TimeZone"`        // 时区，如 "Local" / Timezone
	MaxIdleConns    int             `yaml:"MaxIdleConns"`    // 连接池最大空闲连接数，默认 10 / Max idle connections
	MaxOpenConns    int             `yaml:"MaxOpenConns"`    // 连接池最大打开连接数，默认 50 / Max open connections
	ConnMaxLifeTime time.Duration   `yaml:"ConnMaxLifeTime"` // 连接最大存活时间，默认 30m / Conn max lifetime
	ConnMaxIdleTime time.Duration   `yaml:"ConnMaxIdleTime"` // 连接最大空闲时间，默认 5m；用于回收被上游（MySQL wait_timeout / LB 空闲超时）静默关闭的连接 / Conn max idle time; reclaim conns silently closed by upstream
	LongQueryTime   time.Duration   `yaml:"LongQueryTime"`   // 慢查询阈值，超过打 SLOW 日志，默认 3s / Slow query threshold
	Output          io.Writer       `yaml:"-"`               // SQL 日志输出，默认 os.Stdout / SQL log output
	Log             Logger          `yaml:"-"`               // 业务日志（连接、重试），对接 slog / Business logger
	LogLevel        logger.LogLevel `yaml:"-"`               // GORM 日志级别，默认 Warn / GORM log level
}

// LoadOptions 将 Options 中所有已设置的字段转为 []Option 切片，供 setup 使用。
// 仅包含非零值字段，未设置的字段由 newOptions 的默认值兜底。
//
// Converts all set fields in Options to []Option slice for use with setup.
// Only includes non-zero-value fields; unset fields fall back to newOptions defaults.
func (o *Options) LoadOptions() []Option {
	var opts []Option
	if o.Name != "" {
		opts = append(opts, Name(o.Name))
	}
	if o.URL != "" {
		opts = append(opts, URL(o.URL))
	}
	if o.TimeZone != "" {
		opts = append(opts, TimeZone(o.TimeZone))
	}
	if o.MaxIdleConns != 0 {
		opts = append(opts, MaxIdleConns(o.MaxIdleConns))
	}
	if o.MaxOpenConns != 0 {
		opts = append(opts, MaxOpenConns(o.MaxOpenConns))
	}
	if o.ConnMaxLifeTime != 0 {
		opts = append(opts, ConnMaxLifeTime(o.ConnMaxLifeTime))
	}
	if o.ConnMaxIdleTime != 0 {
		opts = append(opts, ConnMaxIdleTime(o.ConnMaxIdleTime))
	}
	if o.LongQueryTime != 0 {
		opts = append(opts, LongQueryTime(o.LongQueryTime))
	}
	if o.Output != nil {
		opts = append(opts, WithOutput(o.Output))
	}
	if o.Log != nil {
		opts = append(opts, Log(o.Log))
	}
	if o.LogLevel != 0 {
		opts = append(opts, LogLevel(o.LogLevel))
	}
	return opts
}

// Option 函数式选项，用于灵活配置 Options。
// Functional option for configuring Options.
type Option func(*Options)

// Name 设置数据库实例名称。用于全局注册与按名获取。
// Sets the database instance name for global registration and lookup.
func Name(name string) Option {
	return func(opts *Options) {
		opts.Name = name
	}
}

// GenMySQL 根据 host、port、user、password、database 等参数生成 MySQL 连接 URL。
// Generates MySQL connection URL from host, port, user, password, database, etc.
//
// 示例 / Example: GenMySQL("localhost", 3306, "root", "pwd", "mydb", "utf8mb4", "Local")
func GenMySQL(host string, port int, username, password, database, charset, location string) Option {
	return func(opts *Options) {
		opts.URL = fmt.Sprintf(
			"mysql://%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=True&loc=%s",
			username, password, host, port, database, charset, location,
		)
	}
}

// URL 直接设置连接串，格式 scheme://dsn。内部会解析 scheme 填充 Schema 字段。
// Sets connection URL (scheme://dsn). Parses scheme to populate Schema.
//
// 示例 / Example: URL("mysql://user:pass@tcp(host:3306)/db") 或 URL("sqlite://:memory:")
func URL(url string) Option {
	return func(opts *Options) {
		var ok bool
		if opts.schema, opts.URL, ok = strings.Cut(url, "://"); !ok {
			panic(fmt.Errorf("url must have scheme (sqlite:// or mysql://) and dsn is required: %s", url))
		}
	}
}

// WithOutput 设置 SQL 日志输出目标，使用标准 log 格式（不走 slog）。
// Sets SQL log output target using standard log format (not slog).
func WithOutput(w io.Writer) Option {
	return func(opts *Options) {
		opts.Output = w
	}
}

// TimeZone 设置数据库时区，如 "Local"、"UTC"、"Asia/Shanghai"。
// Sets database timezone, e.g. "Local", "UTC", "Asia/Shanghai".
func TimeZone(tz string) Option {
	return func(opts *Options) {
		opts.TimeZone = tz
	}
}

// Log 设置业务日志（连接、重试等），通常对接 slog。
// Sets business logger (connect, retry, etc.), typically wired to slog.
func Log(l Logger) Option {
	return func(opts *Options) {
		opts.Log = l
	}
}

// LogLevel 设置 GORM 日志级别：Silent、Error、Warn、Info。
// Sets GORM log level: Silent, Error, Warn, Info.
func LogLevel(lv logger.LogLevel) Option {
	return func(opts *Options) {
		opts.LogLevel = lv
	}
}

// LongQueryTime 设置慢查询阈值。超过此时间的 SQL 会在日志中标记为 "SLOW:"，默认 3 秒。
// Sets slow query threshold. SQLs exceeding this duration are logged with "SLOW:" prefix, default 3s.
func LongQueryTime(d time.Duration) Option {
	return func(opts *Options) {
		opts.LongQueryTime = d
	}
}

// MaxOpenConns 设置连接池最大打开连接数。默认 200，0 表示不限。
// Sets max open connections in pool. Default 200; 0 means unlimited.
func MaxOpenConns(n int) Option {
	return func(opts *Options) {
		opts.MaxOpenConns = n
	}
}

// MaxIdleConns 设置连接池最大空闲连接数。默认 200。
// Sets max idle connections in pool. Default 200.
func MaxIdleConns(n int) Option {
	return func(opts *Options) {
		opts.MaxIdleConns = n
	}
}

// ConnMaxLifeTime 设置连接的最大存活时间。默认 30 分钟。超时的连接会被关闭并回收。
// 取值建议严格小于 MySQL 的 wait_timeout 以及链路上任何 LB 的 TCP 空闲超时，避免拿到已被对端关闭的死连接（broken pipe）。
// Sets max connection lifetime. Default 30 minutes. Should be strictly less than MySQL's wait_timeout
// and any LB TCP idle timeout to avoid using connections silently closed by upstream (broken pipe).
func ConnMaxLifeTime(d time.Duration) Option {
	return func(opts *Options) {
		opts.ConnMaxLifeTime = d
	}
}

// ConnMaxIdleTime 设置连接的最大空闲时间。默认 5 分钟。空闲超过该时长的连接会被关闭。
// 主要用于回收被上游（MySQL wait_timeout / 中间 LB 空闲超时）静默关闭的空闲连接，
// 避免 "write tcp ... broken pipe" 错误。取值应小于链路上最小的空闲超时（一般 LB 为 900s）。
// Sets max idle duration of a connection. Default 5 minutes. Reclaims idle connections that may have been
// silently closed by upstream (MySQL wait_timeout / LB idle timeout), preventing "broken pipe" errors.
func ConnMaxIdleTime(d time.Duration) Option {
	return func(opts *Options) {
		opts.ConnMaxIdleTime = d
	}
}

// newOptions 合并默认值与用户传入的 Option，返回完整 Options。
// Merges defaults with user Options and returns the full Options.
func newOptions(opts ...Option) Options {
	options := Options{
		MaxIdleConns:    10,
		MaxOpenConns:    50,
		ConnMaxLifeTime: 5 * time.Minute,
		ConnMaxIdleTime: 1 * time.Minute,
		LongQueryTime:   3 * time.Second,
		Log:             &noLogger{},
		LogLevel:        logger.Warn,
		Output:          os.Stdout,
	}
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

// db 封装 GORM 连接、配置与懒初始化，实现 DB 接口。
// Wraps GORM connection, config, and lazy init; implements DB interface.
type db struct {
	db        *gorm.DB       // GORM 实例，Connect 后可用 / GORM instance, valid after Connect
	opts      Options        // 配置选项 / Options
	once      sync.Once      // 保证 Connect 只执行一次 / Ensures Connect runs once
	dialector gorm.Dialector // 驱动（mysql / sqlite） / Driver
}

// setup 根据 Options 结构体创建 db 实例，不立即建连。
// 先通过 LoadOptions 将 options 中非零值字段转换为 []Option，与额外 opts 合并后，
// 再通过 newOptions 生成带默认值的最终配置。
// 确保调用方未设置的字段（零值）自动获得合理默认值（如 MaxIdleConns=200、Output=os.Stdout 等）。
//
// Creates db instance from Options; does not connect yet.
// Uses LoadOptions to convert non-zero fields in options to []Option, merges with
// additional opts, then generates final config with defaults via newOptions.
// Unset (zero-value) fields get sensible defaults (e.g. MaxIdleConns=200, Output=os.Stdout).
func setup(options Options, opts ...Option) *db {
	// 将 Options 结构体中的非零值字段转换为 []Option，追加到额外选项之后
	// Convert non-zero fields from Options struct to []Option, append after additional opts
	opts = append(opts, options.LoadOptions()...)
	return &db{opts: newOptions(opts...)}
}

// open 使用驱动工厂（如 mysql.Open、sqlite.Open）设置 dialector，并将实例注册到全局 dbm。
// 若 Name 为空则返回错误，不会注册。
// Sets dialector via driver factory (e.g. mysql.Open, sqlite.Open) and registers
// the instance to global dbm. Returns error if Name is empty.
func (my *db) open(f func(dsn string) gorm.Dialector) error {
	if my.opts.Name == "" {
		return fmt.Errorf("name is required, dsnURL: %s", my.opts.URL)
	}
	my.dialector = f(my.opts.URL)
	return nil
}

// OpenDB 使用 dialector 打开 GORM 连接并配置连接池参数。
// 若 Name 非空，连接成功后自动 Load 到全局 dbm。
// Opens GORM connection with dialector and configures connection pool. If Name is set, auto-registers to dbm.
func (my *db) OpenDB() (*gorm.DB, error) {
	if my.opts.Name == "" {
		return nil, fmt.Errorf("name is required, dsnURL: %s", my.opts.URL)
	}
	config := &gorm.Config{
		SkipDefaultTransaction:                   true,
		PrepareStmt:                              false,
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger: &DBLogger{
			output:        log.New(my.opts.Output, "", log.LstdFlags|log.Lmicroseconds),
			LogLevel:      my.opts.LogLevel,
			LongQueryTime: my.opts.LongQueryTime,
		},
	}

	// ClickHouse 不能跳过默认事务，否则无法插入数据 / ClickHouse requires default transaction for inserts
	if my.opts.schema == "clickhouse" {
		config.SkipDefaultTransaction = false
	}

	var err error
	if my.db, err = gorm.Open(my.dialector, config); err != nil {
		return nil, fmt.Errorf("open db error: %w", err)
	}

	// 获取 sql.DB 实例，设置连接池参数
	db, err := my.db.DB()
	if err != nil {
		return nil, fmt.Errorf("get sql.DB error: %w", err)
	}
	db.SetMaxIdleConns(my.opts.MaxIdleConns)
	db.SetMaxOpenConns(my.opts.MaxOpenConns)
	db.SetConnMaxLifetime(my.opts.ConnMaxLifeTime)
	db.SetConnMaxIdleTime(my.opts.ConnMaxIdleTime)
	if my.opts.schema == "sqlite" && my.opts.URL != ":memory:" {
		if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
			return nil, fmt.Errorf("set journal mode error: %w", err)
		}
	}
	return my.db, nil
}

// Connect 建立数据库连接。sync.Once 保证只执行一次；失败时每 10 秒重试，Logger 输出重试日志。
// ctx 传入 Logger，便于结构化字段与测试；重试间隔固定为 10s（与 ctx 是否取消无关）。
//
// Establishes DB connection. sync.Once ensures single execution; retries every 10s on failure.
// ctx is passed to Logger for structured fields and tests; retry interval is fixed at 10s.
func (my *db) Connect(ctx context.Context) *db {
	my.once.Do(func() {
		for {
			if _, err := my.OpenDB(); err != nil {
				my.opts.Log.Error(ctx, "connect db error", "url", my.opts.URL, "err", err)
				time.Sleep(10 * time.Second)
				continue
			}
			my.opts.Log.Info(ctx, "db connected", "name", my.opts.Name)
			return
		}
	})
	return my
}

// Close 关闭底层连接池（*sql.DB）。未 Open 或 my.db 为 nil 时视为无需关闭。
// Closes the underlying sql.DB pool. No-op when not opened or my.db is nil.
func (my *db) Close() error {
	if my.db == nil {
		return nil
	}
	db, err := my.db.DB()
	if err != nil {
		return err
	}
	return db.Close()
}

// DB 返回带 context 的 GORM 实例，支持请求超时与取消。
// Returns context-aware GORM instance for timeout and cancellation.
func (my *db) DB(ctx context.Context) *gorm.DB {
	return my.db.WithContext(ctx)
}

// WithDB 注入 GORM 实例，用于单测或 mock 替换。
// Injects GORM instance for testing or mocking.
func (my *db) WithDB(db *gorm.DB) *db {
	my.db = db
	return my
}

// GetDB 主入口：懒连接后返回带 context 的 *gorm.DB。业务层应通过 db.GetDB(ctx, name) 调用。
// Main entry: lazy-connects and returns context-aware *gorm.DB. Call via db.GetDB(ctx, name).
func (my *db) GetDB(ctx context.Context) *gorm.DB {
	return my.Connect(ctx).DB(ctx)
}

// CreateOrUpdate 预留接口，当前未实现。用于 upsert 等场景。
// Placeholder; not implemented. Reserved for upsert scenarios.
func (my *db) CreateOrUpdate(ctx context.Context, models ...any) error {
	return nil
}

// Query 执行原始 SQL，返回 []map[string]any。[]byte 列自动转为 string。
// Executes raw SQL and returns []map[string]any; []byte columns are converted to string.
func (my *db) Query(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
	var results []map[string]any

	// 执行原始 SQL 查询 / Execute raw SQL query
	rows, err := my.DB(ctx).Raw(sql, args...).Rows()
	if err != nil {
		return nil, fmt.Errorf("查询失败 (query failed): %w", err)
	}
	defer rows.Close()

	// 获取结果集的列名 / Get column names from result set
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("获取列名失败 (get columns failed): %w", err)
	}

	// 逐行扫描结果 / Scan results row by row
	for rows.Next() {
		// 为每列创建接收变量和指针 / Create receivers and pointers for each column
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("扫描行失败 (scan row failed): %w", err)
		}
		// 构造 map，将 []byte 类型自动转为 string / Build map, auto-convert []byte to string
		row := make(map[string]any)
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	// 检查遍历过程中是否有错误 / Check for errors during iteration
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历结果集失败 (iterate rows failed): %w", err)
	}
	return results, nil
}

// Name 返回数据库实例名称，用于 dbm 注册与查找。
// Returns instance name for registration and lookup.
func (my *db) Name() string {
	return my.opts.Name
}

// ---------- 全局多实例管理 / Global multi-instance management ----------

// DB 数据库接口，用于测试与依赖注入。
// Database interface for testing and dependency injection.
type DB interface {
	Name() string
	GetDB(ctx context.Context) *gorm.DB
}

// dbm 全局实例注册表 name->DB。在初始化阶段完成 Load，非并发安全。
// Global registry name->DB. Load during init; not concurrency-safe.
var dbm = map[string]DB{}

// Load 将 DB 实例注册到全局 dbm。由 NewMySQL/NewSQLite 内部调用。
// Registers DB instance into dbm. Called internally by NewMySQL/NewSQLite.
func Load(db *db) *db {
	dbm[db.Name()] = db
	return db
}

// Get 按名称获取已注册实例，不存在则 panic。调用方需确保已通过 NewMySQL/NewSQLite 注册。
// Gets registered instance by name; panics if not found.
func Get(name string) DB {
	if db, ok := dbm[name]; ok {
		return db
	}
	panic(fmt.Sprintf("db %s not found", name))
}

// GetDB 按名称获取 GORM 实例，懒连接 + 带 context。业务层主要入口。
// Gets GORM instance by name with lazy connect and context. Main entry for business layer.
func GetDB(ctx context.Context, name string) *gorm.DB {
	return Get(name).GetDB(ctx)
}
