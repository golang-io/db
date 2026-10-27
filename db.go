// Package db 提供数据库连接与管理的核心能力。
//
// 主要特性:
//   - 多驱动支持：导入 sqlite / mysql / duckdb 子包自动注册；其他方言在 init() 中通过 Register 注册（见 Register 示例）
//   - Options 结构体配置连接参数，支持 YAML 反序列化
//   - 连接池：MaxIdleConns、MaxOpenConns、ConnMaxLifeTime、ConnMaxIdleTime
//   - 懒连接：首次 GetDB 时开始拨号；还没连上就等待，失败每 10 秒重试，返回的连接已经可用
//   - 双通道日志：SQL 走 Output（标准 log），业务（连接/重试）走 Logger（对接 slog）
//   - 全局多实例：按 Name 注册，按名获取
//
// 典型用法:
//
//	// 导入子包即自动注册 "sqlite"：import _ "github.com/golang-io/db/sqlite"
//
//	// 注册后，CreateDB 按 URL scheme 分发；内存库的连接池参数由 sqlite 子包自动设置。
//	db.CreateDB(db.Options{Name: "default", URL: "sqlite://:memory:"})
//
//	// 还没连上时 GetDB 会一直等待，返回的 *gorm.DB 已经可用。
//	gormDB := db.GetDB(ctx, "default")
package db

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Options 数据库连接配置，支持 YAML 反序列化。
// 零值字段使用默认值；负数表示按 database/sql 原生语义（不限制 / 不保留 / 不回收 / 不超时）。
type Options struct {
	Name            string          `yaml:"Name"`            // 实例名，用于 GetDB(ctx, name)；CreateDB 必填，OpenDB / Open 默认 "default"
	URL             string          `yaml:"URL"`             // 连接串，格式 scheme://dsn，如 mysql://... 或 sqlite://...
	MaxIdleConns    int             `yaml:"MaxIdleConns"`    // 连接池最大空闲连接数，默认 10
	MaxOpenConns    int             `yaml:"MaxOpenConns"`    // 连接池最大打开连接数，默认 50
	ConnMaxLifeTime time.Duration   `yaml:"ConnMaxLifeTime"` // 连接最大存活时间，默认 5m
	ConnMaxIdleTime time.Duration   `yaml:"ConnMaxIdleTime"` // 连接最大空闲时间，默认 1m；用于回收被上游（MySQL wait_timeout / LB 空闲超时）静默关闭的连接
	DialTimeout     time.Duration   `yaml:"DialTimeout"`     // 建连超时，默认 3s；由注册工厂应用（如 mysql 子包）
	ReadTimeout     time.Duration   `yaml:"ReadTimeout"`     // 读超时，默认不设置；由注册工厂应用，设置后可能中断长查询
	WriteTimeout    time.Duration   `yaml:"WriteTimeout"`    // 写超时，默认不设置；由注册工厂应用
	LongQueryTime   time.Duration   `yaml:"LongQueryTime"`   // 慢查询阈值，超过打 SLOW 日志，默认 3s
	Output          io.Writer       `yaml:"-"`               // SQL 日志输出，默认 os.Stdout
	Log             Logger          `yaml:"-"`               // 业务日志（连接、重试），对接 slog，默认不输出
	LogLevel        logger.LogLevel `yaml:"-"`               // GORM 日志级别，默认 Warn
}

// withDefaults 返回补齐默认值后的配置，是默认值的唯一来源。
func (o Options) withDefaults() Options {
	if o.Name == "" {
		o.Name = "default"
	}
	if o.MaxIdleConns == 0 {
		o.MaxIdleConns = 10
	}
	if o.MaxOpenConns == 0 {
		o.MaxOpenConns = 50
	}
	if o.ConnMaxLifeTime == 0 {
		o.ConnMaxLifeTime = 5 * time.Minute
	}
	if o.ConnMaxIdleTime == 0 {
		o.ConnMaxIdleTime = time.Minute
	}
	if o.DialTimeout == 0 {
		o.DialTimeout = 3 * time.Second
	}
	if o.LongQueryTime == 0 {
		o.LongQueryTime = 3 * time.Second
	}
	if o.Output == nil {
		o.Output = os.Stdout
	}
	if o.Log == nil {
		o.Log = noLogger{}
	}
	if o.LogLevel == 0 {
		o.LogLevel = logger.Warn
	}
	return o
}

// open 唯一的建连实现：组装 GORM 配置、gorm.Open（默认自动 Ping）、设置连接池。
// 保留 GORM 自动 Ping，不额外执行 PingContext；初始化不受请求 ctx 约束，也没有本包设置的独立探活超时。
// 网络超时由驱动配置控制；MySQL 的 timeout 仅限制 TCP 建连，握手及查询读写还需 readTimeout / writeTimeout。
// 失败时关掉已经打开的池，只返回 error，不把半初始化的 *gorm.DB 交给调用方。
// 默认关闭 PrepareStmt；当前 GORM 的缓存清理 goroutine 不随缓存关闭退出。
// 调用方自行启用预编译时，应复用长生命周期实例，避免反复创建、销毁独立缓存。
func open(dialector gorm.Dialector, opts Options) (*gorm.DB, error) {
	// ClickHouse 批量写入通过默认事务的 Commit 发送数据。
	config := &gorm.Config{
		SkipDefaultTransaction:                   dialector.Name() != "clickhouse",
		PrepareStmt:                              false,
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger: &DBLogger{
			output:        log.New(opts.Output, "", log.LstdFlags|log.Lmicroseconds),
			LogLevel:      opts.LogLevel,
			LongQueryTime: opts.LongQueryTime,
		},
	}
	gdb, err := gorm.Open(dialector, config)
	if err != nil {
		_ = closeDB(gdb)
		return nil, fmt.Errorf("open db error: name=%s, %w", opts.Name, err)
	}
	sqlDB, err := gdb.DB()
	if err != nil {
		_ = closeDB(gdb)
		return nil, fmt.Errorf("get sql.DB error: name=%s, %w", opts.Name, err)
	}
	sqlDB.SetMaxIdleConns(opts.MaxIdleConns)
	sqlDB.SetMaxOpenConns(opts.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(opts.ConnMaxLifeTime)
	sqlDB.SetConnMaxIdleTime(opts.ConnMaxIdleTime)
	return gdb, nil
}

// instance 由 CreateDB 登记的托管实例。首次 GetDB 启动拨号循环；连上后所有等待者拿到同一个 *gorm.DB。
//
// GetDB 用 cond 等到 db 非 nil。Close 只把 closed 设上，不叫醒正在等待的 GetDB。
type instance struct {
	opts      Options        // 已补齐默认值的配置
	dialector gorm.Dialector // 建连用的方言，失败重试时复用
	mu        sync.Mutex     // 保护 db、closed，并作为 cond 的锁
	cond      *sync.Cond     // db 从 nil 变为可用时 Broadcast
	db        *gorm.DB       // 非 nil 表示已经可用
	closed    bool           // 已 Close；之后不再拨号，也不再发布连接
	retry     time.Duration  // 后台重试间隔
	once      sync.Once      // 首次 GetDB 启动拨号循环
}

// newInstance 创建托管实例，不建连、不注册。opts 须已补齐默认值。
func newInstance(opts Options, dialector gorm.Dialector) *instance {
	in := &instance{
		opts:      opts,
		dialector: dialector,
		retry:     10 * time.Second,
	}
	in.cond = sync.NewCond(&in.mu)
	return in
}

// dial 反复建连，直到成功或实例被 Close。请求 ctx 不参与这里，一次取消不会让实例停在失败状态。
// Close 不会打断当前这次睡眠或正在进行的拨号；睡眠结束后若已 Close 就退出，进行中的拨号由 publish 丢掉。
// 日志不打 URL，避免泄露密码。
func (in *instance) dial() {
	// 第一次间隔为 0，进入循环立刻拨号。之后失败才按 retry 等待。
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		<-timer.C
		if in.isClosed() {
			return
		}
		gdb, err := open(in.dialector, in.opts)
		if err != nil {
			in.opts.Log.Error(context.Background(), "connect db error", "name", in.opts.Name, "err", err)
			if in.isClosed() {
				return
			}
			timer.Reset(in.retry)
			continue
		}
		if in.publish(gdb) {
			in.opts.Log.Info(context.Background(), "db connected", "name", in.opts.Name)
		}
		return
	}
}

// isClosed 报告实例是否已 Close。调用方不得持有 mu。
func (in *instance) isClosed() bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.closed
}

// GetDB 返回已经连上的 *gorm.DB。还没连上就一直等待。
// ctx 的取消和超时不会结束这次等待，只挂到返回的会话上，用来取消之后的查询。
// 实例 Close 之后不会再有可用连接，调用会一直停在这里。已连上之后每次调用返回同一个连接池。
func (in *instance) GetDB(ctx context.Context) *gorm.DB {
	in.mu.Lock()
	defer in.mu.Unlock()
	for in.db == nil {
		// 已 Close 时不再拨号，cond 也不会再被叫醒。
		if !in.closed {
			in.once.Do(func() { go in.dial() })
		}
		in.cond.Wait()
	}
	return in.db.WithContext(ctx)
}

// publish 保存建连结果并唤醒等待者。已 Close 则关闭这个新连接池、不保存，返回 false。
func (in *instance) publish(gdb *gorm.DB) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.closed {
		_ = closeDB(gdb)
		return false
	}
	in.db = gdb
	in.cond.Broadcast()
	return true
}

// Close 标记实例已关闭，并关闭底层连接池。正在进行的拨号由 publish 丢掉。
// 重试睡眠不会被打断，最长再等一个重试间隔后循环退出。
// 不叫醒正在等待的 GetDB：关闭之后已经没有可返回的连接。
func (in *instance) Close() error {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.closed {
		return nil
	}
	in.closed = true
	db := in.db
	in.db = nil
	return closeDB(db)
}

// closeDB 关闭 gdb 的连接池；gdb 为 nil 或没有连接池（建连失败的实例）时无需关闭。
func closeDB(gdb *gorm.DB) error {
	if gdb == nil {
		return nil
	}
	sqlDB, err := gdb.DB()
	if err != nil {
		return nil
	}
	return sqlDB.Close()
}

// Name 返回实例名称，用于 dbm 注册与查找。
func (in *instance) Name() string {
	return in.opts.Name
}

// ---------- 全局多实例管理 ----------

// DB 数据库接口，用于测试与依赖注入。
type DB interface {
	Name() string
	GetDB(ctx context.Context) *gorm.DB
	Close() error
}

// dbm 全局实例注册表 name->DB，不加锁：只允许在启动阶段（CreateDB / Load）注册，之后只读。
// 运行期（有并发 GetDB 时）注册会产生数据竞争。
var dbm = map[string]DB{}

// Load 将实例注册到全局 dbm（同名覆盖），可注册自定义实现（如测试替身）。仅限启动阶段调用。
func Load(in DB) DB {
	dbm[in.Name()] = in
	return in
}

// Get 按名称获取已注册实例，不存在则 panic。调用方需确保已通过 CreateDB 注册。
func Get(name string) DB {
	if in, ok := dbm[name]; ok {
		return in
	}
	panic(fmt.Sprintf("db %s not found", name))
}

// GetDB 按名称获取已经连上的 *gorm.DB。还没连上就一直等待，ctx 取消和实例 Close 都不会让它返回。
func GetDB(ctx context.Context, name string) *gorm.DB {
	return Get(name).GetDB(ctx)
}
