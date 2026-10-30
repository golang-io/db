// Package db 封装 GORM，提供多驱动、命名实例和流式查询。
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
	Name            string          `yaml:"Name"`            // 实例名；CreateDB 必填，OpenDB / Open 默认 "default"
	URL             string          `yaml:"URL"`             // 连接串，格式 scheme://dsn，如 mysql://... 或 sqlite://...
	MaskPassword    bool            `yaml:"MaskPassword"`    // URL 格式错误中的密码打码，默认关闭
	MaxIdleConns    int             `yaml:"MaxIdleConns"`    // 连接池最大空闲连接数，默认 10
	MaxOpenConns    int             `yaml:"MaxOpenConns"`    // 连接池最大打开连接数，默认 50
	ConnMaxLifeTime time.Duration   `yaml:"ConnMaxLifeTime"` // 连接最大存活时间，默认 1h
	ConnMaxIdleTime time.Duration   `yaml:"ConnMaxIdleTime"` // 连接最大空闲时间，默认 10m
	DialTimeout     time.Duration   `yaml:"DialTimeout"`     // 建连超时，默认 3s；由驱动工厂应用
	ReadTimeout     time.Duration   `yaml:"ReadTimeout"`     // 读超时，默认不设置；作用于后续查询
	WriteTimeout    time.Duration   `yaml:"WriteTimeout"`    // 写超时，默认不设置；由驱动工厂应用
	LongQueryTime   time.Duration   `yaml:"LongQueryTime"`   // 慢查询阈值，默认 5s
	Output          io.Writer       `yaml:"-"`               // SQL 日志输出，默认 os.Stdout
	Log             Logger          `yaml:"-"`               // 连接与重试日志，默认不输出
	LogLevel        logger.LogLevel `yaml:"-"`               // GORM 日志级别，默认 Warn
}

// Option 在默认值和 Options 配置之后覆盖字段，可显式设置零值。
type Option func(*Options)

// newOptions 补齐默认值，再应用显式覆盖项。
func newOptions(o Options, opts ...Option) Options {
	if o.MaxIdleConns == 0 {
		o.MaxIdleConns = 10
	}
	if o.MaxOpenConns == 0 {
		o.MaxOpenConns = 50
	}
	if o.ConnMaxLifeTime == 0 {
		o.ConnMaxLifeTime = time.Hour
	}
	if o.ConnMaxIdleTime == 0 {
		o.ConnMaxIdleTime = 10 * time.Minute
	}
	if o.DialTimeout == 0 {
		o.DialTimeout = 3 * time.Second
	}
	if o.LongQueryTime == 0 {
		o.LongQueryTime = 5 * time.Second
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
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

func Name(name string) Option                { return func(o *Options) { o.Name = name } }
func MaskPassword(enabled bool) Option       { return func(o *Options) { o.MaskPassword = enabled } }
func MaxIdleConns(n int) Option              { return func(o *Options) { o.MaxIdleConns = n } }
func MaxOpenConns(n int) Option              { return func(o *Options) { o.MaxOpenConns = n } }
func ConnMaxLifeTime(d time.Duration) Option { return func(o *Options) { o.ConnMaxLifeTime = d } }
func ConnMaxIdleTime(d time.Duration) Option { return func(o *Options) { o.ConnMaxIdleTime = d } }
func DialTimeout(d time.Duration) Option     { return func(o *Options) { o.DialTimeout = d } }
func LongQueryTime(d time.Duration) Option   { return func(o *Options) { o.LongQueryTime = d } }
func LogLevel(level logger.LogLevel) Option  { return func(o *Options) { o.LogLevel = level } }

// SQLOutput 设置 SQL 日志输出。
func SQLOutput(w io.Writer) Option {
	return func(o *Options) {
		if w != nil {
			o.Output = w
		}
	}
}

// Log 设置连接与重试日志输出。
func Log(l Logger) Option {
	return func(o *Options) {
		if l != nil {
			o.Log = l
		}
	}
}

// open 建立连接并配置连接池，初始化失败时关闭已创建的池。
func open(dialector gorm.Dialector, opts Options) (*gorm.DB, error) {
	// ClickHouse 批量写入通过默认事务的 Commit 发送数据。
	config := &gorm.Config{
		SkipDefaultTransaction:                   dialector.Name() != "clickhouse",
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
	// 先更新打开连接数上限，避免空闲连接数被已有池的旧上限截断。
	sqlDB.SetMaxOpenConns(opts.MaxOpenConns)
	sqlDB.SetMaxIdleConns(opts.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(opts.ConnMaxLifeTime)
	sqlDB.SetConnMaxIdleTime(opts.ConnMaxIdleTime)
	return gdb, nil
}

// instance 在首次 GetDB 时建连，成功后复用同一个连接池。
type instance struct {
	opts      Options
	dialector gorm.Dialector
	mu        sync.Mutex // 保护 db、closed，并作为 cond 的锁
	cond      *sync.Cond
	db        *gorm.DB
	closed    bool
	retry     time.Duration
	once      sync.Once
}

// newInstance 创建未注册的实例；opts 须已补齐默认值。
func newInstance(opts Options, dialector gorm.Dialector) *instance {
	in := &instance{
		opts:      opts,
		dialector: dialector,
		retry:     10 * time.Second,
	}
	in.cond = sync.NewCond(&in.mu)
	return in
}

// dial 重试建连，直到成功或实例关闭。
func (in *instance) dial() {
	for !in.isClosed() {
		gdb, err := open(in.dialector, in.opts)
		if err != nil {
			in.opts.Log.Error(context.Background(), "connect db error", "name", in.opts.Name, "err", err)
			if in.isClosed() {
				return
			}
			time.Sleep(in.retry)
			continue
		}
		if in.publish(gdb) {
			in.opts.Log.Info(context.Background(), "db connected", "name", in.opts.Name)
		}
		return
	}
}

// isClosed 调用时不能持有 mu。
func (in *instance) isClosed() bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.closed
}

// GetDB 等待连接成功，再返回带 ctx 的会话。
// ctx 取消和 Close 均不会结束等待。
func (in *instance) GetDB(ctx context.Context) *gorm.DB {
	in.mu.Lock()
	defer in.mu.Unlock()
	for in.db == nil {
		if !in.closed {
			in.once.Do(func() { go in.dial() })
		}
		in.cond.Wait()
	}
	return in.db.WithContext(ctx)
}

// publish 发布连接并唤醒等待者；已关闭时丢弃新连接。
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

// Close 关闭连接池，停止后续重试。
// 不打断当前拨号或休眠，也不唤醒 GetDB 等待者。
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

// closeDB 关闭连接池，忽略尚未完成初始化的实例。
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

// Name 返回实例名。
func (in *instance) Name() string {
	return in.opts.Name
}

// DB 是按名称管理的数据库实例。
type DB interface {
	Name() string
	GetDB(ctx context.Context) *gorm.DB
	Close() error
}

// dbm 仅在启动阶段写入，运行期只读。
var dbm = map[string]DB{}

// Load 注册实例，同名覆盖。仅限启动阶段调用。
func Load(in DB) DB {
	dbm[in.Name()] = in
	return in
}

// Get 按名获取实例，未注册时 panic。
func Get(name string) DB {
	if in, ok := dbm[name]; ok {
		return in
	}
	panic(fmt.Sprintf("db %s not found", name))
}

// GetDB 返回已建连的会话，尚未连上时阻塞。
// ctx 仅用于后续查询；取消和 Close 不会结束建连等待。
func GetDB(ctx context.Context, name string) *gorm.DB {
	return Get(name).GetDB(ctx)
}
