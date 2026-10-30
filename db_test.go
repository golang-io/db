// db_test.go 包含 db 包的单元测试，主要验证 DBLogger、驱动注册与建连重试等核心能力。

package db_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/golang-io/db"
	"github.com/libtnb/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// seq 为测试名加后缀，保证 go test -count=N 时全局注册名不冲突
var seq atomic.Int64

func uniq(name string) string { return fmt.Sprintf("%s-%d", name, seq.Add(1)) }

// testLogger 是 Logger 接口的测试实现，将业务日志输出到 testing.T。
type testLogger struct {
	t *testing.T
}

func (l *testLogger) Info(_ context.Context, msg string, args ...any) {
	l.t.Helper()
	l.t.Logf("[DB INFO] "+msg, args...)
}

func (l *testLogger) Warn(_ context.Context, msg string, args ...any) {
	l.t.Helper()
	l.t.Logf("[DB WARN] "+msg, args...)
}

func (l *testLogger) Error(_ context.Context, msg string, args ...any) {
	l.t.Helper()
	l.t.Logf("[DB ERROR] "+msg, args...)
}

// Test_DBLogger 验证 DBLogger 各日志方法（Info、Warn、Error）和 Trace 的输出行为。
func Test_DBLogger(t *testing.T) {
	output := log.New(os.Stdout, "[SQL] ", log.LstdFlags)
	dl := db.NewDBLogger(output, logger.Info, 100*time.Millisecond)

	ctx := context.Background()

	dl.Info(ctx, "test info %s", "hello")
	dl.Warn(ctx, "test warn %s", "world")
	dl.Error(ctx, "test error %s", "oops")

	dl.Trace(ctx, time.Now().Add(-50*time.Millisecond), func() (string, int64) {
		return "SELECT 1", 1
	}, nil)

	dl.Trace(ctx, time.Now().Add(-200*time.Millisecond), func() (string, int64) {
		return "SELECT slow_query FROM big_table", 100
	}, nil)

	dl.Trace(ctx, time.Now(), func() (string, int64) {
		return "SELECT err", 0
	}, fmt.Errorf("test error"))
}

// TestDBLogger_RecordNotFound ErrRecordNotFound 不应打印 ERROR
func TestDBLogger_RecordNotFound(t *testing.T) {
	var buf bytes.Buffer
	dl := db.NewDBLogger(log.New(&buf, "", 0), logger.Warn, time.Second)
	dl.Trace(context.Background(), time.Now(), func() (string, int64) { return "SELECT 1", 0 }, gorm.ErrRecordNotFound)
	if buf.Len() != 0 {
		t.Fatalf("unexpected log: %s", buf.String())
	}
	dl.Trace(context.Background(), time.Now(), func() (string, int64) { return "SELECT 1", 0 }, errors.New("boom"))
	if !strings.Contains(buf.String(), "ERROR: db_test.go:") {
		t.Fatalf("want ERROR with test caller, got: %s", buf.String())
	}
}

func TestDBLogger_PreservesSQLSemantics(t *testing.T) {
	var output bytes.Buffer
	g, err := db.Open(context.Background(), sqlite.Open(":memory:"), db.Options{
		Output: &output, LogLevel: logger.Info, MaxOpenConns: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := g.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	for _, tt := range []struct{ name, query string }{
		{"literal spaces", "SELECT 'a  b'"},
		{"line comment", "SELECT 1 -- comment\n+ 1 AS n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			output.Reset()
			var actual, replay any
			if err := g.Raw(" \n\t" + tt.query + " \n").Row().Scan(&actual); err != nil {
				t.Fatal(err)
			}
			parts := strings.SplitN(output.String(), " | ", 3)
			if len(parts) != 3 {
				t.Fatalf("unexpected SQL log: %q", output.String())
			}
			if err := pool.QueryRow(parts[2]).Scan(&replay); err != nil {
				t.Fatalf("execute logged SQL: %v", err)
			}
			if !reflect.DeepEqual(actual, replay) {
				t.Fatalf("query result=%#v, logged SQL result=%#v", actual, replay)
			}
		})
	}
}

// TestDBLogger_Truncate 超长 SQL 截断时不截断多字节字符。
func TestDBLogger_Truncate(t *testing.T) {
	var buf bytes.Buffer
	dl := db.NewDBLogger(log.New(&buf, "", 0), logger.Info, time.Second)
	q := "SELECT '" + strings.Repeat("中", 1000) + "'"
	dl.Trace(context.Background(), time.Now(), func() (string, int64) { return q, 0 }, nil)
	out := strings.TrimSpace(buf.String())
	if !utf8.ValidString(out) || !strings.HasSuffix(out, "中...") {
		t.Fatalf("bad truncation: %q", out[len(out)-20:])
	}
}

func TestOptions_WithScheme(t *testing.T) {
	for _, tt := range []struct{ scheme, url, want string }{
		{"mysql", "user:pass@tcp(localhost:3306)/test", "mysql://user:pass@tcp(localhost:3306)/test"},
		{"sqlite", ":memory:", "sqlite://:memory:"},
		{"sqlite", "file:///tmp/app.db?mode=rwc", "file:///tmp/app.db?mode=rwc"},
		{"sqlite", "file:/cache?vfs=memdb", "sqlite://file:/cache?vfs=memdb"},
		{"sqlite", "sqlite://file:///tmp/app.db", "sqlite://file:///tmp/app.db"},
		{"sqlite", "./dir/with://name.db", "sqlite://./dir/with://name.db"},
		{"mysql", "user:https://example@tcp(localhost:3306)/db", "mysql://user:https://example@tcp(localhost:3306)/db"},
		{"mysql", "user:pass@tcp(localhost:3306)/db?note=https://example", "mysql://user:pass@tcp(localhost:3306)/db?note=https://example"},
		{"mysql", "mysql://test", "mysql://test"},
		{"mysql", "MYSQL://test", "MYSQL://test"},
		{"mysql", "sqlite://:memory:", "sqlite://:memory:"},
		{"mysql", "custom+db.v1://test", "custom+db.v1://test"},
		{"sqlite", "unknown://test", "unknown://test"},
		{"sqlite", "", "sqlite://"},
	} {
		opts := db.Options{Name: "test", URL: tt.url, MaxOpenConns: 1}
		want := opts
		want.URL = tt.want
		if got := opts.WithScheme(tt.scheme); !reflect.DeepEqual(got, want) {
			t.Fatalf("WithScheme(%q): got %+v, want %+v", tt.scheme, got, want)
		}
		if opts.URL != tt.url {
			t.Fatal("WithScheme modified the original options")
		}
	}
}

func TestDatabaseURL_PreservesDSN(t *testing.T) {
	scheme := uniq("custom+db.v1")
	var gotDSN, gotURL string
	db.Register(scheme, func(dsn string, opts *db.Options) (gorm.Dialector, error) {
		gotDSN, gotURL = dsn, opts.URL
		return sqlite.Open(":memory:"), nil
	})
	for _, dsn := range []string{
		"file:///tmp/app%20file.db?_pragma=journal_mode(WAL)",
		"user:https://example@tcp([::1]:3306)/db%25prod?loc=Asia%2FShanghai",
		"user:pass@tcp(localhost:3306)/db?note=https://example",
	} {
		for _, prefix := range []string{scheme, strings.ToUpper(scheme), " " + scheme + " "} {
			opts := db.Options{Name: uniq("url-dsn"), URL: prefix + "://" + dsn}
			if err := db.CreateDB(opts); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Get(opts.Name).Close() })
			if gotDSN != dsn || gotURL != opts.URL {
				t.Fatalf("DSN=%q, URL=%q; want %q, %q", gotDSN, gotURL, dsn, opts.URL)
			}
		}
	}
}

func TestDatabaseURL_Invalid(t *testing.T) {
	for _, raw := range []string{"", ":memory:", "://dsn", "1db://dsn", "db_name://dsn", "user:https://example@tcp(localhost:3306)/db"} {
		if _, err := db.OpenDB(context.Background(), db.Options{URL: raw}); err == nil || !strings.Contains(err.Error(), "invalid database url") {
			t.Fatalf("URL=%q: want invalid URL error, got %v", raw, err)
		}
	}
	raw := uniq("unregistered") + "://dsn"
	if _, err := db.OpenDB(context.Background(), db.Options{URL: raw}); err == nil || !strings.Contains(err.Error(), "unknown database schema") {
		t.Fatalf("want unknown scheme error, got %v", err)
	}
}

// TestCreateDB_Duplicate 同名重复注册应报错；Name 为空不应注册
func TestCreateDB_Duplicate(t *testing.T) {
	opts := db.Options{Name: uniq("test-dup"), URL: "sqlite://:memory:"}
	if err := db.CreateDB(opts); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	if err := db.CreateDB(opts); err == nil {
		t.Fatal("expect duplicate error")
	}
	if err := db.CreateDB(db.Options{URL: "sqlite://:memory:"}); err == nil {
		t.Fatal("expect name required error")
	}
}

// TestCreateDB_NameOptions 名称由结构体与 Option 共同决定，Option 后应用；最终名为空则报错
func TestCreateDB_NameOptions(t *testing.T) {
	for _, tt := range []struct {
		name, original, override string
		applyOverride, wantError bool
	}{
		{name: "struct", original: "struct"},
		{name: "option", override: "option", applyOverride: true},
		{name: "override", original: "original", override: "override", applyOverride: true},
		{name: "missing", wantError: true},
		{name: "empty override", original: "original", applyOverride: true, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opts := db.Options{URL: "sqlite://:memory:"}
			if tt.original != "" {
				opts.Name = uniq(tt.original)
			}
			wantName := opts.Name
			var overrides []db.Option
			calls := 0
			if tt.applyOverride {
				wantName = tt.override
				if wantName != "" {
					wantName = uniq(wantName)
				}
				overrides = append(overrides, func(o *db.Options) {
					calls++
					o.Name = wantName
				})
			}
			err := db.CreateDB(opts, overrides...)
			if tt.applyOverride && calls != 1 {
				t.Fatalf("option called %d times, want 1", calls)
			}
			if tt.wantError {
				if err == nil || !strings.Contains(err.Error(), "name is required") {
					t.Fatalf("want name required error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			in := db.Get(wantName)
			t.Cleanup(func() { _ = in.Close() })
			if in.Name() != wantName {
				t.Fatalf("registered name=%q, want %q", in.Name(), wantName)
			}
			if err := db.CreateDB(db.Options{Name: uniq("other"), URL: opts.URL}, db.Name(wantName)); err == nil {
				t.Fatal("duplicate final name must be rejected")
			}
		})
	}
}

// TestQuery_Types 驱动原生类型透传，NULL 为 nil，不 panic
func TestQuery_Types(t *testing.T) {
	name := uniq("test-query-types")
	if err := db.CreateDB(db.Options{Name: name, URL: "sqlite://:memory:", LogLevel: logger.Silent}); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	rows, err := db.Query(context.Background(), name, "SELECT 1 AS a, NULL AS b, 'x' AS c, 1.5 AS d")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	want := map[string]any{"a": int64(1), "b": nil, "c": "x", "d": 1.5}
	if len(rows) != 1 || !reflect.DeepEqual(rows[0], want) {
		t.Fatalf("got %#v, want %#v", rows, want)
	}
}

func TestQuery_DuplicateColumns(t *testing.T) {
	name := uniq("test-duplicate-columns")
	if err := db.CreateDB(db.Options{Name: name, URL: "sqlite://:memory:", LogLevel: logger.Silent}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Get(name).Close() })
	ctx := context.Background()
	for _, query := range []string{
		"SELECT 1 AS id, 2 AS id",
		"SELECT 1 AS id, 2 AS id WHERE 0",
	} {
		t.Run(query, func(t *testing.T) {
			called := false
			cnt, err := db.StreamBatch(ctx, name, query, nil, 1, func([]map[string]any) error {
				called = true
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), `duplicate column name "id"`) || cnt != 0 || called {
				t.Fatalf("StreamBatch: cnt=%d, called=%v, err=%v", cnt, called, err)
			}
			rows, err := db.Query(ctx, name, query)
			if err == nil || !strings.Contains(err.Error(), `duplicate column name "id"`) || len(rows) != 0 {
				t.Fatalf("Query: rows=%v, err=%v", rows, err)
			}
		})
	}
	rows, err := db.Query(ctx, name, "SELECT 1 AS first_id, 2 AS second_id")
	want := []map[string]any{{"first_id": int64(1), "second_id": int64(2)}}
	if err != nil || !reflect.DeepEqual(rows, want) {
		t.Fatalf("unique aliases: rows=%v, err=%v, want=%v", rows, err, want)
	}
}

// TestDriverSchemeMatchesDialectorName Open(ctx, dialector, opts) 按 dialector.Name() 检查注册，scheme 必须与之一致
func TestDriverSchemeMatchesDialectorName(t *testing.T) {
	for _, scheme := range []string{"mysql", "sqlite"} {
		d, err := db.GetDriver(scheme)
		if err != nil {
			t.Fatalf("GetDriver(%s): %v", scheme, err)
		}
		dialector, err := d("user:pass@tcp(localhost:3306)/x", &db.Options{})
		if err != nil {
			t.Fatalf("Dialector(%s): %v", scheme, err)
		}
		if name := dialector.Name(); name != scheme {
			t.Fatalf("scheme %q != Dialector.Name() %q", scheme, name)
		}
	}
}

// customDialector 模拟本库没有子包的方言（如 postgres）
type customDialector struct {
	gorm.Dialector
	name string
}

func (d customDialector) Name() string { return d.name }

var errConfigProbe = errors.New("config probe")

type configProbeDialector struct {
	customDialector
	skip bool
}

func (d *configProbeDialector) Initialize(g *gorm.DB) error {
	d.skip = g.SkipDefaultTransaction
	return errConfigProbe
}

func TestDefaultTransaction(t *testing.T) {
	for _, name := range []string{"mysql", "sqlite", "clickhouse"} {
		t.Run(name, func(t *testing.T) {
			d := &configProbeDialector{customDialector: customDialector{sqlite.Open(":memory:"), name}}
			ctx := context.Background()
			opts := db.Options{LogLevel: logger.Silent}
			if _, err := db.Open(ctx, d, opts); !errors.Is(err, errConfigProbe) {
				t.Fatalf("Open: %v", err)
			}
			if want := name != "clickhouse"; d.skip != want {
				t.Fatalf("Open SkipDefaultTransaction=%v, want %v", d.skip, want)
			}
			scheme := uniq("config-probe")
			db.Register(scheme, func(string, *db.Options) (gorm.Dialector, error) { return d, nil })
			opts.URL = scheme + "://test"
			if _, err := db.OpenDB(ctx, opts); !errors.Is(err, errConfigProbe) {
				t.Fatalf("OpenDB: %v", err)
			}
			if want := name != "clickhouse"; d.skip != want {
				t.Fatalf("OpenDB SkipDefaultTransaction=%v, want %v", d.skip, want)
			}
		})
	}
}

func TestPrepareStmtDisabledByDefault(t *testing.T) {
	ctx := context.Background()
	for _, entry := range []string{"Open", "OpenDB", "GetDB"} {
		t.Run(entry, func(t *testing.T) {
			opts := memoryOpts(uniq("prepare-disabled"))
			var g *gorm.DB
			var err error
			switch entry {
			case "Open":
				g, err = db.Open(ctx, sqlite.Open(":memory:"), opts)
			case "OpenDB":
				g, err = db.OpenDB(ctx, opts)
			case "GetDB":
				err = db.CreateDB(opts)
				if err == nil {
					t.Cleanup(func() { _ = db.Get(opts.Name).Close() })
					g = db.GetDB(ctx, opts.Name)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if entry != "GetDB" {
				pool, err := g.DB()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = pool.Close() })
			}
			if g.PrepareStmt {
				t.Fatal("PrepareStmt must be disabled by default")
			}
			for _, pool := range []gorm.ConnPool{g.ConnPool, g.Statement.ConnPool} {
				if _, ok := pool.(*gorm.PreparedStmtDB); ok {
					t.Fatal("unexpected prepared statement cache")
				}
			}
		})
	}
}

// TestOpen_WithoutRegister Open 直接使用传入的方言，无需 Register
func TestOpen_WithoutRegister(t *testing.T) {
	d := customDialector{sqlite.Open(":memory:"), uniq("custom-test")}
	g, err := db.Open(context.Background(), d, db.Options{LogLevel: logger.Silent})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if pool, err := g.DB(); err == nil {
		t.Cleanup(func() { _ = pool.Close() })
	}
	var n int
	if err := g.Raw("SELECT 1").Scan(&n).Error; err != nil || n != 1 {
		t.Fatalf("query: n=%d, err=%v", n, err)
	}
}

// TestOpen_NameDefaults Open / OpenDB 的名称为空时回落到 default，Option 覆盖结构体
func TestOpen_NameDefaults(t *testing.T) {
	for _, tt := range []struct {
		name, original, override, want string
		applyOverride                  bool
	}{
		{name: "default", want: "default"},
		{name: "struct", original: "original", want: "original"},
		{name: "option", original: "original", override: "override", want: "override", applyOverride: true},
		{name: "empty option", original: "original", want: "default", applyOverride: true},
	} {
		for _, entry := range []string{"Open", "OpenDB"} {
			t.Run(entry+"/"+tt.name, func(t *testing.T) {
				opts := db.Options{Name: tt.original, LogLevel: logger.Silent}
				var overrides []db.Option
				calls := 0
				if tt.applyOverride {
					overrides = append(overrides, func(o *db.Options) {
						calls++
						o.Name = tt.override
					})
				}
				probe := &configProbeDialector{customDialector: customDialector{sqlite.Open(":memory:"), "sqlite"}}
				if entry == "OpenDB" {
					scheme := uniq("name-probe")
					db.Register(scheme, func(_ string, o *db.Options) (gorm.Dialector, error) {
						if o.Name != tt.want {
							t.Errorf("factory name=%q, want %q", o.Name, tt.want)
						}
						return probe, nil
					})
					opts.URL = scheme + "://probe"
				}
				var err error
				if entry == "Open" {
					_, err = db.Open(context.Background(), probe, opts, overrides...)
				} else {
					_, err = db.OpenDB(context.Background(), opts, overrides...)
				}
				if !errors.Is(err, errConfigProbe) || !strings.Contains(err.Error(), "name="+tt.want+",") {
					t.Fatalf("want probe error with name %q, got %v", tt.want, err)
				}
				if tt.applyOverride && calls != 1 {
					t.Fatalf("option called %d times, want 1", calls)
				}
			})
		}
	}
}

// TestStreamBatch_InvalidBatch batch<=0 应返回错误，而不是 panic 或退化为一次性加载
func TestStreamBatch_InvalidBatch(t *testing.T) {
	name := uniq("test-batch-invalid")
	if err := db.CreateDB(db.Options{Name: name, URL: "sqlite://:memory:", LogLevel: logger.Silent}); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	for _, b := range []int{0, -1} {
		if _, err := db.StreamBatch(context.Background(), name, "SELECT 1", nil, b, func([]map[string]any) error { return nil }); err == nil {
			t.Fatalf("batch=%d: expect error", b)
		}
	}
}

// TestGetDB_Unreachable 数据库连不上时 GetDB 一直等待，ctx 超时也不会返回。
func TestGetDB_Unreachable(t *testing.T) {
	name := uniq("test-unreachable")
	if err := db.CreateDB(db.Options{Name: name, URL: "mysql://root:root@tcp(127.0.0.1:1)/db?charset=utf8mb4"}); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	defer db.Get(name).Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	returned := make(chan struct{})
	go func() {
		db.GetDB(ctx, name)
		close(returned)
	}()
	select {
	case <-returned:
		t.Fatal("GetDB returned while the database is unreachable")
	case <-ctx.Done():
	}
}

// flakyDialector 前 failures 次 Initialize 直接失败，之后委托真实 dialector。
type flakyDialector struct {
	gorm.Dialector
	failures int
}

func (d *flakyDialector) Initialize(gdb *gorm.DB) error {
	if d.failures > 0 {
		d.failures--
		return errors.New("initialize failed (test)")
	}
	return d.Dialector.Initialize(gdb)
}

// failingPlugin 在 Initialize 里记录建出的连接池。Initialize 由拨号 goroutine 调用，
// 测试主 goroutine 会并发读取，因此用原子指针保存，避免数据竞争。
type failingPlugin struct {
	pool atomic.Pointer[sql.DB]
	err  error
}

func (*failingPlugin) Name() string { return "failing-plugin" }

func (p *failingPlugin) Initialize(g *gorm.DB) error {
	pool, _ := g.DB()
	p.pool.Store(pool)
	return p.err
}

// Pool 安全读取 Initialize 记录的连接池，未初始化前返回 nil。
func (p *failingPlugin) Pool() *sql.DB { return p.pool.Load() }

type pluginDialector struct {
	gorm.Dialector
	plugin *failingPlugin
}

func (d pluginDialector) Apply(c *gorm.Config) error {
	c.Plugins = map[string]gorm.Plugin{d.plugin.Name(): d.plugin}
	return nil
}

func TestOpen_PluginFailureClosesPool(t *testing.T) {
	for _, entry := range []string{"Open", "OpenDB", "GetDB"} {
		t.Run(entry, func(t *testing.T) {
			want := errors.New("plugin initialization failed")
			plugin := &failingPlugin{err: want}
			d := pluginDialector{Dialector: sqlite.Open(":memory:"), plugin: plugin}
			opts := db.Options{Name: uniq("plugin-failure"), LogLevel: logger.Silent}
			var g *gorm.DB
			var err error
			switch entry {
			case "Open":
				g, err = db.Open(context.Background(), d, opts)
			case "OpenDB":
				scheme := uniq("plugin-failure")
				db.Register(scheme, func(string, *db.Options) (gorm.Dialector, error) { return d, nil })
				opts.URL = scheme + "://test"
				g, err = db.OpenDB(context.Background(), opts)
			case "GetDB":
				in := db.NewForTest(opts, d).WithRetry(time.Hour)
				t.Cleanup(func() { _ = in.Close() })
				go in.GetDB(context.Background())
				deadline := time.Now().Add(2 * time.Second)
				for {
					if pool := plugin.Pool(); pool != nil {
						if err := pool.Ping(); err != nil && strings.Contains(err.Error(), "closed") {
							break
						}
					}
					if time.Now().After(deadline) {
						t.Fatal("failed initialization must close pool")
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
			if entry != "GetDB" && !errors.Is(err, want) {
				t.Fatalf("want plugin error, got %v", err)
			}
			if g != nil {
				t.Fatal("failed open must not return a GORM instance")
			}
			pool := plugin.Pool()
			if pool == nil {
				t.Fatal("plugin did not receive initialized pool")
			}
			t.Cleanup(func() { _ = pool.Close() })
			if err := pool.Ping(); err == nil || !strings.Contains(err.Error(), "closed") {
				t.Fatalf("failed initialization must close pool, got %v", err)
			}
		})
	}
}

// TestQuery_NotFound 未注册的实例名返回 error 而不是 panic
func TestQuery_NotFound(t *testing.T) {
	if _, err := db.Query(context.Background(), uniq("not-registered"), "SELECT 1"); err == nil {
		t.Fatal("expect not found error")
	}
}

// blockingDialector Initialize 先通知 started，等 release 后再真正建连，并记下建出的连接池。
// pool 由拨号 goroutine 写入、测试主 goroutine 读取，用原子指针避免数据竞争。
type blockingDialector struct {
	gorm.Dialector
	started, release chan struct{}
	pool             atomic.Pointer[sql.DB]
}

func (d *blockingDialector) Initialize(gdb *gorm.DB) error {
	close(d.started)
	<-d.release
	if err := d.Dialector.Initialize(gdb); err != nil {
		return err
	}
	pool, _ := gdb.DB()
	d.pool.Store(pool)
	return nil
}

// Pool 安全读取建出的连接池，未建出前返回 nil。
func (d *blockingDialector) Pool() *sql.DB { return d.pool.Load() }

func newBlocking() *blockingDialector {
	return &blockingDialector{Dialector: sqlite.Open(":memory:"), started: make(chan struct{}), release: make(chan struct{})}
}

// TestClose_DuringConnect 建连过程中 Close：建连完成后新连接池必须被关闭，不能泄漏
func TestClose_DuringConnect(t *testing.T) {
	name := uniq("test-close-connect")
	d := newBlocking()
	in := db.Load(db.NewForTest(db.Options{Name: name, LogLevel: logger.Silent}, d))
	// GetDB 走实例方法：等待者会一直阻塞，期间不再访问全局 dbm，避免与后续注册的写入竞争。
	returned := make(chan struct{})
	go func() {
		in.GetDB(context.Background())
		close(returned)
	}()
	<-d.started
	if err := in.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(d.release)
	select {
	case <-returned:
		t.Fatal("GetDB returned without a usable connection")
	case <-time.After(200 * time.Millisecond):
	}
	deadline := time.Now().Add(2 * time.Second)
	for pool := d.Pool(); pool == nil || pool.Ping() == nil; {
		if time.Now().After(deadline) {
			t.Fatal("pool created during Close must be closed")
		}
		time.Sleep(5 * time.Millisecond)
		pool = d.Pool()
	}
}

// TestClose_BeforeConnect 未建连就 Close：之后 GetDB 不再拨号，并一直等待。
func TestClose_BeforeConnect(t *testing.T) {
	name := uniq("test-close-before")
	d := newBlocking()
	close(d.release) // 若错误地发起了建连，让它立即完成，由下面的断言报错而不是挂住
	in := db.Load(db.NewForTest(db.Options{Name: name, LogLevel: logger.Silent}, d))
	if err := in.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// GetDB 走实例方法：等待者会一直阻塞，期间不再访问全局 dbm，避免与后续注册的写入竞争。
	returned := make(chan struct{})
	go func() {
		in.GetDB(context.Background())
		close(returned)
	}()
	select {
	case <-returned:
		t.Fatal("GetDB returned after Close")
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-d.started:
		t.Fatal("dialed after Close")
	default:
	}
}

// TestTransaction_CommitAndRollback 已连上的实例可以提交和回滚。
func TestTransaction_CommitAndRollback(t *testing.T) {
	ctx := context.Background()
	run := func(name string, fn func(*gorm.DB) error) error {
		return db.GetDB(ctx, name).Transaction(fn)
	}

	name := uniq("transaction-healthy")
	if err := db.CreateDB(memoryOpts(name)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Get(name).Close() })
	g := db.GetDB(ctx, name)
	if err := g.Exec("CREATE TABLE t(id INTEGER)").Error; err != nil {
		t.Fatal(err)
	}
	if err := run(name, func(tx *gorm.DB) error { return tx.Exec("INSERT INTO t VALUES (1)").Error }); err != nil {
		t.Fatalf("commit: %v", err)
	}
	rollbackErr := errors.New("rollback test")
	if err := run(name, func(tx *gorm.DB) error {
		if err := tx.Exec("INSERT INTO t VALUES (2)").Error; err != nil {
			return err
		}
		return rollbackErr
	}); !errors.Is(err, rollbackErr) {
		t.Fatalf("rollback: %v", err)
	}
	var count int64
	g = db.GetDB(ctx, name)
	if err := g.Table("t").Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("count=%d, err=%v; want only committed row", count, err)
	}
}

// TestGetDB_RetryUntilConnected 第一次拨号失败时 GetDB 继续等，重试成功后返回可用连接。
func TestGetDB_RetryUntilConnected(t *testing.T) {
	name := uniq("test-retry")
	in := db.NewForTest(db.Options{Name: name, LogLevel: logger.Silent}, &flakyDialector{Dialector: sqlite.Open(":memory:"), failures: 1})
	db.Load(in.WithRetry(100 * time.Millisecond))
	t.Cleanup(func() { _ = in.Close() })

	start := time.Now()
	got := make(chan *gorm.DB, 1)
	go func() { got <- db.GetDB(context.Background(), name) }()
	var g *gorm.DB
	select {
	case g = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("GetDB still waiting")
	}
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Fatalf("GetDB returned in %v, want to wait for the retry", d)
	}
	var n int
	if err := g.Raw("SELECT 1").Scan(&n).Error; err != nil || n != 1 {
		t.Fatalf("SELECT 1 = %d, err=%v", n, err)
	}
}
