package db_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang-io/db"
	_ "github.com/golang-io/db/sqlite"
)

func TestSQLiteRegistration(t *testing.T) {
	ctx := context.Background()

	// 1. 测试 OpenDB 单次打开
	gdb, err := db.OpenDB(ctx, db.Options{
		URL: "sqlite://:memory:",
	})
	if err != nil {
		t.Fatalf("db.OpenDB failed: %v", err)
	}
	if gdb.DisableAutomaticPing {
		t.Fatal("automatic ping should be enabled")
	}
	var n int
	if err := gdb.Raw("SELECT 1").Scan(&n).Error; err != nil || n != 1 {
		t.Fatalf("query failed: err=%v, n=%d", err, n)
	}

	// 2. 测试通过 scheme 动态分发 CreateDB + GetDB
	name := uniq("registered-sqlite")
	if err := db.CreateDB(db.Options{
		Name: name,
		URL:  "sqlite://:memory:",
	}); err != nil {
		t.Fatalf("db.CreateDB failed: %v", err)
	}

	g := db.GetDB(ctx, name)
	if err := g.Raw("SELECT 42").Scan(&n).Error; err != nil || n != 42 {
		t.Fatalf("query failed: err=%v, n=%d", err, n)
	}
}

// 以下测试直接用 db.CreateDB，不手动配置连接池，验证内存库参数由 sqlite 工厂自动设置。

// memoryOpts :memory:：自动单连接、连接永不回收
func memoryOpts(name string) db.Options {
	return db.Options{Name: name, URL: "sqlite://:memory:"}
}

// memdbOpts memdb：多连接共享，自动连接永不回收
func memdbOpts(name string) db.Options {
	return db.Options{Name: name, URL: "sqlite://file:/" + name + "?vfs=memdb"}
}

func TestBusyTimeout(t *testing.T) {
	for _, tt := range []struct {
		name   string
		params string
		want   int
	}{
		{"default", "", 5000},
		{"disabled", "?_pragma=busy_timeout(0)", 0},
		{"custom", "?_pragma=busy_timeout(1000)", 1000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g, err := db.OpenDB(context.Background(), db.Options{URL: "sqlite://:memory:" + tt.params, MaxOpenConns: 1})
			if err != nil {
				t.Fatal(err)
			}
			pool, err := g.DB()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = pool.Close() })
			var timeout int
			if err := g.Raw("PRAGMA busy_timeout").Scan(&timeout).Error; err != nil || timeout != tt.want {
				t.Fatalf("busy_timeout = %d, err=%v, want %d", timeout, err, tt.want)
			}
		})
	}
}

// TestFileWAL 文件库通过 DSN 的 _pragma 开启 WAL 与 busy_timeout
func TestFileWAL(t *testing.T) {
	name := uniq("file-wal")
	url := "sqlite://" + filepath.Join(t.TempDir(), "app.db") + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	if err := db.CreateDB(db.Options{Name: name, URL: url}); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	var mode string
	var timeout int
	g := db.GetDB(context.Background(), name)
	if err := g.Raw("PRAGMA journal_mode").Scan(&mode).Error; err != nil || mode != "wal" {
		t.Fatalf("journal_mode = %q, err=%v, want wal", mode, err)
	}
	if err := g.Raw("PRAGMA busy_timeout").Scan(&timeout).Error; err != nil || timeout != 5000 {
		t.Fatalf("busy_timeout = %d, err=%v, want 5000", timeout, err)
	}
}

// TestMemoryConcurrent 内存库并发查询必须落在同一个库上，不能因连接池开新连接而丢表
func TestMemoryConcurrent(t *testing.T) {
	for _, opts := range []db.Options{memoryOpts(uniq("mem-concurrent")), memdbOpts(uniq("memdb-concurrent"))} {
		t.Run(opts.Name, func(t *testing.T) {
			name := opts.Name
			if err := db.CreateDB(opts); err != nil {
				t.Fatalf("CreateDB: %v", err)
			}
			ctx := context.Background()
			g := db.GetDB(ctx, name)
			if err := g.Exec("CREATE TABLE t(id int)").Error; err != nil {
				t.Fatalf("create table: %v", err)
			}

			var wg sync.WaitGroup
			for range 20 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					g := db.GetDB(ctx, name)
					var n int
					if err := g.Raw("SELECT count(*) FROM t").Scan(&n).Error; err != nil {
						t.Errorf("query: %v", err)
					}
				}()
			}
			wg.Wait()
		})
	}
}

// TestMemdbKeepAlive 内存库在最后一个连接关闭时销毁：配置连接永不回收后，空闲一段时间表仍在
func TestMemdbKeepAlive(t *testing.T) {
	opts := memdbOpts(uniq("memdb-keepalive"))
	name := opts.Name
	if err := db.CreateDB(opts); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	ctx := context.Background()
	g := db.GetDB(ctx, name)
	if err := g.Exec("CREATE TABLE t(id int)").Error; err != nil {
		t.Fatalf("create table: %v", err)
	}
	time.Sleep(1100 * time.Millisecond) // database/sql 的回收协程最短 1s 运行一次
	g = db.GetDB(ctx, name)
	var n int
	if err := g.Raw("SELECT count(*) FROM t").Scan(&n).Error; err != nil {
		t.Fatalf("table lost after idle: %v", err)
	}
}

// TestMemdbNested memdb 支持多连接：流式查询回调里再查同一个库不会因等连接而卡死（:memory: 单连接会卡死）
func TestMemdbNested(t *testing.T) {
	opts := memdbOpts(uniq("memdb-nested"))
	name := opts.Name
	if err := db.CreateDB(opts); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	g := db.GetDB(ctx, name)
	if err := g.Exec("CREATE TABLE t(id int); INSERT INTO t VALUES (1), (2)").Error; err != nil {
		t.Fatalf("init: %v", err)
	}

	cnt, err := db.Stream(ctx, name, "SELECT id FROM t", nil, func(_ int64, _ []any) error {
		g := db.GetDB(ctx, name)
		var n int
		return g.Raw("SELECT count(*) FROM t").Scan(&n).Error
	})
	if err != nil || cnt != 2 {
		t.Fatalf("Stream: cnt=%d, err=%v", cnt, err)
	}
}
