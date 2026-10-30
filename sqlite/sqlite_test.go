package sqlite_test

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/golang-io/db"
	"github.com/golang-io/db/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestSQLiteSubpackage(t *testing.T) {
	ctx := context.Background()
	if name := sqlite.Dialector(":memory:").Name(); name != "sqlite" {
		t.Fatalf("expected dialector name sqlite, got %s", name)
	}
	gdb, err := sqlite.Open(ctx, db.Options{URL: ":memory:"})
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	pool, err := gdb.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	var n int
	if err := gdb.Raw("SELECT 1").Scan(&n).Error; err != nil || n != 1 {
		t.Fatalf("query failed: err=%v, n=%d", err, n)
	}

	name := t.TempDir()
	if err := sqlite.New(db.Options{Name: name, URL: ":memory:"}); err != nil {
		t.Fatalf("sqlite.New failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Get(name).Close() })
	g := db.GetDB(ctx, name)
	if err := g.Raw("SELECT 42").Scan(&n).Error; err != nil || n != 42 {
		t.Fatalf("query failed: err=%v, n=%d", err, n)
	}
}

func TestSQLiteSavePoints(t *testing.T) {
	ctx := context.Background()
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("direct=%v", direct), func(t *testing.T) {
			opts := db.Options{URL: ":memory:", LogLevel: logger.Silent}
			var g *gorm.DB
			var err error
			if direct {
				g, err = db.Open(ctx, sqlite.Dialector(":memory:"), opts)
			} else {
				g, err = sqlite.Open(ctx, opts)
			}
			if err != nil {
				t.Fatal(err)
			}
			pool, err := g.DB()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = pool.Close() })
			if err := g.Exec("CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1)").Error; err != nil {
				t.Fatal(err)
			}
			tx := g.Begin()
			if tx.Error != nil {
				t.Fatal(tx.Error)
			}
			defer tx.Rollback()
			if err := tx.Session(&gorm.Session{}).RollbackTo("missing").Error; err == nil {
				t.Fatal("missing savepoint must return an error")
			}
			name := `save"point; test`
			if err := tx.SavePoint(name).Error; err != nil {
				t.Fatalf("SavePoint: %v", err)
			}
			if err := tx.Exec("INSERT INTO t VALUES (2)").Error; err != nil {
				t.Fatal(err)
			}
			if err := tx.RollbackTo(name).Error; err != nil {
				t.Fatalf("RollbackTo: %v", err)
			}
			var count int64
			if err := tx.Table("t").Count(&count).Error; err != nil || count != 1 {
				t.Fatalf("rollback: count=%d, err=%v", count, err)
			}
			if err := tx.Commit().Error; err != nil {
				t.Fatal(err)
			}
			if err := tx.Session(&gorm.Session{}).SavePoint("done").Error; err == nil {
				t.Fatal("SavePoint on committed transaction must return an error")
			}
			if err := tx.Session(&gorm.Session{}).RollbackTo(name).Error; err == nil {
				t.Fatal("RollbackTo on committed transaction must return an error")
			}
		})
	}
}

// TestSQLiteMemoryNew 私有内存库默认单连接，并发查询都落在建表的那个库上。
func TestSQLiteMemoryNew(t *testing.T) {
	for _, dsn := range []string{":memory:", "file:private?mode=memory", "sqlite://file:private?mode=memory&cache=private"} {
		t.Run(dsn, func(t *testing.T) {
			ctx := context.Background()
			name := t.TempDir()
			if err := sqlite.New(db.Options{Name: name, URL: dsn}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Get(name).Close() })
			g := db.GetDB(ctx, name)
			if err := g.Exec("CREATE TABLE t(id INTEGER)").Error; err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for range 20 {
				wg.Go(func() {
					g := db.GetDB(ctx, name)
					var n int
					if err := g.Raw("SELECT count(*) FROM t").Scan(&n).Error; err != nil {
						t.Errorf("query: %v", err)
					}
				})
			}
			wg.Wait()
		})
	}
}

// TestSQLiteMemdb memdb 多连接共享同一个库；未设置时连接永不回收
func TestSQLiteMemdb(t *testing.T) {
	ctx := context.Background()
	name := fmt.Sprintf("memdb-subpackage-%d", time.Now().UnixNano()) // 也是 memdb 库名，不能含路径分隔符
	if err := sqlite.New(db.Options{Name: name, URL: "file:/" + name + "?vfs=memdb"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Get(name).Close() })
	g := db.GetDB(ctx, name)
	if err := g.Exec("CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (42)").Error; err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			g := db.GetDB(ctx, name)
			var n int
			if err := g.Raw("SELECT id FROM t").Scan(&n).Error; err != nil || n != 42 {
				t.Errorf("query failed: n=%d, err=%v", n, err)
			}
		})
	}
	wg.Wait()
}

func TestSQLiteMemoryDefaults(t *testing.T) {
	for _, tt := range []struct {
		name, dsn string
		memory    bool
		shared    bool
	}{
		{"plain", ":memory:?_pragma=busy_timeout(1000)", true, false},
		{"private-uri", "file:private?mode=memory", true, false},
		{"private-cache", "file:private?mode=memory&cache=private", true, false},
		{"shared-cache", "file:shared?mode=memory&cache=shared", true, true},
		{"shared-memory-name", "file::memory:?cache=shared", true, true},
		{"encoded-memory-name", "file:%3Amemory%3A", true, false},
		{"encoded-mode", "file:encoded?mo%64e=mem%6fry&cache=shared", true, true},
		{"memdb", "file:/shared?vfs=memdb", true, true},
		{"encoded-vfs", "file:/encoded?%76%66%73=%6D%65%6D%64%62", true, true},
		{"private-memdb", "file:private?vfs=memdb", true, false},
		{"memory-in-filename", "db:memory:.db", false, false},
		{"vfs-in-filename", "vfs=memdb.db", false, false},
		{"memory-in-query", "app.db?note=:memory:&other=vfs=memdb", false, false},
		{"non-uri-mode", "app.db?mode=memory&cache=shared", false, false},
		{"similar-mode", "file:app.db?mode=rwc&note=mode=memory", false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dsn := tt.dsn
			if !tt.memory {
				dsn = filepath.Join(t.TempDir(), dsn)
			}
			for _, prefix := range []string{"", "sqlite://", "SQLITE://"} {
				for _, explicit := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/explicit=%v", prefix, explicit), func(t *testing.T) {
						opts := db.Options{URL: prefix + dsn}
						wantMax, wantLife, wantIdle := 50, time.Hour, 10*time.Minute
						if explicit {
							opts.MaxOpenConns, opts.ConnMaxLifeTime, opts.ConnMaxIdleTime = 3, 2*time.Minute, 3*time.Minute
							wantMax, wantLife, wantIdle = 3, 2*time.Minute, 3*time.Minute
						}
						if tt.memory { // 内存库直接设置为永不回收、私有内存库单连接，手动值被忽略
							wantLife, wantIdle = 0, 0
							if !tt.shared {
								wantMax = 1
							}
						}
						g, err := sqlite.Open(context.Background(), opts)
						if err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(g.Dialector, sqlite.Dialector(dsn)) {
							t.Fatalf("DSN changed: %#v", g.Dialector)
						}
						pool, err := g.DB()
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = pool.Close() })
						if got := pool.Stats().MaxOpenConnections; got != wantMax {
							t.Fatalf("MaxOpenConns=%d, want %d", got, wantMax)
						}
						v := reflect.ValueOf(pool).Elem()
						if life, idle := time.Duration(v.FieldByName("maxLifetime").Int()), time.Duration(v.FieldByName("maxIdleTime").Int()); life != wantLife || idle != wantIdle {
							t.Fatalf("lifetime=%v, idle=%v; want %v, %v", life, idle, wantLife, wantIdle)
						}
						if tt.memory && tt.shared && !explicit {
							ctx, cancel := context.WithTimeout(context.Background(), time.Second)
							defer cancel()
							held, err := pool.Conn(ctx)
							if err != nil {
								t.Fatal(err)
							}
							defer held.Close()
							if _, err := held.ExecContext(ctx, "CREATE TABLE t(id INTEGER)"); err != nil {
								t.Fatal(err)
							}
							var n int
							if err := pool.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&n); err != nil {
								t.Fatalf("second connection cannot access shared memory: %v", err)
							}
						}
					})
				}
			}
		})
	}
}

// TestSQLiteMemoryRetainsIdleConnection 内存库至少保留一个空闲连接，避免连接归还时销毁数据库
func TestSQLiteMemoryRetainsIdleConnection(t *testing.T) {
	for _, kind := range []string{"private", "shared", "file"} {
		for _, entry := range []string{"OpenDB", "CreateDB"} {
			for _, config := range []string{"zero option", "negative struct"} {
				t.Run(kind+"/"+entry+"/"+config, func(t *testing.T) {
					ctx := context.Background()
					// private 保持独立内存库 :memory:；shared 用 memdb 在连接间共享；file 落到临时文件。
					dsn := ":memory:"
					switch kind {
					case "shared":
						dsn = fmt.Sprintf("file:/keepalive-%d?vfs=memdb", time.Now().UnixNano())
					case "file":
						dsn = filepath.Join(t.TempDir(), "app.db")
					}
					opts := db.Options{Name: t.TempDir(), URL: "sqlite://" + dsn, LogLevel: logger.Silent}
					var overrides []db.Option
					if config == "zero option" {
						overrides = append(overrides, db.MaxIdleConns(0))
					} else {
						opts.MaxIdleConns = -1
					}
					var g *gorm.DB
					var err error
					if entry == "OpenDB" {
						g, err = db.OpenDB(ctx, opts, overrides...)
					} else {
						err = db.CreateDB(opts, overrides...)
						if err == nil {
							t.Cleanup(func() { _ = db.Get(opts.Name).Close() })
							g = db.GetDB(ctx, opts.Name)
						}
					}
					if err != nil {
						t.Fatal(err)
					}
					pool, err := g.DB()
					if err != nil {
						t.Fatal(err)
					}
					if entry == "OpenDB" {
						t.Cleanup(func() { _ = pool.Close() })
					}
					for _, query := range []string{"CREATE TABLE t(id INTEGER)", "INSERT INTO t VALUES (42)"} {
						if err := g.Exec(query).Error; err != nil {
							t.Fatalf("%s: %v", query, err)
						}
					}
					var n int
					if err := g.Raw("SELECT id FROM t").Scan(&n).Error; err != nil || n != 42 {
						t.Fatalf("query: value=%d, err=%v", n, err)
					}
					wantIdle := 1
					if kind == "file" {
						wantIdle = 0
					}
					if got := pool.Stats().Idle; got != wantIdle {
						t.Fatalf("idle connections=%d, want %d", got, wantIdle)
					}
				})
			}
		}
	}
}

func TestSQLiteFileURI(t *testing.T) {
	ctx := context.Background()
	uri := (&url.URL{Scheme: "file", Path: filepath.ToSlash(filepath.Join(t.TempDir(), "app file.db"))}).String()
	uri += "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(1000)"
	g, err := sqlite.Open(ctx, db.Options{URL: uri, MaxOpenConns: 1})
	if err != nil {
		t.Fatalf("sqlite.Open URI: %v", err)
	}
	pool, err := g.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	if err := g.Exec("CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (42)").Error; err != nil {
		t.Fatal(err)
	}

	for _, raw := range []string{uri, "sqlite://" + uri} {
		name := "file-uri-" + raw
		if err := sqlite.New(db.Options{Name: name, URL: raw, MaxOpenConns: 1}); err != nil {
			t.Fatalf("sqlite.New URI: %v", err)
		}
		t.Cleanup(func() { _ = db.Get(name).Close() })
		g = db.GetDB(ctx, name)
		var n, timeout int
		var mode string
		if err := g.Raw("SELECT id FROM t").Scan(&n).Error; err != nil || n != 42 {
			t.Fatalf("wrong file: n=%d, err=%v", n, err)
		}
		if err := g.Raw("PRAGMA journal_mode").Scan(&mode).Error; err != nil || mode != "wal" {
			t.Fatalf("journal_mode=%q, err=%v", mode, err)
		}
		if err := g.Raw("PRAGMA busy_timeout").Scan(&timeout).Error; err != nil || timeout != 1000 {
			t.Fatalf("busy_timeout=%d, err=%v", timeout, err)
		}
	}
}

// TestSQLiteConcurrentTx 多连接下"先读后写"的事务：默认 _txlock=immediate，不会因升级写锁报 database is locked
func TestSQLiteConcurrentTx(t *testing.T) {
	for _, dsn := range []string{
		filepath.Join(t.TempDir(), "app.db"),
		filepath.Join(t.TempDir(), "wal.db") + "?_pragma=journal_mode(WAL)",
		"file:/concurrent-tx?vfs=memdb",
	} {
		t.Run(dsn, func(t *testing.T) {
			ctx := context.Background()
			name := t.TempDir()
			if err := sqlite.New(db.Options{Name: name, URL: dsn, LogLevel: logger.Silent}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Get(name).Close() })
			g := db.GetDB(ctx, name)
			if err := g.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, n INT); INSERT INTO t VALUES (1, 0)").Error; err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for range 20 {
				wg.Go(func() {
					for range 5 {
						g := db.GetDB(ctx, name)
						if err := g.Transaction(func(tx *gorm.DB) error {
							var n int
							if err := tx.Raw("SELECT n FROM t WHERE id = 1").Scan(&n).Error; err != nil {
								return err
							}
							return tx.Exec("UPDATE t SET n = ? WHERE id = 1", n+1).Error
						}); err != nil {
							t.Errorf("transaction: %v", err)
						}
					}
				})
			}
			wg.Wait()
			g = db.GetDB(ctx, name)
			var n int
			if err := g.Raw("SELECT n FROM t WHERE id = 1").Scan(&n).Error; err != nil || n != 100 {
				t.Fatalf("n=%d, err=%v, want 100", n, err)
			}
		})
	}
}
