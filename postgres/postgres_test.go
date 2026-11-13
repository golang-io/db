package postgres_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/golang-io/db"
	"github.com/golang-io/db/postgres"
	"github.com/libtnb/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func dialectorDSN(t *testing.T, d gorm.Dialector) string {
	t.Helper()
	v := reflect.ValueOf(d)
	for v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	f := v.FieldByName("DSN")
	if !f.IsValid() {
		t.Fatalf("dialector %T does not expose DSN field", d)
	}
	return f.String()
}

func dialectorOptionCount(t *testing.T, d gorm.Dialector) int {
	t.Helper()
	v := reflect.ValueOf(d)
	for v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	f := v.FieldByName("OptionOpenDB")
	if !f.IsValid() {
		t.Fatalf("dialector %T does not expose OptionOpenDB field", d)
	}
	return f.Len()
}

func TestPostgresDriverRegistration(t *testing.T) {
	const want = "postgres://user:pass@localhost:5432/mydb?sslmode=disable"
	for _, scheme := range []string{"postgres", "postgresql"} {
		factory, err := db.GetDriver(scheme)
		if err != nil {
			t.Fatalf("GetDriver(%s) failed: %v", scheme, err)
		}
		d, err := factory("user:pass@localhost:5432/mydb?sslmode=disable", &db.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if d.Name() != "postgres" {
			t.Fatalf("expected dialector name postgres, got %s", d.Name())
		}
		if dsn := dialectorDSN(t, d); dsn != want {
			t.Fatalf("%s dsn = %q, want %q", scheme, dsn, want)
		}
	}
}

func TestPostgresDialectorDSN(t *testing.T) {
	for _, tt := range []struct{ name, dsn, want string }{
		{"url", "user:pass@localhost:5432/mydb", "postgres://user:pass@localhost:5432/mydb"},
		{"query", "user:pass@localhost:5432/mydb?sslmode=require&connect_timeout=5", "postgres://user:pass@localhost:5432/mydb?sslmode=require&connect_timeout=5"},
		{"empty", "", "postgres://"},
		{"postgres prefix", "postgres://host/db", "postgres://host/db"},
		{"postgresql prefix", "postgresql://host/db", "postgresql://host/db"},
		{"uppercase postgres prefix", "POSTGRES://host/db", "postgres://host/db"},
		{"uppercase postgresql prefix", "POSTGRESQL://host/db", "postgresql://host/db"},
		{"mixed-case prefix", "PoStGrEsQl://host/db", "postgresql://host/db"},
		{"url in query", "user:pass@localhost/db?note=https%3A%2F%2Fexample.com", "postgres://user:pass@localhost/db?note=https%3A%2F%2Fexample.com"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if dsn := dialectorDSN(t, postgres.Dialector(tt.dsn)); dsn != tt.want {
				t.Fatalf("dsn = %q, want %q", dsn, tt.want)
			}
		})
	}
}

func TestPostgresNewDSN(t *testing.T) {
	for _, prefix := range []string{"", "postgres://", "POSTGRESQL://"} {
		name := t.TempDir()
		opts := db.Options{URL: prefix + "user:pass@localhost:5432/mydb?sslmode=disable"}
		if err := postgres.New(opts, db.Name(name)); err != nil {
			t.Fatalf("postgres.New DSN: %v", err)
		}
		t.Cleanup(func() { _ = db.Get(name).Close() })
	}
}

// TestPostgresInvalidDSN 非法 URL 或非法参数在 CreateDB / OpenDB / New / Open 阶段立即返回错误，不登记实例。
func TestPostgresInvalidDSN(t *testing.T) {
	for _, tt := range []struct {
		name string
		dsn  string
	}{
		{"key value format", "host=localhost user=demo dbname=app"},
		{"invalid port", "user:pass@localhost:notaport/mydb"},
		{"unclosed ipv6 bracket", "user:pass@[::1:5432/mydb"},
		{"invalid percent encoding", "user:pa%zz@localhost:5432/mydb"},
		{"invalid connect_timeout", "user:pass@localhost:5432/mydb?connect_timeout=abc"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			name := t.TempDir()
			if err := postgres.New(db.Options{Name: name, URL: tt.dsn}); err == nil {
				_ = db.Get(name).Close()
				t.Fatalf("postgres.New(%q) succeeded, want parse error", tt.dsn)
			}
			if _, err := postgres.Open(context.Background(), db.Options{URL: tt.dsn, LogLevel: logger.Silent}); err == nil {
				t.Fatalf("postgres.Open(%q) succeeded, want parse error", tt.dsn)
			}
			if err := db.CreateDB(db.Options{Name: name, URL: "postgres://" + tt.dsn}); err == nil {
				_ = db.Get(name).Close()
				t.Fatalf("db.CreateDB(%q) succeeded, want parse error", tt.dsn)
			}
		})
	}
}

// TestPostgresRetryDoesNotMutateDialector 含 TimeZone 参数的方言重复 Initialize（模拟后台重试）不得污染原切片。
func TestPostgresRetryDoesNotMutateDialector(t *testing.T) {
	d := postgres.Dialector("user:pass@127.0.0.1:1/mydb?sslmode=disable&TimeZone=UTC&connect_timeout=1")
	for range 3 {
		g, err := gorm.Open(d, &gorm.Config{
			DisableAutomaticPing: true,
			Logger:               logger.Default.LogMode(logger.Silent),
		})
		if err != nil {
			t.Fatalf("Initialize without ping failed: %v", err)
		}
		if sqlDB, dbErr := g.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
	}
	if got := dialectorOptionCount(t, d); got != 0 {
		t.Fatalf("shared dialector OptionOpenDB len = %d after retries, want 0", got)
	}
}

// TestPostgresSavePointPropagatesAndQuotes 验证包装后的 SavePoint / RollbackTo 正确转义双引号并透传执行错误。
func TestPostgresSavePointPropagatesAndQuotes(t *testing.T) {
	sp, ok := postgres.Dialector("localhost/mydb").(gorm.SavePointerDialectorInterface)
	if !ok {
		t.Fatal("postgres.Dialector does not implement gorm.SavePointerDialectorInterface")
	}

	g, err := db.Open(context.Background(), sqlite.Open(":memory:"), db.Options{
		MaxOpenConns: 1,
		LogLevel:     logger.Silent,
	})
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

	// 回滚不存在的保存点必须返回错误，不能静默吞掉。
	if err := sp.RollbackTo(tx.Session(&gorm.Session{}), "missing"); err == nil {
		t.Fatal("RollbackTo missing savepoint returned nil error")
	}

	// 含双引号与分号的保存点名称必须被安全引用，不发生语法错误或 SQL 注入。
	name := `save"point; DROP TABLE t; --`
	if err := sp.SavePoint(tx, name); err != nil {
		t.Fatalf("SavePoint(%q): %v", name, err)
	}
	if err := tx.Exec("INSERT INTO t VALUES (2)").Error; err != nil {
		t.Fatal(err)
	}
	if err := sp.RollbackTo(tx, name); err != nil {
		t.Fatalf("RollbackTo(%q): %v", name, err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}

	var count int64
	if err := g.Table("t").Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("count = %d, err = %v, want 1", count, err)
	}

	// 已提交事务上执行 SavePoint / RollbackTo 必须返回错误。
	if err := sp.SavePoint(tx.Session(&gorm.Session{}), "after_commit"); err == nil {
		t.Fatal("SavePoint on committed tx returned nil error")
	}
	if err := sp.RollbackTo(tx.Session(&gorm.Session{}), name); err == nil {
		t.Fatal("RollbackTo on committed tx returned nil error")
	}

	// DryRun 核对生成的 SQL 文本。
	dry := g.Session(&gorm.Session{DryRun: true})
	stmt := dry.Exec(`SAVEPOINT "` + strings.ReplaceAll(name, `"`, `""`) + `"`).Statement
	if !strings.Contains(stmt.SQL.String(), `SAVEPOINT "save""point; DROP TABLE t; --"`) {
		t.Fatalf("unexpected quoted SQL: %s", stmt.SQL.String())
	}
}
