// db_test.go 包含 db 包的单元测试，主要验证 DBLogger、驱动注册与建连重试等核心能力。

package db_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/golang-io/db"
	_ "github.com/golang-io/db/mysql"
	_ "github.com/golang-io/db/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

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

// TestGetDB_Unreachable 覆盖"数据库连不上"这条路径
func TestGetDB_Unreachable(t *testing.T) {
	const name = "test-unreachable"
	if err := db.CreateDB(db.Options{Name: name, URL: "mysql://root:root@tcp(127.0.0.1:1)/db?charset=utf8mb4"}); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	g := db.GetDB(ctx, name)
	if g == nil {
		t.Fatal("GetDB returns nil")
	}
	var n int
	if err := g.Raw("SELECT 1").Scan(&n).Error; err == nil {
		t.Fatal("expect connect error, got nil")
	} else {
		t.Logf("got expected error: %v", err)
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

// TestGetDB_RetryUntilConnected 覆盖"首连失败 → 后台重试 → 自愈"这条路径
func TestGetDB_RetryUntilConnected(t *testing.T) {
	const name = "test-retry"
	my := db.SetupForTest(db.Options{Name: name, URL: "sqlite://:memory:", LogLevel: logger.Silent})
	if err := my.OpenForTest(sqlite.Open); err != nil {
		t.Fatalf("open: %v", err)
	}
	my.SetDialectorForTest(&flakyDialector{Dialector: my.DialectorForTest(), failures: 1})
	db.Load(my)

	ctx := context.Background()
	start := time.Now()
	if err := db.GetDB(ctx, name).Error; err == nil {
		t.Fatal("expect connect error on first call")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("first GetDB blocked %v, expect immediate return", d)
	}

	for deadline := time.Now().Add(20 * time.Second); ; {
		var n int
		if err := db.GetDB(ctx, name).Raw("SELECT 1").Scan(&n).Error; err == nil {
			if n != 1 {
				t.Fatalf("SELECT 1 = %d, want 1", n)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("db not recovered after background retry")
		}
		time.Sleep(200 * time.Millisecond)
	}
}
