// db_test.go 包含 db 包的单元测试，主要验证 DBLogger（GORM 自定义日志实现）的正确性。
// db_test.go contains unit tests for the db package, primarily verifying the correctness
// of DBLogger (custom GORM logger implementation).

package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// testLogger 是 Logger 接口的测试实现，将业务日志（连接、重试等）输出到 testing.T 的日志。
// 通过 t.Helper() 标记，使测试失败时报告调用者而非 testLogger 内部行号。
//
// testLogger is a test implementation of the Logger interface that redirects
// business logs (connect, retry, etc.) to testing.T's log output.
// Uses t.Helper() to report the caller's line rather than testLogger internals.
type testLogger struct {
	t *testing.T // 关联的测试实例 / Associated test instance
}

// Info 输出 Info 级别的业务日志到测试日志。
// Logs Info-level business message to test output.
func (l *testLogger) Info(_ context.Context, msg string, args ...any) {
	l.t.Helper()
	l.t.Logf("[DB INFO] "+msg, args...)
}

// Warn 输出 Warn 级别的业务日志到测试日志。
// Logs Warn-level business message to test output.
func (l *testLogger) Warn(_ context.Context, msg string, args ...any) {
	l.t.Helper()
	l.t.Logf("[DB WARN] "+msg, args...)
}

// Error 输出 Error 级别的业务日志到测试日志。
// Logs Error-level business message to test output.
func (l *testLogger) Error(_ context.Context, msg string, args ...any) {
	l.t.Helper()
	l.t.Logf("[DB ERROR] "+msg, args...)
}

// Test_DBLogger 验证 DBLogger 各日志方法（Info、Warn、Error）和 Trace 的输出行为。
// 测试覆盖以下场景：
//   - 基础日志级别：Info、Warn、Error 消息输出
//   - 普通 SQL：执行时间 < LongQueryTime，输出 "SQL:" 前缀
//   - 慢查询：执行时间 > LongQueryTime（100ms），输出 "SLOW:" 前缀
//   - SQL 出错：传入 error，输出 "ERROR:" 前缀
//
// Test_DBLogger verifies DBLogger's log methods (Info, Warn, Error) and Trace behavior.
// Covers the following scenarios:
//   - Basic log levels: Info, Warn, Error message output
//   - Normal SQL: execution time < LongQueryTime, outputs "SQL:" prefix
//   - Slow query: execution time > LongQueryTime (100ms), outputs "SLOW:" prefix
//   - SQL error: with error passed, outputs "ERROR:" prefix
func Test_DBLogger(t *testing.T) {
	// 创建标准 log 输出，带 [SQL] 前缀 / Create std log output with [SQL] prefix
	output := log.New(os.Stdout, "[SQL] ", log.LstdFlags)
	dl := &DBLogger{
		output:        output,
		LogLevel:      logger.Info,            // 设置为 Info 级别以捕获所有日志 / Set to Info to capture all logs
		LongQueryTime: 100 * time.Millisecond, // 慢查询阈值 100ms / Slow query threshold 100ms
	}

	ctx := context.Background()

	// 测试基础日志级别输出 / Test basic log level output
	dl.Info(ctx, "test info %s", "hello")
	dl.Warn(ctx, "test warn %s", "world")
	dl.Error(ctx, "test error %s", "oops")

	// 测试普通 SQL：50ms < 100ms 阈值，应输出 "SQL:" 前缀
	// Test normal SQL: 50ms < 100ms threshold, should output "SQL:" prefix
	dl.Trace(ctx, time.Now().Add(-50*time.Millisecond), func() (string, int64) {
		return "SELECT 1", 1
	}, nil)

	// 测试慢查询：200ms > 100ms 阈值，应输出 "SLOW:" 前缀
	// Test slow query: 200ms > 100ms threshold, should output "SLOW:" prefix
	dl.Trace(ctx, time.Now().Add(-200*time.Millisecond), func() (string, int64) {
		return "SELECT slow_query FROM big_table", 100
	}, nil)

	// 测试 SQL 出错：传入 error，应输出 "ERROR:" 前缀
	// Test SQL error: with error, should output "ERROR:" prefix
	dl.Trace(ctx, time.Now(), func() (string, int64) {
		return "SELECT err", 0
	}, fmt.Errorf("test error"))
}

// TestGetDB_Unreachable 覆盖"数据库连不上"这条路径：
//   - GetDB 必须返回一个可用实例，拿到的 .Error 是建连错误；
//   - 链式调用（Raw/Scan）不得 panic（gorm 各 callback 均以 db.Error == nil 为前提，会短路）。
//
// 修复前 gorm.Open 失败会留下一个 Statement==nil 的实例，WithContext → Statement.clone()
// 空指针 panic；且重试绑定在调用方 ctx 上，ctx 取消后 sync.Once 已消耗、进程重启前不再重试。
func TestGetDB_Unreachable(t *testing.T) {
	const name = "test-unreachable"
	if err := CreateDB(Options{Name: name, URL: "mysql://root:root@tcp(127.0.0.1:1)/db?charset=utf8mb4"}); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	g := GetDB(ctx, name)
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
// 用于在测试中确定性地产出"首连失败、重试成功"，不依赖真实网络的时序。
// flakyDialector fails Initialize for the first `failures` calls, then delegates to the real
// dialector, so "first connect fails, retry succeeds" is deterministic and network-independent.
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

// TestGetDB_RetryUntilConnected 覆盖"首连失败 → 后台重试 → 自愈"这条路径：
//   - 首连失败时 GetDB 立即返回带错误的实例，不阻塞调用方（重试在后台协程里，永不退出）；
//   - 重试成功后实例被替换，后续 GetDB 自动拿到可用实例。
//
// 耗时约 10s，受 retry 的 10s 重试间隔限制。
// Takes ~10s, bounded by the 10s retry interval.
func TestGetDB_RetryUntilConnected(t *testing.T) {
	const name = "test-retry"
	// LogLevel=Silent：轮询期间失败实例会被反复查询，静音避免刷屏
	my := setup(Options{Name: name, URL: "sqlite://:memory:", LogLevel: logger.Silent})
	if err := my.open(sqlite.Open); err != nil {
		t.Fatalf("open: %v", err)
	}
	my.dialector = &flakyDialector{Dialector: my.dialector, failures: 1}
	Load(my)

	ctx := context.Background()
	start := time.Now()
	if err := GetDB(ctx, name).Error; err == nil {
		t.Fatal("expect connect error on first call")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("first GetDB blocked %v, expect immediate return", d)
	}

	for deadline := time.Now().Add(20 * time.Second); ; {
		var n int
		if err := GetDB(ctx, name).Raw("SELECT 1").Scan(&n).Error; err == nil {
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
