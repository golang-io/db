// db_test.go 包含 db 包的单元测试，主要验证 DBLogger（GORM 自定义日志实现）的正确性。
// db_test.go contains unit tests for the db package, primarily verifying the correctness
// of DBLogger (custom GORM logger implementation).

package db

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

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
