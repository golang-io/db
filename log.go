package db

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"time"

	"gorm.io/gorm/logger"
)

// log.go 实现 GORM 自定义日志 logger.Interface。
// Implements GORM custom logger (gorm/logger.Interface).
//
// 设计原则 / Design:
//   - SQL 日志：通过 Output 接口，使用标准 log 风格，便于 grep、重定向
//     SQL logs: via Output interface, std log style, grep-friendly
//   - 业务日志（连接、重试）：通过 Logger 接口，对接 slog，结构化输出
//     Business logs (connect, retry): via Logger, wired to slog
//   - 调用栈：跳过 GORM 内部帧，输出业务代码 文件:行号，便于定位
//     Call stack: skips GORM frames, outputs caller file:line for debugging

// Logger 业务日志接口（连接、重试等），外部注入，通常对接 slog。
// Business logger for connect/retry etc.; injected, typically wired to slog.
type Logger interface {
	Info(ctx context.Context, msg string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, args ...any)
}

// noLogger Logger 的空实现，作为默认值，避免 nil 判断。
// Empty Logger implementation, used as default to avoid nil checks.
type noLogger struct{}

func (noLogger) Info(context.Context, string, ...any)  {}
func (noLogger) Warn(context.Context, string, ...any)  {}
func (noLogger) Error(context.Context, string, ...any) {}

// Output SQL 日志输出接口，标准 log.Printf 风格。由 *log.Logger 实现。
// SQL log output interface, std log.Printf style. Implemented by *log.Logger.
type Output interface {
	Printf(format string, v ...any)
}

// DBLogger 实现 gorm.io/gorm/logger.Interface，将 GORM 的 SQL 执行日志写入 Output。
// Implements gorm/logger.Interface; writes SQL execution logs to Output.
//
// 日志前缀 / Log prefixes:
//   - SQL: 普通 SQL 语句及执行时间、影响行数
//     Normal SQL with execution time and rows affected
//   - SLOW: 执行时间超过 LongQueryTime 的慢查询
//     Queries exceeding LongQueryTime
//   - ERROR: 执行出错的 SQL 及错误信息
//     Failed SQL with error
//   - I/W/E: GORM 内部 Info/Warn/Error 级别消息
//     GORM internal Info/Warn/Error messages
type DBLogger struct {
	output        Output          // SQL 日志输出目标 / SQL log output target
	LogLevel      logger.LogLevel // 当前日志级别 Silent|Error|Warn|Info / Current log level
	LongQueryTime time.Duration   // 慢查询阈值，超过打 SLOW 前缀 / Slow query threshold for SLOW prefix
}

// gormFiles GORM 内部源文件名列表，getCallerInfo 在调用栈中跳过这些文件。
// GORM internal source files; getCallerInfo skips these when walking call stack.
var gormFiles = []string{
	"callbacks.go", "finisher_api.go", "chainable_api.go",
	"statement.go", "scan.go", "create.go", "update.go",
	"delete.go", "query.go", "association.go", "migrator.go",
	"db_logger.go", "logger.go", "log.go",
}

// gormPaths GORM 库的路径特征串，containsGormPath 用来判断是否为 GORM 内部调用。
// Path patterns for GORM package; used to detect GORM internal call frames.
var gormPaths = []string{"/gorm.io/gorm/", "/vendor/gorm.io/gorm/", "gorm@"}

// containsGormPath 判断 file 路径是否属于 GORM 内部。用于调用栈过滤。
// Returns whether file path is inside GORM package. Used for call stack filtering.
func containsGormPath(file string) bool {
	return slices.ContainsFunc(gormPaths, func(p string) bool { return strings.Contains(file, p) })
}

// getCallerInfo 从 skip 层起向上遍历调用栈，跳过 GORM 内部帧，返回第一个业务代码的 "文件:行号"。
// Walks call stack from skip, skips GORM frames, returns first caller "file:line" from application code.
func getCallerInfo(skip int) string {
	for i := skip; i < skip+20; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			return "??:0"
		}
		base := filepath.Base(file)
		if !slices.Contains(gormFiles, base) && !containsGormPath(file) {
			return fmt.Sprintf("%s:%d", base, line)
		}
	}
	return "??:-"
}

// LogMode 返回指定日志级别的新 DBLogger 实例。实现 logger.Interface 所需。
// Returns new DBLogger at given log level. Required by logger.Interface.
func (l *DBLogger) LogMode(level logger.LogLevel) logger.Interface {
	return &DBLogger{
		output:        l.output,
		LogLevel:      level,
		LongQueryTime: l.LongQueryTime,
	}
}

// Info 输出 GORM Info 级别消息。带调用者 文件:行号。
// Logs GORM Info message with caller file:line.
func (l *DBLogger) Info(ctx context.Context, msg string, data ...any) {
	if l.LogLevel < logger.Info {
		return
	}
	l.output.Printf("I: %s %s", getCallerInfo(2), fmt.Sprintf(msg, data...))
}

// Warn 输出 GORM Warn 级别消息。带调用者 文件:行号。
// Logs GORM Warn message with caller file:line.
func (l *DBLogger) Warn(ctx context.Context, msg string, data ...any) {
	if l.LogLevel < logger.Warn {
		return
	}
	l.output.Printf("W: %s %s", getCallerInfo(2), fmt.Sprintf(msg, data...))
}

// Error 输出 GORM Error 级别消息。带调用者 文件:行号。
// Logs GORM Error message with caller file:line.
func (l *DBLogger) Error(ctx context.Context, msg string, data ...any) {
	if l.LogLevel < logger.Error {
		return
	}
	l.output.Printf("E: %s %s", getCallerInfo(2), fmt.Sprintf(msg, data...))
}

// sqlSpaceRe 正则，将 SQL 中连续空白压缩为单个空格，便于单行输出和阅读。
// Regex to collapse consecutive whitespace in SQL for single-line output.
var sqlSpaceRe = regexp.MustCompile(`\s+`)

// Trace 记录每次 SQL 执行的详情，是 logger.Interface 的核心方法。
// Logs each SQL execution; core method of logger.Interface.
//
// 输出格式 / Output format:
//   - 正常：SQL: file.go:42 0.35ms | 5 rows | SELECT …
//     Normal: SQL: file.go:42 0.35ms | 5 rows | SELECT …
//   - 慢查询：SLOW: file.go:42 1520.00ms (>3s) | 100 rows | SELECT …
//     Slow: SLOW: file.go:42 1520.00ms (>3s) | 100 rows | SELECT …
//   - 出错：ERROR: file.go:42 record not found | 0.12ms | 0 rows | SELECT …
//     Error: ERROR: file.go:42 record not found | 0.12ms | 0 rows | SELECT …
func (l *DBLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	if l.LogLevel <= logger.Silent {
		return
	}
	elapsed := time.Since(begin)
	sql, rows := fc()
	caller := getCallerInfo(2)
	if len(sql) > 2048 {
		sql = sql[:2048] + "..."
	}
	sql = strings.TrimSpace(sqlSpaceRe.ReplaceAllString(sql, " "))
	ms := float64(elapsed.Nanoseconds()) / 1e6
	switch {
	case err != nil && l.LogLevel >= logger.Error:
		l.output.Printf("ERROR: %s %v | %.3fms | %d rows | %s", caller, err, ms, rows, sql)
	case elapsed > l.LongQueryTime && l.LongQueryTime > 0 && l.LogLevel >= logger.Warn:
		l.output.Printf("SLOW: %s %.3fms (>%v) | %d rows | %s", caller, ms, l.LongQueryTime, rows, sql)
	case l.LogLevel >= logger.Info:
		l.output.Printf("SQL: %s %.3fms | %d rows | %s", caller, ms, rows, sql)
	}
}
