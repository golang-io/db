package db

import (
	"context"
	"errors"
	"fmt"
	"path"
	"runtime"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm/logger"
)

// Logger 接收连接与重试日志。
type Logger interface {
	Info(ctx context.Context, msg string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, args ...any)
}

type noLogger struct{}

func (noLogger) Info(context.Context, string, ...any)  {}
func (noLogger) Warn(context.Context, string, ...any)  {}
func (noLogger) Error(context.Context, string, ...any) {}

// Output 是 SQL 日志的 Printf 接口。
type Output interface {
	Printf(format string, v ...any)
}

// DBLogger 将 GORM SQL 日志写入 Output。
type DBLogger struct {
	output        Output          // SQL 日志输出目标
	LogLevel      logger.LogLevel // 当前日志级别 Silent|Error|Warn|Info
	LongQueryTime time.Duration   // 慢查询阈值，超过打 SLOW 前缀
}

// NewDBLogger 创建 SQL 日志记录器。
func NewDBLogger(output Output, level logger.LogLevel, longQueryTime time.Duration) *DBLogger {
	return &DBLogger{
		output:        output,
		LogLevel:      level,
		LongQueryTime: longQueryTime,
	}
}

// pkgDir 用于过滤本包调用栈。
var pkgDir = func() string {
	_, file, _, _ := runtime.Caller(0)
	return path.Dir(file) + "/"
}()

// getCallerInfo 跳过 GORM 和本包，返回业务调用位置。
func getCallerInfo() string {
	pcs := make([]uintptr, 30)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		f, more := frames.Next()
		if f.File != "" && !strings.Contains(f.File, "gorm.io/") && (!strings.HasPrefix(f.File, pkgDir) || strings.HasSuffix(f.File, "_test.go")) {
			return fmt.Sprintf("%s:%d", path.Base(f.File), f.Line)
		}
		if !more {
			return "??:0"
		}
	}
}

// LogMode 返回指定级别的日志器副本。
func (l *DBLogger) LogMode(level logger.LogLevel) logger.Interface {
	return &DBLogger{
		output:        l.output,
		LogLevel:      level,
		LongQueryTime: l.LongQueryTime,
	}
}

// Info 输出信息日志。
func (l *DBLogger) Info(ctx context.Context, msg string, data ...any) {
	if l.LogLevel < logger.Info {
		return
	}
	l.output.Printf("I: %s %s", getCallerInfo(), fmt.Sprintf(msg, data...))
}

// Warn 输出警告日志。
func (l *DBLogger) Warn(ctx context.Context, msg string, data ...any) {
	if l.LogLevel < logger.Warn {
		return
	}
	l.output.Printf("W: %s %s", getCallerInfo(), fmt.Sprintf(msg, data...))
}

// Error 输出错误日志。
func (l *DBLogger) Error(ctx context.Context, msg string, data ...any) {
	if l.LogLevel < logger.Error {
		return
	}
	l.output.Printf("E: %s %s", getCallerInfo(), fmt.Sprintf(msg, data...))
}

// Trace 记录 SQL 耗时、行数和错误；ErrRecordNotFound 不作为错误记录。
// 超过 2048 字节时截断，保留完整的 UTF-8 字符。
func (l *DBLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	elapsed := time.Since(begin)
	isErr := err != nil && !errors.Is(err, logger.ErrRecordNotFound)
	slow := l.LongQueryTime > 0 && elapsed > l.LongQueryTime
	if !(isErr && l.LogLevel >= logger.Error || slow && l.LogLevel >= logger.Warn || l.LogLevel >= logger.Info) {
		return
	}

	sql, rows := fc()
	sql = strings.TrimSpace(sql)
	if len(sql) > 2048 {
		cut := 2048
		for cut > 0 && !utf8.RuneStart(sql[cut]) { // 不截断多字节字符
			cut--
		}
		sql = sql[:cut] + "..."
	}
	caller := getCallerInfo()
	ms := float64(elapsed.Nanoseconds()) / 1e6
	switch {
	case isErr:
		l.output.Printf("ERROR: %s %v | %.3fms | %d rows | %s", caller, err, ms, rows, sql)
	case slow:
		l.output.Printf("SLOW: %s %.3fms (>%v) | %d rows | %s", caller, ms, l.LongQueryTime, rows, sql)
	default:
		l.output.Printf("SQL: %s %.3fms | %d rows | %s", caller, ms, rows, sql)
	}
}
