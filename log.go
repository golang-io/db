package db

import (
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"runtime"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm/logger"
)

// log.go 实现 GORM 自定义日志 logger.Interface。
//
// 设计原则:
//   - SQL 日志：通过 Output 接口，使用标准 log 风格，便于 grep、重定向
//   - 业务日志（连接、重试）：通过 Logger 接口，对接 slog，结构化输出
//   - 调用栈：跳过 GORM 内部帧，输出业务代码 文件:行号，便于定位

// Logger 业务日志接口（连接、重试等），外部注入，通常对接 slog。
type Logger interface {
	Info(ctx context.Context, msg string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, args ...any)
}

// noLogger Logger 的空实现，作为默认值，避免 nil 判断。
type noLogger struct{}

func (noLogger) Info(context.Context, string, ...any)  {}
func (noLogger) Warn(context.Context, string, ...any)  {}
func (noLogger) Error(context.Context, string, ...any) {}

// Output SQL 日志输出接口，标准 log.Printf 风格。由 *log.Logger 实现。
type Output interface {
	Printf(format string, v ...any)
}

// DBLogger 实现 gorm.io/gorm/logger.Interface，将 GORM 的 SQL 执行日志写入 Output。
//
// 日志前缀:
//   - SQL: 普通 SQL 语句及执行时间、影响行数
//   - SLOW: 执行时间超过 LongQueryTime 的慢查询
//   - ERROR: 执行出错的 SQL 及错误信息
//   - I/W/E: GORM 内部 Info/Warn/Error 级别消息
type DBLogger struct {
	output        Output          // SQL 日志输出目标
	LogLevel      logger.LogLevel // 当前日志级别 Silent|Error|Warn|Info
	LongQueryTime time.Duration   // 慢查询阈值，超过打 SLOW 前缀
}

// NewDBLogger 创建一个 DBLogger 实例。
func NewDBLogger(output Output, level logger.LogLevel, longQueryTime time.Duration) *DBLogger {
	return &DBLogger{
		output:        output,
		LogLevel:      level,
		LongQueryTime: longQueryTime,
	}
}

// pkgDir 本包源码目录，getCallerInfo 用它跳过本包的非测试帧。
var pkgDir = func() string {
	_, file, _, _ := runtime.Caller(0)
	return path.Dir(file) + "/"
}()

// getCallerInfo 遍历调用栈，跳过 gorm.io 下所有包（gorm 及其驱动）与本包的帧，返回第一个业务代码的 "文件:行号"。
// 一次 runtime.Callers 取完栈，避免逐帧 runtime.Caller 每次从栈顶重新回溯。
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

// LogMode 返回指定日志级别的新 DBLogger 实例。实现 logger.Interface 所需。
func (l *DBLogger) LogMode(level logger.LogLevel) logger.Interface {
	return &DBLogger{
		output:        l.output,
		LogLevel:      level,
		LongQueryTime: l.LongQueryTime,
	}
}

// Info 输出 GORM Info 级别消息。带调用者 文件:行号。
func (l *DBLogger) Info(ctx context.Context, msg string, data ...any) {
	if l.LogLevel < logger.Info {
		return
	}
	l.output.Printf("I: %s %s", getCallerInfo(), fmt.Sprintf(msg, data...))
}

// Warn 输出 GORM Warn 级别消息。带调用者 文件:行号。
func (l *DBLogger) Warn(ctx context.Context, msg string, data ...any) {
	if l.LogLevel < logger.Warn {
		return
	}
	l.output.Printf("W: %s %s", getCallerInfo(), fmt.Sprintf(msg, data...))
}

// Error 输出 GORM Error 级别消息。带调用者 文件:行号。
func (l *DBLogger) Error(ctx context.Context, msg string, data ...any) {
	if l.LogLevel < logger.Error {
		return
	}
	l.output.Printf("E: %s %s", getCallerInfo(), fmt.Sprintf(msg, data...))
}

// sqlSpaceRe 正则，将 SQL 中连续空白压缩为单个空格，便于单行输出和阅读。
var sqlSpaceRe = regexp.MustCompile(`\s+`)

// Trace 记录每次 SQL 执行的详情，是 logger.Interface 的核心方法。
//
// 输出格式:
//   - 正常：SQL: file.go:42 0.35ms | 5 rows | SELECT …
//   - 慢查询：SLOW: file.go:42 1520.00ms (>3s) | 100 rows | SELECT …
//   - 出错：ERROR: file.go:42 Error 1146: Table 'x' doesn't exist | 0.12ms | 0 rows | SELECT …
//
// ErrRecordNotFound 属于正常业务结果，不按 ERROR 打印。不输出时不做 SQL 拼接、正则与栈回溯。
func (l *DBLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	elapsed := time.Since(begin)
	isErr := err != nil && !errors.Is(err, logger.ErrRecordNotFound)
	slow := l.LongQueryTime > 0 && elapsed > l.LongQueryTime
	if !(isErr && l.LogLevel >= logger.Error || slow && l.LogLevel >= logger.Warn || l.LogLevel >= logger.Info) {
		return
	}

	sql, rows := fc()
	sql = strings.TrimSpace(sqlSpaceRe.ReplaceAllString(sql, " "))
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
