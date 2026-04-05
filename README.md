# db

基于 [GORM](https://gorm.io) 的数据库连接封装：支持 **MySQL**、**SQLite**、**ClickHouse**，提供统一 URL 入口、按名称注册的多实例、懒连接与可插拔日志。

**English:** Small GORM wrapper with multi-driver URLs, named instances, lazy connect, and dual-channel logging.

## 功能概览

| 能力 | 说明 |
|------|------|
| 多驱动 | `mysql` / `sqlite` / `clickhouse`，通过 `CreateDB` 按 URL scheme 自动分发 |
| 全局多实例 | `Name` 注册，`GetDB(ctx, name)` 获取带 `context` 的 `*gorm.DB` |
| 懒连接 | 首次 `GetDB` 时建连；失败时每 **10s** 重试（业务日志走 `Logger`） |
| 连接池 | `MaxIdleConns` / `MaxOpenConns` / `ConnMaxLifeTime`（含合理默认值） |
| 日志 | SQL → 标准 `log` 风格（`Output`）；连接/重试 → `Logger`（可对接 `slog`） |
| 配置 | `Options` 支持 YAML 标签；亦可使用函数式 `Option` |

## 环境要求

- Go **1.25+**（见 `go.mod`）

## 安装

```bash
go get github.com/golang-io/db
```

按需已间接依赖 `gorm.io/driver/mysql`、`gorm.io/driver/sqlite`、`gorm.io/driver/clickhouse` 与 `gorm.io/gorm`。

## 快速开始

```go
package main

import (
	"context"
	"log"

	"github.com/golang-io/db"
)

func main() {
	ctx := context.Background()

	if err := db.CreateDB(db.Options{
		Name: "default",
		URL:  "mysql://user:pass@tcp(127.0.0.1:3306)/mydb?charset=utf8mb4&parseTime=True&loc=Local",
	}); err != nil {
		log.Fatal(err)
	}

	gormDB := db.GetDB(ctx, "default")
	_ = gormDB // 使用 GORM API 查询、迁移等
}
```

### 连接 URL（`CreateDB`）

`CreateDB` 要求 `Options.URL` 为 **`scheme://dsn`**，`scheme` 不区分大小写：

| Scheme | 示例 URL（节选） |
|--------|------------------|
| `mysql` | `mysql://user:pass@tcp(host:3306)/db?charset=utf8mb4&parseTime=True&loc=Local` |
| `sqlite` | `sqlite://:memory:` 或 `sqlite:///path/to/file.db` |
| `clickhouse` | `clickhouse://user:pass@localhost:9000/db` |

不支持的 scheme 会返回 `unknown database schema: ...`。

### 直接指定驱动（`NewMySQL` / `NewSQLite` / `NewClickHouse`）

此时 **`URL` 为纯 DSN，不要带 `scheme://` 前缀**（与 `CreateDB` 不同）：

```go
_ = db.NewMySQL(db.Options{
	Name: "default",
	URL:  "user:pass@tcp(127.0.0.1:3306)/mydb?charset=utf8mb4&parseTime=True&loc=Local",
})
_ = db.NewSQLite(db.Options{Name: "local", URL: ":memory:"})
_ = db.NewClickHouse(db.Options{Name: "ch", URL: "user:pass@localhost:9000/mydb"})
```

### 一次性打开连接（不注册全局 `dbm`）

适合迁移脚本、命令行工具等不需要 `GetDB` 按名取实例的场景：

```go
gormDB, err := db.OpenMySQL(opts)
gormDB, err := db.OpenSQLite(opts)
gormDB, err := db.OpenClickHouse(opts)
```

## 主要 API

| 符号 | 作用 |
|------|------|
| `CreateDB(Options) error` | 按 URL scheme 创建并注册实例 |
| `NewMySQL` / `NewSQLite` / `NewClickHouse` | 指定驱动并注册 |
| `GetDB(ctx, name)` | 懒连接后返回 `*gorm.DB`（带 `ctx`） |
| `Get(name)` | 返回已注册的 `DB` 接口（未注册会 panic） |
| `Options` / `Option` | 结构体配置或函数式选项（如 `db.MaxOpenConns(50)`） |

原始 SQL 请通过 `GetDB` 拿到的 `*gorm.DB` 使用 `Raw` / `Exec` 等 GORM API。

完整说明以 [pkg.go.dev](https://pkg.go.dev/github.com/golang-io/db) 与源码注释为准。

## 配置字段（`Options` 摘要）

| 字段 | 含义 | 默认（未设置时由内部合并） |
|------|------|----------------------------|
| `Name` | 实例名，用于 `GetDB` | 必填（注册路径） |
| `URL` | 连接串 | 必填 |
| `MaxIdleConns` / `MaxOpenConns` | 连接池 | 200 / 200 |
| `ConnMaxLifeTime` | 连接最大存活时间 | 1h |
| `LongQueryTime` | 慢查询阈值（日志 `SLOW:`） | 3s |
| `Output` | SQL 日志 `io.Writer` | `os.Stdout` |
| `Log` | 业务日志（连接、重试） | 空实现 |
| `LogLevel` | GORM 日志级别 | `Warn` |

`Output`、`Log`、`LogLevel` 等无 YAML 标签，适合在代码里注入。

## YAML 配置示例

```yaml
Name: default
URL: mysql://user:pass@tcp(127.0.0.1:3306)/mydb?charset=utf8mb4&parseTime=True&loc=Local
MaxIdleConns: 50
MaxOpenConns: 100
ConnMaxLifeTime: 30m
LongQueryTime: 1s
TimeZone: Local
```

反序列化后可在代码中调用 `opts.LoadOptions()` 与 `db.NewMySQL` 等组合使用（见 `Options.LoadOptions`）。

## 许可证

[MIT License](LICENSE)
