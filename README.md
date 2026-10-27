# db

基于 [GORM](https://gorm.io) 的数据库连接封装：支持 **MySQL**、**SQLite**、**DuckDB**、**ClickHouse**，提供统一 URL 入口、按名称注册的多实例、懒连接与可插拔日志。

**English:** Small GORM wrapper with multi-driver URLs, named instances, lazy connect, and dual-channel logging.

## 功能概览

| 能力 | 说明 |
|------|------|
| 多驱动 | `mysql` / `sqlite` / `duckdb` / `clickhouse`，通过 `CreateDB` 按 URL scheme 自动分发 |
| 全局多实例 | `Name` 注册，`GetDB(ctx, name)` 获取带 `context` 的 `*gorm.DB` |
| 懒连接 | 首次 `GetDB` 时开始拨号；还没连上就等待，失败每 **10s** 重试。返回的 `*gorm.DB` 已经可用 |
| 连接池 | `MaxIdleConns` / `MaxOpenConns` / `ConnMaxLifeTime` / `ConnMaxIdleTime`（含合理默认值） |
| 日志 | SQL → 标准 `log` 风格（`Output`）；连接/重试 → `Logger`（可对接 `slog`） |
| 配置 | `Options` 结构体，支持 YAML 标签；零值字段使用默认值 |

## 环境要求

- Go **1.26+**（见 `go.mod`）

## 安装

```bash
go get github.com/golang-io/db
```

本库提供 `github.com/golang-io/db/sqlite` / `github.com/golang-io/db/mysql` / `github.com/golang-io/db/duckdb` 子包，按需导入即可自动注册对应驱动（仅需注册时使用空白导入）。子包均提供 `Dialector`、`Open` 和 `New`：`Open` 立即建连，`New` 按名登记托管实例，两者均支持不带 scheme 的 DSN。ClickHouse 和其他方言仍需手动调用 `db.Register`，见[手动注册驱动](#手动注册驱动)。不要重复注册已导入子包的 scheme。

| scheme | 第三方 GORM 方言 | 安装命令 |
|--------|-----------------|----------|
| `sqlite` | `github.com/libtnb/sqlite`（纯 Go，无需 CGO，底层 `modernc.org/sqlite`） | `go get github.com/libtnb/sqlite@v1.2.2` |
| `mysql` | `gorm.io/driver/mysql` | `go get gorm.io/driver/mysql@v1.6.0 github.com/go-sql-driver/mysql@v1.10.1` |
| `duckdb` | 本库 `duckdb` 子包自带方言，底层 `github.com/duckdb/duckdb-go/v2`（需要 CGO） | `go get github.com/duckdb/duckdb-go/v2@v2.20000.0` |
| `clickhouse` | `gorm.io/driver/clickhouse` | `go get gorm.io/driver/clickhouse`（本仓库未引入，版本组合需自行验证：`clickhouse-go/v2` 与 `ch-go` 须配套） |

子包和根包测试依赖 mysql / sqlite / duckdb 驱动，因此 `go.mod` 列出这些依赖；业务二进制只包含实际导入的驱动。

DuckDB 子包自带迁移器，`AutoMigrate` 会为自增列自动创建序列；不支持改列类型、删除约束和重命名索引，这几项返回 `duckdb.ErrNotSupport`。DuckDB 不支持 `SAVEPOINT`，嵌套 `Transaction` 会返回 `gorm.ErrUnsupportedDriver`。

## 快速开始

以下 SQLite 内存库示例无需启动外部数据库：

```go
package main

import (
	"context"
	"log"

	"github.com/golang-io/db"
	_ "github.com/golang-io/db/sqlite"
)

func main() {
	if err := db.CreateDB(db.Options{Name: "default", URL: "sqlite://:memory:"}); err != nil {
		log.Fatal(err)
	}
	defer db.Get("default").Close()

	gormDB := db.GetDB(context.Background(), "default")
	if err := gormDB.Exec("SELECT 1").Error; err != nil {
		log.Fatal(err)
	}
}
```

### 连接 URL（`CreateDB`）

`CreateDB` 要求 `Options.URL` 为 **`scheme://dsn`**，`scheme` 不区分大小写：

| Scheme | 示例 URL（节选） |
|--------|------------------|
| `mysql` | `mysql://user:pass@tcp(host:3306)/db?charset=utf8mb4&parseTime=True&loc=Local` |
| `sqlite` | `sqlite:///path/to/file.db`；内存库需配置连接池参数，见 [SQLite 使用说明](#sqlite-使用说明) |
| `clickhouse` | `clickhouse://user:pass@localhost:9000/db` |
| `duckdb` | `duckdb:///path/to/file.duckdb`；`duckdb://` 或 `duckdb://:memory:` 为内存库，连接池内所有连接共享同一个库 |

不支持的 scheme 会返回 `unknown database schema: ...`。

URL 处理只识别开头的外层 `scheme://`（scheme 以字母开头，其后允许字母、数字、`+`、`-`、`.`），只剥离一层；DSN 原样交给驱动，不做通用 URL 解码、路径清理或查询参数改写（唯一例外：`sqlite` 子包在 DSN 未指定 `_txlock` 时追加 `_txlock=immediate`，见 SQLite 使用说明）。转义规则仍由驱动定义，例如 MySQL 参数中的 `https://example` 应写成 `https%3A%2F%2Fexample`，本库不会自动修正非法 DSN。

- 根包 `CreateDB` / `OpenDB` 必须带数据库前缀。SQLite 原生 URI 应写为 `sqlite://file:///tmp/app.db`，而不是单独的 `file:///tmp/app.db`。
- `sqlite` 子包的 `Open` / `New` 会为裸 `file:` URI 补上 `sqlite://`；已带数据库前缀时不重复添加。
- `mysql` 子包的 `Open` / `New` 支持裸 DSN，密码或参数内部的 `https://` 不会被当作外层 scheme。原生 DSN 若恰好以合法 `scheme://` 开头则存在歧义，请显式添加数据库前缀。
- 已带其他合法 scheme 时保持原样，不猜测或改换驱动。

例如，导入 `github.com/golang-io/db/sqlite` 后，`sqlite.Open(ctx, db.Options{URL: "file:///tmp/app.db?mode=rwc"})` 与 `db.OpenDB(ctx, db.Options{URL: "sqlite://file:///tmp/app.db?mode=rwc"})` 使用同一 DSN。

MySQL 超时由 `mysql` 子包的注册工厂应用，不是根包自动设置。使用该子包或下文工厂示例时，解析后的零超时取 `Options.DialTimeout`（默认 3s，TCP 建连）/ `ReadTimeout`（默认不设置）/ `WriteTimeout`（默认不设置），DSN 非零超时优先。DSN 显式 `0s` 也会被回填；如需禁用，可设对应 `Options` 为负数，或在工厂中自行调整策略。读写超时作用于后续查询，可能中断长查询。`Open` 直接使用传入的方言，不调用工厂，请自行配置 DSN 校验及超时。

保留 GORM 默认自动 Ping，不额外执行手动探活。MySQL 初始化和探活依赖 DSN 超时；这些超时不是整个初始化的总时限。`GetDB` 会一直等到连上，请求 `ctx` 不结束这次等待，也不取消正在进行的拨号。

**生产环境建议在 MySQL DSN 中配置 `readTimeout`**（如 `readTimeout=30s`）。`timeout`（即 `DialTimeout`）只限制 TCP 建连；若对端接受连接却不回握手包（如负载均衡后的数据库无响应），GORM 方言初始化查询 `SELECT VERSION()` 和自动 Ping 都不受请求 `ctx` 约束，在未设 `readTimeout` 时这一次拨号会一直停住，下一轮重试要等它结束。`readTimeout` 也作用于每条查询，取值需大于最长查询耗时。

### 一次性打开连接（不注册全局 `dbm`）

适合迁移脚本、命令行工具等不需要 `GetDB` 按名取实例的场景；`OpenDB` 须导入驱动子包或事先注册方言，`Open` 直接使用传入的方言（示例中的 `os` 需导入标准库）：

```go
gormDB, err := db.OpenDB(ctx, db.Options{URL: os.Getenv("DB_URL")})
gormDB, err := db.Open(ctx, gormmysql.Open(dsn), db.Options{MaxOpenConns: 20}) // 自带 gorm.Dialector（gormmysql = gorm.io/driver/mysql），无需注册；Options.URL 与超时字段不生效
```

### 自带连接池的使用边界

`CreateDB` 托管实例只支持从 DSN 创建连接的工厂，工厂不得设置自带 `Conn`。首次初始化失败时，GORM 可能关闭该池；后台重试复用同一方言，不能恢复已关闭的自带池。本库不自动检测这一限制，也不在重试时重新执行工厂。

需要传入自带 `*sql.DB` 时，使用一次性的 `Open`，并由调用方负责成功后的关闭和失败后的重建。初始化或自动 Ping 失败可能关闭传入的池；本库也会调整连接池参数，不要传入其他业务共享的池，也不要拿失败后的原池再次打开。重新尝试时应同时创建新池和新方言。

### 手动注册驱动

`CreateDB` / `OpenDB` 按 URL scheme 查找已注册的方言（`Open` 直接使用传入的方言，无需注册）：导入 `sqlite` / `mysql` / `duckdb` 子包会自动注册，其他方言需手动注册。下面是不导入子包时的完整注册代码，放到业务项目的 Go 文件中，按需保留所用驱动及对应导入。每个 scheme 只能注册一次：已导入子包的 scheme 不要再手动注册，否则会 panic。

```go
package main

import (
	"github.com/libtnb/sqlite"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/golang-io/db"
	"gorm.io/driver/clickhouse"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func init() {
	db.Register("sqlite", func(dsn string, _ *db.Options) (gorm.Dialector, error) {
		return sqlite.Open(dsn), nil
	})

	db.Register("mysql", func(dsn string, opts *db.Options) (gorm.Dialector, error) {
		cfg, err := gomysql.ParseDSN(dsn)
		if err != nil {
			return nil, err
		}
		if cfg.Timeout == 0 {
			cfg.Timeout = max(opts.DialTimeout, 0)
		}
		if cfg.ReadTimeout == 0 {
			cfg.ReadTimeout = max(opts.ReadTimeout, 0)
		}
		if cfg.WriteTimeout == 0 {
			cfg.WriteTimeout = max(opts.WriteTimeout, 0)
		}
		return gormmysql.New(gormmysql.Config{DSNConfig: cfg}), nil
	})

	db.Register("clickhouse", func(dsn string, _ *db.Options) (gorm.Dialector, error) {
		return clickhouse.Open("clickhouse://" + dsn), nil
	})
}
```

- **SQLite**：DSN 原样交给第三方方言，PRAGMA 配置见下文。此示例不会自动设置内存库的连接池参数，需要时导入 `sqlite` 子包。
- **MySQL**：工厂先解析 DSN，配置错误立即返回；再将解析后为零的超时回填为 `Options`（负数取零），与 `mysql` 子包行为一致。若仅注册 `gormmysql.Open(dsn)`，则不会校验 DSN 或应用 `Options` 超时。
- **ClickHouse**：工厂必须补回 `clickhouse://`。默认事务由根包按方言名自动保留，无需额外配置钩子。
- 数据库凭据通过环境变量提供，例如 `db.Options{URL: os.Getenv("DB_URL")}`（使用时导入 `os`），不要硬编码到代码或提交到仓库。

### 其他 GORM 方言

例如 PostgreSQL 使用 URL 格式 DSN 时可这样注册：

```go
import (
	"github.com/golang-io/db"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func init() {
	db.Register("postgres", func(dsn string, _ *db.Options) (gorm.Dialector, error) {
		// CreateDB / OpenDB 会去掉 "postgres://" 只传后半段，URL 格式的 DSN 需要补回 scheme
		return postgres.Open("postgres://" + dsn), nil
	})
}

// 注册后与内置驱动用法一致
_ = db.CreateDB(db.Options{Name: "pg", URL: "postgres://user:pass@localhost:5432/mydb"})
```

注意事项：

- **只能在 `init()` 中注册**：驱动注册表不加锁，之后只读。
- **scheme 建议与 `Dialector.Name()` 一致**：例如 gorm 的 postgres 方言 `Name()` 返回 `"postgres"`，就注册为 `"postgres"`，便于在配置和日志中对应。
- **传给注册工厂的 DSN 已去掉 `scheme://`**：驱动若要求 DSN 带 scheme（如 URL 格式的 PostgreSQL、ClickHouse），需在工厂中补回；使用 key=value 格式 DSN（如 `postgres://host=localhost user=x dbname=y`）则无需补回，但此时不要再补 scheme。
- **别名需单独注册**：配置中使用 `postgresql://` 时，需再注册一次 `"postgresql"`。
- 默认跳过 GORM 默认事务；仅当 `dialector.Name()` 为 `"clickhouse"` 时保留默认事务，以支持批量写入。

> 以上 PostgreSQL 示例未在本仓库实测（未引入 postgres 驱动依赖），接入时请验证 DSN 解析。

### 预编译缓存的使用边界

本库默认 `PrepareStmt: false`，不主动创建 GORM 预编译缓存。当前 GORM 版本在业务使用 `Session(&gorm.Session{PrepareStmt: true})` 后，缓存的清理 goroutine 不随连接池关闭或 `PreparedStmtDB.Close()` 退出；反复创建、销毁独立缓存会累积 goroutine。

需要频繁销毁实例时不要启用预编译缓存；已有业务启用时，先评估关闭对性能的影响。长期复用实例不会每次查询都创建一个清理 goroutine，但不等于解决了上游的生命周期限制。设置缓存 TTL 为零或负数会被补成默认 TTL，不能规避此问题。本次不修改 GORM 或 `vendor`，完整生命周期修复需另行验证上游版本。

## SQLite 使用说明

导入 `github.com/golang-io/db/sqlite` 子包即可；也可不导入子包，按[手动注册驱动](#手动注册驱动)中的示例注册 `"sqlite"`。

本库不额外设置 SQLite 的 PRAGMA；当前方言 `github.com/libtnb/sqlite` 在 DSN 未指定时默认追加 `busy_timeout(5000)`（毫秒）。WAL 需在 DSN 中显式开启，锁等待时间可通过 DSN 覆盖；内存库的连接池参数由 `sqlite` 子包自动设置。

`sqlite` 子包在 DSN 未指定 `_txlock` 时自动追加 `_txlock=immediate`（对文件库和内存库都生效，`sqlite.Dialector` 同样处理）：事务开始时就拿写锁。SQLite 默认的 deferred 事务到第一次写才申请写锁，多连接下"先读后写"的事务在升级写锁时会直接报 `database is locked`，`busy_timeout` 对这种情况不生效。代价是事务全部串行，只读事务也一样；事务外的读仍可并发，开启 WAL 后读与写也互不阻塞。DSN 中显式写 `_txlock=deferred` 可恢复 SQLite 默认行为。

### 文件库

```go
_ = db.CreateDB(db.Options{
	Name: "local",
	URL:  "sqlite:///data/app.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)",
})
```

- `_pragma=xxx`：每个新连接建立时都会执行一次 `PRAGMA xxx`。
- `journal_mode(WAL)`：SQLite 默认是 `DELETE` 模式。WAL 模式下读写互不阻塞，并发读性能更好。WAL 状态会写入数据库文件，开启一次后一直生效。
- `busy_timeout(5000)`：设置锁等待时间为 5 秒，也是当前底层驱动的默认值；可通过 `_pragma=busy_timeout(毫秒数)` 调整。需要锁冲突时不等待，可显式设置 `_pragma=busy_timeout(0)`。WAL 只解决读与写的冲突，写与写之间仍然互斥。
- 路径：`sqlite:///data/app.db` 是绝对路径，`sqlite://app.db` 是相对路径。

### 内存库

| | `:memory:` | memdb |
|---|---|---|
| URL | `sqlite://:memory:` | `sqlite://file:/名字?vfs=memdb` |
| 多连接 | 每个连接都是一个独立的空库，只能单连接 | 同一进程内多个连接共享同一个库 |
| 并发 | 不支持：所有查询排队，一次只执行一条 | 读可以并发，写同一时刻只能有一个 |
| 适用场景 | 单元测试 | 正式环境：进程内、读多写少、需要 SQL 查询能力（过滤、`JOIN`、聚合）的数据 |

**正式环境需要并发查询，请用 memdb，不要用 `:memory:`。** 实测（1 万行，每次查询扫全表）：

| | 单核 | 8 核并发 |
|---|---|---|
| `:memory:` | 558 µs | 541 µs（没有提升） |
| memdb | 537 µs | 74 µs（约 7.3 倍） |

```go
// :memory:：自动单连接、连接永不回收
_ = db.CreateDB(db.Options{Name: "test", URL: "sqlite://:memory:"})

// memdb：多连接共享，自动连接永不回收
_ = db.CreateDB(db.Options{Name: "cache", URL: "sqlite://file:/cache?vfs=memdb"})

// sqlite 子包效果相同，URL 可省略 sqlite:// 前缀
_ = sqlite.New(db.Options{Name: "cache", URL: "file:/cache?vfs=memdb"})
```

注意事项：

- **连接池参数自动设置**：内存库的数据挂在连接上，最后一个连接关闭时整个库就被销毁；`:memory:` 每个连接打开的还是独立的空库。导入 `sqlite` 子包后，识别到内存库时（`db.CreateDB` / `db.OpenDB` / `sqlite.New` / `sqlite.Open` 行为一致）直接设置：
  - `ConnMaxLifeTime`、`ConnMaxIdleTime` 设为 `-1`（连接永不回收）；
  - `:memory:` 等私有内存库的 `MaxOpenConns` 设为 1，因此无法并发查询，只适合单元测试。

  这几项手动设置的值会被忽略。
- **`:memory:` 不要嵌套使用连接**：只有一个连接时，在 `Stream` / `StreamBatch` 回调里再用 `GetDB` 查同一个库，或在事务里不用 `tx` 而另外调用 `GetDB`，都会因为等不到连接而永久阻塞。需要嵌套请用 memdb。
- **memdb 的名字必须以 `/` 开头**：`file:/cache` 才会在连接间共享；`file:cache` 是每个连接私有的库。
- **memdb 写入是串行的**：SQLite 同一时刻只允许一个写入，写入时也会阻塞读；内存库不支持 WAL（设置了不报错，但不生效），无法做到读写互不阻塞。写入频繁的场景并发收益有限，可按业务需要通过 `_pragma=busy_timeout(毫秒数)` 调整锁等待时间，当前底层驱动默认值为 5000ms。
- 内存库的数据只存在于当前进程：重启即丢失，多个实例之间也不共享，并且没有过期和容量淘汰机制。按主键查询的耗时在微秒级，大约是 `map` 的 500～1000 倍，**简单的 KV 缓存请用 `map`、本地缓存库或 Redis**。

## 主要 API

| 符号 | 作用 |
|------|------|
| `Register(scheme, factory)` | 在业务 `init()` 中注册第三方方言工厂；重名或 nil 工厂会 panic |
| `GetDriver(scheme)` | 获取已注册工厂；未注册返回错误 |
| `CreateDB(Options) error` | 按名登记托管实例：不建连、不在服务端建库；首次 `GetDB` 时开始拨号，失败每 10s 重试；只返回配置错误 |
| `GetDB(ctx, name) *gorm.DB` | 返回已经连上的 `*gorm.DB`；还没连上就一直等待。`ctx` 只传给返回的会话，不结束等待 |
| `Get(name)` | 返回已注册的 `DB` 接口（未注册会 panic） |
| `OpenDB(ctx, Options)` / `Open(ctx, dialector, Options)` | 立即建连并返回 `*gorm.DB`，不注册、不重试，连接错误直接返回；调用方负责关闭 |
| `Stream` / `StreamBatch` | 原生 `database/sql` 流式查询，逐行 / 分批回调，适合大结果集 |
| `Query(ctx, name, sql, args...)` | 查询并返回 `[]map[string]any` |
| `Options` | 结构体配置，零值字段使用默认值 |

`Stream` / `StreamBatch` / `Query` 返回的列值为驱动原生类型（如 `int64`、`float64`、`time.Time`），`NULL` 为 `nil`，`[]byte` 转为 `string`。MySQL 需在 DSN 中加 `parseTime=True` 才会返回 `time.Time`。

**注册约定**：`CreateDB` 只能在启动阶段调用（全局注册表不加锁），之后只读；同名重复注册返回错误。

原始 SQL 请通过 `GetDB` 拿到的 `*gorm.DB` 使用 `Raw` / `Exec` 等 GORM API。返回的对象已经连上，可以直接查询或开事务。事务内的查询和写入使用回调参数 `tx`，不要另行调用 `GetDB`。

完整说明以 [pkg.go.dev](https://pkg.go.dev/github.com/golang-io/db) 与源码注释为准。

## 配置字段（`Options` 摘要）

| 字段 | 含义 | 默认（未设置时由内部合并） |
|------|------|----------------------------|
| `Name` | 实例名，用于 `GetDB` | `CreateDB` 必填；`OpenDB` / `Open` 为 `default` |
| `URL` | 连接串 | 必填（`Open` 不使用） |
| `MaxIdleConns` / `MaxOpenConns` | 连接池 | 10 / 50 |
| `ConnMaxLifeTime` | 连接最大存活时间（负数表示不回收） | 5m |
| `ConnMaxIdleTime` | 连接最大空闲时间（回收被 MySQL/LB 静默关闭的空闲连接；负数表示不回收） | 1m |
| `DialTimeout` / `ReadTimeout` / `WriteTimeout` | 交给注册工厂应用的建连 / 读 / 写超时；MySQL 示例仅回填 DSN 的零值，负数取零 | 3s / 不设置 / 不设置 |
| `LongQueryTime` | 慢查询阈值（日志 `SLOW:`） | 3s |
| `Output` | SQL 日志 `io.Writer` | `os.Stdout` |
| `Log` | 业务日志（连接、重试） | 空实现 |
| `LogLevel` | GORM 日志级别 | `Warn` |

零值字段使用默认值；负数按 `database/sql` 原生语义（不限制 / 不保留 / 不回收 / 不超时）。`Output`、`Log`、`LogLevel` 无 YAML 标签，适合在代码里注入。

## YAML 配置示例

```yaml
Name: default
URL: mysql://user:pass@tcp(127.0.0.1:3306)/mydb?charset=utf8mb4&parseTime=True&loc=Local
MaxIdleConns: 50
MaxOpenConns: 100
ConnMaxLifeTime: 5m
LongQueryTime: 1s
```

先导入 `github.com/golang-io/db/mysql` 子包（或按上文注册 `"mysql"`），再反序列化得到 `db.Options` 并传给 `db.CreateDB(opts)`；含真实凭据的 URL 应从环境变量注入，不提交到配置文件。

## 许可证

[MIT License](LICENSE)
