# sqlite

SQLite 驱动子包，基于 [`github.com/libtnb/sqlite`](https://github.com/libtnb/sqlite)（纯 Go，无需 CGO）。导入即注册 `sqlite` scheme：

```go
import _ "github.com/golang-io/db/sqlite"

_ = db.CreateDB(db.Options{Name: "default", URL: "sqlite:///data/app.db"})
```

`sqlite.New` / `sqlite.Open` 可省略 `sqlite://` 前缀，效果相同。

## DSN 命名规则

URL 去掉 `sqlite://` 后就是 DSN，有两种形式：

- **普通文件名**：如 `/data/app.db`、`data/app.db`、`:memory:`。`?` 后只有本驱动的 `_` 开头参数和 `vfs` 生效，`mode`、`cache` 等 SQLite 参数会被丢弃。
- **`file:` 开头的 URI**：`file:` + 库名 + `?参数`，`mode`、`cache`、`vfs` 等 SQLite 参数都生效。

`file:/mydb`、`file:mydb`、`file::memory:` 是同一种格式，只是 `file:` 后面的库名不同：

| 写法 | `file:` 之后的库名 |
|---|---|
| `file:/mydb` | `/mydb`，以 `/` 开头的名字 |
| `file:mydb` | `mydb`，普通名字 |
| `file::memory:` | `:memory:`，SQLite 保留的内存库名字；`::` 不是特殊语法，是 `file:` 紧接 `:memory:` |

### 固定写法与自定义部分

| 内容 | 是否固定 | 含义 |
|---|---|---|
| `file:` | 固定 | 按 SQLite URI 解析，SQLite 参数才生效 |
| `:memory:` | 固定 | 内存库的保留名字 |
| `mode=memory` | 固定 | 不论名字是什么，都打开为内存库 |
| `cache=shared` | 固定 | 同一进程内用同名打开的连接共用一个库 |
| `vfs=memdb` | 固定 | 使用 memdb 内存存储 |
| memdb 名字开头的 `/` | 固定规则 | 以 `/` 开头才在连接间共享；这只是命名约定，不是磁盘路径，不会在根目录建文件 |
| 库名（下文的 `mydb`） | 自定义 | 任意名字；同一进程内同名的共享内存库是同一个库 |

### 是否在连接间共享（实测）

一个连接建表，另一个连接查询：

| DSN | 第二个连接能否看到表 |
|---|---|
| `file:/mydb?vfs=memdb` | 能 |
| `file:mydb?vfs=memdb` | 不能，名字不以 `/` 开头 |
| `file:mydb?mode=memory&cache=shared` | 能 |
| `file::memory:?cache=shared` | 能 |
| `file:mydb?mode=memory` | 不能，缺少 `cache=shared` |
| `file::memory:` | 不能 |
| `:memory:` | 不能 |

规律：memdb 看名字是否以 `/` 开头；普通内存库看是否有 `cache=shared`。

## URL 写法与最终配置

以下均为实测结果。"最终 DSN"是去掉 `sqlite://` 并处理后实际交给驱动的字符串；连接池参数均为未手动设置时的值。

本包对所有写法的处理：

- DSN 未指定 `_txlock` 时追加 `_txlock=immediate`：事务开始即拿写锁，避免多连接下"先读后写"的事务报 `database is locked`。显式写 `_txlock=deferred` 可恢复 SQLite 默认行为。
- `busy_timeout` 未指定时为 5000ms，由 libtnb 在打开连接时追加，不体现在下表的 DSN 中。

### 1. 文件库

连接池使用根包默认值。

| URL | 最终 DSN | 最大连接数 | 存活 / 空闲回收 |
|---|---|---|---|
| `sqlite:///data/app.db` | `/data/app.db?_txlock=immediate` | 50 | 5m / 1m |
| `sqlite://data/app.db`（相对路径） | `data/app.db?_txlock=immediate` | 50 | 5m / 1m |
| `sqlite:///data/app.db?_pragma=journal_mode(WAL)` | `/data/app.db?_pragma=journal_mode(WAL)&_txlock=immediate` | 50 | 5m / 1m |
| `sqlite:///data/app.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)` | `/data/app.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)&_txlock=immediate` | 50 | 5m / 1m |
| `sqlite://file:/data/app.db?mode=rwc` | `file:/data/app.db?mode=rwc&_txlock=immediate` | 50 | 5m / 1m |
| `sqlite:///data/app.db?_txlock=deferred` | `/data/app.db?_txlock=deferred`（已指定，不追加） | 50 | 5m / 1m |
| `sqlite:///data/app.db?mode=memory` | `/data/app.db?mode=memory&_txlock=immediate` | 50 | 5m / 1m |

文件库的 `file:/data/app.db` 中，`/data/app.db` 是真实的磁盘路径。最后一行不以 `file:` 开头，`mode=memory` 被丢弃，实际仍是文件库。

### 2. 共享内存库

同一进程内多个连接共用一个库；连接永不回收。

| URL | 最终 DSN | 最大连接数 | 存活 / 空闲回收 |
|---|---|---|---|
| `sqlite://file:/mydb?vfs=memdb` | `file:/mydb?vfs=memdb&_txlock=immediate` | 50 | 不回收 |
| `sqlite://file:mydb?mode=memory&cache=shared` | `file:mydb?mode=memory&cache=shared&_txlock=immediate` | 50 | 不回收 |
| `sqlite://file::memory:?cache=shared` | `file::memory:?cache=shared&_txlock=immediate` | 50 | 不回收 |

`file::memory:?cache=shared` 的名字固定为 `:memory:`，同一进程内只有一个这样的库；需要多个共享内存库时用前两种写法并起不同名字。

### 3. 私有内存库

每个连接各自一个独立的库；只能单连接，连接永不回收。

| URL | 最终 DSN | 最大连接数 | 存活 / 空闲回收 |
|---|---|---|---|
| `sqlite://:memory:` | `:memory:?_txlock=immediate` | 1 | 不回收 |
| `sqlite://file:mydb?mode=memory` | `file:mydb?mode=memory&_txlock=immediate` | 1 | 不回收 |
| `sqlite://file:mydb?vfs=memdb` | `file:mydb?vfs=memdb&_txlock=immediate`（名字不以 `/` 开头） | 1 | 不回收 |

## 注意事项

- 内存库的数据挂在连接上，最后一个连接关闭时整个库就被销毁，因此本包识别到内存库时直接设置：
  - `ConnMaxLifeTime`、`ConnMaxIdleTime` 设为 `-1`（连接永不回收），手动设置的值会被忽略；
  - 私有内存库的 `MaxOpenConns` 设为 1，手动设置的值会被忽略。
- "不回收"在连接池内部为 `0s`：`database/sql` 把 0 和负数都存为 0，表示不限时。
- `MaxIdleConns` 不处理，不要设为负数，否则连接用完即关，内存库会在每次查询后丢失。
- 私有内存库只有一个连接：在 `Stream` / `StreamBatch` 回调里再用 `GetDB` 查同一个库，或在事务里不用 `tx` 而另外调用 `GetDB`，都会永久阻塞。需要嵌套请用 memdb。
- `_txlock=immediate` 让所有事务串行，只读事务也一样；事务外的读仍可并发，开启 WAL 后读与写互不阻塞。
- WAL 不会自动开启，需要时在 DSN 中写 `_pragma=journal_mode(WAL)`。它会永久写入数据库文件，且不适用于网络文件系统。
- 空 DSN（`sqlite://`）直接返回配置错误。
- 直接用 `sqlite.Dialector(dsn)` 配合 `db.Open` 时，仍会追加 `_txlock`，但内存库的连接池参数不会自动设置，需在 `Options` 中自行配置。
