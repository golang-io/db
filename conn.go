// conn.go 提供统一的数据库创建与打开入口，根据 URL Schema 自动分发到已注册的驱动。

package db

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// WithScheme 返回补齐外层数据库 scheme 的配置副本；已有合法 scheme 时保持不变，DSN 内部的 :// 不视为外层前缀。
func (o Options) WithScheme(scheme string) Options {
	if _, _, ok := splitDatabaseURL(o.URL); !ok {
		o.URL = scheme + "://" + o.URL
	}
	return o
}

// splitDatabaseURL 只拆外层 scheme://，不解析、解码或规范化驱动 DSN。
func splitDatabaseURL(raw string) (scheme, dsn string, ok bool) {
	scheme, dsn, ok = strings.Cut(raw, "://")
	scheme = strings.TrimSpace(scheme)
	if !ok || scheme == "" {
		return "", "", false
	}
	for i, c := range scheme {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' {
			continue
		}
		if i > 0 && (c >= '0' && c <= '9' || c == '+' || c == '-' || c == '.') {
			continue
		}
		return "", "", false
	}
	return scheme, dsn, true
}

// CreateDB 登记一个按名访问的托管实例：不建连，也不会在服务端建库。
// 首次 GetDB 时开始拨号；失败则每 10s 重试，调用方停在 GetDB 上直到连上。
// 只返回配置错误（缺 Name、重名、URL 非法、驱动未注册）。URL 格式为 scheme://dsn，如 "mysql://user:pass@tcp(...)/db"。
// 托管约定：工厂应从 DSN 创建方言，不得绑定自带 Conn。初始化失败时 GORM 可能关闭池，
// 本实例复用方言重试，不能恢复已关闭的自带池；需要自带池时使用 Open 并自行管理重建。
func CreateDB(options Options) error {
	if options.Name == "" {
		return fmt.Errorf("name is required")
	}
	if _, ok := dbm[options.Name]; ok {
		return fmt.Errorf("db %q already exists", options.Name)
	}
	opts := options.withDefaults()
	dialector, err := opts.dialector()
	if err != nil {
		return err
	}
	Load(newInstance(opts, dialector))
	return nil
}

// OpenDB 按 URL 立即建连并返回 *gorm.DB。不注册、不重试；失败直接返回 error（含配置错误和连接错误）。
// 调用方持有并负责关闭。适用于迁移脚本、命令行工具、测试等一次性任务。
// 上下文约定：当前不使用 ctx 取消初始化，也不自动绑定到返回会话；查询时须自行调用 WithContext(ctx)。
// 建连使用 GORM 自动 Ping，初始化超时由驱动配置控制，不额外执行 PingContext。
func OpenDB(_ context.Context, options Options) (*gorm.DB, error) {
	opts := options.withDefaults()
	dialector, err := opts.dialector()
	if err != nil {
		return nil, err
	}
	gdb, err := open(dialector, opts)
	if err != nil {
		return nil, err
	}
	return gdb, nil
}

// Open 使用调用方构造的 gorm.Dialector 立即建连并返回 *gorm.DB，不注册、不重试。
// 上下文与探活约定同 OpenDB：ctx 不参与初始化、不绑定返回会话；查询时须自行调用 WithContext(ctx)。
// 适用于 DSN 表达不了的配置（如自带连接、自定义 TLS）；options.URL 与超时字段不使用，需在 dialector 中自行设置。
// 自带池须独占：本包会设置池参数，初始化失败时 GORM 可能关闭池；失败重试须重建池和方言，成功后由调用方关闭。
// 直接使用传入的方言，无需 Register。
func Open(_ context.Context, dialector gorm.Dialector, options Options) (*gorm.DB, error) {
	gdb, err := open(dialector, options.withDefaults())
	if err != nil {
		return nil, err
	}
	return gdb, nil
}

// dialector 拆分 scheme://dsn，按 scheme 查找驱动并生成 dialector（不建连）。o 须已补齐默认值，工厂可按 DSN 调整 o。
func (o *Options) dialector() (gorm.Dialector, error) {
	scheme, dsn, ok := splitDatabaseURL(o.URL)
	if !ok {
		return nil, fmt.Errorf("invalid database url: %q (must be in format scheme://dsn)", o.URL)
	}
	factory, err := GetDriver(scheme)
	if err != nil {
		return nil, err
	}
	dialector, err := factory(dsn, o)
	if err != nil {
		return nil, fmt.Errorf("invalid %s dsn: %w", scheme, err)
	}
	return dialector, nil
}
