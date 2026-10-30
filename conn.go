package db

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// WithScheme 在缺少外层 scheme 时补齐前缀，保留原始 DSN。
func (o Options) WithScheme(scheme string) Options {
	if _, _, ok := splitDatabaseURL(o.URL); !ok {
		o.URL = scheme + "://" + o.URL
	}
	return o
}

// splitDatabaseURL 只拆分外层 scheme://，不解码 DSN。
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

// CreateDB 按名注册实例，首次 GetDB 才建连，失败每 10s 重试。
// Name 必填，URL 格式为 scheme://dsn；工厂须从 DSN 创建连接池。
func CreateDB(options Options, opts ...Option) error {
	o := newOptions(options, opts...)
	if o.Name == "" {
		return fmt.Errorf("name is required")
	}
	if _, ok := dbm[o.Name]; ok {
		return fmt.Errorf("db %q already exists", o.Name)
	}
	dialector, err := o.dialector()
	if err != nil {
		return err
	}
	Load(newInstance(o, dialector))
	return nil
}

// OpenDB 按 URL 打开连接，不注册、不重试；调用方负责关闭。
// ctx 不参与初始化，也不绑定返回会话；查询时使用 WithContext(ctx)。
func OpenDB(_ context.Context, options Options, opts ...Option) (*gorm.DB, error) {
	o := newOptions(options, opts...)
	if o.Name == "" {
		o.Name = "default"
	}
	dialector, err := o.dialector()
	if err != nil {
		return nil, err
	}
	return open(dialector, o)
}

// Open 使用指定方言打开连接，忽略 Options 的 URL 和超时字段。
// ctx 约定同 OpenDB。自带池须独占，初始化失败时可能被关闭。
func Open(_ context.Context, dialector gorm.Dialector, options Options, opts ...Option) (*gorm.DB, error) {
	o := newOptions(options, opts...)
	if o.Name == "" {
		o.Name = "default"
	}
	return open(dialector, o)
}

// dialector 按 URL 选择驱动；工厂可调整连接配置。
func (o *Options) dialector() (gorm.Dialector, error) {
	scheme, dsn, ok := splitDatabaseURL(o.URL)
	if !ok {
		displayURL := o.URL
		if o.MaskPassword {
			displayURL = maskURLPassword(displayURL)
		}
		return nil, fmt.Errorf("invalid database url: %q (must be in format scheme://dsn)", displayURL)
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

// maskURLPassword 隐藏密码；无法可靠拆分的输入整体打码。
func maskURLPassword(raw string) string {
	slash := strings.LastIndexByte(raw, '/')
	if slash < 0 || strings.ContainsRune(raw[slash+1:], '@') {
		return "**"
	}
	at := strings.LastIndexByte(raw[:slash], '@')
	if at < 0 {
		return "**"
	}
	network := raw[at+1 : slash]
	if network != "" && (!strings.ContainsRune(network, '(') || !strings.HasSuffix(network, ")")) {
		return "**"
	}
	colon := strings.IndexByte(raw[:at], ':')
	if colon < 0 {
		return raw
	}
	password := []rune(raw[colon+1 : at])
	if len(password) == 0 {
		return raw
	}
	masked := "**"
	if len(password) > 4 {
		masked = string(password[:2]) + "**" + string(password[len(password)-2:])
	}
	return raw[:colon+1] + masked + raw[at:]
}
