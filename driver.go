package db

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// drivers 仅在 init 阶段注册，之后只读。
var drivers = make(map[string]func(string, *Options) (gorm.Dialector, error))

// Register 注册方言工厂，scheme 不区分大小写；重复或 nil 工厂会 panic。
// 仅限 init 阶段调用。工厂接收去掉 scheme:// 的 DSN，可调整 Options 的连接参数。
// 托管实例会复用方言重试，工厂不得绑定自带连接池；自带池使用 Open。
func Register(scheme string, d func(string, *Options) (gorm.Dialector, error)) {
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	if d == nil {
		panic(fmt.Sprintf("db: Register driver %q has nil Dialector", scheme))
	}
	if _, dup := drivers[scheme]; dup {
		panic(fmt.Sprintf("db: Register called twice for driver %q", scheme))
	}
	drivers[scheme] = d
}

// GetDriver 获取方言工厂，未注册时返回错误。
func GetDriver(scheme string) (func(string, *Options) (gorm.Dialector, error), error) {
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	d, ok := drivers[scheme]
	if !ok {
		return nil, fmt.Errorf("unknown database schema: %q (call db.Register in init with a GORM dialector factory)", scheme)
	}
	return d, nil
}
