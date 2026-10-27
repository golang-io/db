package db

import (
	"time"

	"gorm.io/gorm"
)

// NewForTest 用指定 dialector 创建托管实例（补齐默认值，不注册），供测试包注入失败的 dialector。
func NewForTest(opts Options, d gorm.Dialector) *instance {
	return newInstance(opts.withDefaults(), d)
}

// WithRetry 修改后台重试间隔，须在首次 GetDB 之前调用。仅供测试使用。
func (in *instance) WithRetry(d time.Duration) *instance {
	in.retry = d
	return in
}
