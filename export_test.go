package db

import (
	"time"

	"gorm.io/gorm"
)

// NewForTest 用指定 dialector 创建托管实例（补齐默认值，不注册），供测试包注入失败的 dialector。
func NewForTest(opts Options, d gorm.Dialector) *instance {
	return newInstance(newOptions(opts), d)
}

// PrepareDialectorForTest 复现 OpenDB / CreateDB 建连前的路径：先 newOptions，再按 URL 调用已注册工厂。
// 不建连、不注册，供测试检查超时是否进入驱动实际使用的 DSN。
func PrepareDialectorForTest(opts Options, optFns ...Option) (gorm.Dialector, error) {
	o := newOptions(opts, optFns...)
	return o.dialector()
}

// WithRetry 修改后台重试间隔，须在首次 GetDB 之前调用。仅供测试使用。
func (in *instance) WithRetry(d time.Duration) *instance {
	in.retry = d
	return in
}
