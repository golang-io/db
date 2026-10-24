package db

import "gorm.io/gorm"

// SetupForTest 暴露 setup 给测试包使用
func SetupForTest(options Options, opts ...Option) *db {
	return setup(options, opts...)
}

// OpenForTest 暴露 my.open 给测试包使用
func (my *db) OpenForTest(f func(dsn string) gorm.Dialector) error {
	return my.open(f)
}

// DialectorForTest 暴露 my.dialector 给测试包使用
func (my *db) DialectorForTest() gorm.Dialector {
	return my.dialector
}

// SetDialectorForTest 暴露 my.dialector 注入给测试包使用
func (my *db) SetDialectorForTest(d gorm.Dialector) {
	my.dialector = d
}
