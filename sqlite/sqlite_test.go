package sqlite_test

import (
	"context"
	"testing"

	"github.com/golang-io/db"
	"github.com/golang-io/db/sqlite"
)

func TestSQLiteSubpackage(t *testing.T) {
	ctx := context.Background()

	// 1. 测试显式子包 Open
	gdb, err := sqlite.Open(ctx, db.Options{
		URL: ":memory:",
	})
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	var n int
	if err := gdb.Raw("SELECT 1").Scan(&n).Error; err != nil || n != 1 {
		t.Fatalf("query failed: err=%v, n=%d", err, n)
	}

	// 2. 测试通过 scheme 动态分发 CreateDB + GetDB
	const name = "subpkg-sqlite"
	if err := db.CreateDB(db.Options{
		Name: name,
		URL:  "sqlite://:memory:",
	}); err != nil {
		t.Fatalf("db.CreateDB failed: %v", err)
	}

	g := db.GetDB(ctx, name)
	if g == nil {
		t.Fatal("db.GetDB returned nil")
	}
	if err := g.Raw("SELECT 42").Scan(&n).Error; err != nil || n != 42 {
		t.Fatalf("query failed: err=%v, n=%d", err, n)
	}
}
