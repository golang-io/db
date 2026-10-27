package mysql_test

import (
	"testing"

	"github.com/golang-io/db"
	"github.com/golang-io/db/mysql"
)

func TestMySQLDriverRegistration(t *testing.T) {
	driver, err := db.GetDriver("mysql")
	if err != nil {
		t.Fatalf("GetDriver(mysql) failed: %v", err)
	}
	dialector, err := driver("user:pass@tcp(localhost:3306)/test", &db.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if dialector.Name() != "mysql" {
		t.Fatalf("expected dialector name mysql, got %s", dialector.Name())
	}
	if name := mysql.Dialector("user:pass@tcp(localhost:3306)/test").Name(); name != "mysql" {
		t.Fatalf("expected dialector name mysql, got %s", name)
	}
}

func TestMySQLNewDSN(t *testing.T) {
	for _, dsn := range []string{
		"user:https://example@tcp(localhost:3306)/db",
		"user:pass@tcp([::1]:3306)/db%25prod?note=https%3A%2F%2Fexample",
	} {
		for _, prefix := range []string{"", "MYSQL://"} {
			opts := db.Options{Name: t.TempDir(), URL: prefix + dsn}
			if err := mysql.New(opts); err != nil {
				t.Fatalf("mysql.New DSN: %v", err)
			}
			t.Cleanup(func() { _ = db.Get(opts.Name).Close() })
		}
	}
}
