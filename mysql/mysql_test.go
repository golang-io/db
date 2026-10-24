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
	if driver.Dialector == nil {
		t.Fatal("driver.Dialector is nil")
	}

	dialector := mysql.Dialector("user:pass@tcp(localhost:3306)/test")
	if dialector.Name() != "mysql" {
		t.Fatalf("expected dialector name mysql, got %s", dialector.Name())
	}
}
