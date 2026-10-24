package clickhouse_test

import (
	"testing"

	"github.com/golang-io/db"
	"github.com/golang-io/db/clickhouse"
	"gorm.io/gorm"
)

func TestClickHouseDriverRegistration(t *testing.T) {
	driver, err := db.GetDriver("clickhouse")
	if err != nil {
		t.Fatalf("GetDriver(clickhouse) failed: %v", err)
	}
	if driver.Dialector == nil {
		t.Fatal("driver.Dialector is nil")
	}

	dialector := clickhouse.Dialector("clickhouse://default:password@localhost:9000/default")
	if dialector.Name() != "clickhouse" {
		t.Fatalf("expected dialector name clickhouse, got %s", dialector.Name())
	}

	cfg := &gorm.Config{SkipDefaultTransaction: true}
	if driver.Config != nil {
		driver.Config(cfg)
	}
	if cfg.SkipDefaultTransaction != false {
		t.Fatalf("expected SkipDefaultTransaction to be false for clickhouse, got true")
	}
}
