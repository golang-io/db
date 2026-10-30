package db_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/golang-io/db"
	"github.com/libtnb/sqlite"
	"gorm.io/gorm/logger"
)

func TestOpenOverridesExistingPoolLimits(t *testing.T) {
	for _, tt := range []struct {
		name                                      string
		previousMax, maxOpen, maxIdle, held, idle int
	}{
		{"expand", 1, 4, 4, 4, 4},
		{"unlimited", 1, 0, 4, 4, 4},
		{"shrink", 8, 2, 4, 2, 2},
		{"no idle", 1, 4, 0, 4, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := sql.Open("sqlite", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = pool.Close() })
			pool.SetMaxOpenConns(tt.previousMax)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err = db.Open(ctx, sqlite.New(sqlite.Config{Conn: pool}), db.Options{LogLevel: logger.Silent},
				db.MaxOpenConns(tt.maxOpen), db.MaxIdleConns(tt.maxIdle))
			if err != nil {
				t.Fatal(err)
			}
			var held []*sql.Conn
			t.Cleanup(func() {
				for _, conn := range held {
					_ = conn.Close()
				}
			})
			for range tt.held {
				conn, err := pool.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				held = append(held, conn)
			}
			for _, conn := range held {
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
			}
			stats := pool.Stats()
			if stats.MaxOpenConnections != tt.maxOpen || stats.Idle != tt.idle {
				t.Fatalf("max open=%d, idle=%d; want %d, %d", stats.MaxOpenConnections, stats.Idle, tt.maxOpen, tt.idle)
			}
		})
	}
}
