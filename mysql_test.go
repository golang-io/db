package db_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/golang-io/db"
	_ "github.com/golang-io/db/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm/logger"
)

// TestInvalidDSN 非法 DSN 是配置错误：CreateDB / OpenDB 直接返回，不注册、不进入重试
func TestInvalidDSN(t *testing.T) {
	opts := db.Options{Name: "mysql-invalid-dsn", URL: "mysql://user:pass@tcp(localhost:3306)/test?loc=Asia/Shanghai"}
	if err := db.CreateDB(opts); err == nil {
		t.Fatal("CreateDB: expect invalid dsn error")
	}
	if _, err := db.OpenDB(context.Background(), opts); err == nil {
		t.Fatal("OpenDB: expect invalid dsn error")
	}
}

func TestDialectorTimeouts(t *testing.T) {
	driver, err := db.GetDriver("mysql")
	if err != nil {
		t.Fatal(err)
	}
	opts := db.Options{DialTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	for _, tt := range []struct {
		name                 string
		params               string
		timeout, read, write time.Duration
	}{
		{"defaults", "", 3 * time.Second, 10 * time.Second, 10 * time.Second},
		{"custom", "?timeout=1s&readTimeout=7s&writeTimeout=8s", time.Second, 7 * time.Second, 8 * time.Second},
		{"partial", "?readTimeout=20s", 3 * time.Second, 20 * time.Second, 10 * time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dialector, err := driver("user:pass@tcp(localhost:3306)/test"+tt.params, &opts)
			if err != nil {
				t.Fatal(err)
			}
			d := dialector.(*gormmysql.Dialector)
			cfg := d.DSNConfig
			if cfg.Timeout != tt.timeout || cfg.ReadTimeout != tt.read || cfg.WriteTimeout != tt.write {
				t.Fatalf("timeouts = %v/%v/%v, want %v/%v/%v", cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout, tt.timeout, tt.read, tt.write)
			}
			parsed := gormmysql.Open(d.DSN).(*gormmysql.Dialector).DSNConfig
			if parsed.Timeout != tt.timeout || parsed.ReadTimeout != tt.read || parsed.WriteTimeout != tt.write {
				t.Fatalf("effective DSN timeouts = %v/%v/%v", parsed.Timeout, parsed.ReadTimeout, parsed.WriteTimeout)
			}
		})
	}
}

func TestOpenDBHandshakeTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _ = io.Copy(io.Discard, conn)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
	})

	start := time.Now()
	_, err = db.OpenDB(context.Background(), db.Options{
		URL:      "mysql://user:pass@tcp(" + listener.Addr().String() + ")/test?readTimeout=50ms",
		LogLevel: logger.Silent,
	})
	elapsed := time.Since(start)
	if err == nil || elapsed < 50*time.Millisecond || elapsed >= time.Second {
		t.Fatalf("expected handshake timeout within 1s, elapsed=%v, err=%v", elapsed, err)
	}
}
