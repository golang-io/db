package db_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/golang-io/db"
	_ "github.com/golang-io/db/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
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

// TestDatabaseURLMaskPassword URL 格式错误时按配置展示连接串：开启后密码打码，传给驱动的 DSN 不变
func TestDatabaseURLMaskPassword(t *testing.T) {
	for _, tt := range []struct {
		name, raw, masked string
	}{
		{"normal", "user:ab123456yz@tcp(localhost:3306)/app", "user:ab**yz@tcp(localhost:3306)/app"},
		{"special characters", "user:ab:/?@secret@yz@tcp(localhost:3306)/app", "user:ab**yz@tcp(localhost:3306)/app"},
		{"unicode", "user:甲乙中间密码丙丁@tcp(localhost:3306)/app", "user:甲乙**丙丁@tcp(localhost:3306)/app"},
		{"short", "user:abcd@tcp(localhost:3306)/app", "user:**@tcp(localhost:3306)/app"},
		{"one character", "user:a@tcp(localhost:3306)/app", "user:**@tcp(localhost:3306)/app"},
		{"empty password", "user:@tcp(localhost:3306)/app", "user:@tcp(localhost:3306)/app"},
		{"no password", "user@tcp(localhost:3306)/app", "user@tcp(localhost:3306)/app"},
		{"no credentials", "tcp(localhost:3306)/app", "**"},
		{"parameter at sign", "user:ab123456yz@tcp(localhost:3306)/app?note=a@b", "**"},
		{"password containing url", "user:https://secret.example@tcp(localhost:3306)/app", "user:ht**le@tcp(localhost:3306)/app"},
		{"missing database separator", "user:ab@hidden/secret@tcp(localhost:3306)", "**"},
		{"missing address", "user:ab123456yz", "**"},
		{"key value dsn", "password=ab123456yz user=demo host=localhost dbname=app", "**"},
		{"ambiguous delimiter", "user:ab@tcp(fake)/db?secret@tcp(real)", "**"},
		{"unknown network", "user:ab123456yz@network/app", "**"},
	} {
		for _, config := range []string{"default", "enabled", "option on", "option off"} {
			for _, entry := range []string{"CreateDB", "OpenDB"} {
				t.Run(tt.name+"/"+config+"/"+entry, func(t *testing.T) {
					opts := db.Options{Name: uniq("mask-password"), URL: tt.raw}
					var overrides []db.Option
					want := tt.raw
					switch config {
					case "enabled":
						opts.MaskPassword = true
						want = tt.masked
					case "option on":
						overrides = append(overrides, db.MaskPassword(true))
						want = tt.masked
					case "option off":
						opts.MaskPassword = true
						overrides = append(overrides, db.MaskPassword(false))
					}
					var err error
					if entry == "CreateDB" {
						err = db.CreateDB(opts, overrides...)
					} else {
						_, err = db.OpenDB(context.Background(), opts, overrides...)
					}
					if err == nil || !strings.Contains(err.Error(), "invalid database url: "+fmt.Sprintf("%q", want)) {
						t.Fatalf("want displayed URL %q, got %v", want, err)
					}
					if opts.URL != tt.raw {
						t.Fatal("masking changed the original URL")
					}
				})
			}
		}
	}
}

// TestMaskPasswordPreservesDriverDSN 打码只影响错误展示，驱动收到的仍是原始 DSN
func TestMaskPasswordPreservesDriverDSN(t *testing.T) {
	scheme := uniq("mask-probe")
	const dsn = "user:ab123456yz@tcp(localhost:3306)/app"
	stop := errors.New("stop before connecting")
	db.Register(scheme, func(got string, opts *db.Options) (gorm.Dialector, error) {
		if got != dsn || !opts.MaskPassword {
			t.Errorf("driver received DSN=%q, mask=%v", got, opts.MaskPassword)
		}
		return nil, stop
	})
	_, err := db.OpenDB(context.Background(), db.Options{URL: scheme + "://" + dsn}, db.MaskPassword(true))
	if !errors.Is(err, stop) {
		t.Fatalf("driver error chain changed: %v", err)
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

// TestOpenDBOptionsTimeouts 走 OpenDB / CreateDB 的 newOptions 路径。
// DSN 非零超时优先；DSN 没写或写成 0s 时，用 Options 回填。负数 Options 表示不回填。
func TestOpenDBOptionsTimeouts(t *testing.T) {
	const dsn = "mysql://user:pass@tcp(localhost:3306)/test"
	base := db.Options{DialTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	for _, tt := range []struct {
		name                 string
		url                  string
		opts                 db.Options
		timeout, read, write time.Duration
	}{
		{"options fill empty dsn", dsn, base, 3 * time.Second, 10 * time.Second, 10 * time.Second},
		{"dsn readTimeout wins", dsn + "?readTimeout=20s", base, 3 * time.Second, 20 * time.Second, 10 * time.Second},
		{"dsn timeouts win", dsn + "?timeout=1s&readTimeout=7s&writeTimeout=8s", base, time.Second, 7 * time.Second, 8 * time.Second},
		{"dsn zero is backfilled", dsn + "?readTimeout=0s&writeTimeout=0s", base, 3 * time.Second, 10 * time.Second, 10 * time.Second},
		{"negative options stay unset", dsn, db.Options{ReadTimeout: -1, WriteTimeout: -1}, 3 * time.Second, 0, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.opts.URL = tt.url
			dialector, err := db.PrepareDialectorForTest(tt.opts)
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
				t.Fatalf("effective DSN timeouts = %v/%v/%v, want %v/%v/%v", parsed.Timeout, parsed.ReadTimeout, parsed.WriteTimeout, tt.timeout, tt.read, tt.write)
			}
		})
	}
}

func TestOpenDBHandshakeTimeout(t *testing.T) {
	for _, tt := range []struct {
		name        string
		rawQuery    string
		readTimeout time.Duration
	}{
		// DSN 查询参数本身带 readTimeout。
		{"dsn", "?readTimeout=50ms", 0},
		// DSN 不写 readTimeout，只靠 Options.ReadTimeout，必须经过 newOptions。
		{"options", "", 50 * time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
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
				URL:         "mysql://user:pass@tcp(" + listener.Addr().String() + ")/test" + tt.rawQuery,
				ReadTimeout: tt.readTimeout,
				LogLevel:    logger.Silent,
			})
			elapsed := time.Since(start)
			if err == nil || elapsed < 50*time.Millisecond || elapsed >= time.Second {
				t.Fatalf("expected handshake timeout within 1s, elapsed=%v, err=%v", elapsed, err)
			}
		})
	}
}
