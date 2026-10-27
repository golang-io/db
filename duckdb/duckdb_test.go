package duckdb_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang-io/db"
	"github.com/golang-io/db/duckdb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type product struct {
	ID        uint `gorm:"primaryKey"`
	Code      string
	Price     int
	CreatedAt time.Time
	UpdatedAt time.Time
}

type bar struct {
	Market int     `gorm:"primaryKey"`
	Code   string  `gorm:"primaryKey;size:32"`
	Ts     int64   `gorm:"primaryKey"`
	Close  float64 `gorm:"column:close"`
}

// open 打开内存库，测试结束后关闭。
func open(t *testing.T, url string) *gorm.DB {
	t.Helper()
	g, err := duckdb.Open(context.Background(), db.Options{URL: url, LogLevel: logger.Silent})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := g.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return g
}

// products 建表：本子包不支持迁移，测试用 CREATE TABLE 建表，自增列自己建序列。
func products(g *gorm.DB) *gorm.DB {
	if err := g.Exec(`
		CREATE SEQUENCE products_id_seq START 1;
		CREATE TABLE products(
			id BIGINT PRIMARY KEY DEFAULT nextval('products_id_seq'),
			code VARCHAR, price INTEGER,
			created_at TIMESTAMP, updated_at TIMESTAMP
		)`).Error; err != nil {
		panic(err)
	}
	return g
}

func TestCRUD(t *testing.T) {
	g := products(open(t, ""))
	p := product{Code: "D42", Price: 100}
	if err := g.Create(&p).Error; err != nil || p.ID == 0 {
		t.Fatalf("create: id=%d, err=%v", p.ID, err)
	}
	if err := g.Model(&p).Update("Price", 200).Error; err != nil {
		t.Fatalf("update: %v", err)
	}
	var got product
	if err := g.First(&got, p.ID).Error; err != nil || got.Price != 200 {
		t.Fatalf("first: price=%d, err=%v", got.Price, err)
	}
	if err := g.Delete(&got).Error; err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := g.First(&product{}, got.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
}

// TestAutoMigrate 测试使用 DuckDB 专属迁移器自动建表，自增主键自动绑定序列。
func TestAutoMigrate(t *testing.T) {
	g := open(t, "")
	if err := g.AutoMigrate(&product{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	p := product{Code: "X", Price: 50}
	if err := g.Create(&p).Error; err != nil || p.ID == 0 {
		t.Fatalf("insert into auto-migrated table failed: id=%d, err=%v", p.ID, err)
	}
	// 二次迁移幂等测试
	if err := g.AutoMigrate(&product{}); err != nil {
		t.Fatalf("re-AutoMigrate failed: %v", err)
	}
}

func TestAutoMigrateDefaults(t *testing.T) {
	g := open(t, "")
	type item struct {
		ID      uint
		Kind    string `gorm:"default:forward"`
		Quoted  string `gorm:"default:a'b"`
		Count   int    `gorm:"default:42"`
		Enabled bool   `gorm:"default:true"`
	}
	if err := g.AutoMigrate(&item{}); err != nil {
		t.Fatal(err)
	}
	if err := g.AutoMigrate(&item{}); err != nil {
		t.Fatalf("unchanged defaults: %v", err)
	}
	if err := g.Exec("INSERT INTO items DEFAULT VALUES").Error; err != nil {
		t.Fatal(err)
	}
	var got item
	if err := g.First(&got).Error; err != nil || got.Kind != "forward" || got.Quoted != "a'b" || got.Count != 42 || !got.Enabled {
		t.Fatalf("defaults: got=%+v, err=%v", got, err)
	}
	changed := &struct {
		Kind string `gorm:"default:backward"`
	}{}
	if err := g.Table("items").AutoMigrate(changed); err == nil {
		t.Fatal("changing defaults must not silently succeed")
	}
}

func TestAutoMigrateChecks(t *testing.T) {
	g := open(t, "")
	type item struct {
		ID    uint
		Price int    `gorm:"check:price_positive,price>0"`
		Code  string `gorm:"check:code_allowed,code <> 'a b'"`
	}
	if err := g.AutoMigrate(&item{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"price_positive", "Price", "code_allowed"} {
		if !g.Migrator().HasConstraint(&item{}, name) {
			t.Fatalf("existing constraint %q not found", name)
		}
	}
	if err := g.AutoMigrate(&item{}); err != nil {
		t.Fatalf("unchanged checks: %v", err)
	}
	if err := g.Exec("INSERT INTO items (price, code) VALUES (0, 'ok')").Error; err == nil {
		t.Fatal("CHECK constraint no longer enforced")
	}
	changed := &struct {
		Code string `gorm:"check:code_allowed,code <> 'ab'"`
	}{}
	if g.Table("items").Migrator().HasConstraint(changed, "code_allowed") {
		t.Fatal("different CHECK expression matched")
	}
	if err := g.Table("items").AutoMigrate(changed); err == nil {
		t.Fatal("changing CHECK must not silently succeed")
	}
}

func TestOnConflict(t *testing.T) {
	g := open(t, ":memory:")
	if err := g.Exec(`CREATE TABLE bars(market INTEGER, code VARCHAR(32), ts BIGINT, "close" DOUBLE, PRIMARY KEY (market, code, ts))`).Error; err != nil {
		t.Fatal(err)
	}
	row := bar{Market: 1, Code: "00700", Ts: 100, Close: 1.5}
	if err := g.Create(&row).Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	row.Close = 9
	if err := g.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "market"}, {Name: "code"}, {Name: "ts"}},
		DoUpdates: clause.AssignmentColumns([]string{"close"}),
	}).Create(&row).Error; err != nil {
		t.Fatalf("upsert: %v", err)
	}
	var got bar
	if err := g.First(&got, "market = ? AND code = ? AND ts = ?", 1, "00700", 100).Error; err != nil || got.Close != 9 {
		t.Fatalf("first: close=%v, err=%v", got.Close, err)
	}
}

// TestOffsetOnly 只有 Offset 时不能生成 LIMIT -1
func TestOffsetOnly(t *testing.T) {
	g := products(open(t, ""))
	if err := g.Create(&[]product{{Code: "A"}, {Code: "B"}, {Code: "C"}}).Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	var ps []product
	if err := g.Order("id").Offset(1).Find(&ps).Error; err != nil || len(ps) != 2 || ps[0].Code != "B" {
		t.Fatalf("offset: len=%d, err=%v", len(ps), err)
	}
	if err := g.Order("id").Offset(1).Limit(1).Find(&ps).Error; err != nil || len(ps) != 1 || ps[0].Code != "B" {
		t.Fatalf("offset+limit: len=%d, err=%v", len(ps), err)
	}
}

// TestExplainQuotes 字符串值用单引号：日志里的 SQL 可直接执行
func TestExplainQuotes(t *testing.T) {
	if got := duckdb.Dialector("").Explain("SELECT ?", `a'b"c`); got != `SELECT 'a''b"c'` {
		t.Fatalf("Explain = %s", got)
	}
	g := open(t, "")
	if err := g.Exec(`CREATE TABLE quoteds(id BIGINT, kind VARCHAR DEFAULT 'a"b')`).Error; err != nil {
		t.Fatal(err)
	}
	if err := g.Exec(`INSERT INTO quoteds (id) VALUES (1)`).Error; err != nil {
		t.Fatalf("insert: %v", err)
	}
	var kind string
	if err := g.Raw(`SELECT kind FROM quoteds`).Scan(&kind).Error; err != nil || kind != `a"b` {
		t.Fatalf("kind=%q, err=%v", kind, err)
	}
}

func TestTransactionRollback(t *testing.T) {
	g := products(open(t, ""))
	err := g.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&product{Code: "TX1"}).Error; err != nil {
			return err
		}
		return errors.New("rollback")
	})
	if err == nil {
		t.Fatal("expected tx error")
	}
	var n int64
	if err := g.Model(&product{}).Where("code = ?", "TX1").Count(&n).Error; err != nil || n != 0 {
		t.Fatalf("rollback failed: count=%d, err=%v", n, err)
	}
}

// TestNestedTransaction DuckDB 不支持 SAVEPOINT：嵌套事务返回 ErrUnsupportedDriver，外层回滚后不留数据
func TestNestedTransaction(t *testing.T) {
	g := products(open(t, ""))
	err := g.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&product{Code: "OUT"}).Error; err != nil {
			return err
		}
		return tx.Transaction(func(tx2 *gorm.DB) error {
			return tx2.Create(&product{Code: "IN"}).Error
		})
	})
	if !errors.Is(err, gorm.ErrUnsupportedDriver) {
		t.Fatalf("want ErrUnsupportedDriver, got %v", err)
	}
	var n int64
	if err := g.Model(&product{}).Count(&n).Error; err != nil || n != 0 {
		t.Fatalf("count=%d, err=%v; want 0", n, err)
	}
}

// TestStringDefault DEFAULT 取列默认值
func TestStringDefault(t *testing.T) {
	g := open(t, "")
	if err := g.Exec(`CREATE TABLE items(id BIGINT, kind VARCHAR NOT NULL DEFAULT 'forward')`).Error; err != nil {
		t.Fatal(err)
	}
	if err := g.Exec(`INSERT INTO items (id) VALUES (DEFAULT)`).Error; err != nil {
		t.Fatalf("insert: %v", err)
	}
	var kind string
	if err := g.Raw(`SELECT kind FROM items LIMIT 1`).Scan(&kind).Error; err != nil || kind != "forward" {
		t.Fatalf("kind=%q, err=%v", kind, err)
	}
}

func TestUnsignedIntegerRange(t *testing.T) {
	g := open(t, "")
	type numbers struct {
		Small  uint8
		Medium uint16
		Large  uint32
		Max    uint64 `gorm:"column:max_value"` // full 是 DuckDB 关键字
		Signed int64
	}
	if err := g.Exec(`CREATE TABLE numbers(small UTINYINT, medium USMALLINT, large UINTEGER, max_value UBIGINT, signed BIGINT)`).Error; err != nil {
		t.Fatal(err)
	}
	want := numbers{Small: ^uint8(0), Medium: ^uint16(0), Large: ^uint32(0), Max: ^uint64(0), Signed: -1}
	if err := g.Create(&want).Error; err != nil {
		t.Fatalf("create max unsigned integers: %v", err)
	}
	var got numbers
	if err := g.Where(clause.Eq{Column: clause.Column{Name: "max_value"}, Value: want.Max}).First(&got).Error; err != nil || got != want {
		t.Fatalf("got=%+v, want=%+v, err=%v", got, want, err)
	}
	var value uint64
	if err := g.Raw("SELECT ?", &want.Max).Scan(&value).Error; err != nil || value != want.Max {
		t.Fatalf("pointer parameter: value=%d, err=%v", value, err)
	}
}

func TestTranslateErrors(t *testing.T) {
	g := open(t, "")
	if err := g.Exec(`
		CREATE TABLE parents(id INTEGER PRIMARY KEY);
		INSERT INTO parents VALUES (1);
		CREATE TABLE children(id INTEGER REFERENCES parents(id));
		CREATE TABLE unique_rows(value INTEGER UNIQUE);
		INSERT INTO unique_rows VALUES (1);
		CREATE TABLE checked(unique_value INTEGER CHECK(unique_value > 0));
		CREATE TABLE required(duplicate_value INTEGER NOT NULL);
	`).Error; err != nil {
		t.Fatal(err)
	}
	translator := g.Dialector.(gorm.ErrorTranslator)
	if err := translator.Translate(nil); err != nil {
		t.Fatalf("Translate(nil)=%v", err)
	}
	plain := errors.New("unique duplicate primary key")
	if err := translator.Translate(plain); err != plain {
		t.Fatalf("non-driver error changed: %v", err)
	}
	for _, tt := range []struct {
		name  string
		query string
		want  error
	}{
		{"catalog", "SELECT * FROM unique_accounts", nil},
		{"binder", "SELECT unique_missing FROM parents", nil},
		{"primary_key", "INSERT INTO parents VALUES (1)", gorm.ErrDuplicatedKey},
		{"unique", "INSERT INTO unique_rows VALUES (1)", gorm.ErrDuplicatedKey},
		{"foreign_key", "INSERT INTO children VALUES (999)", gorm.ErrForeignKeyViolated},
		{"check", "INSERT INTO checked VALUES (-1)", nil},
		{"not_null", "INSERT INTO required VALUES (NULL)", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := g.Exec(tt.query).Error
			if err == nil {
				t.Fatal("expected query to fail")
			}
			wrapped := fmt.Errorf("operation: %w", err)
			got := translator.Translate(wrapped)
			if tt.want == nil {
				if got != wrapped {
					t.Fatalf("original error changed: %v", got)
				}
			} else if !errors.Is(got, tt.want) {
				t.Fatalf("got=%v, want=%v", got, tt.want)
			}
		})
	}
}

// TestNewConcurrent 托管实例：内存库在多连接间共享，并发查询都能读到同一张表
func TestNewConcurrent(t *testing.T) {
	ctx := context.Background()
	name := t.TempDir()
	if err := duckdb.New(db.Options{Name: name, URL: ":memory:", LogLevel: logger.Silent}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Get(name).Close() })
	g := db.GetDB(ctx, name)
	if err := g.Exec("CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (42)").Error; err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			g := db.GetDB(ctx, name)
			var n int
			if err := g.Raw("SELECT id FROM t").Scan(&n).Error; err != nil || n != 42 {
				t.Errorf("query failed: n=%d, err=%v", n, err)
			}
		})
	}
	wg.Wait()
}

func TestFilePersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.duckdb")
	g1, err := duckdb.Open(context.Background(), db.Options{URL: path, LogLevel: logger.Silent})
	if err != nil {
		t.Fatal(err)
	}
	if err := g1.Exec(`CREATE TABLE products(id BIGINT PRIMARY KEY, code VARCHAR, price INTEGER,
		created_at TIMESTAMP, updated_at TIMESTAMP)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := g1.Create(&product{ID: 1, Code: "FILE", Price: 7}).Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	pool, _ := g1.DB()
	_ = pool.Close()

	g2 := open(t, "duckdb://"+path)
	var got product
	if err := g2.First(&got, "code = ?", "FILE").Error; err != nil || got.Price != 7 {
		t.Fatalf("first: price=%d, err=%v", got.Price, err)
	}
}
