// migrator_test.go 验证 DuckDB 迁移器的自增列与唯一约束处理。

package duckdb_test

import (
	"errors"
	"testing"

	"github.com/golang-io/db/duckdb"
	"gorm.io/gorm"
)

func TestAddColumnRejectsAutoIncrement(t *testing.T) {
	for _, direct := range []bool{false, true} {
		name := "AutoMigrate"
		if direct {
			name = "AddColumn"
		}
		t.Run(name, func(t *testing.T) {
			g := open(t, "")
			if err := g.Exec("CREATE TABLE items(id UBIGINT PRIMARY KEY)").Error; err != nil {
				t.Fatal(err)
			}
			model := &struct {
				ID     uint
				Label  string
				Serial int64 `gorm:"autoIncrement"`
			}{}
			var err error
			if direct {
				err = g.Table("items").Migrator().AddColumn(model, "Label")
			} else {
				err = g.Table("items").AutoMigrate(&struct {
					ID    uint
					Label string
				}{})
			}
			if err != nil {
				t.Fatalf("ordinary column: %v", err)
			}
			if direct {
				err = g.Table("items").Migrator().AddColumn(model, "Serial")
			} else {
				err = g.Table("items").AutoMigrate(model)
			}
			if !errors.Is(err, duckdb.ErrNotSupport) {
				t.Fatalf("want unsupported, got %v", err)
			}
			if g.Migrator().HasColumn("items", "serial") {
				t.Fatal("rejected column was created")
			}
			var count int64
			if err := g.Raw("SELECT count(*) FROM duckdb_sequences()").Scan(&count).Error; err != nil || count != 0 {
				t.Fatalf("sequence count=%d, err=%v", count, err)
			}
			ignored := &struct {
				Serial int64 `gorm:"autoIncrement;-:migration"`
			}{}
			if err := g.Table("items").Migrator().AddColumn(ignored, "Serial"); err != nil {
				t.Fatalf("ignored field: %v", err)
			}
		})
	}
}

func TestAutoIncrementRepeatedMigration(t *testing.T) {
	type item struct {
		ID     uint
		Serial int64 `gorm:"autoIncrement;column:serial_no;unique"`
	}
	g := open(t, "")
	model := &item{}
	if err := g.Table("renamed").AutoMigrate(model); err != nil {
		t.Fatal(err)
	}
	stmt := &gorm.Statement{DB: g}
	if err := stmt.ParseWithSpecialTableName(model, "renamed"); err != nil {
		t.Fatal(err)
	}
	field := stmt.Schema.LookUpField("Serial")
	before := field.DefaultValue
	for range 2 {
		if err := g.Table("renamed").AutoMigrate(model); err != nil {
			t.Fatalf("repeat: %v", err)
		}
	}
	if field.DefaultValue != before {
		t.Fatal("cached schema field mutated")
	}
	row := &item{}
	if err := g.Table("renamed").Create(row).Error; err != nil || row.Serial == 0 {
		t.Fatalf("insert serial=%d err=%v", row.Serial, err)
	}
	noUnique := &struct {
		ID     uint
		Serial int64 `gorm:"autoIncrement;column:serial_no"`
	}{}
	if err := g.Table("renamed").AutoMigrate(noUnique); !errors.Is(err, duckdb.ErrNotSupport) {
		t.Fatalf("unique check skipped: %v", err)
	}
}

func TestAutoIncrementMigrationMismatch(t *testing.T) {
	for _, kind := range []string{"missing default", "wrong sequence", "wrong type"} {
		t.Run(kind, func(t *testing.T) {
			g := open(t, "")
			model := &struct {
				ID     uint
				Serial int64 `gorm:"autoIncrement"`
			}{}
			if err := g.Table("items").AutoMigrate(model); err != nil {
				t.Fatal(err)
			}
			var query string
			switch kind {
			case "missing default":
				query = "ALTER TABLE items ALTER COLUMN serial DROP DEFAULT"
			case "wrong sequence":
				if err := g.Exec("CREATE SEQUENCE other_seq").Error; err != nil {
					t.Fatal(err)
				}
				query = "ALTER TABLE items ALTER COLUMN serial SET DEFAULT nextval('other_seq')"
			case "wrong type":
				query = "ALTER TABLE items ALTER COLUMN serial TYPE VARCHAR"
			}
			if err := g.Exec(query).Error; err != nil {
				t.Fatal(err)
			}
			if err := g.Table("items").AutoMigrate(model); !errors.Is(err, duckdb.ErrNotSupport) {
				t.Fatalf("mismatch accepted: %v", err)
			}
		})
	}
}

func TestAutoIncrementExplicitDefault(t *testing.T) {
	for _, tt := range []struct {
		name          string
		model         any
		want          [2]int64
		wantSequences int64
	}{
		{
			name: "constant",
			model: &struct {
				ID     uint
				Serial int64 `gorm:"autoIncrement;default:7"`
			}{},
			want: [2]int64{7, 7},
		},
		{
			name: "dash",
			model: &struct {
				ID     uint
				Serial int64 `gorm:"autoIncrement;default:(-)"`
			}{},
			want:          [2]int64{1, 2},
			wantSequences: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g := open(t, "")
			for range 2 {
				if err := g.Table("items").AutoMigrate(tt.model); err != nil {
					t.Fatalf("migrate: %v", err)
				}
			}
			for range 2 {
				if err := g.Exec("INSERT INTO items DEFAULT VALUES").Error; err != nil {
					t.Fatal(err)
				}
			}
			var values []int64
			if err := g.Raw("SELECT serial FROM items ORDER BY id").Scan(&values).Error; err != nil {
				t.Fatal(err)
			}
			if len(values) != 2 || values[0] != tt.want[0] || values[1] != tt.want[1] {
				t.Fatalf("serial values=%v, want %v", values, tt.want)
			}
			var count int64
			if err := g.Raw("SELECT count(*) FROM duckdb_sequences() WHERE sequence_name = 'items_serial_seq'").Scan(&count).Error; err != nil {
				t.Fatal(err)
			}
			if count != tt.wantSequences {
				t.Fatalf("serial sequences=%d, want %d", count, tt.wantSequences)
			}
		})
	}
}

func TestAutoMigrateUnique(t *testing.T) {
	type account struct {
		ID    uint
		Email string `gorm:"unique"`
	}
	g := open(t, "")
	for range 2 {
		if err := g.AutoMigrate(&account{}); err != nil {
			t.Fatalf("migrate unchanged unique constraint: %v", err)
		}
	}
	if err := g.Create(&account{Email: "a@example.com"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := g.Create(&account{Email: "a@example.com"}).Error; err == nil {
		t.Fatal("unique constraint must reject duplicate email")
	}

	withoutUnique := &struct {
		ID    uint
		Email string
	}{}
	if err := g.Table("accounts").AutoMigrate(withoutUnique); !errors.Is(err, duckdb.ErrNotSupport) {
		t.Fatalf("removing unique constraint must return ErrNotSupport, got %v", err)
	}
	if err := g.Create(&account{Email: "a@example.com"}).Error; err == nil {
		t.Fatal("failed migration must leave unique constraint in place")
	}
}

func TestAutoMigrateAddUnique(t *testing.T) {
	g := open(t, "")
	if err := g.Exec("CREATE TABLE accounts(id UBIGINT PRIMARY KEY, email VARCHAR)").Error; err != nil {
		t.Fatal(err)
	}
	withUnique := &struct {
		ID    uint
		Email string `gorm:"unique"`
	}{}
	if err := g.Table("accounts").AutoMigrate(withUnique); !errors.Is(err, duckdb.ErrNotSupport) {
		t.Fatalf("adding unique constraint must return ErrNotSupport, got %v", err)
	}
}

func TestAddUniqueColumn(t *testing.T) {
	for _, entry := range []string{"AddColumn", "AutoMigrate"} {
		t.Run(entry, func(t *testing.T) {
			g := open(t, "")
			if err := g.Exec("CREATE TABLE accounts(id UBIGINT PRIMARY KEY); INSERT INTO accounts VALUES (1),(2)").Error; err != nil {
				t.Fatal(err)
			}
			model := &struct {
				ID    uint
				Email string `gorm:"column:contact_email;unique"`
			}{}
			var err error
			if entry == "AddColumn" {
				err = g.Table("accounts").Migrator().AddColumn(model, "Email")
			} else {
				err = g.Table("accounts").AutoMigrate(model)
			}
			if err != nil {
				t.Fatal(err)
			}
			var nullCount int64
			if err := g.Table("accounts").Where("contact_email IS NULL").Count(&nullCount).Error; err != nil || nullCount != 2 {
				t.Fatalf("existing rows: null count=%d, err=%v", nullCount, err)
			}
			if err := g.Exec("INSERT INTO accounts(id,contact_email) VALUES (3,'same')").Error; err != nil {
				t.Fatal(err)
			}
			if err := g.Exec("INSERT INTO accounts(id,contact_email) VALUES (4,'same')").Error; err == nil {
				t.Fatal("new unique column must reject duplicate values")
			}
			if err := g.Table("accounts").AutoMigrate(model); err != nil {
				t.Fatalf("repeat migration: %v", err)
			}
		})
	}
}

func TestAddUniqueColumnDuplicateDefault(t *testing.T) {
	for _, entry := range []string{"AddColumn", "AutoMigrate"} {
		t.Run(entry, func(t *testing.T) {
			g := open(t, "")
			if err := g.Exec("CREATE TABLE accounts(id UBIGINT PRIMARY KEY); INSERT INTO accounts VALUES (1),(2)").Error; err != nil {
				t.Fatal(err)
			}
			model := &struct {
				ID    uint
				Email string `gorm:"unique;default:same"`
			}{}
			var err error
			if entry == "AddColumn" {
				err = g.Table("accounts").Migrator().AddColumn(model, "Email")
			} else {
				err = g.Table("accounts").AutoMigrate(model)
			}
			if err == nil {
				t.Fatal("duplicate default values must fail to add a unique column")
			}
			if g.Migrator().HasColumn("accounts", "email") {
				t.Fatal("failed statement must not leave a column without its unique constraint")
			}
			var count int64
			if err := g.Table("accounts").Count(&count).Error; err != nil || count != 2 {
				t.Fatalf("existing rows: count=%d, err=%v", count, err)
			}
		})
	}
}

func TestColumnTypesUniqueMetadata(t *testing.T) {
	g := open(t, "")
	if err := g.Exec(`
		CREATE TABLE accounts(
			id UBIGINT PRIMARY KEY,
			email VARCHAR UNIQUE,
			tenant_id BIGINT,
			code VARCHAR,
			indexed VARCHAR,
			UNIQUE(tenant_id, code)
		);
		CREATE UNIQUE INDEX idx_accounts_indexed ON accounts(indexed);
		CREATE SCHEMA other;
		CREATE TABLE other.accounts(code VARCHAR UNIQUE);
		ATTACH ':memory:' AS other_db;
		CREATE TABLE other_db.accounts(code VARCHAR UNIQUE);
	`).Error; err != nil {
		t.Fatal(err)
	}
	columns, err := g.Migrator().ColumnTypes("accounts")
	if err != nil {
		t.Fatal(err)
	}
	for _, column := range columns {
		unique, known := column.Unique()
		if !known || unique != (column.Name() == "email") {
			t.Errorf("column %q: unique=%v, known=%v", column.Name(), unique, known)
		}
	}
	model := &struct {
		ID       uint
		Email    string `gorm:"unique"`
		TenantID int64
		Code     string
		Indexed  string `gorm:"uniqueIndex:idx_accounts_indexed"`
	}{}
	if err := g.Table("accounts").AutoMigrate(model); err != nil {
		t.Fatalf("composite unique and unique index must not be mistaken for column unique: %v", err)
	}
}

// TestAttachedDatabaseCatalogIsolation 验证 ATTACH 挂载的其他数据库存在同名表、列、索引、约束时，
// 当前数据库的 HasTable / GetTables / HasColumn / HasIndex / HasConstraint / AutoMigrate 不会被跨库污染。
func TestAttachedDatabaseCatalogIsolation(t *testing.T) {
	g := open(t, "")
	if err := g.Exec(`
		ATTACH ':memory:' AS other_db;
		CREATE TABLE other_db.widgets(
			id UBIGINT PRIMARY KEY,
			code VARCHAR,
			extra VARCHAR,
			CONSTRAINT uni_widgets_code UNIQUE (code)
		);
		CREATE INDEX idx_widgets_code ON other_db.widgets(code);
	`).Error; err != nil {
		t.Fatal(err)
	}

	type widget struct {
		ID   uint   `gorm:"primaryKey"`
		Code string `gorm:"index:idx_widgets_code"`
	}

	mig := g.Migrator()
	if mig.HasTable(&widget{}) {
		t.Fatal("HasTable matched table in attached database")
	}
	tables, err := mig.GetTables()
	if err != nil {
		t.Fatalf("GetTables: %v", err)
	}
	if len(tables) != 0 {
		t.Fatalf("GetTables = %v, want empty in current database", tables)
	}

	// 在当前库创建简版表（不含 extra 列、索引和唯一约束），确认各项检查不会误命中 other_db.widgets。
	if err := g.Exec(`CREATE TABLE widgets(id UBIGINT PRIMARY KEY, code VARCHAR)`).Error; err != nil {
		t.Fatal(err)
	}
	if mig.HasColumn(&widget{}, "extra") {
		t.Fatal("HasColumn matched column in attached database")
	}
	if mig.HasIndex(&widget{}, "idx_widgets_code") {
		t.Fatal("HasIndex matched index in attached database")
	}
	if mig.HasConstraint(&widget{}, "uni_widgets_code") {
		t.Fatal("HasConstraint matched constraint in attached database")
	}

	// AutoMigrate 应在当前库的 widgets 上补齐 idx_widgets_code 索引，而不是因误判 other_db 已有索引而跳过。
	if err := g.AutoMigrate(&widget{}); err != nil {
		t.Fatalf("AutoMigrate with attached database: %v", err)
	}
	if !mig.HasIndex(&widget{}, "idx_widgets_code") {
		t.Fatal("AutoMigrate skipped creating index in current database")
	}

	// 新模型在当前库不存在而 other_db 存在同名表时，AutoMigrate 必须在当前库建表。
	if err := g.Exec(`CREATE TABLE other_db.orders(id UBIGINT PRIMARY KEY)`).Error; err != nil {
		t.Fatal(err)
	}
	type order struct {
		ID uint `gorm:"primaryKey"`
	}
	if err := g.AutoMigrate(&order{}); err != nil {
		t.Fatalf("AutoMigrate order: %v", err)
	}
	if err := g.Create(&order{}).Error; err != nil {
		t.Fatalf("insert into current database orders: %v", err)
	}
}
