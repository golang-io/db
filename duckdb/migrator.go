package duckdb

import (
	"database/sql"
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

// Migrator DuckDB schema 迁移器：HasTable/HasColumn/索引走 DuckDB catalog，Alter 复杂操作明确拒绝。
type Migrator struct {
	migrator.Migrator
}

// seqName 为自增列生成序列名：{table}_{column}_seq。
func seqName(table, column string) string {
	return fmt.Sprintf("%s_%s_seq", table, column)
}

// CurrentDatabase 返回当前数据库名。
func (m Migrator) CurrentDatabase() (name string) {
	m.DB.Raw("SELECT current_database()").Scan(&name)
	return
}

// CreateTable 建表；AutoIncrement 列用 SEQUENCE + DEFAULT nextval（DuckDB 无可用 IDENTITY）。
// 序列名直接拼接到 SQL，自增列所在表名、列名只允许字母、数字和下划线。
func (m Migrator) CreateTable(values ...interface{}) error {
	for _, value := range m.ReorderModels(values, false) {
		tx := m.DB.Session(&gorm.Session{})
		if err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
			if stmt.Schema == nil {
				return fmt.Errorf("duckdb: CreateTable: schema is nil")
			}

			// 先为自增列建序列，再 CREATE TABLE。
			for _, field := range stmt.Schema.Fields {
				if field.AutoIncrement {
					name := seqName(stmt.Table, field.DBName)
					if strings.ContainsFunc(name, func(r rune) bool {
						return r != '_' && !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9')
					}) {
						return fmt.Errorf("duckdb: invalid sequence name %q: auto increment table/column name must only contain letters, digits and _", name)
					}
					if err := tx.Exec("CREATE SEQUENCE IF NOT EXISTS " + name + " START 1").Error; err != nil {
						return fmt.Errorf("duckdb: create sequence %s: %w", name, err)
					}
				}
			}

			var (
				createTableSQL          = "CREATE TABLE ? ("
				args                    = []interface{}{m.CurrentTable(stmt)}
				hasPrimaryKeyInDataType bool
			)

			for _, dbName := range stmt.Schema.DBNames {
				field := stmt.Schema.FieldsByDBName[dbName]
				if field.IgnoreMigration {
					continue
				}
				createTableSQL += "? ?"
				colType := m.DB.Migrator().FullDataTypeOf(field)
				if field.AutoIncrement {
					// DEFAULT nextval('table_col_seq')；RETURNING 可取回新 id（LastInsertId 不可靠）。
					colType.SQL += " DEFAULT nextval('" + seqName(stmt.Table, field.DBName) + "')"
				}
				hasPrimaryKeyInDataType = hasPrimaryKeyInDataType ||
					strings.Contains(strings.ToUpper(m.DataTypeOf(field)), "PRIMARY KEY")
				args = append(args, clause.Column{Name: dbName}, colType)
				createTableSQL += ","
			}

			if !hasPrimaryKeyInDataType && len(stmt.Schema.PrimaryFields) > 0 {
				createTableSQL += "PRIMARY KEY ?,"
				pks := make([]interface{}, 0, len(stmt.Schema.PrimaryFields))
				for _, field := range stmt.Schema.PrimaryFields {
					pks = append(pks, clause.Column{Name: field.DBName})
				}
				args = append(args, pks)
			}

			for _, uni := range stmt.Schema.ParseUniqueConstraints() {
				createTableSQL += "CONSTRAINT ? UNIQUE (?),"
				args = append(args, clause.Column{Name: uni.Name}, clause.Column{Name: uni.Field.DBName})
			}

			for _, chk := range stmt.Schema.ParseCheckConstraints() {
				createTableSQL += "CONSTRAINT ? CHECK (?),"
				args = append(args, clause.Column{Name: chk.Name}, clause.Expr{SQL: chk.Constraint})
			}

			createTableSQL = strings.TrimSuffix(createTableSQL, ",") + ")"

			if tableOption, ok := m.DB.Get("gorm:table_options"); ok {
				createTableSQL += fmt.Sprint(tableOption)
			}

			if err := tx.Exec(createTableSQL, args...).Error; err != nil {
				return err
			}

			if m.CreateIndexAfterCreateTable {
				for _, idx := range stmt.Schema.ParseIndexes() {
					if err := m.CreateIndex(value, idx.Name); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// HasTable 检查表是否存在（duckdb_tables，避免 information_schema + schema 绑定歧义）。
func (m Migrator) HasTable(value interface{}) bool {
	var count int64
	_ = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			`SELECT count(*) FROM duckdb_tables() WHERE schema_name = current_schema() AND table_name = ?`,
			stmt.Table,
		).Scan(&count).Error
	})
	return count > 0
}

// GetTables 列出当前 schema 下用户表。
func (m Migrator) GetTables() (tableList []string, err error) {
	err = m.DB.Raw(
		`SELECT table_name FROM duckdb_tables() WHERE schema_name = current_schema() ORDER BY table_name`,
	).Scan(&tableList).Error
	return
}

// DropTable 删除表（IF EXISTS）。
func (m Migrator) DropTable(values ...interface{}) error {
	values = m.ReorderModels(values, false)
	tx := m.DB.Session(&gorm.Session{})
	for i := len(values) - 1; i >= 0; i-- {
		if err := m.RunWithValue(values[i], func(stmt *gorm.Statement) error {
			return tx.Exec("DROP TABLE IF EXISTS ?", m.CurrentTable(stmt)).Error
		}); err != nil {
			return err
		}
	}
	return nil
}

// HasColumn 检查列是否存在。
func (m Migrator) HasColumn(value interface{}, field string) bool {
	var count int64
	_ = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		name := field
		if stmt.Schema != nil {
			if f := stmt.Schema.LookUpField(field); f != nil {
				name = f.DBName
			}
		}
		return m.DB.Raw(
			`SELECT count(*) FROM duckdb_columns() WHERE schema_name = current_schema() AND table_name = ? AND column_name = ?`,
			stmt.Table, name,
		).Scan(&count).Error
	})
	return count > 0
}

// ColumnTypes 补齐驱动未提供的默认值元数据，避免重复迁移误判为改列。
func (m Migrator) ColumnTypes(value any) ([]gorm.ColumnType, error) {
	columnTypes, err := m.Migrator.ColumnTypes(value)
	if err != nil {
		return nil, err
	}
	err = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		var columns []struct {
			ColumnName    string
			ColumnDefault sql.NullString
		}
		if err := m.DB.Raw(
			`SELECT column_name, column_default FROM duckdb_columns()
			 WHERE database_name = current_database() AND schema_name = current_schema() AND table_name = ?`,
			stmt.Table,
		).Scan(&columns).Error; err != nil {
			return err
		}
		defaults := make(map[string]sql.NullString, len(columns))
		for _, column := range columns {
			dv := column.ColumnDefault
			if strings.EqualFold(dv.String, "NULL") {
				dv.Valid = false
			} else if len(dv.String) >= 2 && dv.String[0] == '\'' && dv.String[len(dv.String)-1] == '\'' {
				dv.String = strings.ReplaceAll(dv.String[1:len(dv.String)-1], "''", "'")
			}
			defaults[column.ColumnName] = dv
		}
		for i, column := range columnTypes {
			ct := column.(migrator.ColumnType)
			ct.DefaultValueValue = defaults[ct.Name()]
			columnTypes[i] = ct
		}
		return nil
	})
	return columnTypes, err
}

// AlterColumn P0 不支持改列类型（DuckDB ALTER 能力有限，避免半迁移）。
func (m Migrator) AlterColumn(value interface{}, field string) error {
	return fmt.Errorf("duckdb: AlterColumn field=%s (recreate table or AddColumn only): %w", field, ErrNotSupport)
}

// HasIndex 检查索引是否存在。
func (m Migrator) HasIndex(value interface{}, name string) bool {
	var count int64
	_ = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema != nil {
			if idx := stmt.Schema.LookIndex(name); idx != nil {
				name = idx.Name
			}
		}
		return m.DB.Raw(
			`SELECT count(*) FROM duckdb_indexes() WHERE schema_name = current_schema() AND table_name = ? AND index_name = ?`,
			stmt.Table, name,
		).Scan(&count).Error
	})
	return count > 0
}

// CreateIndex 创建普通/唯一索引（IF NOT EXISTS）。
func (m Migrator) CreateIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema == nil {
			return fmt.Errorf("duckdb: CreateIndex: schema is nil")
		}
		idx := stmt.Schema.LookIndex(name)
		if idx == nil {
			return fmt.Errorf("duckdb: failed to create index with name %v", name)
		}
		opts := m.BuildIndexOptions(idx.Fields, stmt)
		values := []interface{}{clause.Column{Name: idx.Name}, m.CurrentTable(stmt), opts}

		createIndexSQL := "CREATE "
		if idx.Class != "" {
			createIndexSQL += idx.Class + " "
		}
		createIndexSQL += "INDEX IF NOT EXISTS ? ON ?"
		if idx.Type != "" {
			createIndexSQL += " USING " + idx.Type + " ?"
		} else {
			createIndexSQL += " ?"
		}
		return m.DB.Exec(createIndexSQL, values...).Error
	})
}

// DropIndex 删除索引。
func (m Migrator) DropIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema != nil {
			if idx := stmt.Schema.LookIndex(name); idx != nil {
				name = idx.Name
			}
		}
		return m.DB.Exec("DROP INDEX IF EXISTS ?", clause.Column{Name: name}).Error
	})
}

// HasConstraint 检查表级约束；DuckDB 不保留 CHECK 自定义名称，改为比较规范化表达式。
func (m Migrator) HasConstraint(value interface{}, name string) bool {
	var count int64
	_ = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		constraint, table := m.GuessConstraintInterfaceAndTable(stmt, name)
		if check, ok := constraint.(*schema.CheckConstraint); ok {
			return m.DB.Raw(
				`SELECT count(*) FROM duckdb_constraints()
				 WHERE database_name = current_database() AND schema_name = current_schema() AND table_name = ?
				 AND constraint_type = 'CHECK'
				 AND json_deserialize_sql(json_serialize_sql('SELECT ' || expression))
				   = json_deserialize_sql(json_serialize_sql('SELECT ' || ?))`,
				table, check.Constraint,
			).Scan(&count).Error
		}
		if constraint != nil {
			name = constraint.GetName()
		}
		return m.DB.Raw(
			`SELECT count(*) FROM information_schema.table_constraints
			 WHERE table_schema = current_schema() AND table_name = ? AND constraint_name = ?`,
			table, name,
		).Scan(&count).Error
	})
	return count > 0
}

// DropConstraint P0 不支持（DuckDB 对部分约束 DROP 支持不完整）。
func (m Migrator) DropConstraint(value interface{}, name string) error {
	return fmt.Errorf("duckdb: DropConstraint name=%s: %w", name, ErrNotSupport)
}

// RenameIndex P0 不支持。
func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	return fmt.Errorf("duckdb: RenameIndex: %w", ErrNotSupport)
}

// GetTypeAliases 类型别名，供 AutoMigrate 比较列类型。
func (m Migrator) GetTypeAliases(databaseTypeName string) []string {
	n := strings.ToLower(databaseTypeName)
	aliases := map[string][]string{
		"int": {"integer", "int4"}, "integer": {"int", "int4"}, "int4": {"integer", "int"},
		"bigint": {"int8"}, "int8": {"bigint"},
		"smallint": {"int2"}, "int2": {"smallint"},
		"double": {"float8", "float"}, "float8": {"double"}, "float": {"double", "float8"},
		"boolean": {"bool"}, "bool": {"boolean"},
		"varchar": {"character varying", "text"}, "text": {"varchar", "character varying"},
		"timestamp": {"datetime", "timestamp without time zone"},
		"blob":      {"bytea", "binary"},
	}
	return aliases[n]
}
