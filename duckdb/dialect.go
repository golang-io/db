package duckdb

import (
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"strconv"
	"strings"

	goduckdb "github.com/duckdb/duckdb-go/v2"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

// dialect GORM 的 DuckDB 方言实现。不支持 gorm.DeletedAt 软删除（DuckDB 的 UNIQUE 约束与软删冲突）。
type dialect struct {
	dsn string
}

func (dialect) Name() string {
	return "duckdb"
}

// Initialize 打开连接池并注册 callbacks（支持 ON CONFLICT / RETURNING）。
func (d dialect) Initialize(db *gorm.DB) error {
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		CreateClauses: []string{"INSERT", "VALUES", "ON CONFLICT", "RETURNING"},
		UpdateClauses: []string{"UPDATE", "SET", "FROM", "WHERE", "RETURNING"},
		DeleteClauses: []string{"DELETE", "FROM", "WHERE", "RETURNING"},
	})
	conn, err := sql.Open("duckdb", d.dsn)
	if err != nil {
		return err
	}
	db.ConnPool = conn
	maps.Copy(db.ClauseBuilders, d.ClauseBuilders())
	return nil
}

// ClauseBuilders 定制 INSERT / LIMIT，忽略 FOR UPDATE（DuckDB 没有行锁）。
func (dialect) ClauseBuilders() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{
		"INSERT": func(c clause.Clause, builder clause.Builder) {
			if insert, ok := c.Expression.(clause.Insert); ok {
				if stmt, ok := builder.(*gorm.Statement); ok {
					stmt.WriteString("INSERT ")
					if insert.Modifier != "" {
						stmt.WriteString(insert.Modifier)
						stmt.WriteByte(' ')
					}
					stmt.WriteString("INTO ")
					if insert.Table.Name == "" {
						stmt.WriteQuoted(clause.Table{Name: clause.CurrentTable})
					} else {
						stmt.WriteQuoted(insert.Table)
					}
					return
				}
			}
			c.Build(builder)
		},
		"LIMIT": func(c clause.Clause, builder clause.Builder) {
			if limit, ok := c.Expression.(clause.Limit); ok {
				hasLimit := limit.Limit != nil && *limit.Limit >= 0
				if hasLimit {
					builder.WriteString("LIMIT ")
					builder.WriteString(strconv.Itoa(*limit.Limit))
				}
				if limit.Offset > 0 {
					if hasLimit {
						builder.WriteByte(' ')
					}
					builder.WriteString("OFFSET ")
					builder.WriteString(strconv.Itoa(limit.Offset))
				}
			}
		},
		"FOR": func(c clause.Clause, builder clause.Builder) {
			if _, ok := c.Expression.(clause.Locking); ok {
				return
			}
			c.Build(builder)
		},
	}
}

func (dialect) DefaultValueOf(*schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

// Migrator 返回 DuckDB 专属 Migrator。
func (d dialect) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:                          db,
		Dialector:                   d,
		CreateIndexAfterCreateTable: true,
	}}}
}

// DataTypeOf 把 GORM 字段类型映射为 DuckDB 类型。自增不用 IDENTITY（DuckDB 未实现）。
func (dialect) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "BOOLEAN"
	case schema.Int, schema.Uint:
		size := field.Size
		if field.DataType == schema.Uint {
			if size == 0 || size >= 64 {
				return "UBIGINT"
			}
			size++
		}
		switch {
		case size > 0 && size <= 16:
			return "SMALLINT"
		case size > 0 && size <= 32:
			return "INTEGER"
		}
		return "BIGINT"
	case schema.Float:
		if field.Precision > 0 {
			if field.Scale > 0 {
				return fmt.Sprintf("DECIMAL(%d,%d)", field.Precision, field.Scale)
			}
			return fmt.Sprintf("DECIMAL(%d)", field.Precision)
		}
		return "DOUBLE"
	case schema.String:
		if field.Size > 0 {
			return fmt.Sprintf("VARCHAR(%d)", field.Size)
		}
		return "VARCHAR"
	case schema.Time:
		if val, ok := field.TagSettings["TYPE"]; ok {
			return val
		}
		return "TIMESTAMP"
	case schema.Bytes:
		return "BLOB"
	default:
		return string(field.DataType)
	}
}

func (dialect) BindVarTo(writer clause.Writer, stmt *gorm.Statement, value any) {
	v := reflect.ValueOf(value)
	for v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	}
	if v.CanUint() && v.Uint() > math.MaxInt64 {
		// database/sql 拒绝高位为 1 的 uint64；用绑定的十进制字符串无损转换。
		stmt.Vars[len(stmt.Vars)-1] = strconv.FormatUint(v.Uint(), 10)
		writer.WriteString("CAST(? AS UBIGINT)")
		return
	}
	writer.WriteByte('?')
}

// QuoteTo 用双引号引用标识符，逻辑同 SQLite / Postgres 方言。
func (dialect) QuoteTo(writer clause.Writer, str string) {
	var (
		underQuoted, selfQuoted bool
		continuousQuote         int8
		shiftDelimiter          int8
	)
	for _, v := range []byte(str) {
		switch v {
		case '"':
			continuousQuote++
			if continuousQuote == 2 {
				writer.WriteString(`""`)
				continuousQuote = 0
			}
		case '.':
			if continuousQuote > 0 || !selfQuoted {
				shiftDelimiter = 0
				underQuoted = false
				continuousQuote = 0
				writer.WriteByte('"')
			}
			writer.WriteByte(v)
			continue
		default:
			if shiftDelimiter-continuousQuote <= 0 && !underQuoted {
				writer.WriteByte('"')
				underQuoted = true
				if selfQuoted = continuousQuote > 0; selfQuoted {
					continuousQuote -= 1
				}
			}
			for ; continuousQuote > 0; continuousQuote -= 1 {
				writer.WriteString(`""`)
			}
			writer.WriteByte(v)
		}
		shiftDelimiter++
	}
	if continuousQuote > 0 && !selfQuoted {
		writer.WriteString(`""`)
	}
	writer.WriteByte('"')
}

// Explain 字符串值用单引号：DuckDB 中双引号是标识符。迁移生成的字符串 DEFAULT 也走这里。
func (dialect) Explain(sql string, vars ...any) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

// Translate 只翻译驱动的重复键和外键约束错误，其他错误保持原样。
// 本库默认不开启 gorm.Config.TranslateError，需要时设置 gdb.Config.TranslateError = true，
// 或手动调用 gdb.Dialector.(gorm.ErrorTranslator).Translate(err)。开启后原始错误信息会被替换。
func (dialect) Translate(err error) error {
	var driverErr *goduckdb.Error
	if !errors.As(err, &driverErr) || driverErr == nil || driverErr.Type != goduckdb.ErrorTypeConstraint {
		return err
	}
	msg := strings.TrimPrefix(strings.ToLower(driverErr.Msg), "constraint error: ")
	switch {
	case strings.HasPrefix(msg, "duplicate key ") &&
		(strings.Contains(msg, "violates unique constraint") || strings.Contains(msg, "violates primary key constraint")):
		return gorm.ErrDuplicatedKey
	case strings.HasPrefix(msg, "violates foreign key constraint"):
		return gorm.ErrForeignKeyViolated
	default:
		return err
	}
}
