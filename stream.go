// stream.go 提供基于原生 database/sql 的流式查询能力，绕过 GORM ORM 层，
// 适用于大结果集的逐行或分批处理场景，避免一次性加载所有数据到内存。
//
// 主要函数:
//   - Stream:      逐行流式处理，每行回调 handle(index, row)
//   - StreamBatch: 分批流式处理，每 batch 行回调 fun(rows)
//   - Query:       基于 StreamBatch 的便捷查询，返回所有结果
package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// Stream 逐行流式查询，对每一行调用 handle 回调。
// 使用原生 database/sql 接口绕过 GORM，每行数据根据列的 Go 类型自动格式转换。
//
// 参数:
//   - ctx:    上下文，支持超时与取消
//   - name:   数据库实例名（已通过 NewMySQL/NewSQLite 等注册）
//   - query:  SQL 查询语句
//   - args:   参数化查询的参数（防止 SQL 注入）
//   - handle: 回调函数，index 为当前行号（从 1 开始），row 为该行各列的值
//
// 返回处理的总行数和错误。
//
//	cnt, err := db.Stream(ctx, "default", "SELECT id, name FROM users WHERE age > ?", []any{18},
//	    func(index int64, row []any) error {
//	        fmt.Printf("Row %d: %v\n", index, row)
//	        return nil
//	    })
func Stream(ctx context.Context, name string, query string, args []any, handle func(index int64, row []any) error) (int64, error) {
	tx, err := GetDB(ctx, name).DB()
	if err != nil {
		return 0, fmt.Errorf("get db: %w", err)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("query rows: %w", err)
	}
	defer rows.Close()

	types, err := rows.ColumnTypes()
	if err != nil {
		return 0, fmt.Errorf("column types: %w", err)
	}

	// v 存原始字节，scans 存指针供 Scan 使用
	cnt, v, scans := int64(0), make([]sql.RawBytes, len(types)), make([]any, len(types))
	for i := range v {
		scans[i] = &v[i]
	}

	for rows.Next() {
		if err = rows.Scan(scans...); err != nil {
			return 0, fmt.Errorf("rows scan: %w", err)
		}
		cnt += 1
		row := make([]any, len(v))

		for i := range v {
			if row[i], err = format(*(*string)(unsafe.Pointer(&v[i])), types[i].ScanType().Name()); err != nil {
				fmt.Println(pErr(types[i].Name(), types[i].ScanType().Name(), v[i], v, err))
			}
		}

		if err = handle(cnt, row); err != nil {
			return cnt, err
		}
	}
	return cnt, rows.Err()
}

// pErr 构造列解析错误信息，包含列名、类型、原始值等调试信息。
func pErr(name, kind string, v sql.RawBytes, row any, err error) error {
	return fmt.Errorf("name=%s, kind=%s, v=%s, row=%#v, err=%w", name, kind, v, row, err)
}

// format 根据 Go 类型名称将原始字符串值转换为对应的 Go 原生类型。
// 这是流式查询的核心类型转换函数，负责将 sql.RawBytes 的字符串表示
// 转为 string、int、float64、time.Time 或 bool。
//
// 类型映射:
//   - "RawBytes", "string", "NullString" → string（克隆，避免底层缓冲区复用）
//   - "NullInt64", "uint8"..."int64"     → int64（空串返回 0）
//   - "NullTime"                         → *time.Time（零值/空串返回零时间）
//   - "float64"                          → float64
//   - "bool"                             → bool
func format(s, format string) (any, error) {
	switch format {
	case "RawBytes", "string", "NullString":
		// 克隆字符串，因为 sql.RawBytes 底层缓冲区会被后续 Scan 复用
		return strings.Clone(s), nil
	case "NullInt64", "uint8", "uint32", "uint64", "int", "int8", "int32", "int64":
		if s == "" {
			return 0, nil
		}
		return strconv.ParseInt(s, 10, 64)
	case "NullTime":
		if s == "0001-01-01T00:00:00Z" || s == "" {
			return &time.Time{}, nil
		}
		// 尝试多种时间格式：RFC3339（ISO 8601）、MySQL DateTime
		for _, layout := range []string{time.RFC3339, time.DateTime, "2006-01-02T15:04:05"} {
			if t, err := time.Parse(layout, s); err == nil {
				return &t, nil
			}
		}
		// 最后用 Local 时区尝试 DateTime 格式
		t, err := time.ParseInLocation(time.DateTime, s, time.Local)
		if err != nil {
			return nil, err
		}
		return &t, nil
	case "float64":
		return strconv.ParseFloat(s, 64)
	case "bool":
		return strconv.ParseBool(s)
	default:
		return nil, fmt.Errorf("format unknown, format=%s", format)
	}
}

// StreamBatch 分批流式查询，每积累 batch 行后调用一次 fun 回调。
// 结果组织为 []map[string]any（列名→值），适合批量插入、批量发送等场景。
// 最后不足 batch 的剩余行也会触发一次回调。
//
// 参数:
//   - ctx:   上下文，支持超时与取消
//   - name:  数据库实例名
//   - query: SQL 查询语句
//   - args:  参数化查询的参数
//   - batch: 每批处理的行数
//   - fun:   批处理回调函数，接收当前批次的所有行数据
//
// 返回处理的总行数和错误。
//
//	cnt, err := db.StreamBatch(ctx, "default", "SELECT * FROM logs", nil, 500,
//	    func(rows []map[string]any) error {
//	        return bulkInsert(rows)
//	    })
func StreamBatch(ctx context.Context, name, query string, args []any, batch int, fun func(rows []map[string]any) error) (int64, error) {
	tx, err := GetDB(ctx, name).DB()
	if err != nil {
		return 0, fmt.Errorf("get db: %w", err)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("query rows: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("columns: %w", err)
	}

	types, err := rows.ColumnTypes()
	if err != nil {
		return 0, fmt.Errorf("column types: %w", err)
	}

	cnt, v, scans := int64(0), make([]sql.RawBytes, len(types)), make([]any, len(types))
	for i := range v {
		scans[i] = &v[i]
	}

	batchs := make([]map[string]any, 0, batch)
	for rows.Next() {
		if err = rows.Scan(scans...); err != nil {
			return 0, fmt.Errorf("rows scan: %w", err)
		}
		cnt += 1
		row := make(map[string]any, len(v))

		for i := range v {
			if row[columns[i]], err = format(*(*string)(unsafe.Pointer(&v[i])), types[i].ScanType().Name()); err != nil {
				log.Printf("%s", pErr(types[i].Name(), types[i].ScanType().Name(), v[i], v, err))
			}
		}
		batchs = append(batchs, row)

		// 达到批次大小时触发回调，然后清空缓冲区复用
		if len(batchs) == batch {
			if err = fun(slices.Clone(batchs)); err != nil {
				return cnt, err
			}
			batchs = batchs[:0]
		}
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rows err: %w", err)
	}

	// 处理最后不足一批的剩余行
	if len(batchs) > 0 {
		if err := fun(slices.Clone(batchs)); err != nil {
			return cnt, err
		}
	}
	return cnt, nil
}

// Query 便捷查询函数，基于 StreamBatch 实现，内部以 100 行为一批，
// 将所有结果收集到 []map[string]any 后一次性返回。
// 适合结果集较小、不需要流式处理的普通查询场景。
//
//	results, err := db.Query(ctx, "default", "SELECT id, name FROM users WHERE status = ?", "active")
func Query(ctx context.Context, name string, query string, args ...any) ([]map[string]any, error) {
	var results []map[string]any
	_, err := StreamBatch(ctx, name, query, args, 100, func(rows []map[string]any) error {
		results = append(results, rows...)
		return nil
	})
	return results, err
}
