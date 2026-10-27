// stream.go 提供基于原生 database/sql 的流式查询能力，绕过 GORM ORM 层，
// 适用于大结果集的逐行或分批处理场景，避免一次性加载所有数据到内存。
//
// 主要函数:
//   - Stream:      逐行流式处理，每行回调 handle(index, row)
//   - StreamBatch: 分批流式处理，每 batch 行回调 fun(rows)
//   - Query:       基于 StreamBatch 的便捷查询，返回所有结果
//
// 列值类型由驱动决定（如 int64、float64、time.Time），NULL 为 nil，[]byte 转为 string。
package db

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
)

// queryRows 按实例名获取原生 *sql.DB 并执行查询。实例未注册时返回 error，不 panic。
// 还没连上时停在 GetDB，直到连上；实例已 Close 时 GetDB 不再返回。
func queryRows(ctx context.Context, name, query string, args []any) (*sql.Rows, error) {
	in, ok := dbm[name]
	if !ok {
		return nil, fmt.Errorf("db %q not found", name)
	}
	g := in.GetDB(ctx)
	sqlDB, err := g.DB()
	if err != nil {
		return nil, fmt.Errorf("get db: %w", err)
	}
	rows, err := sqlDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query rows: %w", err)
	}
	return rows, nil
}

// scanRow 扫描当前行到 []any，[]byte 转为 string（同时完成拷贝，避免驱动缓冲区复用）。
func scanRow(rows *sql.Rows, n int) ([]any, error) {
	values, ptrs := make([]any, n), make([]any, n)
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	for i, v := range values {
		if b, ok := v.([]byte); ok {
			values[i] = string(b)
		}
	}
	return values, nil
}

// Stream 逐行流式查询，对每一行调用 handle 回调。
// 使用原生 database/sql 接口绕过 GORM。
//
// 参数:
//   - ctx:    上下文，支持超时与取消
//   - name:   数据库实例名（已通过 CreateDB 等注册）
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
	rows, err := queryRows(ctx, name, query, args)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("columns: %w", err)
	}

	var cnt int64
	for rows.Next() {
		row, err := scanRow(rows, len(columns))
		if err != nil {
			return cnt, fmt.Errorf("rows scan: %w", err)
		}
		cnt++
		if err = handle(cnt, row); err != nil {
			return cnt, err
		}
	}
	return cnt, rows.Err()
}

// StreamBatch 分批流式查询，每积累 batch 行后调用一次 fun 回调。
// 结果组织为 []map[string]any（列名→值），适合批量插入、批量发送等场景。
// 列名重复时直接返回错误，需使用唯一列别名。
// 最后不足 batch 的剩余行也会触发一次回调。
//
// 参数:
//   - ctx:   上下文，支持超时与取消
//   - name:  数据库实例名
//   - query: SQL 查询语句
//   - args:  参数化查询的参数
//   - batch: 每批处理的行数，必须大于 0
//   - fun:   批处理回调函数，接收当前批次的所有行数据
//
// 返回处理的总行数和错误。
//
//	cnt, err := db.StreamBatch(ctx, "default", "SELECT * FROM logs", nil, 500,
//	    func(rows []map[string]any) error {
//	        return bulkInsert(rows)
//	    })
func StreamBatch(ctx context.Context, name, query string, args []any, batch int, fun func(rows []map[string]any) error) (int64, error) {
	if batch <= 0 {
		return 0, fmt.Errorf("batch must be > 0, got %d", batch)
	}
	rows, err := queryRows(ctx, name, query, args)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("columns: %w", err)
	}
	seen := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		if _, exists := seen[col]; exists {
			return 0, fmt.Errorf("duplicate column name %q: use unique column aliases", col)
		}
		seen[col] = struct{}{}
	}

	var cnt int64
	batchs := make([]map[string]any, 0, batch)
	for rows.Next() {
		values, err := scanRow(rows, len(columns))
		if err != nil {
			return cnt, fmt.Errorf("rows scan: %w", err)
		}
		cnt++
		row := make(map[string]any, len(columns))
		for i, col := range columns {
			row[col] = values[i]
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
		return cnt, fmt.Errorf("rows err: %w", err)
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
