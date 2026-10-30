package db

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
)

// queryRows 使用已注册实例查询原生行，未注册时返回错误。
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

// scanRow 扫描当前行，将 []byte 转为独立的 string。
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

// Stream 逐行查询，index 从 1 开始，回调错误立即返回。
// 返回读取行数；列值为驱动原生类型，NULL 为 nil，[]byte 转为 string。
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

// StreamBatch 分批查询，batch 必须大于 0，最后不足一批也会回调。
// 列名须唯一，列值规则同 Stream；返回读取行数和错误。
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

		if len(batchs) == batch {
			// 回调可能保留批次，不能直接复用同一切片。
			if err = fun(slices.Clone(batchs)); err != nil {
				return cnt, err
			}
			batchs = batchs[:0]
		}
	}

	if err := rows.Err(); err != nil {
		return cnt, fmt.Errorf("rows err: %w", err)
	}

	if len(batchs) > 0 {
		if err := fun(slices.Clone(batchs)); err != nil {
			return cnt, err
		}
	}
	return cnt, nil
}

// Query 返回全部查询结果，适用于较小的结果集。
func Query(ctx context.Context, name string, query string, args ...any) ([]map[string]any, error) {
	var results []map[string]any
	_, err := StreamBatch(ctx, name, query, args, 100, func(rows []map[string]any) error {
		results = append(results, rows...)
		return nil
	})
	return results, err
}
