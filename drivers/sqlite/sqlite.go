package sqlite

import (
	"database/sql"
	"dbhelper/dbtools"
	"dbhelper/types"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteDriver 实现 dbhelper.Driver
type SQLiteDriver struct{}

const DriverName = "sqlite3"
const DriverID uint8 = 0

func (d *SQLiteDriver) Open(cfg types.DBConfig) (types.Conn, error) {
	conn, err := sql.Open(DriverName, cfg.DSN)
	if err != nil {
		return nil, err
	}
	if cfg.MaxOpen > 0 {
		conn.SetMaxOpenConns(cfg.MaxOpen)
	}
	if cfg.MaxIdle > 0 {
		conn.SetMaxIdleConns(cfg.MaxIdle)
	}
	return &SQLiteDB{conn: conn, driver: d}, nil
}

func (d *SQLiteDriver) Quote(identifier string) string {
	return fmt.Sprintf("`%s`", identifier)
}

func (d *SQLiteDriver) Placeholder(n int) string {
	return "?"
}
func (d *SQLiteDriver) ParseNewCond(op types.OpType, where *types.ConditionExpr, set *types.ConditionExpr) (string, error) {
	var sb strings.Builder

	switch op {
	case types.OpInsert:
		if where == nil || where.Op != types.OpAnd || len(where.Exprs) == 0 {
			return "", fmt.Errorf("Insert data must be AND expr with fields")
		}
		sb.WriteString("INSERT INTO %s (")
		for i, expr := range where.Exprs {
			if expr.Op != types.OpEq {
				return "", fmt.Errorf("Insert only supports EQ expr")
			}
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(d.Quote(expr.Field))
		}
		sb.WriteString(") VALUES (")
		for i := range where.Exprs {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('?')
		}
		sb.WriteByte(')')

	case types.OpQuery:
		sb.WriteString("SELECT * FROM %s")
		if where != nil {
			whereStr, _ := d.parseWhere(where)
			if whereStr != "" {
				sb.WriteString(" WHERE ")
				sb.WriteString(whereStr)
			}
		}

	case types.OpUpdate:
		if set == nil {
			return "", fmt.Errorf("Update data cannot be empty")
		}
		sb.WriteString("UPDATE %s SET ")
		first := true
		if set.Op == types.OpAnd && len(set.Exprs) > 0 {
			for _, expr := range set.Exprs {
				if !first {
					sb.WriteByte(',')
				}
				sb.WriteString(d.Quote(expr.Field))
				sb.WriteString("=?")
				first = false
			}
		} else if set.Op == types.OpEq && set.Field != "" {
			sb.WriteString(d.Quote(set.Field))
			sb.WriteString("=?")
		} else {
			return "", fmt.Errorf("Invalid update data")
		}
		if where != nil {
			whereStr, _ := d.parseWhere(where)
			if whereStr != "" {
				sb.WriteString(" WHERE ")
				sb.WriteString(whereStr)
			}
		}

	case types.OpDelete:
		sb.WriteString("DELETE FROM %s")
		if where != nil {
			whereStr, _ := d.parseWhere(where)
			if whereStr != "" {
				sb.WriteString(" WHERE ")
				sb.WriteString(whereStr)
			}
		}

	case types.OpExec:
		if where == nil || where.Op != types.OpRaw {
			return "", fmt.Errorf("Exec only supports OpRaw ConditionExpr")
		}
		execStr, ok := where.Value.(string)
		if !ok {
			return "", fmt.Errorf("Exec OpRaw ConditionExpr.Value must be string")
		}
		sb.WriteString(execStr)

	default:
		return "", fmt.Errorf("unsupported op: %s", op)
	}

	return sb.String(), nil
}

func (d *SQLiteDriver) ParseAndCacheCond(op types.OpType, where *types.ConditionExpr, set *types.ConditionExpr) (string, error) {
	cacheKeyExpr := where
	if sqlStr, ok := dbtools.GetCondCache(DriverID, op, cacheKeyExpr); ok {
		return sqlStr, nil
	}
	cond, err := d.ParseNewCond(op, where, set)
	if err != nil {
		return "", err
	}
	dbtools.SetCondCache(DriverID, op, cacheKeyExpr, cond)
	return cond, nil
}

// parseWhere 仅生成 WHERE 子句和参数（内部辅助）
func (d *SQLiteDriver) parseWhere(cond *types.ConditionExpr) (string, []interface{}) {
	if cond == nil {
		return "", nil
	}
	switch cond.Op {
	case types.OpAnd, types.OpOr:
		parts := []string{}
		args := []interface{}{}
		for _, expr := range cond.Exprs {
			part, a := d.parseWhere(expr)
			if part != "" {
				parts = append(parts, "("+part+")")
				args = append(args, a...)
			}
		}
		if len(parts) == 0 {
			return "", nil
		}
		sep := " AND "
		if cond.Op == types.OpOr {
			sep = " OR "
		}
		return strings.Join(parts, sep), args
	case types.OpEq, types.OpNe, types.OpGt, types.OpGte, types.OpLt, types.OpLte, types.OpLike:
		opStr := map[types.ConditionOp]string{
			types.OpEq:   "=",
			types.OpNe:   "<>",
			types.OpGt:   ">",
			types.OpGte:  ">=",
			types.OpLt:   "<",
			types.OpLte:  "<=",
			types.OpLike: "LIKE",
		}[cond.Op]
		return fmt.Sprintf("%s %s ?", d.Quote(cond.Field), opStr), []interface{}{cond.Value}
	case types.OpIn:
		if len(cond.Values) == 0 {
			return "1=0", nil
		}
		phs := make([]string, len(cond.Values))
		for i := range cond.Values {
			phs[i] = "?"
		}
		return fmt.Sprintf("%s IN (%s)", d.Quote(cond.Field), strings.Join(phs, ",")), cond.Values
	case types.OpRaw:
		s, _ := cond.Value.(string)
		return s, cond.Values
	default:
		return "", nil
	}
}

// SQLiteDB 实现 dbhelper.Conn
type SQLiteDB struct {
	conn   *sql.DB
	driver *SQLiteDriver
}

func (db *SQLiteDB) Begin() (types.Tx, error) {
	tx, err := db.conn.Begin()
	if err != nil {
		return nil, err
	}
	return &SQLiteTx{tx: tx, driver: db.driver}, nil
}

func (db *SQLiteDB) Insert(table string, data *types.ConditionExpr) (int64, error) {
	sqlTmpl, err := db.driver.ParseAndCacheCond(types.OpInsert, data, nil)
	if err != nil {
		return 0, err
	}
	// 生成参数
	vals := make([]interface{}, 0, len(data.Exprs))
	for _, expr := range data.Exprs {
		vals = append(vals, expr.Value)
	}
	query := fmt.Sprintf(sqlTmpl, db.driver.Quote(table))
	res, err := db.conn.Exec(query, vals...)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (db *SQLiteDB) Query(table string, cond *types.ConditionExpr) (*types.Rows, error) {
	sqlTmpl, err := db.driver.ParseAndCacheCond(types.OpQuery, cond, nil)
	if err != nil {
		return nil, err
	}
	_, args := db.driver.parseWhere(cond)
	query := fmt.Sprintf(sqlTmpl, db.driver.Quote(table))

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	result := []map[string]interface{}{}
	for rows.Next() {
		row := make([]interface{}, len(columns))
		rowPtrs := make([]interface{}, len(columns))
		for i := range row {
			rowPtrs[i] = &row[i]
		}
		if err := rows.Scan(rowPtrs...); err != nil {
			return nil, err
		}
		m := map[string]interface{}{}
		for i, col := range columns {
			m[col] = row[i]
		}
		result = append(result, m)
	}
	return types.NewRows(result), nil
}

func (db *SQLiteDB) Update(table string, where, set *types.ConditionExpr) (int64, error) {
	// 生成 SQL 模板
	sqlTmpl, err := db.driver.ParseAndCacheCond(types.OpUpdate, where, set)
	if err != nil {
		return 0, err
	}

	// 收集参数：先 SET，再 WHERE
	args := []interface{}{}
	if set != nil {
		if set.Op == types.OpAnd {
			for _, expr := range set.Exprs {
				args = append(args, expr.Value)
			}
		} else if set.Op == types.OpEq {
			args = append(args, set.Value)
		}
	}

	_, whereArgs := db.driver.parseWhere(where)
	args = append(args, whereArgs...)

	// 格式化 SQL
	query := fmt.Sprintf(sqlTmpl, db.driver.Quote(table))
	res, err := db.conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (db *SQLiteDB) Delete(table string, cond *types.ConditionExpr) (int64, error) {
	sqlTmpl, err := db.driver.ParseAndCacheCond(types.OpDelete, cond, nil)
	if err != nil {
		return 0, err
	}
	_, args := db.driver.parseWhere(cond)
	query := fmt.Sprintf(sqlTmpl, db.driver.Quote(table))
	res, err := db.conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (db *SQLiteDB) Exec(cond *types.ConditionExpr) (int64, error) {
	sqlStr, err := db.driver.ParseAndCacheCond(types.OpExec, cond, nil)
	if err != nil {
		return 0, err
	}
	args := cond.Values
	res, err := db.conn.Exec(sqlStr, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// SQLiteTx 实现 dbhelper.Tx
type SQLiteTx struct {
	tx     *sql.Tx
	driver *SQLiteDriver
}

func (tx *SQLiteTx) Query(table string, cond *types.ConditionExpr) (*types.Rows, error) {
	sqlTmpl, err := tx.driver.ParseAndCacheCond(types.OpQuery, cond, nil)
	if err != nil {
		return nil, err
	}
	_, args := tx.driver.parseWhere(cond)
	query := fmt.Sprintf(sqlTmpl, tx.driver.Quote(table))
	rows, err := tx.tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	result := []map[string]interface{}{}
	for rows.Next() {
		row := make([]interface{}, len(columns))
		rowPtrs := make([]interface{}, len(columns))
		for i := range row {
			rowPtrs[i] = &row[i]
		}
		if err := rows.Scan(rowPtrs...); err != nil {
			return nil, err
		}
		m := map[string]interface{}{}
		for i, col := range columns {
			m[col] = row[i]
		}
		result = append(result, m)
	}
	return types.NewRows(result), nil
}

func (tx *SQLiteTx) Insert(table string, data *types.ConditionExpr) (int64, error) {
	sqlTmpl, err := tx.driver.ParseAndCacheCond(types.OpInsert, data, nil)
	if err != nil {
		return 0, err
	}
	// 生成参数
	vals := make([]interface{}, 0, len(data.Exprs))
	for _, expr := range data.Exprs {
		vals = append(vals, expr.Value)
	}
	query := fmt.Sprintf(sqlTmpl, tx.driver.Quote(table))
	res, err := tx.tx.Exec(query, vals...)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}
func (tx *SQLiteTx) Update(table string, where, set *types.ConditionExpr) (int64, error) {
	// 生成 SQL 模板
	sqlTmpl, err := tx.driver.ParseAndCacheCond(types.OpUpdate, where, set)
	if err != nil {
		return 0, err
	}

	// 收集参数：先 SET，再 WHERE
	args := []interface{}{}
	if set != nil {
		if set.Op == types.OpAnd {
			for _, expr := range set.Exprs {
				args = append(args, expr.Value)
			}
		} else if set.Op == types.OpEq {
			args = append(args, set.Value)
		}
	}

	_, whereArgs := tx.driver.parseWhere(where)
	args = append(args, whereArgs...)

	// 格式化 SQL
	query := fmt.Sprintf(sqlTmpl, tx.driver.Quote(table))
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
func (tx *SQLiteTx) Delete(table string, cond *types.ConditionExpr) (int64, error) {
	sqlTmpl, err := tx.driver.ParseAndCacheCond(types.OpDelete, cond, nil)
	if err != nil {
		return 0, err
	}
	_, args := tx.driver.parseWhere(cond)
	query := fmt.Sprintf(sqlTmpl, tx.driver.Quote(table))
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
func (tx *SQLiteTx) Exec(cond *types.ConditionExpr) (int64, error) {
	sqlStr, err := tx.driver.ParseAndCacheCond(types.OpExec, cond, nil)
	if err != nil {
		return 0, err
	}
	args := cond.Values
	res, err := tx.tx.Exec(sqlStr, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (tx *SQLiteTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *SQLiteTx) Rollback() error {
	return tx.tx.Rollback()
}
