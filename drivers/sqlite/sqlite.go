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

var opStrMap = map[types.ConditionOp]string{
	types.OpEq:   "=",
	types.OpNe:   "<>",
	types.OpGt:   ">",
	types.OpGte:  ">=",
	types.OpLt:   "<",
	types.OpLte:  "<=",
	types.OpLike: "LIKE",
}

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

func (d *SQLiteDriver) ParseNewCond(op types.OpType, where *types.ConditionExpr, set *types.ConditionExpr) (string, []interface{}, error) {
	var sb strings.Builder
	var args []interface{}
	switch op {
	case types.OpInsert:
		if where == nil || where.Op != types.OpAnd || len(where.Exprs) == 0 {
			return "", nil, fmt.Errorf("Insert data must be AND expr with fields")
		}
		sb.WriteString("INSERT INTO %s (")
		for i, expr := range where.Exprs {
			if expr.Op != types.OpEq {
				return "", nil, fmt.Errorf("Insert only supports EQ expr")
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
		for _, expr := range where.Exprs {
			args = append(args, expr.Value)
		}

	case types.OpQuery:
		sb.WriteString("SELECT * FROM %s")
		if where != nil {
			sb.WriteString(" WHERE ")
			d.buildWhere(&sb, where, &args)
		}

	case types.OpUpdate:
		if set == nil {
			return "", nil, fmt.Errorf("Update data cannot be empty")
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
			return "", nil, fmt.Errorf("Invalid update data")
		}
		if where != nil {
			sb.WriteString(" WHERE ")
			var whereArgs []interface{}
			var nilsb strings.Builder
			d.buildWhere(&sb, where, &whereArgs)
			d.buildWhere(&nilsb, set, &args)
			args = append(args, whereArgs...)
		}

	case types.OpDelete:
		sb.WriteString("DELETE FROM %s")
		if where != nil {
			sb.WriteString(" WHERE ")
			d.buildWhere(&sb, where, &args)
		}

	case types.OpExec:
		if where == nil || where.Op != types.OpRaw {
			return "", nil, fmt.Errorf("Exec only supports OpRaw ConditionExpr")
		}
		execStr, ok := where.Value.(string)
		if !ok {
			return "", nil, fmt.Errorf("Exec OpRaw ConditionExpr.Value must be string")
		}
		sb.WriteString(execStr)

	default:
		return "", nil, fmt.Errorf("unsupported op: %s", op)
	}

	return sb.String(), args, nil
}

func (d *SQLiteDriver) ParseAndCacheCond(op types.OpType, where *types.ConditionExpr, set *types.ConditionExpr) (string, []interface{}, error) {
	if sqlStr, args, ok := dbtools.GetCondCache(DriverID, op, where); ok {
		return sqlStr, args, nil
	}

	sqlStr, args, err := d.ParseNewCond(op, where, set) // 让 ParseNewCond 负责收集 args
	if err != nil {
		return "", nil, err
	}

	dbtools.SetCondCache(DriverID, op, where, sqlStr, args)
	return sqlStr, args, nil
}

// buildWhere 递归构建 WHERE 子句
func (d *SQLiteDriver) buildWhere(sb *strings.Builder, cond *types.ConditionExpr, args *[]interface{}) {
	if cond == nil {
		return
	}
	switch cond.Op {
	case types.OpAnd, types.OpOr:
		sep := " AND "
		if cond.Op == types.OpOr {
			sep = " OR "
		}
		first := true
		for _, expr := range cond.Exprs {
			if expr == nil {
				continue
			}
			if !first {
				sb.WriteString(sep)
			}
			sb.WriteByte('(')
			d.buildWhere(sb, expr, args)
			sb.WriteByte(')')
			first = false
		}
	case types.OpEq, types.OpNe, types.OpGt, types.OpGte, types.OpLt, types.OpLte, types.OpLike:
		if opStr, ok := opStrMap[cond.Op]; ok {
			sb.WriteString(d.Quote(cond.Field))
			sb.WriteByte(' ')
			sb.WriteString(opStr)
			sb.WriteString(" ?")
			*args = append(*args, cond.Value)
		}
	case types.OpIn:
		if len(cond.Values) == 0 {
			sb.WriteString("1=0")
			return
		}
		sb.WriteString(d.Quote(cond.Field))
		sb.WriteString(" IN (")
		for i := range cond.Values {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('?')
		}
		sb.WriteByte(')')
		*args = append(*args, cond.Values...)
	case types.OpRaw:
		if s, ok := cond.Value.(string); ok {
			sb.WriteString(s)
		}
		if cond.Values != nil {
			*args = append(*args, cond.Values...)
		}
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
	sqlTmpl, args, err := db.driver.ParseAndCacheCond(types.OpInsert, data, nil)
	if err != nil {
		return 0, err
	}
	query := fmt.Sprintf(sqlTmpl, db.driver.Quote(table))
	res, err := db.conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (db *SQLiteDB) Query(table string, cond *types.ConditionExpr) (*types.Rows, error) {
	sqlTmpl, args, err := db.driver.ParseAndCacheCond(types.OpQuery, cond, nil)
	if err != nil {
		return nil, err
	}

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
	sqlTmpl, args, err := db.driver.ParseAndCacheCond(types.OpUpdate, where, set)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf(sqlTmpl, db.driver.Quote(table))
	res, err := db.conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (db *SQLiteDB) Delete(table string, cond *types.ConditionExpr) (int64, error) {
	sqlTmpl, args, err := db.driver.ParseAndCacheCond(types.OpDelete, cond, nil)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf(sqlTmpl, db.driver.Quote(table))
	res, err := db.conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (db *SQLiteDB) Exec(cond *types.ConditionExpr) (int64, error) {
	sqlStr, args, err := db.driver.ParseAndCacheCond(types.OpExec, cond, nil)
	if err != nil {
		return 0, err
	}
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
	sqlTmpl, args, err := tx.driver.ParseAndCacheCond(types.OpQuery, cond, nil)
	if err != nil {
		return nil, err
	}

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
	sqlTmpl, args, err := tx.driver.ParseAndCacheCond(types.OpInsert, data, nil)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf(sqlTmpl, tx.driver.Quote(table))
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (tx *SQLiteTx) Update(table string, where, set *types.ConditionExpr) (int64, error) {
	sqlTmpl, args, err := tx.driver.ParseAndCacheCond(types.OpUpdate, where, set)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf(sqlTmpl, tx.driver.Quote(table))
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (tx *SQLiteTx) Delete(table string, cond *types.ConditionExpr) (int64, error) {
	sqlTmpl, args, err := tx.driver.ParseAndCacheCond(types.OpDelete, cond, nil)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf(sqlTmpl, tx.driver.Quote(table))
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
func (tx *SQLiteTx) Exec(cond *types.ConditionExpr) (int64, error) {
	sqlStr, args, err := tx.driver.ParseAndCacheCond(types.OpExec, cond, nil)
	if err != nil {
		return 0, err
	}
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
