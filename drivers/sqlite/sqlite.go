package sqlite

import (
	"database/sql"
	"dbhelper/parser"
	"dbhelper/types"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteDriver 实现 dbhelper.Driver

func GetDriver() *SQLiteDriver {
	return &SQLiteDriver{
		parser: &parser.SQLParser{
			DriverName: DriverName,
			DriverID:   DriverID,
			QuoteFunc:  func(identifier string) string { return "`" + identifier + "`" },
		},
	}
}

type SQLiteDriver struct {
	parser types.DSLParser
}

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
	return &SQLiteConn{conn: conn, driver: d}, nil
}

func (d *SQLiteDriver) Quote(identifier string) string {
	return "`" + identifier + "`"
}

func (d *SQLiteDriver) Placeholder(n int) string {
	return "?"
}

func (d *SQLiteDriver) Parser() types.DSLParser {
	return d.parser
}

// SQLiteConn 实现 dbhelper.Conn
type SQLiteConn struct {
	conn   *sql.DB
	driver *SQLiteDriver
}

func (db *SQLiteConn) Begin() (types.Tx, error) {
	tx, err := db.conn.Begin()
	if err != nil {
		return nil, err
	}
	return &SQLiteTx{tx: tx, driver: db.driver}, nil
}

func (db *SQLiteConn) Insert(table string, data *types.ConditionExpr) (int64, error) {
	sqlTmpl, args, err := db.driver.Parser().ParseAndCache(types.OpInsert, data, nil)
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

func (db *SQLiteConn) Query(table string, cond *types.ConditionExpr) (*types.Rows, error) {
	sqlTmpl, args, err := db.driver.Parser().ParseAndCache(types.OpQuery, cond, nil)
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

func (db *SQLiteConn) Update(table string, where, set *types.ConditionExpr) (int64, error) {
	sqlTmpl, args, err := db.driver.Parser().ParseAndCache(types.OpUpdate, where, set)
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

func (db *SQLiteConn) Delete(table string, cond *types.ConditionExpr) (int64, error) {
	sqlTmpl, args, err := db.driver.Parser().ParseAndCache(types.OpDelete, cond, nil)
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

func (db *SQLiteConn) Exec(cond *types.ConditionExpr) (int64, error) {
	sqlStr, args, err := db.driver.Parser().ParseAndCache(types.OpExec, cond, nil)
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
	sqlTmpl, args, err := tx.driver.Parser().ParseAndCache(types.OpQuery, cond, nil)
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
	sqlTmpl, args, err := tx.driver.Parser().ParseAndCache(types.OpInsert, data, nil)
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
	sqlTmpl, args, err := tx.driver.Parser().ParseAndCache(types.OpUpdate, where, set)
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
	sqlTmpl, args, err := tx.driver.Parser().ParseAndCache(types.OpDelete, cond, nil)
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
	sqlStr, args, err := tx.driver.Parser().ParseAndCache(types.OpExec, cond, nil)
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
