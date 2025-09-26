package mysql

import (
	"database/sql"
	"fmt"

	"github.com/Kaguya154/dbhelper/parser"
	"github.com/Kaguya154/dbhelper/types"

	_ "github.com/go-sql-driver/mysql"
)

const DriverName = "mysql"
const DriverID uint8 = 1

func GetDriver() *MySQLDriver {
	return &MySQLDriver{
		parser: &parser.SQLParser{
			DriverName: DriverName,
			DriverID:   DriverID,
			QuoteFunc:  func(identifier string) string { return "`" + identifier + "`" },
		},
	}
}

// MySQLDriver 实现 dbhelper.Driver
type MySQLDriver struct {
	parser types.DSLParser
}

func (d *MySQLDriver) Open(cfg types.DBConfig) (types.Conn, error) {
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
	return &MySQLConn{conn: conn, driver: d}, nil
}

func (d *MySQLDriver) Quote(identifier string) string {
	return "`" + identifier + "`"
}

func (d *MySQLDriver) Placeholder(n int) string {
	return "?"
}

func (d *MySQLDriver) Parser() types.DSLParser {
	return d.parser
}

// MySQLConn 实现 dbhelper.Conn
type MySQLConn struct {
	conn   *sql.DB
	driver *MySQLDriver
}

func (db *MySQLConn) Begin() (types.Tx, error) {
	tx, err := db.conn.Begin()
	if err != nil {
		return nil, err
	}
	return &MySQLTx{tx: tx, driver: db.driver}, nil
}

func (db *MySQLConn) Insert(table string, data *types.ConditionExpr) (int64, error) {
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

func (db *MySQLConn) Query(table string, cond *types.ConditionExpr) (*types.Rows, error) {
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

func (db *MySQLConn) Update(table string, where, set *types.ConditionExpr) (int64, error) {
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

func (db *MySQLConn) Delete(table string, cond *types.ConditionExpr) (int64, error) {
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

func (db *MySQLConn) Exec(cond *types.ConditionExpr) (int64, error) {
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

// MySQLTx 实现 dbhelper.Tx
type MySQLTx struct {
	tx     *sql.Tx
	driver *MySQLDriver
}

func (tx *MySQLTx) Query(table string, cond *types.ConditionExpr) (*types.Rows, error) {
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

func (tx *MySQLTx) Insert(table string, data *types.ConditionExpr) (int64, error) {
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

func (tx *MySQLTx) Update(table string, where, set *types.ConditionExpr) (int64, error) {
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

func (tx *MySQLTx) Delete(table string, cond *types.ConditionExpr) (int64, error) {
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

func (tx *MySQLTx) Exec(cond *types.ConditionExpr) (int64, error) {
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

func (tx *MySQLTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *MySQLTx) Rollback() error {
	return tx.tx.Rollback()
}
