package postgresql

import (
	"database/sql"
	"fmt"

	"github.com/Kaguya154/dbhelper/parser"
	"github.com/Kaguya154/dbhelper/types"

	_ "github.com/lib/pq"
)

const DriverName = "postgres"
const DriverID uint8 = 2

func GetDriver() *PostgreSQLDriver {
	return &PostgreSQLDriver{
		parser: &parser.SQLParser{
			DriverName: DriverName,
			DriverID:   DriverID,
			QuoteFunc:  func(identifier string) string { return "\"" + identifier + "\"" },
		},
	}
}

// PostgreSQLDriver 实现 dbhelper.Driver

type PostgreSQLDriver struct {
	parser types.DSLParser
}

func (d *PostgreSQLDriver) Open(cfg types.DBConfig) (types.Conn, error) {
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
	return &PostgreSQLConn{conn: conn, driver: d}, nil
}

func (d *PostgreSQLDriver) Quote(identifier string) string {
	return "\"" + identifier + "\""
}

func (d *PostgreSQLDriver) Placeholder(n int) string {
	return fmt.Sprintf("$%d", n)
}

func (d *PostgreSQLDriver) Parser() types.DSLParser {
	return d.parser
}

// PostgreSQLConn 实现 dbhelper.Conn

type PostgreSQLConn struct {
	conn   *sql.DB
	driver *PostgreSQLDriver
}

func (db *PostgreSQLConn) Begin() (types.Tx, error) {
	tx, err := db.conn.Begin()
	if err != nil {
		return nil, err
	}
	return &PostgreSQLTx{tx: tx, driver: db.driver}, nil
}

func (db *PostgreSQLConn) Insert(table string, data *types.ConditionExpr) (int64, error) {
	sqlTmpl, args, err := db.driver.Parser().ParseAndCache(types.OpInsert, data, nil)
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

func (db *PostgreSQLConn) Query(table string, cond *types.ConditionExpr) (*types.Rows, error) {
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

func (db *PostgreSQLConn) Update(table string, where, set *types.ConditionExpr) (int64, error) {
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

func (db *PostgreSQLConn) Delete(table string, cond *types.ConditionExpr) (int64, error) {
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

func (db *PostgreSQLConn) Exec(cond *types.ConditionExpr) (int64, error) {
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

// PostgreSQLTx 实现 dbhelper.Tx

type PostgreSQLTx struct {
	tx     *sql.Tx
	driver *PostgreSQLDriver
}

func (tx *PostgreSQLTx) Query(table string, cond *types.ConditionExpr) (*types.Rows, error) {
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

func (tx *PostgreSQLTx) Insert(table string, data *types.ConditionExpr) (int64, error) {
	sqlTmpl, args, err := tx.driver.Parser().ParseAndCache(types.OpInsert, data, nil)
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

func (tx *PostgreSQLTx) Update(table string, where, set *types.ConditionExpr) (int64, error) {
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

func (tx *PostgreSQLTx) Delete(table string, cond *types.ConditionExpr) (int64, error) {
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

func (tx *PostgreSQLTx) Exec(cond *types.ConditionExpr) (int64, error) {
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

func (tx *PostgreSQLTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *PostgreSQLTx) Rollback() error {
	return tx.tx.Rollback()
}
