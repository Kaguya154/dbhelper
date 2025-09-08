package mysql

import (
	_ "github.com/go-sql-driver/mysql"
)

// MySQLDriver 实现 dbhelper.Driver
type MySQLDriver struct{}
