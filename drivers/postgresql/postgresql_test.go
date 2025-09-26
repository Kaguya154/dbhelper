package postgresql_test

import (
	"dbhelper"
	"dbhelper/drivers/postgresql"
	"dbhelper/types"
	"testing"
)

func init() {
	// 注册驱动
	_ = dbhelper.RegisterDriver(postgresql.DriverName, postgresql.GetDriver())
}

func TestCondBuilder_PostgreSQL(t *testing.T) {
	driver, err := dbhelper.GetDriver(postgresql.DriverName)
	if err != nil {
		t.Fatalf("获取驱动失败: %v", err)
	}
	// 测试Query条件并Parse打印
	queryCond := dbhelper.Cond().Eq("name", "Tom").Gt("age", 18).Like("email", "%@example.com").And(
		dbhelper.Cond().Eq("status", "active").Or().Eq("status", "pending"),
	).Build()
	querySQL, queryArgs, err := driver.Parser().ParseAndCache(types.OpQuery, queryCond, nil)
	if err != nil {
		t.Fatalf("解析条件失败: %v", err)
	}
	t.Logf("生成的查询SQL: %s", querySQL)
	t.Logf("生成的查询Args: %v", queryArgs)

	// 测试插入条件
	insertCond := dbhelper.Cond().Eq("name", "Tom").Eq("age", 20).Build()
	insertSQL, insertArgs, err := driver.Parser().ParseAndCache(types.OpInsert, insertCond, nil)
	if err != nil {
		t.Fatalf("解析插入条件失败: %v", err)
	}
	t.Logf("生成的插入SQL: %s", insertSQL)
	t.Logf("生成的插入Args: %v", insertArgs)

	// 测试更新条件
	updateData := dbhelper.Cond().Eq("age", 21).Build()
	updateCond := dbhelper.Cond().Eq("name", "Tom").Build()
	updateSQL, updateArgs, err := driver.Parser().ParseAndCache(types.OpUpdate, updateCond, updateData)
	if err != nil {
		t.Fatalf("解析更新条件失败: %v", err)
	}
	t.Logf("生成的更新SQL: %s", updateSQL)
	t.Logf("生成的更新Args: %v", updateArgs)
}

func TestCRUD_PostgreSQL(t *testing.T) {
	cfg := types.DBConfig{
		DSN:     "user=postgres password=postgres dbname=testdb sslmode=disable", // 请根据实际环境修改
		MaxOpen: 1,
		MaxIdle: 1,
	}
	driver := postgresql.GetDriver()
	connRaw, err := driver.Open(cfg)
	if err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}
	conn := connRaw.(*postgresql.PostgreSQLConn)

	// 1. 创建测试表
	createTable := dbhelper.Cond().Raw("CREATE TABLE IF NOT EXISTS test_user (id SERIAL PRIMARY KEY, name TEXT, age INT)").Build()
	_, err = conn.Exec(createTable)
	if err != nil {
		t.Fatalf("建表失败: %v", err)
	}

	// 2. 插入数据
	insertData := dbhelper.Cond().Eq("name", "Alice").Eq("age", 30).Build()
	rows, err := conn.Insert("test_user", insertData)
	if err != nil || rows != 1 {
		t.Fatalf("插入失败: %v, rows=%d", err, rows)
	}

	// 3. 查询数据
	queryCond := dbhelper.Cond().Eq("name", "Alice").Build()
	result, err := conn.Query("test_user", queryCond)
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if result.Count() != 1 {
		t.Fatalf("查询结果数量错误: %d", result.Count())
	}

	// 4. 更新数据
	updateData := dbhelper.Cond().Eq("age", 31).Build()
	updateCond := dbhelper.Cond().Eq("name", "Alice").Build()
	rows, err = conn.Update("test_user", updateCond, updateData)
	if err != nil || rows != 1 {
		t.Fatalf("更新失败: %v, rows=%d", err, rows)
	}

	// 5. 删除数据
	deleteCond := dbhelper.Cond().Eq("name", "Alice").Build()
	rows, err = conn.Delete("test_user", deleteCond)
	if err != nil || rows != 1 {
		t.Fatalf("删除失败: %v, rows=%d", err, rows)
	}

	// 6. 清理测试表
	dropTable := dbhelper.Cond().Raw("DROP TABLE IF EXISTS test_user").Build()
	_, _ = conn.Exec(dropTable)
}
