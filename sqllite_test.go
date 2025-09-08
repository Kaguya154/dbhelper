package dbhelper

import (
	"dbhelper/drivers"
	"dbhelper/types"
	"testing"
)

func init() {
	// 注册驱动
	err := RegisterDriver(drivers.SQLiteDriverName, &drivers.SQLiteDriver{})
	if err != nil {
		return
	}
}

func TestCondBuilder(t *testing.T) {
	driver, err := getDriver(drivers.SQLiteDriverName)
	if err != nil {
		t.Fatalf("获取驱动失败: %v", err)
	}
	// 测试Query条件并Prase打印
	queryCond := Cond().Eq("name", "Tom").Gt("age", 18).Like("email", "%@example.com").And(
		Cond().Eq("status", "active").Or().Eq("status", "pending"),
	).Build()
	querySQL, err := driver.ParseAndCacheCond("Query", queryCond, nil)
	if err != nil {
		t.Fatalf("解析条件失败: %v", err)
	}
	t.Logf("生成的查询SQL: %s", querySQL)

	// 测试插入条件
	insertCond := Cond().Eq("name", "Tom").Eq("age", 20).Build()
	insertSQL, err := driver.ParseAndCacheCond("Insert", insertCond, nil)
	if err != nil {
		t.Fatalf("解析插入条件失败: %v", err)
	}
	t.Logf("生成的插入SQL: %s", insertSQL)
	// 测试更新条件
	// SET age=21
	updateData := Cond().Eq("age", 21).Build()
	// WHERE name='Tom'
	updateCond := Cond().Eq("name", "Tom").Build()

	updateSQL, err := driver.ParseAndCacheCond("Update", updateCond, updateData)
	if err != nil {
		t.Fatalf("解析更新条件失败: %v", err)
	}
	t.Logf("生成的更新SQL: %s", updateSQL)
	// 测试删除条件
	deleteCond := Cond().Eq("name", "Tom").Build()
	deleteSQL, err := driver.ParseAndCacheCond("Delete", deleteCond, nil)
	if err != nil {
		t.Fatalf("解析删除条件失败: %v", err)
	}
	t.Logf("生成的删除SQL: %s", deleteSQL)
	// 测试Exec条件
	execCond := Cond().Raw("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)").Build()
	execSQL, err := driver.ParseAndCacheCond("Exec", execCond, nil)
	if err != nil {
		t.Fatalf("解析Exec条件失败: %v", err)
	}
	t.Logf("生成的Exec SQL: %s", execSQL)

}

func TestSQLiteDriver_CRUD(t *testing.T) {
	db, err := Open(types.DBConfig{
		Driver: drivers.SQLiteDriverName,
		DSN:    ":memory:",
	})
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	createTable := Cond().Raw("CREATE TABLE user (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INT)").Build()
	// 使用 Exec 方法建表
	_, err = db.Exec(createTable)
	if err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	// 插入
	data := Cond().Eq("name", "Tom").Eq("age", 20).Build()
	id, err := db.Insert("user", data)
	if err != nil || id == 0 {
		t.Fatalf("插入失败: %v, id=%d", err, id)
	}
	t.Logf("插入成功, id=%d", id)
	// 查询
	cond := Cond().Eq("name", "Tom").Build()
	rows, err := db.Query("user", cond)
	if err != nil || rows.Count() == 0 {
		t.Fatalf("查询失败: %v, rows=%v", err, rows)
	}
	t.Logf("查询成功, rows=%v", rows.All())
	// 更新
	upd := Cond().Eq("age", 21).Build()
	n, err := db.Update("user", cond, upd)
	if err != nil || n == 0 {
		t.Fatalf("更新失败: %v, n=%d", err, n)
	}
	t.Logf("更新成功, 影响行数=%d", n)
	// 删除
	n, err = db.Delete("user", cond)
	if err != nil || n == 0 {
		t.Fatalf("删除失败: %v, n=%d", err, n)
	}
	t.Logf("删除成功, 影响行数=%d", n)
}

// Tx测试
func TestSQLiteDriver_Tx(t *testing.T) {
	db, err := Open(types.DBConfig{
		Driver: drivers.SQLiteDriverName,
		DSN:    ":memory:",
	})
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	createTable := Cond().Raw("CREATE TABLE user (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INT)").Build()
	// 使用 Exec 方法建表
	_, err = db.Exec(createTable)
	if err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	// 开始事务
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("开始事务失败: %v", err)
	}
	// 插入
	data := Cond().Eq("name", "Alice").Eq("age", 25).Build()
	id, err := tx.Insert("user", data)
	if err != nil || id == 0 {
		tx.Rollback()
		t.Fatalf("插入失败: %v, id=%d", err, id)
	}
	t.Logf("插入成功, id=%d", id)
	// 查询
	cond := Cond().Eq("name", "Alice").Build()
	rows, err := tx.Query("user", cond)
	if err != nil || rows.Count() == 0 {
		tx.Rollback()
		t.Fatalf("查询失败: %v, rows=%v", err, rows)
	}
	t.Logf("查询成功, rows=%v", rows.All())
	// 提交事务
	err = tx.Commit()
	if err != nil {
		t.Fatalf("提交事务失败: %v", err)
	}
	t.Log("事务提交成功")
}
