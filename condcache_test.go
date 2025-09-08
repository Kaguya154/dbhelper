package dbhelper

import (
	"dbhelper/dbtools"
	"dbhelper/drivers/sqlite"
	"dbhelper/types"
	"testing"
)

func init() {
	// 注册驱动
	err := RegisterDriver(sqlite.DriverName, &sqlite.SQLiteDriver{})
	if err != nil {
		return
	}
}

func TestCondCache(t *testing.T) {

	driver, err := getDriver(sqlite.DriverName)
	if err != nil {
		t.Fatalf("获取驱动失败: %v", err)
	}
	// 测试Query条件并Prase打印
	queryCond := Cond().Eq("name", "Tom").Gt("age", 18).
		Like("email", "%@example.com").And(
		Cond().Eq("status", "active").Or().Eq("status", "pending"),
	).Build()
	querySQL, quaryArgs, err := driver.ParseAndCacheCond(types.OpQuery, queryCond, nil)
	if err != nil {
		t.Fatalf("解析条件失败: %v", err)
	}
	t.Logf("生成的查询SQL: %s", querySQL)
	t.Logf("生成的查询Args: %v", quaryArgs)
	// 测试cache
	cache, argCache, b := dbtools.GetCondCache(sqlite.DriverID, types.OpQuery, queryCond)
	if !b {
		t.Fatalf("查询条件缓存未命中")
	}
	t.Logf("查询条件缓存命中: %s", cache)
	t.Logf("查询条件缓存Args: %v", argCache)

	// 测试插入条件
	insertCond := Cond().Eq("name", "Tom").Eq("age", 20).
		Build()
	insertSQL, insertArgs, err := driver.ParseAndCacheCond(types.OpInsert, insertCond, nil)
	if err != nil {
		t.Fatalf("解析插入条件失败: %v", err)
	}
	t.Logf("生成的插入SQL: %s", insertSQL)
	t.Logf("生成的插入Args: %v", insertArgs)
	// 测试cache
	cache, argCache, b = dbtools.GetCondCache(sqlite.DriverID, types.OpInsert, insertCond)
	if !b {
		t.Fatalf("插入条件缓存未命中")
	}
	t.Logf("插入条件缓存命中: %s", cache)
	t.Logf("插入条件缓存Args: %v", argCache)
}

func mockCondition() *types.ConditionExpr {
	return &types.ConditionExpr{
		Op: types.OpAnd,
		Exprs: []*types.ConditionExpr{
			{Op: types.OpEq, Field: "id", Value: 123},
			{Op: types.OpEq, Field: "name", Value: "test"},
		},
	}
}

func BenchmarkParseCond(b *testing.B) {

	driver, err := getDriver(sqlite.DriverName)
	if err != nil {
		b.Fatalf("获取驱动失败: %v", err)
	}

	where := mockCondition()
	set := &types.ConditionExpr{
		Op: types.OpAnd,
		Exprs: []*types.ConditionExpr{
			{Op: types.OpEq, Field: "age", Value: 20},
		},
	}

	b.Run("ParseAndCacheCond", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := driver.ParseAndCacheCond(types.OpUpdate, where, set)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})

	b.Run("ParseNewCond", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := driver.ParseNewCond(types.OpUpdate, where, set)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})
}
