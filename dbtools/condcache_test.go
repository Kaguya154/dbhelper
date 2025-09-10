package dbtools_test

import (
	"dbhelper"
	"dbhelper/dbtools"
	"dbhelper/drivers/sqlite"
	"dbhelper/types"
	"testing"
)

func init() {
	// 注册驱动
	err := dbhelper.RegisterDriver(sqlite.DriverName, sqlite.GetDriver())
	if err != nil {
		return
	}
}

func TestSqliteCondCache(t *testing.T) {

	driver, err := dbhelper.GetDriver(sqlite.DriverName)
	if err != nil {
		t.Fatalf("获取驱动失败: %v", err)
	}
	// 测试Query条件并Prase打印
	queryCond := dbhelper.Cond().Eq("name", "Tom").Gt("age", 18).
		Like("email", "%@example.com").And(
		dbhelper.Cond().Eq("status", "active").Or().Eq("status", "pending"),
	).Build()
	querySQL, quaryArgs, err := driver.Parser().ParseAndCache(types.OpQuery, queryCond, nil)
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
	insertCond := dbhelper.Cond().Eq("name", "Tom").Eq("age", 20).
		Build()
	insertSQL, insertArgs, err := driver.Parser().ParseAndCache(types.OpInsert, insertCond, nil)
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

func mockSqlCondition() *types.ConditionExpr {
	return &types.ConditionExpr{
		Op: types.OpAnd,
		Exprs: []*types.ConditionExpr{
			{Op: types.OpEq, Field: "id", Value: 123},
			{Op: types.OpEq, Field: "name", Value: "test"},
		},
	}
}
func mockComplexCondition() *types.ConditionExpr {
	return &types.ConditionExpr{
		Op: types.OpOr,
		Exprs: []*types.ConditionExpr{
			{
				Op: types.OpAnd,
				Exprs: []*types.ConditionExpr{
					{Op: types.OpEq, Field: "status", Value: "active"},
					{Op: types.OpGt, Field: "age", Value: 18},
				},
			},
			{
				Op: types.OpAnd,
				Exprs: []*types.ConditionExpr{
					{Op: types.OpEq, Field: "status", Value: "pending"},
					{Op: types.OpLt, Field: "age", Value: 18},
				},
			},
			{Op: types.OpIn, Field: "role", Values: []interface{}{"admin", "user"}},
			{Op: types.OpLike, Field: "email", Value: "%@example.com"},
		},
	}
}

func BenchmarkSqlParseCond(b *testing.B) {

	driver, err := dbhelper.GetDriver(sqlite.DriverName)
	if err != nil {
		b.Fatalf("获取驱动失败: %v", err)
	}

	where := mockSqlCondition()
	set := &types.ConditionExpr{
		Op: types.OpAnd,
		Exprs: []*types.ConditionExpr{
			{Op: types.OpEq, Field: "age", Value: 20},
		},
	}

	b.Run("ParseAndCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := driver.Parser().ParseAndCache(types.OpUpdate, where, set)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})

	b.Run("Parse", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := driver.Parser().Parse(types.OpUpdate, where, set)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})
}

func BenchmarkSqlParseComplexCond(b *testing.B) {
	driver, err := dbhelper.GetDriver(sqlite.DriverName)
	if err != nil {
		b.Fatalf("获取驱动失败: %v", err)
	}

	where := mockComplexCondition()
	set := &types.ConditionExpr{
		Op: types.OpAnd,
		Exprs: []*types.ConditionExpr{
			{Op: types.OpEq, Field: "age", Value: 20},
		},
	}

	b.Run("ParseAndCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := driver.Parser().ParseAndCache(types.OpUpdate, where, set)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})

	b.Run("Parse", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := driver.Parser().Parse(types.OpUpdate, where, set)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})
}
