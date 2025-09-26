package dbtools_test

import (
	"testing"

	"github.com/Kaguya154/dbhelper"
	"github.com/Kaguya154/dbhelper/dbtools"
	"github.com/Kaguya154/dbhelper/drivers/sqlite"
	"github.com/Kaguya154/dbhelper/types"
)

func init() {
	// 注册驱动
	err := dbhelper.RegisterDriver(sqlite.DriverName, sqlite.GetDriver())
	if err != nil {
		return
	}
}

func quoteSql(field string) string {
	return "`" + field + "`"
}

func TestSqliteCondCache(t *testing.T) {

	driver, err := dbhelper.GetDriver(sqlite.DriverName)
	if err != nil {
		t.Fatalf("获取驱动失败: %v", err)
	}
	p := driver.Parser()

	// 测试Query条件并Prase打印
	queryCond := dbhelper.Cond().Eq("name", "Tom").Gt("age", 18).
		Like("email", "%@example.com").And(
		dbhelper.Cond().Eq("status", "active").Or().Eq("status", "pending"),
	).Build()
	querySQL, quaryArgs, err := p.ParseAndCache(types.OpQuery, queryCond, nil)
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
	insertSQL, insertArgs, err := p.ParseAndCache(types.OpInsert, insertCond, nil)
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

	// 测试复杂条件
	complexSQL, complexArgs, err := p.ParseAndCache(types.OpUpdate, complexCondition, set)
	if err != nil {
		t.Fatalf("解析复杂条件失败: %v", err)
	}
	t.Logf("生成的复杂SQL: %s", complexSQL)
	t.Logf("生成的复杂Args: %v", complexArgs)
	// 测试cache
	cache, argCache, b = dbtools.GetCondCache(sqlite.DriverID, types.OpUpdate, complexCondition)
	if !b {
		t.Fatalf("复杂条件缓存未命中")
	}
	t.Logf("复杂条件缓存命中: %s", cache)
	t.Logf("复杂条件缓存Args: %v", argCache)

}

var condition = dbhelper.Cond().Or(dbhelper.Cond().Eq("id", 123).Eq("name", "test")).Build()
var complexCondition = dbhelper.Cond().Or(
	dbhelper.Cond().And(
		dbhelper.Cond().Eq("status", "active"),
		dbhelper.Cond().Gt("age", 18),
	),
	dbhelper.Cond().And(
		dbhelper.Cond().Eq("status", "pending"),
		dbhelper.Cond().Lt("age", 18),
	),
).In("role", []interface{}{"admin", "user"}).Like("email", "%@example.com").Build()
var set = dbhelper.Cond().Eq("age", 20).Build()

func BenchmarkSqlParseCond(b *testing.B) {

	driver, err := dbhelper.GetDriver(sqlite.DriverName)
	if err != nil {
		b.Fatalf("获取驱动失败: %v", err)
	}

	where := condition

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

	where := complexCondition

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
