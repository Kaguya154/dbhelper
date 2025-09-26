package parser_test

import (
	"testing"

	"github.com/Kaguya154/dbhelper"
	"github.com/Kaguya154/dbhelper/parser"
	"github.com/Kaguya154/dbhelper/types"
)

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

func BenchmarkJsonParseCond(b *testing.B) {
	p := &parser.JsonParser{DriverName: "json", DriverID: 1}
	where := condition

	b.Run("Parse", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := p.Parse(types.OpUpdate, where, set)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})

	b.Run("ParseAndCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := p.ParseAndCache(types.OpUpdate, where, set)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})
}

func BenchmarkJsonParseComplexCond(b *testing.B) {
	p := &parser.JsonParser{DriverName: "json", DriverID: 1}
	where := complexCondition

	b.Run("Parse", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := p.Parse(types.OpUpdate, where, set)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})

	b.Run("ParseAndCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := p.ParseAndCache(types.OpUpdate, where, set)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})
}
