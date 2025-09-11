package parser_test

import (
	"dbhelper/parser"
	"dbhelper/types"
	"testing"
)

func quoteSql(field string) string {
	return "`" + field + "`"
}

func BenchmarkSqlParseCond(b *testing.B) {
	p := &parser.SQLParser{
		DriverName: "mysql",
		DriverID:   1,
		QuoteFunc:  quoteSql,
	}
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

func BenchmarkSqlParseComplexCond(b *testing.B) {
	p := &parser.SQLParser{
		DriverName: "mysql",
		DriverID:   1,
		QuoteFunc:  quoteSql,
	}
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
