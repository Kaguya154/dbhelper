package parser

import (
	"dbhelper/types"
	"testing"
)

func quoteSql(field string) string {
	return "`" + field + "`"
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

func mockSqlComplexCondition() *types.ConditionExpr {
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
	parser := &SQLParser{
		DriverName: "mysql",
		DriverID:   1,
		QuoteFunc:  quoteSql,
	}
	where := mockSqlCondition()
	set := &types.ConditionExpr{
		Op: types.OpAnd,
		Exprs: []*types.ConditionExpr{
			{Op: types.OpEq, Field: "age", Value: 20},
		},
	}

	b.Run("Parse", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := parser.Parse(types.OpUpdate, where, set)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})

	b.Run("ParseAndCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := parser.ParseAndCache(types.OpUpdate, where, set)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})
}

func BenchmarkSqlParseComplexCond(b *testing.B) {
	parser := &SQLParser{
		DriverName: "mysql",
		DriverID:   1,
		QuoteFunc:  quoteSql,
	}
	where := mockSqlComplexCondition()
	set := &types.ConditionExpr{
		Op: types.OpAnd,
		Exprs: []*types.ConditionExpr{
			{Op: types.OpEq, Field: "age", Value: 20},
		},
	}

	b.Run("Parse", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := parser.Parse(types.OpUpdate, where, set)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})

	b.Run("ParseAndCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := parser.ParseAndCache(types.OpUpdate, where, set)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})
}
