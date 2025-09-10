package parser

import (
	"dbhelper/types"
	"testing"
)

func mockJsonCondition() *types.ConditionExpr {
	return &types.ConditionExpr{
		Op: types.OpAnd,
		Exprs: []*types.ConditionExpr{
			{Op: types.OpEq, Field: "id", Value: 123},
			{Op: types.OpEq, Field: "name", Value: "test"},
		},
	}
}

func mockJsonComplexCondition() *types.ConditionExpr {
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

func BenchmarkJsonParseCond(b *testing.B) {
	parser := &JsonParser{DriverName: "json", DriverID: 1}
	where := mockJsonCondition()
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

func BenchmarkJsonParseComplexCond(b *testing.B) {
	parser := &JsonParser{DriverName: "json", DriverID: 1}
	where := mockJsonComplexCondition()
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
