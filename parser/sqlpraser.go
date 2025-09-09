package parser

import (
	"dbhelper/dbtools"
	"dbhelper/types"
	"fmt"
	"strings"
)

type SQLParser struct {
	DriverID       uint8
	DriverInternal *types.Driver
}

var opStrMap = map[types.ConditionOp]string{
	types.OpEq:   "=",
	types.OpNe:   "<>",
	types.OpGt:   ">",
	types.OpGte:  ">=",
	types.OpLt:   "<",
	types.OpLte:  "<=",
	types.OpLike: "LIKE",
}

func (d *SQLParser) Driver() *types.Driver {
	return d.DriverInternal
}

func (d *SQLParser) ParseNewCond(op types.OpType, where *types.ConditionExpr, set *types.ConditionExpr) (string, []interface{}, error) {
	var sb strings.Builder
	var args []interface{}
	switch op {
	case types.OpInsert:
		if where == nil || where.Op != types.OpAnd || len(where.Exprs) == 0 {
			return "", nil, fmt.Errorf("Insert data must be AND expr with fields")
		}
		sb.WriteString("INSERT INTO %s (")
		for i, expr := range where.Exprs {
			if expr.Op != types.OpEq {
				return "", nil, fmt.Errorf("Insert only supports EQ expr")
			}
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(d.Quote(expr.Field))
		}
		sb.WriteString(") VALUES (")
		for i := range where.Exprs {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('?')
		}
		sb.WriteByte(')')
		for _, expr := range where.Exprs {
			args = append(args, expr.Value)
		}

	case types.OpQuery:
		sb.WriteString("SELECT * FROM %s")
		if where != nil {
			sb.WriteString(" WHERE ")
			d.buildWhere(&sb, where, &args)
		}

	case types.OpUpdate:
		if set == nil {
			return "", nil, fmt.Errorf("Update data cannot be empty")
		}
		sb.WriteString("UPDATE %s SET ")
		first := true
		if set.Op == types.OpAnd && len(set.Exprs) > 0 {
			for _, expr := range set.Exprs {
				if !first {
					sb.WriteByte(',')
				}
				sb.WriteString(d.Quote(expr.Field))
				sb.WriteString("=?")
				first = false
			}
		} else if set.Op == types.OpEq && set.Field != "" {
			sb.WriteString(d.Quote(set.Field))
			sb.WriteString("=?")
		} else {
			return "", nil, fmt.Errorf("Invalid update data")
		}
		if where != nil {
			sb.WriteString(" WHERE ")
			var whereArgs []interface{}
			var nilsb strings.Builder
			d.buildWhere(&sb, where, &whereArgs)
			d.buildWhere(&nilsb, set, &args)
			args = append(args, whereArgs...)
		}

	case types.OpDelete:
		sb.WriteString("DELETE FROM %s")
		if where != nil {
			sb.WriteString(" WHERE ")
			d.buildWhere(&sb, where, &args)
		}

	case types.OpExec:
		if where == nil || where.Op != types.OpRaw {
			return "", nil, fmt.Errorf("Exec only supports OpRaw ConditionExpr")
		}
		execStr, ok := where.Value.(string)
		if !ok {
			return "", nil, fmt.Errorf("Exec OpRaw ConditionExpr.Value must be string")
		}
		sb.WriteString(execStr)

	default:
		return "", nil, fmt.Errorf("unsupported op: %s", op)
	}

	return sb.String(), args, nil
}

func (d *SQLParser) ParseAndCacheCond(op types.OpType, where *types.ConditionExpr, set *types.ConditionExpr) (string, []interface{}, error) {
	if sqlStr, args, ok := dbtools.GetCondCache(d.DriverID, op, where); ok {
		return sqlStr, args, nil
	}

	sqlStr, args, err := d.ParseNewCond(op, where, set) // 让 ParseNewCond 负责收集 args
	if err != nil {
		return "", nil, err
	}

	dbtools.SetCondCache(d.DriverID, op, where, sqlStr, args)
	return sqlStr, args, nil
}

func (d *SQLParser) Quote(identifier string) string {
	return d.DriverInternal
}

// buildWhere 递归构建 WHERE 子句
func (d *SQLParser) buildWhere(sb *strings.Builder, cond *types.ConditionExpr, args *[]interface{}) {
	if cond == nil {
		return
	}
	switch cond.Op {
	case types.OpAnd, types.OpOr:
		sep := " AND "
		if cond.Op == types.OpOr {
			sep = " OR "
		}
		first := true
		for _, expr := range cond.Exprs {
			if expr == nil {
				continue
			}
			if !first {
				sb.WriteString(sep)
			}
			sb.WriteByte('(')
			d.buildWhere(sb, expr, args)
			sb.WriteByte(')')
			first = false
		}
	case types.OpEq, types.OpNe, types.OpGt, types.OpGte, types.OpLt, types.OpLte, types.OpLike:
		if opStr, ok := opStrMap[cond.Op]; ok {
			sb.WriteString(d.Quote(cond.Field))
			sb.WriteByte(' ')
			sb.WriteString(opStr)
			sb.WriteString(" ?")
			*args = append(*args, cond.Value)
		}
	case types.OpIn:
		if len(cond.Values) == 0 {
			sb.WriteString("1=0")
			return
		}
		sb.WriteString(d.Quote(cond.Field))
		sb.WriteString(" IN (")
		for i := range cond.Values {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('?')
		}
		sb.WriteByte(')')
		*args = append(*args, cond.Values...)
	case types.OpRaw:
		if s, ok := cond.Value.(string); ok {
			sb.WriteString(s)
		}
		if cond.Values != nil {
			*args = append(*args, cond.Values...)
		}
	}
}
