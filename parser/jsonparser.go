package parser

import (
	"dbhelper/types"
	"encoding/json"
	"fmt"
)

// JsonParser 支持非 SQL 数据库，生成 JSON 风格的操作结构
type JsonParser struct {
	DriverName string
	DriverID   uint8
}

var opNameMap = map[types.OpType]string{
	types.OpInsert: "insert",
	types.OpQuery:  "query",
	types.OpUpdate: "update",
	types.OpDelete: "delete",
	types.OpExec:   "exec",
}

func (p *JsonParser) Parse(op types.OpType, where *types.ConditionExpr, set *types.ConditionExpr) (string, []interface{}, error) {
	result := map[string]interface{}{
		"op": opNameMap[op],
	}

	switch op {
	case types.OpInsert:
		if where == nil || where.Op != types.OpAnd || len(where.Exprs) == 0 {
			return "", nil, fmt.Errorf("Insert data must be AND expr with fields")
		}
		data := make(map[string]interface{})
		for _, expr := range where.Exprs {
			if expr.Op != types.OpEq {
				return "", nil, fmt.Errorf("Insert only supports EQ expr")
			}
			data[expr.Field] = expr.Value
		}
		result["data"] = data

	case types.OpQuery, types.OpDelete, types.OpUpdate:
		if where != nil {
			result["filter"] = buildJsonFilter(where)
		}
		if op == types.OpUpdate {
			if set == nil {
				return "", nil, fmt.Errorf("Update data cannot be empty")
			}
			result["update"] = buildJsonUpdate(set)
		}

	case types.OpExec:
		if where == nil || where.Op != types.OpRaw {
			return "", nil, fmt.Errorf("Exec only supports OpRaw ConditionExpr")
		}
		raw, ok := where.Value.(map[string]interface{})
		if !ok {
			return "", nil, fmt.Errorf("Exec OpRaw Value must be map[string]interface{}")
		}
		result["raw"] = raw

	default:
		return "", nil, fmt.Errorf("unsupported op: %d", op)
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return "", nil, err
	}

	return string(jsonBytes), nil, nil
}

func (p *JsonParser) ParseAndCache(op types.OpType, where *types.ConditionExpr, set *types.ConditionExpr) (string, []interface{}, error) {
	// NoSQL 通常不需要 args 参数，缓存可以仅基于操作 + 条件
	return p.Parse(op, where, set)
}

// buildJsonFilter 递归构建查询条件
func buildJsonFilter(cond *types.ConditionExpr) map[string]interface{} {
	if cond == nil {
		return nil
	}
	switch cond.Op {
	case types.OpAnd, types.OpOr:
		arr := make([]interface{}, 0, len(cond.Exprs))
		for _, expr := range cond.Exprs {
			arr = append(arr, buildJsonFilter(expr))
		}
		opName := "$and"
		if cond.Op == types.OpOr {
			opName = "$or"
		}
		return map[string]interface{}{opName: arr}
	case types.OpEq:
		return map[string]interface{}{cond.Field: cond.Value}
	case types.OpNe:
		return map[string]interface{}{cond.Field: map[string]interface{}{"$ne": cond.Value}}
	case types.OpGt:
		return map[string]interface{}{cond.Field: map[string]interface{}{"$gt": cond.Value}}
	case types.OpGte:
		return map[string]interface{}{cond.Field: map[string]interface{}{"$gte": cond.Value}}
	case types.OpLt:
		return map[string]interface{}{cond.Field: map[string]interface{}{"$lt": cond.Value}}
	case types.OpLte:
		return map[string]interface{}{cond.Field: map[string]interface{}{"$lte": cond.Value}}
	case types.OpLike:
		return map[string]interface{}{cond.Field: map[string]interface{}{"$like": cond.Value}}
	case types.OpIn:
		return map[string]interface{}{cond.Field: map[string]interface{}{"$in": cond.Values}}
	case types.OpRaw:
		if raw, ok := cond.Value.(map[string]interface{}); ok {
			return raw
		}
	}
	return nil
}

func buildJsonUpdate(set *types.ConditionExpr) map[string]interface{} {
	update := make(map[string]interface{})
	if set.Op == types.OpAnd && len(set.Exprs) > 0 {
		for _, expr := range set.Exprs {
			if expr.Op != types.OpEq {
				continue // 简单实现只支持 EQ 更新
			}
			update[expr.Field] = expr.Value
		}
	} else if set.Op == types.OpEq {
		update[set.Field] = set.Value
	}
	return map[string]interface{}{"$set": update}
}
