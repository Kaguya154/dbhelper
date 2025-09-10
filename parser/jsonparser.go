package parser

import (
	"dbhelper/dbtools"
	"dbhelper/types"
	"encoding/json"
	"fmt"
	"sync"
)

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

// sync.Pool 复用 map[string]interface{} 和 []interface{}
var mapPool = sync.Pool{
	New: func() interface{} { return make(map[string]interface{}, 8) },
}
var slicePool = sync.Pool{
	New: func() interface{} { return make([]interface{}, 0, 8) },
}

func getMap() map[string]interface{} {
	return mapPool.Get().(map[string]interface{})
}
func putMap(m map[string]interface{}) {
	for k := range m {
		delete(m, k)
	}
	mapPool.Put(m)
}
func getSlice() []interface{} {
	return slicePool.Get().([]interface{})
}
func putSlice(s []interface{}) {
	s = s[:0]
	slicePool.Put(s)
}

func (p *JsonParser) Parse(op types.OpType, where *types.ConditionExpr, set *types.ConditionExpr) (string, []interface{}, error) {
	result := getMap()
	defer putMap(result)
	result["op"] = opNameMap[op]

	switch op {
	case types.OpInsert:
		if where == nil || where.Op != types.OpAnd || len(where.Exprs) == 0 {
			return "", nil, fmt.Errorf("Insert data must be AND expr with fields")
		}
		data := getMap()
		defer putMap(data)
		for _, expr := range where.Exprs {
			if expr.Op != types.OpEq {
				return "", nil, fmt.Errorf("Insert only supports EQ expr")
			}
			data[expr.Field] = expr.Value
		}
		result["data"] = data

	case types.OpQuery, types.OpDelete, types.OpUpdate:
		if where != nil {
			filter := buildJsonFilterOpt(where)
			result["filter"] = filter
		}
		if op == types.OpUpdate {
			if set == nil {
				return "", nil, fmt.Errorf("Update data cannot be empty")
			}
			result["update"] = buildJsonUpdateOpt(set)
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
	if sqlStr, args, ok := dbtools.GetCondCache(p.DriverID, op, where); ok {
		return sqlStr, args, nil
	}
	sqlStr, args, err := p.Parse(op, where, set)
	if err != nil {
		return "", nil, err
	}
	dbtools.SetCondCache(p.DriverID, op, where, sqlStr, args)
	return sqlStr, args, nil
}

// 优化递归构建，尽量复用 slice/map
func buildJsonFilterOpt(cond *types.ConditionExpr) interface{} {
	if cond == nil {
		return nil
	}
	switch cond.Op {
	case types.OpAnd, types.OpOr:
		arr := getSlice()
		defer putSlice(arr)
		for _, expr := range cond.Exprs {
			arr = append(arr, buildJsonFilterOpt(expr))
		}
		opName := "$and"
		if cond.Op == types.OpOr {
			opName = "$or"
		}
		m := getMap()
		defer putMap(m)
		m[opName] = append([]interface{}(nil), arr...)
		return m
	case types.OpEq:
		m := getMap()
		defer putMap(m)
		m[cond.Field] = cond.Value
		return m
	case types.OpNe:
		m := getMap()
		defer putMap(m)
		m[cond.Field] = map[string]interface{}{"$ne": cond.Value}
		return m
	case types.OpGt:
		m := getMap()
		defer putMap(m)
		m[cond.Field] = map[string]interface{}{"$gt": cond.Value}
		return m
	case types.OpGte:
		m := getMap()
		defer putMap(m)
		m[cond.Field] = map[string]interface{}{"$gte": cond.Value}
		return m
	case types.OpLt:
		m := getMap()
		defer putMap(m)
		m[cond.Field] = map[string]interface{}{"$lt": cond.Value}
		return m
	case types.OpLte:
		m := getMap()
		defer putMap(m)
		m[cond.Field] = map[string]interface{}{"$lte": cond.Value}
		return m
	case types.OpLike:
		m := getMap()
		defer putMap(m)
		m[cond.Field] = map[string]interface{}{"$like": cond.Value}
		return m
	case types.OpIn:
		m := getMap()
		defer putMap(m)
		m[cond.Field] = map[string]interface{}{"$in": cond.Values}
		return m
	case types.OpRaw:
		if raw, ok := cond.Value.(map[string]interface{}); ok {
			return raw
		}
	}
	return nil
}

func buildJsonUpdateOpt(set *types.ConditionExpr) map[string]interface{} {
	update := getMap()
	defer putMap(update)
	if set.Op == types.OpAnd && len(set.Exprs) > 0 {
		for _, expr := range set.Exprs {
			if expr.Op != types.OpEq {
				continue
			}
			update[expr.Field] = expr.Value
		}
	} else if set.Op == types.OpEq {
		update[set.Field] = set.Value
	}
	return map[string]interface{}{"$set": update}
}
