package dbtools

import (
	"dbhelper/types"
	"fmt"
	"sync"
)

var globalCondCache sync.Map

func SetCondCache(driver string, op types.OpType, expr *types.ConditionExpr, sql string) {
	key := MakeCondCacheKeyByPtr(driver, op, expr)
	globalCondCache.Store(key, sql)
}

func GetCondCache(driver string, op types.OpType, expr *types.ConditionExpr) (string, bool) {
	key := MakeCondCacheKeyByPtr(driver, op, expr)
	val, ok := globalCondCache.Load(key)
	if !ok {
		return "", false
	}
	return val.(string), true
}

func MakeCondCacheKeyByPtr(driver string, op types.OpType, expr *types.ConditionExpr) string {
	return fmt.Sprintf("%s:%s:%p", driver, op.String(), expr)
}
