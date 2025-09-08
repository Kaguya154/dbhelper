package dbtools

import (
	"dbhelper/types"
	"sync"
	"unsafe"
)

type CondCache struct {
	SQL  string
	Args []interface{}
}

var globalCondCache sync.Map

func SetCondCache(driver uint8, op types.OpType, expr *types.ConditionExpr, sql string, args []interface{}) {
	key := MakeCondCacheFastKey(driver, op, expr)
	globalCondCache.Store(key, CondCache{SQL: sql, Args: args})
}

func GetCondCache(driver uint8, op types.OpType, expr *types.ConditionExpr) (string, []interface{}, bool) {
	key := MakeCondCacheFastKey(driver, op, expr)
	val, ok := globalCondCache.Load(key)
	if !ok {
		return "", nil, false
	}
	cached := val.(CondCache)
	return cached.SQL, cached.Args, true
}

func MakeCondCacheFastKey(driver uint8, op types.OpType, expr *types.ConditionExpr) uintptr {
	h := uintptr(unsafe.Pointer(expr)) // expr 指针
	h ^= uintptr(op) << 56
	h ^= uintptr(driver) << 48
	return h
}
