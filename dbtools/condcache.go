package dbtools

import (
	"dbhelper/types"
	"sync"
	"unsafe"
)

var globalCondCache sync.Map

func SetCondCache(driver uint8, op types.OpType, expr *types.ConditionExpr, sql string) {
	key := MakeCondCacheFastKey(driver, op, expr)
	globalCondCache.Store(key, sql)
}

func GetCondCache(driver uint8, op types.OpType, expr *types.ConditionExpr) (string, bool) {
	key := MakeCondCacheFastKey(driver, op, expr)
	val, ok := globalCondCache.Load(key)
	if !ok {
		return "", false
	}
	return val.(string), true
}

func MakeCondCacheFastKey(driver uint8, op types.OpType, expr *types.ConditionExpr) uintptr {
	h := uintptr(unsafe.Pointer(expr)) // expr 指针
	h ^= uintptr(op) << 56
	h ^= uintptr(driver) << 48
	return h
}
