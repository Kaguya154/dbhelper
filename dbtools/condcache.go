package dbtools

import (
	"dbhelper/types"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type CondCache struct {
	SQL      string
	Args     []interface{}
	expireAt int64 // 过期时间戳（秒）
	freq     int32 // 访问频率
}

var (
	globalCondCache sync.Map
	ttlSeconds      int64 = 300 // 默认缓存5分钟
	maxCacheSize    int   = 4096
)

func init() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			cleanCacheBackground()
		}
	}()
}

func SetCondCache(driver uint8, op types.OpType, expr *types.ConditionExpr, sql string, args []interface{}) {
	key := MakeCondCacheFastKey(driver, op, expr)
	now := time.Now().Unix()
	val := &CondCache{
		SQL:      sql,
		Args:     args,
		expireAt: now + ttlSeconds,
		freq:     1,
	}
	globalCondCache.Store(key, val)
}

func GetCondCache(driver uint8, op types.OpType, expr *types.ConditionExpr) (string, []interface{}, bool) {
	key := MakeCondCacheFastKey(driver, op, expr)
	val, ok := globalCondCache.Load(key)
	if !ok {
		return "", nil, false
	}
	atomic.AddInt32(&val.(*CondCache).freq, 1)
	return val.(*CondCache).SQL, val.(*CondCache).Args, true
}

func MakeCondCacheFastKey(driver uint8, op types.OpType, expr *types.ConditionExpr) uintptr {
	h := uintptr(unsafe.Pointer(expr)) // expr 指针
	h ^= uintptr(op) << 56
	h ^= uintptr(driver) << 48
	return h
}

// 后台定时清理过期和低频缓存
func cleanCacheBackground() {
	var count int
	now := time.Now().Unix()
	var keysToDelete []interface{}
	const cleanBatch = 256
	var minFreq int32 = 2

	globalCondCache.Range(func(key, value interface{}) bool {
		count++
		cache := value.(*CondCache)
		if cache.expireAt < now || (count > maxCacheSize && atomic.LoadInt32(&cache.freq) < minFreq) {
			keysToDelete = append(keysToDelete, key)
			if len(keysToDelete) >= cleanBatch {
				return false // 只清理一批
			}
		}
		return true
	})

	for _, key := range keysToDelete {
		globalCondCache.Delete(key)
	}
}
