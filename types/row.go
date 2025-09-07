package types

import "strconv"

// Next 移动到下一行，返回是否有数据
func (r *Rows) Next() bool {
	if r.pos+1 < len(r.data) {
		r.pos++
		return true
	}
	return false
}

// Get 原始取值
func (r *Rows) Get(col string) interface{} {
	if r.pos < 0 || r.pos >= len(r.data) {
		return nil
	}
	return r.data[r.pos][col]
}

// GetString 取字符串
func (r *Rows) GetString(col string) string {
	if v, ok := r.Get(col).([]byte); ok {
		return string(v)
	}
	if v, ok := r.Get(col).(string); ok {
		return v
	}
	return ""
}

// GetInt 取整数
func (r *Rows) GetInt(col string) int {
	switch v := r.Get(col).(type) {
	case int:
		return v
	case int64:
		return int(v)
	case []byte:
		n, _ := strconv.Atoi(string(v))
		return n
	}
	return 0
}

// All 返回所有行
func (r *Rows) All() []map[string]interface{} {
	return r.data
}

// Count 返回行数
func (r *Rows) Count() int {
	return len(r.data)
}
