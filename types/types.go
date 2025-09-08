package types

type ConditionOp string

type ConditionExpr struct {
	Op     ConditionOp
	Field  string
	Value  interface{}
	Values []interface{}
	Exprs  []*ConditionExpr
}

type DBConfig struct {
	Driver  string
	DSN     string
	MaxOpen int
	MaxIdle int
}

// CondBuilder 用于构建通用条件表达式的结构体。
type CondBuilder struct {
	exprs []*ConditionExpr
}
type Rows struct {
	data []map[string]interface{}
	pos  int
}

func NewRows(data []map[string]interface{}) *Rows {
	return &Rows{data: data, pos: -1}
}

type OpType uint8

const (
	OpInsert OpType = 0
	OpQuery  OpType = 1
	OpUpdate OpType = 2
	OpDelete OpType = 3
	OpExec   OpType = 4
)
