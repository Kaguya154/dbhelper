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

type OpType string

const (
	OpInsert OpType = "Insert"
	OpQuery  OpType = "Query"
	OpUpdate OpType = "Update"
	OpDelete OpType = "Delete"
	OpExec   OpType = "Exec"
)

func (op OpType) String() string {
	return string(op)
}
