package types

type Conn interface {
	Insert(table string, data *ConditionExpr) (int64, error)
	Query(table string, cond *ConditionExpr) (*Rows, error)
	Update(table string, data, cond *ConditionExpr) (int64, error)
	Delete(table string, cond *ConditionExpr) (int64, error)
	Exec(cond *ConditionExpr) (int64, error)
	Begin() (Tx, error)
}

type Tx interface {
	Commit() error
	Rollback() error
	Insert(table string, data *ConditionExpr) (int64, error)
	Query(table string, cond *ConditionExpr) (*Rows, error)
	Update(table string, data, cond *ConditionExpr) (int64, error)
	Delete(table string, cond *ConditionExpr) (int64, error)
	Exec(cond *ConditionExpr) (int64, error)
}

type Driver interface {
	Open(cfg DBConfig) (Conn, error)
	Quote(identifier string) string
	Placeholder(n int) string
	ParseCond(op string, where *ConditionExpr, set *ConditionExpr) (string, error)
}
