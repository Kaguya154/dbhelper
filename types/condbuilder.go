package types

const (
	OpEq   ConditionOp = "EQ"
	OpNe   ConditionOp = "NE"
	OpGt   ConditionOp = "GT"
	OpGte  ConditionOp = "GTE"
	OpLt   ConditionOp = "LT"
	OpLte  ConditionOp = "LTE"
	OpLike ConditionOp = "LIKE"
	OpIn   ConditionOp = "IN"
	OpAnd  ConditionOp = "AND"
	OpOr   ConditionOp = "OR"
	OpRaw  ConditionOp = "RAW"
)

// NewCondition 创建并返回一个新的 CondBuilder 实例。
func NewCondition() *CondBuilder {
	return &CondBuilder{
		exprs: make([]*ConditionExpr, 0),
	}
}

// Eq 添加等于条件（=）。
func (b *CondBuilder) Eq(field string, value interface{}) *CondBuilder {
	b.exprs = append(b.exprs, &ConditionExpr{
		Op:    OpEq,
		Field: field,
		Value: value,
	})
	return b
}

// Ne 添加不等于条件（<>）。
func (b *CondBuilder) Ne(field string, value interface{}) *CondBuilder {
	b.exprs = append(b.exprs, &ConditionExpr{
		Op:    OpNe,
		Field: field,
		Value: value,
	})
	return b
}

// Gt 添加大于条件（>）。
func (b *CondBuilder) Gt(field string, value interface{}) *CondBuilder {
	b.exprs = append(b.exprs, &ConditionExpr{
		Op:    OpGt,
		Field: field,
		Value: value,
	})
	return b
}

// Gte 添加大于等于条件（>=）。
func (b *CondBuilder) Gte(field string, value interface{}) *CondBuilder {
	b.exprs = append(b.exprs, &ConditionExpr{
		Op:    OpGte,
		Field: field,
		Value: value,
	})
	return b
}

// Lt 添加小于条件（<）。
func (b *CondBuilder) Lt(field string, value interface{}) *CondBuilder {
	b.exprs = append(b.exprs, &ConditionExpr{
		Op:    OpLt,
		Field: field,
		Value: value,
	})
	return b
}

// Lte 添加小于等于条件（<=）。
func (b *CondBuilder) Lte(field string, value interface{}) *CondBuilder {
	b.exprs = append(b.exprs, &ConditionExpr{
		Op:    OpLte,
		Field: field,
		Value: value,
	})
	return b
}

// Like 添加模糊匹配条件（LIKE）。
func (b *CondBuilder) Like(field string, pattern string) *CondBuilder {
	b.exprs = append(b.exprs, &ConditionExpr{
		Op:    OpLike,
		Field: field,
		Value: pattern,
	})
	return b
}

// In 添加 IN 查询条件。
func (b *CondBuilder) In(field string, values []interface{}) *CondBuilder {
	b.exprs = append(b.exprs, &ConditionExpr{
		Op:     OpIn,
		Field:  field,
		Values: values,
	})
	return b
}

// And 组合多个条件为 AND。
func (b *CondBuilder) And(conds ...*CondBuilder) *CondBuilder {
	exprs := make([]*ConditionExpr, 0)
	for _, c := range conds {
		exprs = append(exprs, c.exprs...)
	}
	b.exprs = append(b.exprs, &ConditionExpr{
		Op:    OpAnd,
		Exprs: exprs,
	})
	return b
}

// Or 组合多个条件为 OR。
func (b *CondBuilder) Or(conds ...*CondBuilder) *CondBuilder {
	exprs := make([]*ConditionExpr, 0)
	for _, c := range conds {
		exprs = append(exprs, c.exprs...)
	}
	b.exprs = append(b.exprs, &ConditionExpr{
		Op:    OpOr,
		Exprs: exprs,
	})
	return b
}

// Raw 添加原始条件（不安全，慎用）。
func (b *CondBuilder) Raw(raw string) *CondBuilder {
	b.exprs = append(b.exprs, &ConditionExpr{
		Op:     OpRaw,
		Value:  raw,
		Values: nil,
	})
	return b
}

// Build 生成最终的通用条件表达式树。
// 返回值：
//   - *types.ConditionExpr: 根条件表达式（AND 连接所有条件），由数据库驱动器解析
func (b *CondBuilder) Build() *ConditionExpr {
	if len(b.exprs) == 0 {
		return nil
	}
	if len(b.exprs) == 1 {
		return b.exprs[0]
	}
	return &ConditionExpr{
		Op:    OpAnd,
		Exprs: b.exprs,
	}
}
