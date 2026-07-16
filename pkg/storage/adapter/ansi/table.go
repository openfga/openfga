package ansi

import "github.com/openfga/openfga/pkg/storage/adapter"

// Physical tuple columns (see assets/migrations/postgres). The subject is stored packed
// in a single column as "type:id" or "type:id#relation" (or "type:*" wildcard); there is
// no physical subject_type/subject_id/subject_relation column.
const (
	colStore            = "store"
	colObjectType       = "object_type"
	colObjectID         = "object_id"
	colRelation         = "relation"
	colUser             = "_user"
	colConditionName    = "condition_name"
	colConditionContext = "condition_context"
)

// tableName is the sole table this builder targets.
const tableName = "tuple"

// tupleTable is one aliased occurrence of the `tuple` table. Each column accessor names
// a logical Column and lets the dialect render it: the object, store, and condition
// columns map to "<alias>.<column>", while the subject fields are projected however the
// physical schema demands (a view over a packed subject column, or discrete columns).
// Callers never touch the physical encoding.
type tupleTable struct {
	alias string
}

// col renders a logical column by deferring to the renderer's dialect, so the same
// accessor yields different SQL across storage layouts.
func (t *tupleTable) col(c Column) *exprNode {
	return newExpr(writerFunc(func(r *renderer) { r.write(r.dialect.Column(c, t.alias)) }))
}

func (t *tupleTable) ObjectType() adapter.Expression     { return t.col(ColumnObjectType) }
func (t *tupleTable) ObjectID() adapter.Expression       { return t.col(ColumnObjectID) }
func (t *tupleTable) ObjectRelation() adapter.Expression { return t.col(ColumnObjectRelation) }
func (t *tupleTable) Store() adapter.Expression          { return t.col(ColumnStore) }
func (t *tupleTable) Condition() adapter.Expression      { return t.col(ColumnCondition) }
func (t *tupleTable) SubjectType() adapter.Expression    { return t.col(ColumnSubjectType) }
func (t *tupleTable) SubjectID() adapter.Expression      { return t.col(ColumnSubjectID) }
func (t *tupleTable) SubjectRelation() adapter.Expression {
	return t.col(ColumnSubjectRelation)
}

// ConditionContext is select-only (Operand tier): projectable, groupable, and usable as
// a function argument, but never compared.
func (t *tupleTable) ConditionContext() adapter.Operand {
	return &operandNode{inner: writerFunc(func(r *renderer) {
		r.write(r.dialect.Column(ColumnConditionContext, t.alias))
	})}
}

// fromSQL renders the table reference for a FROM / JOIN clause: "tuple <alias>".
func (t *tupleTable) fromSQL(r *renderer) {
	r.write(tableName)
	r.write(" ")
	r.write(t.alias)
}

// joinNode is a single join clause against another tuple instance.
type joinNode struct {
	jt    adapter.JoinType
	table *tupleTable
	on    sqlWriter
}

func (j *joinNode) On(predicate adapter.Predicate) adapter.Join {
	j.on = predWriter(predicate)
	return j
}

func (j *joinNode) writeSQL(r *renderer) {
	r.write(joinSQL(j.jt))
	r.write(" ")
	j.table.fromSQL(r)
	if j.on != nil {
		r.write(" ON ")
		r.node(j.on)
	}
}
