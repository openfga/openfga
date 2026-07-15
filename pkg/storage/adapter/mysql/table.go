package mysql

import "github.com/openfga/openfga/pkg/storage/adapter"

// Physical tuple columns (see assets/migrations/mysql). The subject is stored packed in a
// single column as "type:id", "type:id#relation", or the "type:*" wildcard; there is no
// physical subject_type/subject_id/subject_relation column.
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

// tupleTable is one aliased occurrence of the `tuple` table. Each column accessor renders
// the column's MySQL SQL directly: the object, store, and condition columns map to
// "<alias>.<column>", while the subject fields are decoded from the packed `_user` column
// with MySQL's SUBSTRING_INDEX / SUBSTRING / LOCATE / IF. Callers never touch the physical
// encoding.
type tupleTable struct {
	alias string
}

// raw wraps a literal SQL fragment as an expression node.
func raw(s string) *exprNode {
	return newExpr(writerFunc(func(r *renderer) { r.write(s) }))
}

// qualified renders "<alias>.<column>".
func (t *tupleTable) qualified(column string) string { return t.alias + "." + column }

func (t *tupleTable) ObjectType() adapter.Expression     { return raw(t.qualified(colObjectType)) }
func (t *tupleTable) ObjectID() adapter.Expression       { return raw(t.qualified(colObjectID)) }
func (t *tupleTable) ObjectRelation() adapter.Expression { return raw(t.qualified(colRelation)) }
func (t *tupleTable) Store() adapter.Expression          { return raw(t.qualified(colStore)) }
func (t *tupleTable) Condition() adapter.Expression      { return raw(t.qualified(colConditionName)) }
func (t *tupleTable) SubjectType() adapter.Expression    { return raw(subjectType(t.alias)) }
func (t *tupleTable) SubjectID() adapter.Expression      { return raw(subjectID(t.alias)) }
func (t *tupleTable) SubjectRelation() adapter.Expression {
	return raw(subjectRelation(t.alias))
}

// ConditionContext is select-only (Operand tier): projectable, groupable, and usable as a
// function argument, but never compared.
func (t *tupleTable) ConditionContext() adapter.Operand {
	col := t.qualified(colConditionContext)
	return &operandNode{inner: writerFunc(func(r *renderer) { r.write(col) })}
}

// fromSQL renders the table reference for a FROM / JOIN clause: "tuple <alias>".
func (t *tupleTable) fromSQL(r *renderer) {
	r.write(tableName)
	r.write(" ")
	r.write(t.alias)
}

// The subject is stored packed as "type:id", "type:id#relation", or the "type:*"
// wildcard; the following helpers decode each logical field with MySQL string functions.

// subjectType is everything before the first ':' in `_user`.
func subjectType(alias string) string {
	u := alias + "." + colUser
	return "SUBSTRING_INDEX(" + u + ", ':', 1)"
}

// subjectID is the segment between the first ':' and the optional '#relation' suffix:
// everything after the first ':' (SUBSTRING from LOCATE), then everything before the first
// '#' (SUBSTRING_INDEX, which yields the whole string when '#' is absent).
func subjectID(alias string) string {
	u := alias + "." + colUser
	afterColon := "SUBSTRING(" + u + ", LOCATE(':', " + u + ") + 1)"
	return "SUBSTRING_INDEX(" + afterColon + ", '#', 1)"
}

// subjectRelation is the segment after '#', or the empty string when the subject is not a
// userset. LOCATE returns 0 when '#' is absent, in which case the field is empty.
func subjectRelation(alias string) string {
	u := alias + "." + colUser
	return "IF(LOCATE('#', " + u + ") = 0, '', SUBSTRING(" + u + ", LOCATE('#', " + u + ") + 1))"
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
