package mysql

import (
	"github.com/openfga/openfga/pkg/storage/adapter/internal/ansi"
)

// dialect renders the MySQL-specific constructs, embedding ansi.ANSIDialect for
// everything that is already standard. MySQL's bind placeholder is "?", the ANSI
// default, so Placeholder needs no override; the sole divergence is the subject view
// synthesized from the packed `_user` column, which it decodes with MySQL's native
// SUBSTRING_INDEX / SUBSTRING / LOCATE rather than the ANSI SUBSTRING ... FROM ... FOR
// form.
type dialect struct {
	ansi.ANSIDialect
}

// userColumn is the packed physical subject column, "type:id", "type:id#relation", or
// the "type:*" wildcard.
const userColumn = "_user"

// Column renders the subject fields with MySQL string functions, deferring every other
// column to the embedded ANSIDialect's standard packed-`_user` mapping.
func (d dialect) Column(col ansi.Column, alias string) string {
	switch col {
	case ansi.ColumnSubjectType:
		return subjectType(alias)
	case ansi.ColumnSubjectID:
		return subjectID(alias)
	case ansi.ColumnSubjectRelation:
		return subjectRelation(alias)
	default:
		return d.StandardColumn(col, alias)
	}
}

// subjectType is everything before the first ':' in `_user`.
func subjectType(alias string) string {
	u := alias + "." + userColumn
	return "SUBSTRING_INDEX(" + u + ", ':', 1)"
}

// subjectID is the segment between the first ':' and the optional '#relation' suffix:
// everything after the first ':' (SUBSTRING from LOCATE), then everything before the
// first '#' (SUBSTRING_INDEX, which yields the whole string when '#' is absent).
func subjectID(alias string) string {
	u := alias + "." + userColumn
	afterColon := "SUBSTRING(" + u + ", LOCATE(':', " + u + ") + 1)"
	return "SUBSTRING_INDEX(" + afterColon + ", '#', 1)"
}

// subjectRelation is the segment after '#', or the empty string when the subject is not a
// userset. LOCATE returns 0 when '#' is absent, in which case the field is empty.
func subjectRelation(alias string) string {
	u := alias + "." + userColumn
	return "IF(LOCATE('#', " + u + ") = 0, '', SUBSTRING(" + u + ", LOCATE('#', " + u + ") + 1))"
}
