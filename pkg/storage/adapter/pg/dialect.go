package pg

import (
	"strconv"

	"github.com/openfga/openfga/pkg/storage/adapter/internal/ansi"
)

// dialect renders the PostgreSQL-specific constructs, embedding ansi.ANSIDialect for
// everything that is already standard. It overrides only the two points where pg
// diverges: ordinal $N placeholders, and the subject view synthesized from the packed
// `_user` column with pg string functions.
type dialect struct {
	ansi.ANSIDialect
}

// userColumn is the packed physical subject column, "type:id", "type:id#relation", or
// the "type:*" wildcard.
const userColumn = "_user"

// Placeholder renders PostgreSQL's ordinal bind parameter, "$N".
func (dialect) Placeholder(n int) string { return "$" + strconv.Itoa(n) }

// Column renders the subject fields with pg's split_part, deferring every other column to
// the embedded ANSIDialect's standard packed-`_user` mapping.
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
	return "split_part(" + u + ", ':', 1)"
}

// subjectID is the segment between the first ':' and the optional '#relation' suffix.
func subjectID(alias string) string {
	u := alias + "." + userColumn
	afterColon := "substring(" + u + " FROM position(':' IN " + u + ") + 1)"
	return "split_part(" + afterColon + ", '#', 1)"
}

// subjectRelation is the segment after '#', or the empty string when the subject is not
// a userset (split_part returns empty when the delimiter is absent).
func subjectRelation(alias string) string {
	u := alias + "." + userColumn
	return "split_part(" + u + ", '#', 2)"
}
