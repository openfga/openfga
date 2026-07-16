package sqlite

import (
	"github.com/openfga/openfga/pkg/storage/adapter/ansi"
)

// dialect renders the SQLite-specific constructs, embedding ansi.ANSIDialect for
// everything that is already standard. SQLite's bind placeholder is "?", the ANSI
// default, so Placeholder needs no override. The sole divergence is the physical schema:
// unlike the packed-`_user` layout the ANSIDialect assumes, the SQLite `tuple` table
// stores the subject discretely across user_object_type / user_object_id / user_relation
// columns (see assets/migrations/sqlite), so the subject view maps to plain column
// references with no string-function decoding.
type dialect struct {
	ansi.ANSIDialect
}

// Split physical subject columns of the SQLite `tuple` table. The subject is stored
// discretely rather than packed into a single `_user` column, so each logical subject
// field is a direct column reference. (The physical `user_type` column — 'user' /
// 'userset' / wildcard — is not part of the logical view and is never referenced here.)
const (
	colSubjectType     = "user_object_type"
	colSubjectID       = "user_object_id"
	colSubjectRelation = "user_relation"
)

// Column renders the subject fields as direct references to SQLite's split subject
// columns, deferring every other column to the embedded ANSIDialect's standard mapping.
func (d dialect) Column(col ansi.Column, alias string) string {
	switch col {
	case ansi.ColumnSubjectType:
		return alias + "." + colSubjectType
	case ansi.ColumnSubjectID:
		return alias + "." + colSubjectID
	case ansi.ColumnSubjectRelation:
		return alias + "." + colSubjectRelation
	default:
		return d.StandardColumn(col, alias)
	}
}
