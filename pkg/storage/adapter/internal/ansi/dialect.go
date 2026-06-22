package ansi

// Column identifies a logical column of the tuple relation. A Dialect maps each to the
// SQL expression that projects it for a given table alias. This indirection lets storage
// layouts diverge: an engine that packs the subject into a single column decodes the
// subject fields with string functions, while one with discrete subject_type /
// subject_id / subject_relation columns renders them as plain column references — both
// behind the same logical accessors.
type Column int

const (
	ColumnStore Column = iota
	ColumnObjectType
	ColumnObjectID
	ColumnObjectRelation
	ColumnSubjectType
	ColumnSubjectID
	ColumnSubjectRelation
	ColumnCondition
	ColumnConditionContext
)

// Dialect customizes the small set of constructs where real SQL engines and storage
// layouts diverge from the ANSI core. Everything else — clause order, the
// comparison/logical/set algebra, joins, CASE, aggregates, ordering — is identical
// across engines and rendered directly by this package, so a Dialect stays deliberately
// narrow.
//
// The two points of divergence are:
//
//   - bind-parameter placeholders (ANSI "?", PostgreSQL "$N", ...);
//   - how each logical column maps to SQL, which differs with the physical schema (most
//     notably whether the subject is packed into a single column or stored discretely).
//
// Implement a Dialect by embedding ANSIDialect — which supplies Placeholder and the
// StandardColumn helper — and then writing Column. Column has no default precisely
// because it is schema-specific: each engine must state how its physical layout maps to
// the logical columns. A dialect over the standard schema can satisfy it in one line by
// delegating to StandardColumn; one over a split schema returns its own columns.
type Dialect interface {
	// Placeholder returns the bind-parameter placeholder for the n-th argument (1-based,
	// in the order arguments are bound). ANSI returns "?" regardless of n; an ordinal
	// dialect returns e.g. "$" + n.
	Placeholder(n int) string

	// Column returns the SQL expression that projects the given logical column for the
	// table occurrence at alias. There is no default: the mapping is schema-specific, so
	// every dialect must implement it (delegating to ANSIDialect.StandardColumn for the
	// standard layout, or returning its own columns for a split schema).
	Column(col Column, alias string) string
}

// ANSIDialect is the embeddable base for engine dialects: portable, standard-conforming
// pieces with no engine extensions. It supplies "?" placeholders and, via StandardColumn,
// the standard mapping of logical columns. It deliberately does NOT implement Column, so
// embedding it alone does not satisfy Dialect — each engine is forced to provide its own
// Column for its physical schema.
type ANSIDialect struct{}

func (ANSIDialect) Placeholder(int) string { return "?" }

// StandardColumn maps a logical column to ANSI SQL for the standard schema, where the
// object/store/condition columns are stored directly (rendered as "<alias>.<column>") and
// the subject is packed into a single column (decoded with SUBSTRING/POSITION). A
// dialect's Column delegates here for any column it does not lay out differently.
func (ANSIDialect) StandardColumn(col Column, alias string) string {
	switch col {
	case ColumnStore:
		return alias + "." + colStore
	case ColumnObjectType:
		return alias + "." + colObjectType
	case ColumnObjectID:
		return alias + "." + colObjectID
	case ColumnObjectRelation:
		return alias + "." + colRelation
	case ColumnCondition:
		return alias + "." + colConditionName
	case ColumnConditionContext:
		return alias + "." + colConditionContext
	case ColumnSubjectType:
		return ansiSubjectType(alias)
	case ColumnSubjectID:
		return ansiSubjectID(alias)
	case ColumnSubjectRelation:
		return ansiSubjectRelation(alias)
	default:
		panic("pkg/storage/adapter/internal/ansi: unknown Column")
	}
}

// standardDialect is the reference Dialect over the standard schema. It is the default
// for render-only builders (those constructed without an engine dialect) and the
// rendering baseline the package's own tests pin against.
type standardDialect struct{ ANSIDialect }

var _ Dialect = standardDialect{}

func (d standardDialect) Column(col Column, alias string) string {
	return d.StandardColumn(col, alias)
}

// The subject is stored packed in a single column as "type:id", "type:id#relation", or
// the "type:*" wildcard; the following helpers decode each logical field with ANSI string
// functions.

// ansiSubjectType is everything before the first ':' in the packed subject column.
func ansiSubjectType(alias string) string {
	u := alias + "." + colUser
	return "SUBSTRING(" + u + " FROM 1 FOR POSITION(':' IN " + u + ") - 1)"
}

// ansiSubjectID is the segment between the first ':' and the optional '#relation' suffix.
func ansiSubjectID(alias string) string {
	u := alias + "." + colUser
	rest := "SUBSTRING(" + u + " FROM POSITION(':' IN " + u + ") + 1)"
	// rest is everything after the first ':'; strip any '#relation' suffix. POSITION
	// returns 0 when '#' is absent, in which case rest is already the id.
	return "CASE WHEN POSITION('#' IN " + rest + ") = 0 THEN " + rest +
		" ELSE SUBSTRING(" + rest + " FROM 1 FOR POSITION('#' IN " + rest + ") - 1) END"
}

// ansiSubjectRelation is the segment after '#', or the empty string when the subject is
// not a userset.
func ansiSubjectRelation(alias string) string {
	u := alias + "." + colUser
	return "CASE WHEN POSITION('#' IN " + u + ") = 0 THEN '' ELSE SUBSTRING(" + u +
		" FROM POSITION('#' IN " + u + ") + 1) END"
}
