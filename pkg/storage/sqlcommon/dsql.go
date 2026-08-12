package sqlcommon

import (
	"strings"

	sq "github.com/Masterminds/squirrel"
)

// BuildCTESelectJoin builds a CTE with VALUES and JOIN conditions for DSQL-optimized SELECT.
// Returns the CTE prefix and JOIN conditions that can be used with Squirrel's Prefix() and Join().
// Example usage with Squirrel:
//
//	ctePrefix, joinCond, args := BuildCTESelectJoin(keys)
//	sb := stbl.Select(sqlcommon.SQLIteratorColumns()...).
//	    Prefix(ctePrefix, args...).
//	    From("tuple t").
//	    Join(joinCond).
//	    Where(sq.Eq{"t.store": store})
func BuildCTESelectJoin(keys []TupleLockKey) (string, string, []interface{}) {
	if len(keys) == 0 {
		return "", "", nil
	}

	// Build the CTE VALUES clause
	var sb strings.Builder
	args := make([]interface{}, 0, len(keys)*5) // 5 params per key

	sb.WriteString("WITH keys(object_type, object_id, relation, _user, user_type) AS (VALUES ")
	for i, k := range keys {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(?,?,?,?,?)")
		args = append(args, k.objectType, k.objectID, k.relation, k.user, k.userType)
	}
	sb.WriteString(")")

	// Build the JOIN condition
	joinCond := "keys k ON t.object_type = k.object_type AND t.object_id = k.object_id AND t.relation = k.relation AND t._user = k._user AND t.user_type = k.user_type"

	return sb.String(), joinCond, args
}

// BuildDSQLDeleteUsing builds a USING clause with VALUES for DSQL-optimized DELETE.
// Returns the USING suffix string and args that can be used with Squirrel's Suffix().
func BuildDSQLDeleteUsing(store string, deleteConditions sq.Or) (string, []interface{}) {
	if len(deleteConditions) == 0 {
		return "", nil
	}

	// Build the VALUES clause for USING
	var sb strings.Builder
	args := make([]interface{}, 0, len(deleteConditions)*5+1) // 5 params per condition + store

	sb.WriteString("USING (VALUES ")
	for i, cond := range deleteConditions {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(?,?,?,?,?)")

		// Extract values from sq.Eq condition
		eqCond := cond.(sq.Eq)
		args = append(args, eqCond["object_type"], eqCond["object_id"], eqCond["relation"], eqCond["_user"], eqCond["user_type"])
	}
	sb.WriteString(") AS k(object_type, object_id, relation, _user, user_type)")
	sb.WriteString(" WHERE t.store = ?")
	sb.WriteString(" AND t.object_type = k.object_type")
	sb.WriteString(" AND t.object_id = k.object_id")
	sb.WriteString(" AND t.relation = k.relation")
	sb.WriteString(" AND t._user = k._user")
	sb.WriteString(" AND t.user_type = k.user_type")

	// Append store as last arg (matches the WHERE t.store = ? position)
	args = append(args, store)

	return sb.String(), args
}
