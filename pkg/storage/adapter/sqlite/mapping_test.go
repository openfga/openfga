package sqlite

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/adapter/ast"
)

// Each mapping panics on an unmapped value, so walking every enum value guards against
// new AST members the backend has not yet handled.
func TestMappingsCoverEveryEnumValue(t *testing.T) {
	for c := range ast.ColCount {
		require.NotPanics(t, func() { sqliteColumn(c, "a") }, "column %v", c)
	}
	for ty := range ast.TypeCount {
		require.NotPanics(t, func() { sqliteType(ty) }, "cast type %v", ty)
	}
	for f := range ast.FuncCount {
		require.NotPanics(t, func() { sqliteFunc(f) }, "scalar func %v", f)
	}
	for a := range ast.AggCount_ {
		require.NotPanics(t, func() { sqliteAgg(a) }, "aggregate %v", a)
	}
	for k := range ast.KindAll + 1 {
		require.NotPanics(t, func() { sqliteSubjectKind(k, "a") }, "subject kinds %#b", k)
	}
}
