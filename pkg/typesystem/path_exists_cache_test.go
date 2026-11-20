package typesystem

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

// TestPathExistsCache checks that PathExists answers the same as the uncached search, from the
// cache on repeat calls, and that results with an error are never cached.
func TestPathExistsCache(t *testing.T) {
	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1
		type user
		type employee
		type group
			relations
				define member: [user, group#member]
		type folder
			relations
				define viewer: [user:*, group#member]
		type document
			relations
				define parent: [folder]
				define viewer: [user] or viewer from parent
		`)
	typesys, err := NewAndValidate(context.Background(), model)
	require.NoError(t, err)

	// uncached runs the extracted search directly, with the same user normalization PathExists
	// uses, and also returns the cache key PathExists would use.
	uncached := func(user, relation, objectType string) (pathExistsKey, bool, error) {
		userType, _, userRelation := tuple.ToUserParts(user)
		isUserset := userRelation != ""
		userTypeRelation := userType
		if isUserset {
			userTypeRelation = tuple.ToObjectRelationString(userType, userRelation)
		}
		toLabel := tuple.ToObjectRelationString(objectType, relation)
		exists, err := typesys.pathExists(userType, userTypeRelation, toLabel, isUserset)
		return pathExistsKey{from: userTypeRelation, to: toLabel}, exists, err
	}

	cases := []struct {
		user, relation, objectType string
		expected                   bool
		expectedErr                error
	}{
		{user: "user:a", relation: "viewer", objectType: "document", expected: true},
		{user: "user:b", relation: "viewer", objectType: "document", expected: true}, // same key as user:a
		{user: "user:*", relation: "viewer", objectType: "folder", expected: true},
		{user: "group:x#member", relation: "viewer", objectType: "document", expected: true},
		{user: "employee:a", relation: "viewer", objectType: "document", expected: false},
		{user: "folder:f#viewer", relation: "member", objectType: "group", expected: false},
		{user: "unknown:a", relation: "viewer", objectType: "document", expectedErr: graph.ErrQueryingGraph},
		{user: "user:a", relation: "unknown", objectType: "document", expectedErr: graph.ErrQueryingGraph},
		{user: "group:x#unknown", relation: "viewer", objectType: "document", expectedErr: graph.ErrQueryingGraph},
	}
	for _, c := range cases {
		t.Run(c.objectType+"#"+c.relation+"@"+c.user, func(t *testing.T) {
			key, want, wantErr := uncached(c.user, c.relation, c.objectType)
			require.Equal(t, c.expected, want)
			if c.expectedErr != nil {
				require.ErrorIs(t, wantErr, c.expectedErr)
			} else {
				require.NoError(t, wantErr)
			}

			// The first call fills the cache; the second must return the same answer from it.
			for i := range 2 {
				got, err := typesys.PathExists(c.user, c.relation, c.objectType)
				require.Equal(t, wantErr, err, "call %d", i)
				require.Equal(t, c.expected, got, "call %d", i)
			}

			cached, ok := typesys.pathExistsCache.Load(key)
			if c.expectedErr != nil {
				require.False(t, ok, "an error result must not be cached")
			} else {
				require.True(t, ok)
				require.Equal(t, c.expected, cached)
			}
		})
	}

	// The cache key is the user type, not the user ID: the six error-free cases share five keys.
	n := 0
	typesys.pathExistsCache.Range(func(_, _ any) bool { n++; return true })
	require.Equal(t, 5, n)
}

// TestPathExistsCacheKeyCollision covers type names containing `|`, which the API allows. With a
// key built as from + "|" + to, (user|a -> doc#viewer) and (user -> a|doc#viewer) would share the
// key "user|a|doc#viewer", and whichever answer was cached first would be returned for both.
func TestPathExistsCacheKeyCollision(t *testing.T) {
	viewerOfUsers := func(objectType string) *openfgav1.TypeDefinition {
		return &openfgav1.TypeDefinition{
			Type:      objectType,
			Relations: map[string]*openfgav1.Userset{"viewer": This()},
			Metadata: &openfgav1.Metadata{Relations: map[string]*openfgav1.RelationMetadata{
				"viewer": {DirectlyRelatedUserTypes: []*openfgav1.RelationReference{DirectRelationReference("user", "")}},
			}},
		}
	}
	model := &openfgav1.AuthorizationModel{
		Id:            "01JCKEYCOLLISION0000000000",
		SchemaVersion: SchemaVersion1_1,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
			{Type: "user|a"},
			viewerOfUsers("doc"),
			viewerOfUsers("a|doc"),
		},
	}

	// Ask in both orders, each on a fresh TypeSystem, so either answer can be the one cached first.
	for _, falseFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("false_first=%t", falseFirst), func(t *testing.T) {
			typesys, err := New(model)
			require.NoError(t, err)

			noPath := func() {
				exists, err := typesys.PathExists("user|a:x", "viewer", "doc")
				require.NoError(t, err)
				require.False(t, exists)
			}
			path := func() {
				exists, err := typesys.PathExists("user:x", "viewer", "a|doc")
				require.NoError(t, err)
				require.True(t, exists)
			}
			if falseFirst {
				noPath()
				path()
			} else {
				path()
				noPath()
			}
		})
	}
}

// pathExistsBenchModel builds a chain of n resource types. Only t0 accepts users and group members
// directly; every later type inherits editor and viewer from its parent, so the only path from user
// to the last type's viewer runs through the whole parent chain.
func pathExistsBenchModel(n int) string {
	var b strings.Builder
	b.WriteString("model\n  schema 1.1\ntype user\ntype group\n  relations\n    define member: [user, group#member]\n")
	b.WriteString("type t0\n  relations\n    define viewer: [user, group#member]\n    define editor: [user]\n")
	for i := 1; i < n; i++ {
		fmt.Fprintf(&b, "type t%d\n  relations\n    define parent: [t%d]\n    define editor: editor from parent\n    define viewer: editor or viewer from parent\n", i, i-1)
	}
	return b.String()
}

// BenchmarkPathExists compares the uncached graph search with a cached PathExists call.
func BenchmarkPathExists(b *testing.B) {
	for _, n := range []int{10, 75} {
		model := testutils.MustTransformDSLToProtoWithID(pathExistsBenchModel(n))
		typesys, err := NewAndValidate(context.Background(), model)
		require.NoError(b, err)
		last := fmt.Sprintf("t%d", n-1)
		exists, err := typesys.pathExists("user", "user", last+"#viewer", false)
		require.NoError(b, err)
		require.True(b, exists)

		b.Run(fmt.Sprintf("types=%d/uncached", n), func(b *testing.B) {
			for b.Loop() {
				_, _ = typesys.pathExists("user", "user", last+"#viewer", false)
			}
		})
		b.Run(fmt.Sprintf("types=%d/cached", n), func(b *testing.B) {
			for b.Loop() {
				_, _ = typesys.PathExists("user:anne", "viewer", last)
			}
		})
	}
}
