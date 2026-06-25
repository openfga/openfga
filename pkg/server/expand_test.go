package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/pkg/server/commands/v2breaking"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// setupExpandServer mirrors setupCheckServer in check_test.go but for the
// Expand entrypoint. It returns the server and a base ExpandRequest with
// store and model IDs populated. Callers fill in the tuple key. Defaults
// (when modelDSL == "" / tuples == nil) match a simple `viewer: [user]`
// model with `document:1#viewer @ user:alice`.
func setupExpandServer(t *testing.T, modelDSL string, tuples []*openfgav1.TupleKey) (*Server, *openfgav1.ExpandRequest) {
	t.Helper()

	if modelDSL == "" {
		modelDSL = `
			model
				schema 1.1
			type user
			type document
				relations
					define viewer: [user]
		`
	}

	if len(tuples) == 0 {
		tuples = []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		}
	}

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	s := MustNewServerWithOpts(WithDatastore(ds))
	t.Cleanup(s.Close)

	ctx := context.Background()

	createStoreResp, err := s.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "expand-test"})
	require.NoError(t, err)
	storeID := createStoreResp.GetId()

	model := testutils.MustTransformDSLToProtoWithID(modelDSL)
	writeModelResp, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   model.GetSchemaVersion(),
		TypeDefinitions: model.GetTypeDefinitions(),
	})
	require.NoError(t, err)
	modelID := writeModelResp.GetAuthorizationModelId()

	_, err = s.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Writes:               &openfgav1.WriteRequestWrites{TupleKeys: tuples},
	})
	require.NoError(t, err)

	return s, &openfgav1.ExpandRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
	}
}

// TestExpandBreakingChangeReason mirrors check_test.go's TestBreakingChangeReason
// but exercises v2breaking.ExpandReason. Expand has no user input, so detection
// is purely against the target relation's rewrite (and its directly-related
// usersets, for the alias shape). See ExpandReason's doc comment for why the
// Check-side shapes (self_referential_userset, computed_userset_self_object,
// ttu_userset) are not in the catalogue.
func TestExpandBreakingChangeReason(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name       string
		modelDSL   string
		seedTuple  *openfgav1.TupleKey
		objectType string
		relation   string
		want       string
	}{
		{
			name: "alias_userset",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define reader: [user]
						define allowed: reader
						define viewer: [user, document#allowed]
			`,
			seedTuple:  tuple.NewTupleKey("document:seed", "reader", "user:seed"),
			objectType: "document",
			relation:   "viewer",
			want:       v2breaking.ReasonAliasUserset,
		},
		{
			name: "userset_with_exclusion",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define member: [user]
						define owner: [user]
						define viewer: [user, document#owner] but not member
			`,
			seedTuple:  tuple.NewTupleKey("document:seed", "owner", "user:seed"),
			objectType: "document",
			relation:   "viewer",
			want:       v2breaking.ReasonUsersetWithExclusion,
		},
		{
			name: "wildcard_with_exclusion",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define public: [user:*]
						define blocked: [user]
						define viewer: public but not blocked
			`,
			seedTuple:  tuple.NewTupleKey("document:seed", "public", "user:*"),
			objectType: "document",
			relation:   "viewer",
			want:       v2breaking.ReasonWildcardWithExclusion,
		},
		{
			// Direct-assignment relation has no Difference and no aliased
			// directly-related userset → no shape matches.
			name: "no_match_direct_assignment",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user]
			`,
			seedTuple:  tuple.NewTupleKey("document:seed", "viewer", "user:seed"),
			objectType: "document",
			relation:   "viewer",
			want:       "",
		},
		{
			// Direct userset assignment without any aliasing — the directly-
			// related userset's rewrite is `This`, not a ComputedUserset, so
			// alias_userset must NOT fire.
			name: "no_match_direct_userset_assignable",
			modelDSL: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define viewer: [user, group#member]
			`,
			seedTuple:  tuple.NewTupleKey("document:seed", "viewer", "user:seed"),
			objectType: "document",
			relation:   "viewer",
			want:       "",
		},
		{
			// Difference exists in the model, but on a *sibling* relation —
			// the queried relation's rewrite has no Difference of its own.
			// Must NOT fire userset_with_exclusion (or wildcard_with_exclusion).
			name: "no_match_difference_on_sibling_relation",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define blocked: [user]
						define editor: [user] but not blocked
						define viewer: [user]
			`,
			seedTuple:  tuple.NewTupleKey("document:seed", "blocked", "user:seed"),
			objectType: "document",
			relation:   "viewer",
			want:       "",
		},
		{
			// Difference present, but no branch reachable under the base is
			// directly related to any user:* wildcard. wildcard_with_exclusion
			// must NOT fire — and since there's a Difference, the fallback is
			// userset_with_exclusion (the more conservative reason).
			name: "difference_with_no_wildcard_falls_back_to_userset_with_exclusion",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define a: [user]
						define b: [user]
						define viewer: a but not b
			`,
			seedTuple:  tuple.NewTupleKey("document:seed", "a", "user:seed"),
			objectType: "document",
			relation:   "viewer",
			want:       v2breaking.ReasonUsersetWithExclusion,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, baseReq := setupExpandServer(t, tc.modelDSL, []*openfgav1.TupleKey{tc.seedTuple})

			typesys, err := s.resolveTypesystem(context.Background(), baseReq.GetStoreId(), baseReq.GetAuthorizationModelId())
			require.NoError(t, err)

			got := v2breaking.ExpandReason(typesys, tc.objectType, tc.relation)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestExpandResponseConfirmsReason covers the response-side filter for Expand.
// Exclusion shapes confirm on any non-empty leaf; alias_userset confirms only
// when a leaf contains an aliased directly-related userset.
func TestExpandResponseConfirmsReason(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	leafUsersNode := func(users ...string) *openfgav1.UsersetTree_Node {
		return &openfgav1.UsersetTree_Node{
			Value: &openfgav1.UsersetTree_Node_Leaf{
				Leaf: &openfgav1.UsersetTree_Leaf{
					Value: &openfgav1.UsersetTree_Leaf_Users{
						Users: &openfgav1.UsersetTree_Users{Users: users},
					},
				},
			},
		}
	}
	emptyLeaf := leafUsersNode()
	unionNode := func(children ...*openfgav1.UsersetTree_Node) *openfgav1.UsersetTree_Node {
		return &openfgav1.UsersetTree_Node{
			Value: &openfgav1.UsersetTree_Node_Union{
				Union: &openfgav1.UsersetTree_Nodes{Nodes: children},
			},
		}
	}

	aliasModel := `
		model
			schema 1.1
		type user
		type document
			relations
				define reader: [user]
				define allowed: reader
				define viewer: [user, document#allowed]
	`
	s, baseReq := setupExpandServer(t,
		aliasModel,
		[]*openfgav1.TupleKey{tuple.NewTupleKey("document:seed", "reader", "user:seed")},
	)
	aliasTS, err := s.resolveTypesystem(context.Background(), baseReq.GetStoreId(), baseReq.GetAuthorizationModelId())
	require.NoError(t, err)

	tests := []struct {
		name             string
		reason           string
		targetObjectType string
		targetRelation   string
		typesys          *typesystem.TypeSystem
		tree             *openfgav1.UsersetTree
		expected         bool
	}{
		{
			name:             "userset_with_exclusion_non_empty_leaf",
			reason:           v2breaking.ReasonUsersetWithExclusion,
			targetObjectType: "document",
			targetRelation:   "viewer",
			tree:             &openfgav1.UsersetTree{Root: leafUsersNode("user:alice")},
			expected:         true,
		},
		{
			name:             "userset_with_exclusion_empty_tree",
			reason:           v2breaking.ReasonUsersetWithExclusion,
			targetObjectType: "document",
			targetRelation:   "viewer",
			tree:             &openfgav1.UsersetTree{Root: emptyLeaf},
			expected:         false,
		},
		{
			name:             "wildcard_with_exclusion_non_empty_leaf",
			reason:           v2breaking.ReasonWildcardWithExclusion,
			targetObjectType: "document",
			targetRelation:   "viewer",
			tree:             &openfgav1.UsersetTree{Root: leafUsersNode("user:*")},
			expected:         true,
		},
		{
			name:             "wildcard_with_exclusion_empty_tree",
			reason:           v2breaking.ReasonWildcardWithExclusion,
			targetObjectType: "document",
			targetRelation:   "viewer",
			tree:             &openfgav1.UsersetTree{Root: emptyLeaf},
			expected:         false,
		},
		{
			// document#allowed is a directly-related userset on `viewer` and
			// `allowed`'s rewrite is ComputedUserset(reader) → aliased.
			name:             "alias_userset_aliased_leaf_present",
			reason:           v2breaking.ReasonAliasUserset,
			targetObjectType: "document",
			targetRelation:   "viewer",
			typesys:          aliasTS,
			tree: &openfgav1.UsersetTree{Root: unionNode(
				leafUsersNode("user:alice"),
				leafUsersNode("document:d2#allowed"),
			)},
			expected: true,
		},
		{
			// Tree has users but no aliased userset — only direct users.
			name:             "alias_userset_no_aliased_leaf",
			reason:           v2breaking.ReasonAliasUserset,
			targetObjectType: "document",
			targetRelation:   "viewer",
			typesys:          aliasTS,
			tree:             &openfgav1.UsersetTree{Root: leafUsersNode("user:alice")},
			expected:         false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := v2breaking.ExpandResponseConfirmsReason(tc.reason, tc.typesys, tc.targetObjectType, tc.targetRelation, tc.tree)
			require.Equal(t, tc.expected, got)
		})
	}
}
