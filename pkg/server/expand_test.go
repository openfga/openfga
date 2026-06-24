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
)

// setupExpandServer mirrors setupCheckServer in check_test.go but for the
// Expand entrypoint. It returns the server and a base ExpandRequest with
// store and model IDs populated. Callers fill in the tuple key. Defaults
// (when modelDSL == "" / tuples == nil) match a simple `viewer: [user]`
// model with `document:1#viewer @ user:alice`.
func setupExpandServer(t *testing.T, modelDSL string, tuples []*openfgav1.TupleKey, opts ...OpenFGAServiceV1Option) (*Server, *openfgav1.ExpandRequest) {
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

	defaultOpts := []OpenFGAServiceV1Option{WithDatastore(ds)}
	defaultOpts = append(defaultOpts, opts...)

	s := MustNewServerWithOpts(defaultOpts...)
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
// usersets, for the alias shape). Self-reference is intentionally absent from
// the catalogue — Expand is already v2-aligned for that case.
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
			name: "computed_userset_self_object",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define editor: [user]
						define writer: [user]
						define viewer: editor or writer
			`,
			seedTuple:  tuple.NewTupleKey("document:seed", "editor", "user:seed"),
			objectType: "document",
			relation:   "viewer",
			want:       v2breaking.ReasonComputedUsersetSelfObj,
		},
		{
			name: "ttu_userset",
			modelDSL: `
				model
					schema 1.1
				type user
				type folder
					relations
						define viewer: [user]
				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent
			`,
			seedTuple:  tuple.NewTupleKey("document:seed", "parent", "folder:seed"),
			objectType: "document",
			relation:   "viewer",
			want:       v2breaking.ReasonTTUUserset,
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
			// Direct-assignment relation has no Difference, no ComputedUserset,
			// no TTU, no aliased directly-related userset → no shape matches.
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
		{
			// Self-reference is intentionally NOT in ExpandReason's catalogue.
			// A relation with `define viewer: [user]` queried as document#viewer
			// should not fire any reason.
			name: "no_match_self_reference_excluded",
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
