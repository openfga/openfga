package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/pkg/logger"
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

	defaultOpts := append([]OpenFGAServiceV1Option{WithDatastore(ds)}, opts...)
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

// TestExpandBreakingChangeLog drives an end-to-end Expand call with an in-memory
// store, captures emitted logs via a zap observer, and asserts that the
// "potential v2 Expand resolution breaking change" log fires (or doesn't) for
// each shape — combining the shape predicate (v2breaking.ExpandReason) and the
// response-side confirmation (ExpandResponseConfirmsReason) in a single test.
//
// See ExpandReason's doc comment for why the Check-side shapes
// (self_referential_userset, computed_userset_self_object, ttu_userset) are
// not in the catalogue.
func TestExpandBreakingChangeLog(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	const logMessage = "potential v2 Expand resolution breaking change"

	tests := []struct {
		name       string
		modelDSL   string
		tuples     []*openfgav1.TupleKey
		object     string
		relation   string
		wantReason string // empty means: expect no log
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
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:d1", "viewer", "document:d2#allowed"),
			},
			object:     "document:d1",
			relation:   "viewer",
			wantReason: v2breaking.ReasonAliasUserset,
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
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:d2", "editor", "user:alice"),
			},
			object:     "document:d1",
			relation:   "viewer",
			wantReason: v2breaking.ReasonComputedUsersetSelfObj,
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
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("folder:seed", "viewer", "user:seed"),
			},
			object:     "document:d1",
			relation:   "viewer",
			wantReason: v2breaking.ReasonTTUUserset,
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
			tuples: []*openfgav1.TupleKey{
				// Direct viewer tuple so the difference's base leaf has a user.
				tuple.NewTupleKey("document:d1", "viewer", "user:alice"),
			},
			object:     "document:d1",
			relation:   "viewer",
			wantReason: v2breaking.ReasonUsersetWithExclusion,
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
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:d1", "public", "user:*"),
			},
			object:     "document:d1",
			relation:   "viewer",
			wantReason: v2breaking.ReasonWildcardWithExclusion,
		},
		{
			name: "no_match_alias_userset",
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
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:d1", "viewer", "user:alice"),
			},
			object:     "document:d1",
			relation:   "viewer",
			wantReason: "",
		},
		{
			name: "no_match_user_is_not_userset",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user]
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:d1", "viewer", "user:alice"),
			},
			object:     "document:d1",
			relation:   "viewer",
			wantReason: "",
		},
		{
			name: "no_match_direct_assignment",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user]
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:d1", "viewer", "user:alice"),
			},
			object:     "document:d1",
			relation:   "viewer",
			wantReason: "",
		},
		{
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
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:seed", "viewer", "user:seed"),
			},
			object:     "document:d1",
			relation:   "viewer",
			wantReason: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zap.WarnLevel)
			testLogger := &logger.ZapLogger{Logger: zap.New(core)}

			s, baseReq := setupExpandServer(t, tc.modelDSL, tc.tuples, WithLogger(testLogger))

			res, err := s.Expand(context.Background(), &openfgav1.ExpandRequest{
				StoreId:              baseReq.GetStoreId(),
				AuthorizationModelId: baseReq.GetAuthorizationModelId(),
				TupleKey: &openfgav1.ExpandRequestTupleKey{
					Object:   tc.object,
					Relation: tc.relation,
				},
			})
			fmt.Printf("%s", res)
			require.NoError(t, err)

			breakingLogs := logs.FilterMessage(logMessage)
			if tc.wantReason == "" {
				require.Equal(t, 0, breakingLogs.Len(), "expected no breaking-change log")
				return
			}
			require.Equal(t, 1, breakingLogs.Len(), "expected exactly one breaking-change log")
			fields := fieldMap(breakingLogs.All()[0].Context)
			require.Equal(t, tc.wantReason, fields["reason"])
		})
	}
}
