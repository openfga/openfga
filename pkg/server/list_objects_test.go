package server

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands/v2breaking"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// recordingStreamServer records every object the server streams to the client,
// mirroring what a real gRPC client would have already observed by the time the
// RPC returns its trailing error.
type recordingStreamServer struct {
	grpc.ServerStream
	ctx context.Context

	mu      sync.Mutex
	objects []string
}

func (s *recordingStreamServer) Context() context.Context { return s.ctx }

func (s *recordingStreamServer) Send(r *openfgav1.StreamedListObjectsResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects = append(s.objects, r.GetObject())
	return nil
}

func (s *recordingStreamServer) sent() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.objects...)
}

// TestListObjectsUnevaluableExclusion asserts that ListObjects and
// StreamedListObjects both fail closed when the subtract branch of a `but not`
// cannot be evaluated: an object may not be returned if the exclusion that
// applies to it could not be resolved.
//
// user:anne is a viewer of document:1 but is also `restricted` via a
// conditioned tuple. The request context omits the condition parameter `x`, so
// evaluating the subtract branch errors instead of cleanly returning false.
func TestListObjectsUnevaluableExclusion(t *testing.T) {
	ctx := context.Background()

	ds := memory.New()
	t.Cleanup(ds.Close)

	s := MustNewServerWithOpts(
		WithDatastore(ds),
		WithExperimentals(serverconfig.ExperimentalPipelineListObjects),
	)
	t.Cleanup(s.Close)

	storeID := ulid.Make().String()

	model := parser.MustTransformDSLToProto(`
		model
		  schema 1.1
		type user
		type document
		  relations
		    define restricted: [user with cond]
		    define viewer: [user] but not restricted
		condition cond(x: int) {
		  x > 0
		}`)

	writeModelResp, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		TypeDefinitions: model.GetTypeDefinitions(),
		SchemaVersion:   typesystem.SchemaVersion1_1,
		Conditions:      model.GetConditions(),
	})
	require.NoError(t, err)
	modelID := writeModelResp.GetAuthorizationModelId()

	_, err = s.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{Object: "document:1", Relation: "viewer", User: "user:anne"},
				{
					Object:    "document:1",
					Relation:  "restricted",
					User:      "user:anne",
					Condition: &openfgav1.RelationshipCondition{Name: "cond"},
				},
			},
		},
	})
	require.NoError(t, err)

	// Context deliberately omits the required parameter `x`, so evaluating
	// `cond` on the subtract side errors rather than returning false.
	badContext, err := structpb.NewStruct(map[string]any{})
	require.NoError(t, err)

	t.Run("list_objects_returns_no_objects", func(t *testing.T) {
		resp, err := s.ListObjects(ctx, &openfgav1.ListObjectsRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Type:                 "document",
			Relation:             "viewer",
			User:                 "user:anne",
			Context:              badContext,
		})
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("streamed_list_objects_sends_no_objects", func(t *testing.T) {
		srv := &recordingStreamServer{ctx: ctx}

		err := s.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Type:                 "document",
			Relation:             "viewer",
			User:                 "user:anne",
			Context:              badContext,
		}, srv)

		require.Error(t, err)
		require.Empty(t, srv.sent(),
			"objects were streamed although their exclusion could not be evaluated: %v", srv.sent())
	})
}

func setupListObjectsServer(t *testing.T, modelDSL string, tuples []*openfgav1.TupleKey, opts ...OpenFGAServiceV1Option) (*Server, *openfgav1.ListObjectsRequest) {
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

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	defaultOpts := append([]OpenFGAServiceV1Option{WithDatastore(ds)}, opts...)
	s := MustNewServerWithOpts(defaultOpts...)
	t.Cleanup(s.Close)

	ctx := context.Background()

	createStoreResp, err := s.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "list-objects-test"})
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

	if len(tuples) > 0 {
		_, err = s.Write(ctx, &openfgav1.WriteRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Writes:               &openfgav1.WriteRequestWrites{TupleKeys: tuples},
		})
		require.NoError(t, err)
	}

	return s, &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
	}
}

// TestListObjectsBreakingChangeLog drives an end-to-end ListObjects call with an
// in-memory store, captures emitted logs via a zap observer, and asserts that
// the "potential v2 ListObjects resolution breaking change" log fires (or
// doesn't) for each shape, exercising the shape predicate v2breaking.ListObjectsReason.
//
// The subject (ListObjects `user`) plays the role the filter plays in ListUsers,
// so the cases mirror TestListUsersBreakingChangeLog with the user-side expressed
// as a subject string. Detection is schema-shape only and intentionally fires
// even when the v2 pipeline routes userset/wildcard subjects to the legacy
// algorithm — those are the shapes whose resolution could change once v2 takes
// over that path.
func TestListObjectsBreakingChangeLog(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	const logMessage = "potential v2 ListObjects resolution breaking change"

	tests := []struct {
		name        string
		modelDSL    string
		tuples      []*openfgav1.TupleKey
		objectType  string
		relation    string
		subject     string
		wantObjects []string // objects the response must contain (asserted verbatim)
		wantReason  string   // empty means: expect no log
	}{
		{
			name: "self_referential_userset",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user, document#viewer]
			`,
			tuples:      []*openfgav1.TupleKey{},
			objectType:  "document",
			relation:    "viewer",
			subject:     "document:d1#viewer",
			wantObjects: []string{"document:d1"},
			wantReason:  v2breaking.ReasonSelfReferentialUserset,
		},
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
				tuple.NewTupleKey("document:d1", "viewer", "document:d3#allowed"),
			},
			objectType:  "document",
			relation:    "viewer",
			subject:     "document:d3#reader",
			wantObjects: []string{"document:d1"},
			wantReason:  v2breaking.ReasonAliasUserset,
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
				tuple.NewTupleKey("document:d1", "editor", "user:alice"),
			},
			objectType:  "document",
			relation:    "viewer",
			subject:     "document:d1#writer",
			wantObjects: []string{"document:d1"},
			wantReason:  v2breaking.ReasonComputedUsersetSelfObj,
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
				tuple.NewTupleKey("document:d1", "parent", "folder:f1"),
			},
			objectType:  "document",
			relation:    "viewer",
			subject:     "folder:f1#viewer",
			wantObjects: []string{"document:d1"},
			wantReason:  v2breaking.ReasonTTUUserset,
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
				tuple.NewTupleKey("document:d1", "viewer", "document:d1#owner"),
			},
			objectType:  "document",
			relation:    "viewer",
			subject:     "document:d1#owner",
			wantObjects: []string{"document:d1"},
			wantReason:  v2breaking.ReasonUsersetWithExclusion,
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
				tuple.NewTupleKey("document:d1", "blocked", "user:alice"),
			},
			objectType:  "document",
			relation:    "viewer",
			subject:     "user:*",
			wantObjects: []string{"document:d1"},
			wantReason:  v2breaking.ReasonWildcardWithExclusion,
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
			objectType:  "document",
			relation:    "viewer",
			subject:     "user:alice",
			wantObjects: []string{"document:d1"},
			wantReason:  "",
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
			objectType:  "document",
			relation:    "viewer",
			subject:     "group:g1#member",
			wantObjects: []string{},
			wantReason:  "",
		},
		{
			// Same object as target, but subject's relation is not a ComputedUserset
			// leaf in the rewrite. computed_userset_self_object must NOT fire.
			name: "no_match_computed_userset_relation_not_in_rewrite",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define editor: [user]
						define writer: [user]
						define other: [user]
						define viewer: editor or writer
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:seed", "editor", "user:seed"),
			},
			objectType:  "document",
			relation:    "viewer",
			subject:     "document:d1#other",
			wantObjects: []string{},
			wantReason:  "",
		},
		{
			// TTU exists (viewer from parent) but the subject's relation does not
			// match the TTU's computed relation. ttu_userset must NOT fire.
			name: "no_match_ttu_user_relation_mismatch",
			modelDSL: `
				model
					schema 1.1
				type user
				type folder
					relations
						define viewer: [user]
						define editor: [user]
				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:seed", "parent", "folder:seed"),
			},
			objectType:  "document",
			relation:    "viewer",
			subject:     "folder:f1#editor",
			wantObjects: []string{},
			wantReason:  "",
		},
		{
			// The ttu_userset shape matches, but no document has folder:f1 as a
			// parent, so v1 surfaces no object and the response is empty. The
			// response-confirmation gate must suppress the log even though the
			// shape fires. This models the future state: once the v2 pipeline
			// resolves this shape consistently with v2, the divergent object
			// drops out of the response and the log stops firing — exactly what
			// should retire these tests.
			name: "no_log_shape_matches_but_response_empty",
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
				tuple.NewTupleKey("document:seed", "parent", "folder:seed"),
			},
			objectType:  "document",
			relation:    "viewer",
			subject:     "folder:f1#viewer",
			wantObjects: []string{},
			wantReason:  "",
		},
	}

	// Every divergent shape here uses a userset or wildcard subject, which the
	// v2 pipeline routes to the legacy algorithm regardless of the flag. Running
	// both modes proves the logging behaves identically today; once the pipeline
	// handles these shapes itself, the pipeline-enabled run's response-
	// confirmation gate will change and these tests will flag it.
	for _, pipelineEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("pipeline_enabled=%t", pipelineEnabled), func(t *testing.T) {
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					core, logs := observer.New(zap.InfoLevel)
					testLogger := &logger.ZapLogger{Logger: zap.New(core)}

					s, baseReq := setupListObjectsServer(t, tc.modelDSL, tc.tuples,
						WithLogger(testLogger),
						WithListObjectsPipelineEnabled(pipelineEnabled),
					)

					res, err := s.ListObjects(context.Background(), &openfgav1.ListObjectsRequest{
						StoreId:              baseReq.GetStoreId(),
						AuthorizationModelId: baseReq.GetAuthorizationModelId(),
						Type:                 tc.objectType,
						Relation:             tc.relation,
						User:                 tc.subject,
					})
					require.NoError(t, err)
					require.ElementsMatch(t, tc.wantObjects, res.GetObjects(),
						"response objects drive the confirmation gate; assert them explicitly")

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
		})
	}
}
