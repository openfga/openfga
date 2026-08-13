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

	tests := listObjectsBreakingChangeCases()

	// Every divergent shape here uses a userset or wildcard subject, which the
	// v2 pipeline routes to the legacy algorithm regardless of the flag. Running
	// both modes proves the logging behaves identically today; once the pipeline
	// handles these shapes itself, the pipeline-enabled run's response-
	// confirmation gate will change and these tests will flag it.
	for _, pipelineEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("pipeline_enabled=%t", pipelineEnabled), func(t *testing.T) {
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					core, logs := observer.New(zap.WarnLevel)
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

// TestStreamedListObjectsBreakingChangeLog is the streaming counterpart of
// TestListObjectsBreakingChangeLog. StreamedListObjects emits the divergence log
// from the command layer (ExecuteStreamed) because the handler never sees the
// streamed objects, so we capture the sent objects to drive the same response-
// confirmation gate the unary test asserts.
func TestStreamedListObjectsBreakingChangeLog(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	const logMessage = "potential v2 StreamedListObjects resolution breaking change"

	tests := listObjectsBreakingChangeCases()

	for _, pipelineEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("pipeline_enabled=%t", pipelineEnabled), func(t *testing.T) {
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					// InfoLevel so logger.Level() satisfies the <= InfoLevel gate
					// in ExecuteStreamed while still capturing the Warn log.
					core, logs := observer.New(zap.InfoLevel)
					testLogger := &logger.ZapLogger{Logger: zap.New(core)}

					s, baseReq := setupListObjectsServer(t, tc.modelDSL, tc.tuples,
						WithLogger(testLogger),
						WithListObjectsPipelineEnabled(pipelineEnabled),
					)

					srv := newCapturingStreamServer(context.Background())
					err := s.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
						StoreId:              baseReq.GetStoreId(),
						AuthorizationModelId: baseReq.GetAuthorizationModelId(),
						Type:                 tc.objectType,
						Relation:             tc.relation,
						User:                 tc.subject,
					}, srv)
					require.NoError(t, err)
					require.ElementsMatch(t, tc.wantObjects, srv.objects,
						"streamed objects drive the confirmation gate; assert them explicitly")

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

// capturingStreamServer records the objects sent by StreamedListObjects so tests
// can assert them, mirroring the unary handler's response objects.
type capturingStreamServer struct {
	openfgav1.OpenFGAService_StreamedListObjectsServer
	ctx     context.Context
	objects []string
}

func newCapturingStreamServer(ctx context.Context) *capturingStreamServer {
	return &capturingStreamServer{ctx: ctx}
}

func (m *capturingStreamServer) Context() context.Context {
	return m.ctx
}

func (m *capturingStreamServer) Send(resp *openfgav1.StreamedListObjectsResponse) error {
	m.objects = append(m.objects, resp.GetObject())
	return nil
}

// listObjectsBreakingChangeCase describes a request shape and the breaking-change
// log expected for it. The subject (ListObjects `user`) plays the role the filter
// plays in ListUsers, so the cases mirror TestListUsersBreakingChangeLog with the
// user-side expressed as a subject string.
type listObjectsBreakingChangeCase struct {
	name        string
	modelDSL    string
	tuples      []*openfgav1.TupleKey
	objectType  string
	relation    string
	subject     string
	wantObjects []string // objects the response must contain (asserted verbatim)
	wantReason  string   // empty means: expect no log
}

func listObjectsBreakingChangeCases() []listObjectsBreakingChangeCase {
	return []listObjectsBreakingChangeCase{
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
			// Multiple objects come back but only document:d1 (the subject's own
			// object) is the one v1 structurally surfaces via the self-referential
			// path, so it is the only one that confirms the reason. document:d2 and
			// document:d3 are ordinary viewers of the subject userset and would not
			// confirm on their own. This exercises the streaming fold: it must still
			// fire the log when just one of several streamed objects confirms.
			name: "self_referential_userset_multiple_objects",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user, document#viewer]
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:d2", "viewer", "document:d1#viewer"),
				tuple.NewTupleKey("document:d3", "viewer", "document:d1#viewer"),
			},
			objectType:  "document",
			relation:    "viewer",
			subject:     "document:d1#viewer",
			wantObjects: []string{"document:d1", "document:d2", "document:d3"},
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
}
