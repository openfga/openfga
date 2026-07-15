package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/featureflags"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/server/commands/v2breaking"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestBatchCheckUsesTypesystemModel(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	s := MustNewServerWithOpts(WithDatastore(ds))
	t.Cleanup(s.Close)

	createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-test",
	})
	require.NoError(t, err)

	storeID := createStoreResp.GetId()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user

		type document
			relations
				define viewer: [user]
	`)

	writeAuthModelResp, err := s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   model.GetSchemaVersion(),
		TypeDefinitions: model.GetTypeDefinitions(),
	})
	require.NoError(t, err)

	modelID := writeAuthModelResp.GetAuthorizationModelId()

	_, err = s.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId: storeID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:anne"),
			},
		},
	})
	require.NoError(t, err)

	// no model ID in this request
	batchCheckRequest := &openfgav1.BatchCheckRequest{
		StoreId: storeID,
		Checks: []*openfgav1.BatchCheckItem{
			{
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:     "user:anne",
					Relation: "viewer",
					Object:   "document:1",
				},
				CorrelationId: "id1",
			},
			{
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:     "user:bob",
					Relation: "viewer",
					Object:   "document:1",
				},
				CorrelationId: "id2",
			},
		},
	}
	firstResponse, err := s.BatchCheck(context.Background(), batchCheckRequest)
	require.NoError(t, err)

	// Now add the auth model ID to the request and send it again
	batchCheckRequest.AuthorizationModelId = modelID
	secondResponse, err := s.BatchCheck(context.Background(), batchCheckRequest)
	require.NoError(t, err)

	firstResult := firstResponse.GetResult()
	secondResult := secondResponse.GetResult()

	// Both responses should be identical
	for k, v := range firstResult {
		require.Equal(t, v, secondResult[k])
	}
}

func TestBatchCheckValidatesInboundRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	scenarios := map[string]struct {
		request       *openfgav1.BatchCheckRequest
		errorContains string
	}{
		`test_invalid_correlation_id`: {
			request: &openfgav1.BatchCheckRequest{
				Checks: []*openfgav1.BatchCheckItem{
					{
						TupleKey: &openfgav1.CheckRequestTupleKey{
							User:     "user:bob",
							Relation: "viewer",
							Object:   "document:1",
						},
						CorrelationId: "", // this is invalid and should fail
					},
				},
			},
			errorContains: "invalid BatchCheckItem.CorrelationId",
		},
		`test_empty_checks`: {
			request: &openfgav1.BatchCheckRequest{
				Checks: []*openfgav1.BatchCheckItem{},
			},
			errorContains: "invalid BatchCheckRequest.Checks",
		},
		`test_empty_tuple_key`: {
			request: &openfgav1.BatchCheckRequest{
				Checks: []*openfgav1.BatchCheckItem{
					{
						TupleKey: nil,
					},
				},
			},
			errorContains: "invalid BatchCheckItem.TupleKey",
		},
	}

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")
	s := MustNewServerWithOpts(WithDatastore(ds))
	t.Cleanup(s.Close)

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			batchReq := scenario.request
			batchReq.StoreId = ulid.Make().String()
			_, err := s.BatchCheck(context.Background(), batchReq)
			require.ErrorContains(t, err, scenario.errorContains)
		})
	}
}

func TestBatchCheckFailsIfTooManyChecks(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	numChecks := config.DefaultMaxChecksPerBatchCheck + 1
	_, ds, _ := util.MustBootstrapDatastore(t, "memory")
	s := MustNewServerWithOpts(WithDatastore(ds))
	t.Cleanup(s.Close)

	createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-test",
	})
	require.NoError(t, err)

	storeID := createStoreResp.GetId()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user

		type document
			relations
				define viewer: [user]
	`)

	_, err = s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   model.GetSchemaVersion(),
		TypeDefinitions: model.GetTypeDefinitions(),
	})
	require.NoError(t, err)

	checks := make([]*openfgav1.BatchCheckItem, numChecks)
	for i := 0; i < numChecks; i++ {
		checks[i] = &openfgav1.BatchCheckItem{
			TupleKey: &openfgav1.CheckRequestTupleKey{
				Object:   "doc:doc1",
				Relation: "viewer",
				User:     "user:justin",
			},
			CorrelationId: fmt.Sprintf("abc%d", i),
		}
	}

	request := &openfgav1.BatchCheckRequest{
		StoreId: storeID,
		Checks:  checks,
	}

	_, err = s.BatchCheck(context.Background(), request)
	msg := fmt.Sprintf(
		"batchCheck received %d checks, the maximum allowed is %d",
		numChecks,
		config.DefaultMaxChecksPerBatchCheck,
	)
	require.ErrorContains(t, err, msg)
}
func TestTransformCheckCommandErrorToBatchCheckError(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	errMsg := "oh_no"
	scenarios := map[string]struct {
		inputError     error
		expectedOutput *openfgav1.CheckError
	}{
		`test_invalid_relation_error`: {
			inputError: &commands.InvalidRelationError{Cause: errors.New(errMsg)},
			expectedOutput: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error},
				Message: "invalid relation: " + errMsg,
			},
		},
		`test_invalid_tuple_error`: {
			inputError: &commands.InvalidTupleError{Cause: errors.New(errMsg)},
			expectedOutput: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_invalid_tuple},
				Message: "invalid tuple: " + errMsg,
			},
		},
		`test_resolution_depth_error`: {
			inputError: graph.ErrResolutionDepthExceeded,
			expectedOutput: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_authorization_model_resolution_too_complex},
				Message: graph.ErrResolutionDepthExceeded.Error(),
			},
		},
		`test_condition_error`: {
			inputError: condition.ErrEvaluationFailed,
			expectedOutput: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error},
				Message: condition.ErrEvaluationFailed.Error(),
			},
		},
		`test_throttled_error`: {
			inputError: &commands.ThrottledError{Cause: errors.New(errMsg)},
			expectedOutput: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error},
				Message: "throttled: " + errMsg,
			},
		},
		`test_deadline_exceeded`: {
			inputError: context.DeadlineExceeded,
			expectedOutput: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InternalError{InternalError: openfgav1.InternalErrorCode_deadline_exceeded},
				Message: context.DeadlineExceeded.Error(),
			},
		},
		`test_generic_error`: {
			inputError: errors.New(errMsg),
			expectedOutput: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InternalError{InternalError: openfgav1.InternalErrorCode_internal_error},
				Message: errMsg,
			},
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			result := transformCheckCommandErrorToBatchCheckError(scenario.inputError)
			require.Equal(t, scenario.expectedOutput, result)
		})
	}
}

func TestTransformCheckResultToProto(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	// Create two fake outcomes, one happy path and one error
	// These two make 100% coverage
	happyOutcome := &commands.BatchCheckOutcome{Allowed: true}
	sadOutcome := &commands.BatchCheckOutcome{Err: graph.ErrResolutionDepthExceeded}

	// format the expected final output of the transform function
	expectedHappyResult := &openfgav1.BatchCheckSingleResult{
		CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true},
	}

	expectedSadResult := &openfgav1.BatchCheckSingleResult{
		CheckResult: &openfgav1.BatchCheckSingleResult_Error{
			Error: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_authorization_model_resolution_too_complex},
				Message: graph.ErrResolutionDepthExceeded.Error(),
			},
		},
	}

	t.Run("test_transform_check_result_to_proto", func(t *testing.T) {
		resultOne := transformCheckResultToProto(happyOutcome)
		resultTwo := transformCheckResultToProto(sadOutcome)
		require.Equal(t, expectedHappyResult, resultOne)
		require.Equal(t, expectedSadResult, resultTwo)
	})
}

// TestBatchCheckV2UsedWhenFlagEnabled verifies that when ExperimentalWeightedGraphCheck is
// enabled, BatchCheck routes through v2 and the v2_check_count tag is set on the context.
func TestBatchCheckV2UsedWhenFlagEnabled(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	s := MustNewServerWithOpts(
		WithDatastore(ds),
		WithFeatureFlagClient(featureflags.NewDefaultClient([]string{config.ExperimentalWeightedGraphCheck})),
	)
	t.Cleanup(s.Close)

	ctx := context.Background()

	createStoreResp, err := s.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "test"})
	require.NoError(t, err)
	storeID := createStoreResp.GetId()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]
	`)
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
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:alice"),
			},
		},
	})
	require.NoError(t, err)

	ctx = grpc_ctxtags.SetInContext(ctx, grpc_ctxtags.NewTags())

	resp, err := s.BatchCheck(ctx, &openfgav1.BatchCheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Checks: []*openfgav1.BatchCheckItem{
			{
				TupleKey:      &openfgav1.CheckRequestTupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"},
				CorrelationId: "id1",
			},
			{
				TupleKey:      &openfgav1.CheckRequestTupleKey{Object: "document:1", Relation: "viewer", User: "user:bob"},
				CorrelationId: "id2",
			},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.GetResult()["id1"].GetAllowed())
	require.False(t, resp.GetResult()["id2"].GetAllowed())

	tags := grpc_ctxtags.Extract(ctx).Values()
	v2Count, ok := tags["v2_check_count"]
	require.True(t, ok, "v2_check_count should be set in context tags")
	require.EqualValues(t, uint32(2), v2Count)

	v2Fallback, ok := tags["v2_fallback_count"]
	require.True(t, ok, "v2_fallback_count should be set in context tags")
	require.EqualValues(t, uint32(0), v2Fallback)
}

// TestBatchCheckV2FallbackCountWhenGraphParseFails verifies that when ExperimentalWeightedGraphCheck
// is enabled but the model fails weighted graph construction (ErrInvalidModel), the v2_fallback_count
// tag equals the number of checks in the batch (all fell back to v1).
func TestBatchCheckV2FallbackCountWhenGraphParseFails(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	// An intersection whose branches resolve to different terminal types triggers ErrInvalidModel
	// in the weighted graph builder, causing a batch-level fallback to v1.
	modelDSL := `
		model
			schema 1.1
		type user
		type bot
		type document
			relations
				define user_access: [user]
				define bot_access: [bot]
				define viewer: user_access and bot_access
	`

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	s := MustNewServerWithOpts(
		WithDatastore(ds),
		WithFeatureFlagClient(featureflags.NewDefaultClient([]string{config.ExperimentalWeightedGraphCheck})),
	)
	t.Cleanup(s.Close)

	ctx := context.Background()

	createStoreResp, err := s.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "test"})
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

	ctx = grpc_ctxtags.SetInContext(ctx, grpc_ctxtags.NewTags())

	_, err = s.BatchCheck(ctx, &openfgav1.BatchCheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Checks: []*openfgav1.BatchCheckItem{
			{
				TupleKey:      &openfgav1.CheckRequestTupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"},
				CorrelationId: "id1",
			},
			{
				TupleKey:      &openfgav1.CheckRequestTupleKey{Object: "document:1", Relation: "viewer", User: "user:bob"},
				CorrelationId: "id2",
			},
		},
	})
	require.NoError(t, err)

	tags := grpc_ctxtags.Extract(ctx).Values()

	v2Count, ok := tags["v2_check_count"]
	require.True(t, ok, "v2_check_count should be set in context tags")
	require.EqualValues(t, uint32(0), v2Count)

	v2Fallback, ok := tags["v2_fallback_count"]
	require.True(t, ok, "v2_fallback_count should be set in context tags")
	require.EqualValues(t, uint32(2), v2Fallback)
}

// transformCheckResultToProto takes ~100-200ns per BatchCheckOutcome, or .0001 - .0002ms per.
// At smaller batch sizes that's fine, but if sizes increase into the thousands the
// transform might be better in its own concurrent routine rather than as post-processing.
func BenchmarkTransformCheckResultToProto(b *testing.B) {
	outcomes := map[commands.CorrelationID]*commands.BatchCheckOutcome{
		"abc123": {
			Allowed: true,
		},
		"def456": {
			Err: graph.ErrResolutionDepthExceeded,
		},
	}

	b.Run("benchmark_transform_check_result_to_proto", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, v := range outcomes {
				transformCheckResultToProto(v)
			}
		}
	})
}

// setupBatchCheckStore creates a store, writes the model and tuples, and
// returns the server plus the resolved store and model IDs so BatchCheck
// breaking-change tests can assemble requests.
func setupBatchCheckStore(t *testing.T, modelDSL string, tuples []*openfgav1.TupleKey, opts ...OpenFGAServiceV1Option) (*Server, string, string) {
	t.Helper()

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	defaultOpts := append([]OpenFGAServiceV1Option{WithDatastore(ds)}, opts...)
	s := MustNewServerWithOpts(defaultOpts...)
	t.Cleanup(s.Close)

	ctx := context.Background()

	createStoreResp, err := s.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "batch-v2breaking-test"})
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

	return s, storeID, modelID
}

// TestBatchCheck_LogsBreakingChanges verifies that BatchCheck emits a single
// aggregate "potential v2 BatchCheck resolution breaking change" log covering
// the distinct reasons found across the batch, when the weighted-graph check
// feature flag is enabled.
func TestBatchCheck_LogsBreakingChanges(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	const batchLogMessage = "potential v2 BatchCheck resolution breaking change"

	// The model carries both exclusion shapes and a computed-userset shape so a
	// single batch can exercise multiple distinct reasons at once.
	modelDSL := `
		model
			schema 1.1
		type user
		type document
			relations
				define public: [user:*]
				define blocked: [user]
				define editor: [user]
				define viewer: public but not blocked
				define can_edit: editor
	`

	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:d1", "public", "user:*"),
		tuple.NewTupleKey("document:d1", "editor", "user:anne"),
	}

	t.Run("aggregates_distinct_reasons_across_batch", func(t *testing.T) {
		core, logs := observer.New(zap.WarnLevel)
		testLogger := &logger.ZapLogger{Logger: zap.New(core)}

		s, storeID, modelID := setupBatchCheckStore(t, modelDSL, tuples,
			WithLogger(testLogger),
			WithFeatureFlagClient(featureflags.NewDefaultClient([]string{config.ExperimentalWeightedGraphCheck})),
		)

		req := &openfgav1.BatchCheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Checks: []*openfgav1.BatchCheckItem{
				{
					// wildcard_with_exclusion: viewer = public but not blocked,
					// queried for a wildcard user.
					TupleKey:      &openfgav1.CheckRequestTupleKey{Object: "document:d1", Relation: "viewer", User: "user:*"},
					CorrelationId: "wildcard",
				},
				{
					// computed_userset_self_object: can_edit computes from editor,
					// queried for document:d1#editor against object document:d1.
					TupleKey:      &openfgav1.CheckRequestTupleKey{Object: "document:d1", Relation: "can_edit", User: "document:d1#editor"},
					CorrelationId: "computed",
				},
				{
					// Plain allowed direct check — should not contribute a reason.
					TupleKey:      &openfgav1.CheckRequestTupleKey{Object: "document:d1", Relation: "editor", User: "user:anne"},
					CorrelationId: "plain",
				},
			},
		}

		_, err := s.BatchCheck(context.Background(), req)
		require.NoError(t, err)

		entries := logs.FilterMessage(batchLogMessage).All()
		require.Len(t, entries, 1, "expected exactly one aggregate breaking-change log")

		fields := fieldMap(entries[0].Context)
		// zap.Strings is encoded as []interface{} by the map object encoder.
		rawReasons, ok := fields["reasons"].([]any)
		require.True(t, ok, "reasons field should be a slice")
		reasons := make([]string, len(rawReasons))
		for i, r := range rawReasons {
			reasons[i] = r.(string)
		}
		require.ElementsMatch(t, []string{
			v2breaking.ReasonWildcardWithExclusion,
			v2breaking.ReasonComputedUsersetSelfObj,
		}, reasons)
		require.EqualValues(t, 2, fields["matched_checks"])

		byID, ok := fields["reasons_by_correlation_id"].(map[string]string)
		require.True(t, ok, "reasons_by_correlation_id field should be a map[string]string")
		require.Equal(t, map[string]string{
			"wildcard": v2breaking.ReasonWildcardWithExclusion,
			"computed": v2breaking.ReasonComputedUsersetSelfObj,
		}, byID, "each flagged check should be attributed to its reason; the plain allowed check is absent")
	})

	t.Run("no_log_when_flag_disabled", func(t *testing.T) {
		core, logs := observer.New(zap.WarnLevel)
		testLogger := &logger.ZapLogger{Logger: zap.New(core)}

		// No ExperimentalWeightedGraphCheck flag — there is no v2 comparison to
		// signal about, so nothing should be logged.
		s, storeID, modelID := setupBatchCheckStore(t, modelDSL, tuples,
			WithLogger(testLogger),
		)

		req := &openfgav1.BatchCheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Checks: []*openfgav1.BatchCheckItem{
				{
					TupleKey:      &openfgav1.CheckRequestTupleKey{Object: "document:d1", Relation: "viewer", User: "user:*"},
					CorrelationId: "wildcard",
				},
			},
		}

		_, err := s.BatchCheck(context.Background(), req)
		require.NoError(t, err)

		require.Equal(t, 0, logs.FilterMessage(batchLogMessage).Len(),
			"expected no breaking-change log when weighted-graph check is disabled")
	})

	t.Run("no_log_when_no_shape_matches", func(t *testing.T) {
		core, logs := observer.New(zap.WarnLevel)
		testLogger := &logger.ZapLogger{Logger: zap.New(core)}

		// A model with no exclusion / computed-userset / TTU shapes.
		plainModel := `
			model
				schema 1.1
			type user
			type document
				relations
					define viewer: [user]
		`
		s, storeID, modelID := setupBatchCheckStore(t, plainModel,
			[]*openfgav1.TupleKey{tuple.NewTupleKey("document:d1", "viewer", "user:anne")},
			WithLogger(testLogger),
			WithFeatureFlagClient(featureflags.NewDefaultClient([]string{config.ExperimentalWeightedGraphCheck})),
		)

		req := &openfgav1.BatchCheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Checks: []*openfgav1.BatchCheckItem{
				{
					TupleKey:      &openfgav1.CheckRequestTupleKey{Object: "document:d1", Relation: "viewer", User: "user:anne"},
					CorrelationId: "allowed",
				},
				{
					TupleKey:      &openfgav1.CheckRequestTupleKey{Object: "document:d1", Relation: "viewer", User: "user:bob"},
					CorrelationId: "denied",
				},
			},
		}

		_, err := s.BatchCheck(context.Background(), req)
		require.NoError(t, err)

		require.Equal(t, 0, logs.FilterMessage(batchLogMessage).Len(),
			"expected no breaking-change log when no request shape matches")
	})
}

// --- Benchmarks for logBatchCheckBreakingChanges ---------------------------
//
// logBatchCheckBreakingChanges runs synchronously on the BatchCheck response
// path whenever weighted_graph_check is enabled, so its cost is added directly
// to request latency. The work is O(checks × schema-walk): for each check it
// runs the v2breaking shape predicates, which walk the target relation's
// rewrite tree — and, on the non-userset (wildcard) path, recurse across
// ComputedUserset/TupleToUserset edges into other relations' rewrites.
//
// These benchmarks isolate that cost (no datastore, no check pipeline — the
// function only touches s.logger) across a matrix of model complexity × batch
// size, so we can decide whether the synchronous walk needs the mitigations
// discussed (per-shape dedup, model-level early bail, or moving off the hot
// path) before it ships as the default.

// benchPlainModel has no exclusion / TTU / computed-self shapes: the common
// case where every predicate should bail cheaply.
func benchPlainModel() string {
	return `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]
	`
}

// benchShallowExclusionModel has a single wildcard-with-exclusion shape one
// level deep — the realistic "affected model" case.
func benchShallowExclusionModel() string {
	return `
		model
			schema 1.1
		type user
		type document
			relations
				define public: [user:*]
				define blocked: [user]
				define viewer: public but not blocked
	`
}

// benchDeepTTUChainModel builds the worst case for the recursive wildcard walk:
// a chain of `depth` distinct folder types linked by TTU (folderN viewer =
// viewer from parent → folderN+1), terminating in a wildcard-with-exclusion at
// the deepest level. Distinct types are required because the visited-map guard
// bounds self-recursion — only a chain of *different* (type, relation) pairs
// forces the walk to its full depth. Each check that targets the top relation
// re-walks the entire chain with no memoization.
func benchDeepTTUChainModel(depth int) string {
	var b strings.Builder
	b.WriteString("model\n\tschema 1.1\ntype user\n")

	// The terminal type carries the wildcard-with-exclusion shape.
	b.WriteString("type terminal\n\trelations\n")
	b.WriteString("\t\tdefine public: [user:*]\n")
	b.WriteString("\t\tdefine blocked: [user]\n")
	b.WriteString("\t\tdefine viewer: public but not blocked\n")

	// folder{depth-1} points at terminal; folderN points at folderN+1.
	for i := depth - 1; i >= 0; i-- {
		var childType string
		if i == depth-1 {
			childType = "terminal"
		} else {
			childType = fmt.Sprintf("folder%d", i+1)
		}
		fmt.Fprintf(&b, "type folder%d\n\trelations\n", i)
		fmt.Fprintf(&b, "\t\tdefine parent: [%s]\n", childType)
		b.WriteString("\t\tdefine viewer: viewer from parent\n")
	}

	return b.String()
}

// benchTypesystem transforms a DSL model into a validated TypeSystem for use in
// the benchmarks (mirrors the server's resolveTypesystem output).
func benchTypesystem(b *testing.B, modelDSL string) *typesystem.TypeSystem {
	b.Helper()
	model := testutils.MustTransformDSLToProtoWithID(modelDSL)
	typesys, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(b, err)
	return typesys
}

// benchBatchCheckRequest builds a BatchCheckRequest of numChecks items all
// targeting (object, relation, user). All-distinct correlation IDs match the
// real request-validation contract.
func benchBatchCheckRequest(numChecks int, object, relation, user string) *openfgav1.BatchCheckRequest {
	checks := make([]*openfgav1.BatchCheckItem, numChecks)
	for i := range numChecks {
		checks[i] = &openfgav1.BatchCheckItem{
			TupleKey:      &openfgav1.CheckRequestTupleKey{Object: object, Relation: relation, User: user},
			CorrelationId: fmt.Sprintf("corr-%d", i),
		}
	}
	return &openfgav1.BatchCheckRequest{
		StoreId:              ulid.Make().String(),
		AuthorizationModelId: ulid.Make().String(),
		Checks:               checks,
	}
}

// benchResultFor builds a result map marking every check FALSE (not allowed) so
// the CheckReason path is exercised on the userset cases (it is gated on
// !Allowed). Exclusion-shape detection runs regardless of the result.
func benchResultFor(req *openfgav1.BatchCheckRequest) map[commands.CorrelationID]*commands.BatchCheckOutcome {
	result := make(map[commands.CorrelationID]*commands.BatchCheckOutcome, len(req.GetChecks()))
	for _, check := range req.GetChecks() {
		result[commands.CorrelationID(check.GetCorrelationId())] = &commands.BatchCheckOutcome{Allowed: false}
	}
	return result
}

func BenchmarkLogBatchCheckBreakingChanges(b *testing.B) {
	// s.logger is the only server field logBatchCheckBreakingChanges touches; a
	// noop logger keeps the benchmark focused on the schema-walk cost.
	s := &Server{logger: logger.NewNoopLogger()}
	ctx := context.Background()

	scenarios := []struct {
		name     string
		modelDSL string
		object   string
		relation string
		user     string
	}{
		{
			// Common case: no shape matches, every predicate bails early.
			name:     "plain_model_no_match",
			modelDSL: benchPlainModel(),
			object:   "document:d1",
			relation: "viewer",
			user:     "user:anne",
		},
		{
			// Realistic affected model: shallow wildcard-with-exclusion. The
			// wildcard user forces the recursive walk (one level).
			name:     "shallow_exclusion_wildcard_user",
			modelDSL: benchShallowExclusionModel(),
			object:   "document:d1",
			relation: "viewer",
			user:     "user:*",
		},
	}

	batchSizes := []int{1, 10, config.DefaultMaxChecksPerBatchCheck}

	for _, sc := range scenarios {
		typesys := benchTypesystem(b, sc.modelDSL)
		for _, size := range batchSizes {
			req := benchBatchCheckRequest(size, sc.object, sc.relation, sc.user)
			result := benchResultFor(req)
			b.Run(fmt.Sprintf("%s/batch_%d", sc.name, size), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					s.logBatchCheckBreakingChanges(ctx, req, typesys, result)
				}
			})
		}
	}

	// Worst case: deep TTU chains re-walked per check. Vary depth to expose how
	// the recursive wildcard walk scales with model depth × batch size.
	for _, depth := range []int{5, 25, 50} {
		typesys := benchTypesystem(b, benchDeepTTUChainModel(depth))
		for _, size := range []int{10, config.DefaultMaxChecksPerBatchCheck} {
			// Target folder0 (top of the chain) with a wildcard user so each
			// check recurses the full depth to reach the terminal exclusion.
			req := benchBatchCheckRequest(size, "folder0:f", "viewer", "user:*")
			result := benchResultFor(req)
			b.Run(fmt.Sprintf("deep_ttu_chain/depth_%d/batch_%d", depth, size), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					s.logBatchCheckBreakingChanges(ctx, req, typesys, result)
				}
			})
		}
	}
}
