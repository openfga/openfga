package server

import (
	"context"
	"errors"
	"fmt"
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

	t.Run("logs_when_weighted_graph_fails_to_build", func(t *testing.T) {
		core, logs := observer.New(zap.WarnLevel)
		testLogger := &logger.ZapLogger{Logger: zap.New(core)}

		// This model is valid (WriteAuthorizationModel accepts it) but fails the
		// weighted-graph build: `gated = user_access and bot_access` is an
		// intersection whose branches resolve to different terminal types, which
		// triggers ErrInvalidModel in the builder. With the flag on, that forces
		// a batch-level fallback to v1 — CheckQueryV2 is never constructed — yet
		// the schema-shape detection must still fire, since the relation also
		// carries a wildcard_with_exclusion shape (`viewer = public but not
		// blocked`). This exercises the "v2 skipped because the graph failed to
		// build" branch: detection runs off the type system, not the graph.
		graphFailModel := `
			model
				schema 1.1
			type user
			type bot
			type document
				relations
					define user_access: [user]
					define bot_access: [bot]
					define gated: user_access and bot_access
					define public: [user:*]
					define blocked: [user]
					define viewer: public but not blocked
		`

		s, storeID, modelID := setupBatchCheckStore(t, graphFailModel,
			[]*openfgav1.TupleKey{tuple.NewTupleKey("document:d1", "public", "user:*")},
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
			},
		}

		_, err := s.BatchCheck(context.Background(), req)
		require.NoError(t, err)

		// Confirm we actually took the graph-build-failure branch (which logs
		// this message) rather than v2 running and falling back per item — both
		// would emit the breaking-change log, so this pins the scenario.
		require.Equal(t, 1,
			logs.FilterMessage("Weighted graph model resolution failed for batch check, falling back to main Check").Len(),
			"expected the weighted-graph build to fail for this model")

		entries := logs.FilterMessage(batchLogMessage).All()
		require.Len(t, entries, 1,
			"breaking-change log must still fire when the weighted graph fails to build")

		fields := fieldMap(entries[0].Context)
		byID, ok := fields["reasons_by_correlation_id"].(map[string]string)
		require.True(t, ok)
		require.Equal(t, map[string]string{
			"wildcard": v2breaking.ReasonWildcardWithExclusion,
		}, byID)
	})
}

// logBatchTypesystem transforms a DSL model into a validated TypeSystem for the
// direct logBatchCheckBreakingChanges unit tests below.
func logBatchTypesystem(t *testing.T, modelDSL string) *typesystem.TypeSystem {
	t.Helper()
	model := testutils.MustTransformDSLToProtoWithID(modelDSL)
	typesys, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)
	return typesys
}

// batchLogFields runs logBatchCheckBreakingChanges directly (bypassing the full
// BatchCheck pipeline) and returns the fields of the single aggregate log it
// emits, or nil if it emitted nothing. Driving the function directly lets each
// test control outcome.Allowed / outcome.Err — and the result map's contents —
// deterministically, which is the only way to reach the errored-check,
// absent-outcome, allowed-gating, and memoization branches.
func batchLogFields(t *testing.T, typesys *typesystem.TypeSystem, checks []*openfgav1.BatchCheckItem, result map[commands.CorrelationID]*commands.BatchCheckOutcome) map[string]any {
	t.Helper()

	core, logs := observer.New(zap.WarnLevel)
	s := &Server{logger: &logger.ZapLogger{Logger: zap.New(core)}}

	req := &openfgav1.BatchCheckRequest{
		StoreId:              "store-id",
		AuthorizationModelId: "model-id",
		Checks:               checks,
	}
	s.logBatchCheckBreakingChanges(context.Background(), req, typesys, result)

	entries := logs.FilterMessage("potential v2 BatchCheck resolution breaking change").All()
	if len(entries) == 0 {
		return nil
	}
	require.Len(t, entries, 1, "expected at most one aggregate breaking-change log")
	return fieldMap(entries[0].Context)
}

// item is a small helper to build a BatchCheckItem.
func item(correlationID, object, relation, user string) *openfgav1.BatchCheckItem {
	return &openfgav1.BatchCheckItem{
		TupleKey:      &openfgav1.CheckRequestTupleKey{Object: object, Relation: relation, User: user},
		CorrelationId: correlationID,
	}
}

// TestLogBatchCheckBreakingChanges_AllReasons drives logBatchCheckBreakingChanges
// directly to assert that every reason the detection can emit is produced by the
// corresponding request shape. Each case pins the (shape → reason) mapping so a
// regression in the predicate wiring is caught here rather than only in the
// end-to-end aggregate test.
func TestLogBatchCheckBreakingChanges_AllReasons(t *testing.T) {
	// A model carrying one instance of every detectable shape.
	//
	//   - self_referential_userset:      check(document:d1#viewer, viewer, document:d1)
	//   - alias_userset:                 viewer accepts document#allowed, allowed => reader
	//   - computed_userset_self_object:  can_edit computes from editor
	//   - ttu_userset:                   from_parent = viewer from parent
	//   - userset_with_exclusion:        guarded = [document#editor] but not blocked (userset user)
	//   - wildcard_with_exclusion:       pub_guarded = public but not blocked (wildcard)
	modelDSL := `
		model
			schema 1.1
		type user
		type folder
			relations
				define viewer: [user]
		type document
			relations
				define parent: [folder]
				define reader: [user]
				define allowed: reader
				define editor: [user]
				define blocked: [user]
				define public: [user:*]
				define viewer: [user, document#allowed]
				define can_edit: editor
				define from_parent: viewer from parent
				define guarded: [document#editor] but not blocked
				define pub_guarded: public but not blocked
	`
	typesys := logBatchTypesystem(t, modelDSL)

	tests := []struct {
		name           string
		check          *openfgav1.BatchCheckItem
		allowed        bool
		expectedReason string
	}{
		{
			// self_referential_userset: the user is the object's own userset
			// (document:d1#viewer on document:d1, viewer). v1 short-circuits this
			// to TRUE unconditionally; v2 evaluates it against schema+storage and
			// returns FALSE.
			name:           "self_referential_userset",
			check:          item("c", "document:d1", "viewer", "document:d1#viewer"),
			allowed:        false,
			expectedReason: v2breaking.ReasonSelfReferentialUserset,
		},
		{
			// alias_userset: viewer directly accepts document#allowed, and
			// allowed is a computed_userset alias for reader. Queried with a
			// document#reader user, v1 follows the allowed→reader alias from a
			// stored tuple; v2 does not, so the answers diverge.
			name:           "alias_userset",
			check:          item("c", "document:d1", "viewer", "document:d3#reader"),
			allowed:        false,
			expectedReason: v2breaking.ReasonAliasUserset,
		},
		{
			// computed_userset_self_object: can_edit computes from editor, and
			// the user's object equals the target object (document:d1). v1
			// returned TRUE for the self-object computed userset; v2 returns
			// FALSE.
			name:           "computed_userset_self_object",
			check:          item("c", "document:d1", "can_edit", "document:d1#editor"),
			allowed:        false,
			expectedReason: v2breaking.ReasonComputedUsersetSelfObj,
		},
		{
			// ttu_userset: from_parent = viewer from parent, and the user's
			// relation (viewer) matches the TTU's computed relation while the
			// user's type (folder) is directly related to the parent tupleset.
			// v1 returned TRUE from schema reachability plus a parent tuple; v2
			// requires the userset to be explicit.
			name:           "ttu_userset",
			check:          item("c", "document:d1", "from_parent", "folder:f2#viewer"),
			allowed:        false,
			expectedReason: v2breaking.ReasonTTUUserset,
		},
		{
			// userset_with_exclusion: guarded = editor but not blocked, queried
			// for a userset user (document:d2#editor). v2 rejects a userset user
			// reaching a Difference node at request time and falls back to v1,
			// which still produces an answer — the divergence.
			name:           "userset_with_exclusion",
			check:          item("c", "document:d1", "guarded", "document:d2#editor"),
			allowed:        false,
			expectedReason: v2breaking.ReasonUsersetWithExclusion,
		},
		{
			// Same userset_with_exclusion shape, but allowed=true: v2 rejects at
			// request time regardless of the v1 answer, so the exclusion shape is
			// flagged unconditionally (not gated on !Allowed).
			name:           "userset_with_exclusion_even_when_allowed",
			check:          item("c", "document:d1", "guarded", "document:d2#editor"),
			allowed:        true,
			expectedReason: v2breaking.ReasonUsersetWithExclusion,
		},
		{
			// wildcard_with_exclusion: pub_guarded = public but not blocked,
			// where public accepts user:*. Queried for a wildcard user, v2
			// rejects a wildcard reaching a Difference node at request time and
			// falls back to v1 — the divergence.
			name:           "wildcard_with_exclusion",
			check:          item("c", "document:d1", "pub_guarded", "user:*"),
			allowed:        false,
			expectedReason: v2breaking.ReasonWildcardWithExclusion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := map[commands.CorrelationID]*commands.BatchCheckOutcome{
				commands.CorrelationID(tt.check.GetCorrelationId()): {Allowed: tt.allowed},
			}
			fields := batchLogFields(t, typesys, []*openfgav1.BatchCheckItem{tt.check}, result)
			require.NotNil(t, fields, "expected a breaking-change log for shape %s", tt.name)

			byID, ok := fields["reasons_by_correlation_id"].(map[string]string)
			require.True(t, ok)
			require.Equal(t, tt.expectedReason, byID[tt.check.GetCorrelationId()])
		})
	}
}

// TestLogBatchCheckBreakingChanges_SkipAndGatingBranches covers the branches
// that suppress a reason: errored checks, checks absent from the result map,
// the !Allowed gating on the userset CheckReason path, and no-shape checks.
func TestLogBatchCheckBreakingChanges_SkipAndGatingBranches(t *testing.T) {
	modelDSL := `
		model
			schema 1.1
		type user
		type document
			relations
				define editor: [user]
				define writer: [user]
				define viewer: editor or writer
	`
	typesys := logBatchTypesystem(t, modelDSL)

	// computed_userset_self_object is a userset shape gated on !Allowed.
	shapedCheck := item("shaped", "document:d1", "viewer", "document:d1#writer")

	t.Run("errored_check_is_skipped", func(t *testing.T) {
		result := map[commands.CorrelationID]*commands.BatchCheckOutcome{
			"shaped": {Allowed: false, Err: errors.New("boom")},
		}
		require.Nil(t, batchLogFields(t, typesys, []*openfgav1.BatchCheckItem{shapedCheck}, result),
			"a check whose outcome errored must not contribute a reason")
	})

	t.Run("check_absent_from_result_is_skipped", func(t *testing.T) {
		// Empty result map: the check has no outcome to compare.
		result := map[commands.CorrelationID]*commands.BatchCheckOutcome{}
		require.Nil(t, batchLogFields(t, typesys, []*openfgav1.BatchCheckItem{shapedCheck}, result),
			"a check absent from the result map must not contribute a reason")
	})

	t.Run("allowed_userset_check_is_gated_out", func(t *testing.T) {
		// The userset CheckReason path only fires on !Allowed; an allowed
		// result on this (non-exclusion) shape must not log.
		result := map[commands.CorrelationID]*commands.BatchCheckOutcome{
			"shaped": {Allowed: true},
		}
		require.Nil(t, batchLogFields(t, typesys, []*openfgav1.BatchCheckItem{shapedCheck}, result),
			"an allowed userset check must be gated out of the non-exclusion path")
	})

	t.Run("no_shape_check_is_skipped", func(t *testing.T) {
		plain := item("plain", "document:d1", "editor", "user:anne")
		result := map[commands.CorrelationID]*commands.BatchCheckOutcome{
			"plain": {Allowed: true},
		}
		require.Nil(t, batchLogFields(t, typesys, []*openfgav1.BatchCheckItem{plain}, result),
			"a check whose shape matches no reason must not log")
	})
}

// TestLogBatchCheckBreakingChanges_MemoizesAndDedupes verifies that identical
// shapes across many checks are memoized (all attributed to the same reason)
// and that the aggregate reasons list deduplicates while matched_checks counts
// every flagged check.
func TestLogBatchCheckBreakingChanges_MemoizesAndDedupes(t *testing.T) {
	modelDSL := `
		model
			schema 1.1
		type user
		type document
			relations
				define editor: [user]
				define writer: [user]
				define viewer: editor or writer
	`
	typesys := logBatchTypesystem(t, modelDSL)

	// Three checks with the identical computed_userset_self_object shape (same
	// tuple key + same allowed bit) — the second and third hit the memoization
	// cache — plus one distinct no-shape check.
	checks := []*openfgav1.BatchCheckItem{
		item("a", "document:d1", "viewer", "document:d1#writer"),
		item("b", "document:d1", "viewer", "document:d1#writer"),
		item("c", "document:d1", "viewer", "document:d1#writer"),
		item("plain", "document:d1", "editor", "user:anne"),
	}
	result := map[commands.CorrelationID]*commands.BatchCheckOutcome{
		"a":     {Allowed: false},
		"b":     {Allowed: false},
		"c":     {Allowed: false},
		"plain": {Allowed: true},
	}

	fields := batchLogFields(t, typesys, checks, result)
	require.NotNil(t, fields)

	rawReasons, ok := fields["reasons"].([]any)
	require.True(t, ok)
	require.Len(t, rawReasons, 1, "distinct reasons should be deduplicated")
	require.Equal(t, v2breaking.ReasonComputedUsersetSelfObj, rawReasons[0])

	require.EqualValues(t, 3, fields["matched_checks"],
		"every flagged check counts toward matched_checks even when memoized")

	byID, ok := fields["reasons_by_correlation_id"].(map[string]string)
	require.True(t, ok)
	require.Equal(t, map[string]string{
		"a": v2breaking.ReasonComputedUsersetSelfObj,
		"b": v2breaking.ReasonComputedUsersetSelfObj,
		"c": v2breaking.ReasonComputedUsersetSelfObj,
	}, byID, "the plain no-shape check is absent")
}
