package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestWriteAndReadAssertions(t *testing.T, datastore storage.OpenFGADatastore) {
	store := ulid.Make().String()

	model := parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user

	type repo
		relations
			define reader: [user, user with condX]
			define can_read: reader

	condition condX(x :int) {
		x > 0
	}
	`)

	writeAuthzModelCmd := commands.NewWriteAuthorizationModelCommand(datastore)

	writeModelResponse, err := writeAuthzModelCmd.Execute(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         store,
		TypeDefinitions: model.GetTypeDefinitions(),
		SchemaVersion:   model.GetSchemaVersion(),
		Conditions:      model.GetConditions(),
	})
	require.NoError(t, err)
	modelID := writeModelResponse.GetAuthorizationModelId()

	type writeAssertionsTestSettings struct {
		_name                string
		inputModelID         string
		assertions           []*openfgav1.Assertion
		expectErrWhenWriting string
	}

	var tests = []writeAssertionsTestSettings{
		{
			_name:        "writing_assertion_succeeds",
			inputModelID: modelID,
			assertions: []*openfgav1.Assertion{{
				TupleKey:    tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
				Expectation: false,
			}},
		},
		{
			_name:        "writing_assertion_succeeds_when_it_is_not_directly_assignable",
			inputModelID: modelID,
			assertions: []*openfgav1.Assertion{{
				TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
				Expectation: false,
			}},
		},
		{
			_name:        "writing_multiple_assertions_succeeds",
			inputModelID: modelID,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
					Expectation: false,
				},
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "reader", "user:maria"),
					Expectation: true,
				},
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "reader", "user:jon"),
					Expectation: false,
				},
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "reader", "user:jose"),
					Expectation: true,
				},
			},
		},
		{
			_name:        "writing_multiple_assertions_succeeds_when_it_is_not_directly_assignable",
			inputModelID: modelID,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
					Expectation: false,
				},
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:maria"),
					Expectation: false,
				},
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:jon"),
					Expectation: true,
				},
			},
		},
		{
			_name:        "writing_empty_assertions_succeeds",
			inputModelID: modelID,
			assertions:   []*openfgav1.Assertion{},
		},
		{
			_name:        "writing_assertion_with_contextual_tuple_succeeds",
			inputModelID: modelID,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
					Expectation: false,
					ContextualTuples: []*openfgav1.TupleKey{
						tuple.NewTupleKey("repo:test", "reader", "user:elbuo"),
					},
				},
			},
		},
		{
			_name:        "writing_assertion_with_context_succeeds",
			inputModelID: modelID,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
					Expectation: false,
					Context: testutils.MustNewStruct(t, map[string]interface{}{
						"x": 10,
					}),
				},
			},
		},
		{
			_name:        "writing_assertion_with_contextual_tuple_with_condition_succeeds",
			inputModelID: modelID,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
					Expectation: false,
					ContextualTuples: []*openfgav1.TupleKey{
						tuple.NewTupleKeyWithCondition("repo:test", "reader", "user:elbuo", "condX",
							testutils.MustNewStruct(t, map[string]interface{}{"x": 0})),
					},
				},
			},
		},
		{
			_name:        "writing_assertion_with_contextual_tuple_with_condition_fails_because_invalid_context_parameter",
			inputModelID: modelID,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
					Expectation: false,
					ContextualTuples: []*openfgav1.TupleKey{
						tuple.NewTupleKeyWithCondition("repo:test", "reader", "user:elbuo", "condX",
							testutils.MustNewStruct(t, map[string]interface{}{"unknownparam": 0})),
					},
				},
			},
			expectErrWhenWriting: "Invalid tuple 'repo:test#reader@user:elbuo (condition condX)'. Reason: found invalid context parameter: unknownparam",
		},
		{
			_name:        "writing_assertion_with_contextual_tuple_with_condition_fails_because_undefined_condition",
			inputModelID: modelID,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
					Expectation: false,
					ContextualTuples: []*openfgav1.TupleKey{
						tuple.NewTupleKeyWithCondition("repo:test", "reader", "user:elbuo", "condundefined", nil),
					},
				},
			},
			expectErrWhenWriting: "Invalid tuple 'repo:test#reader@user:elbuo (condition condundefined)'. Reason: undefined condition",
		},
		{
			_name:        "writing_assertion_with_contextual_tuple_fails_because_contextual_tuple_is_not_directly_assignable",
			inputModelID: modelID,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
					Expectation: false,
					ContextualTuples: []*openfgav1.TupleKey{
						tuple.NewTupleKey("repo:test", "can_read", "user:elbuo"),
					},
				},
			},
			expectErrWhenWriting: "Invalid tuple 'repo:test#can_read@user:elbuo'. Reason: type 'user' is not an allowed type restriction for 'repo#can_read'",
		},
		{
			_name:        "writing_assertion_with_contextual_tuple_fails_because_invalid_relation_in_contextual_tuple",
			inputModelID: modelID,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
					Expectation: false,
					ContextualTuples: []*openfgav1.TupleKey{
						tuple.NewTupleKey("repo:test", "invalidrelation", "user:elbuo"),
					},
				},
			},
			expectErrWhenWriting: "Invalid tuple 'repo:test#invalidrelation@user:elbuo'. Reason: relation 'repo#invalidrelation' not found",
		},
		{
			_name:        "writing_assertion_with_contextual_tuple_fails_because_invalid_type_in_contextual_tuple",
			inputModelID: modelID,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
					Expectation: false,
					ContextualTuples: []*openfgav1.TupleKey{
						tuple.NewTupleKey("unknown:test", "reader", "user:elbuo"),
					},
				},
			},
			expectErrWhenWriting: "Invalid tuple 'unknown:test#reader@user:elbuo'. Reason: type 'unknown' not found",
		},
		{
			_name:        "writing_assertion_with_invalid_relation_fails",
			inputModelID: modelID,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey: tuple.NewAssertionTupleKey(
						"repo:test",
						"invalidrelation",
						"user:elbuo",
					),
					Expectation: false,
				},
			},
			expectErrWhenWriting: "relation 'repo#invalidrelation' not found",
		},
		{
			_name:        "writing_assertion_with_invalid_model_id",
			inputModelID: "not_valid_id",
			assertions: []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
					Expectation: false,
				},
			},
			expectErrWhenWriting: "Authorization Model 'not_valid_id' not found",
		},
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			_, err := commands.NewWriteAssertionsCommand(datastore).Execute(context.Background(), &openfgav1.WriteAssertionsRequest{
				StoreId:              store,
				Assertions:           test.assertions,
				AuthorizationModelId: test.inputModelID,
			})
			if test.expectErrWhenWriting != "" {
				require.ErrorContains(t, err, test.expectErrWhenWriting)
			} else {
				require.NoError(t, err)

				actualResponse, err := commands.NewReadAssertionsQuery(datastore).Execute(context.Background(), store, test.inputModelID)
				require.NoError(t, err)

				expectedResponse := &openfgav1.ReadAssertionsResponse{
					AuthorizationModelId: test.inputModelID,
					Assertions:           test.assertions,
				}
				if diff := cmp.Diff(expectedResponse, actualResponse, protocmp.Transform()); diff != "" {
					t.Errorf("store mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}
