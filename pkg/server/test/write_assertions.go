package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestWriteAndReadAssertions(t *testing.T, datastore storage.OpenFGADatastore) {
	type writeAssertionsTestSettings struct {
		_name      string
		model      string
		assertions []*openfgav1.Assertion
	}

	store := testutils.CreateRandomString(10)

	var tests = []writeAssertionsTestSettings{
		{
			_name: "writing_assertions_succeeds",
			model: `model
			schema 1.1
			type user
			
			type repo
			  relations
				define reader: [user]
				define can_read: reader`,
			assertions: []*openfgav1.Assertion{{
				TupleKey:    tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
				Expectation: false,
			}},
		},
		{
			_name: "writing_assertions_succeeds_when_it_is_not_directly_assignable",
			model: `model
			schema 1.1
			type user
			
			type repo
			  relations
				define reader: [user]
				define can_read: reader`,
			assertions: []*openfgav1.Assertion{{
				TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
				Expectation: false,
			}},
		},
		{
			_name: "writing_multiple_assertions_succeeds",
			model: `model
			schema 1.1
			type user
			
			type repo
			  relations
				define reader: [user]
				define can_read: reader`,
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
			_name: "writing_multiple_assertions_succeeds_when_it_is_not_directly_assignable",
			model: `model
			schema 1.1
			type user
			
			type repo
			  relations
				define reader: [user]
				define can_read: reader`,
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
			_name: "writing_empty_assertions_succeeds",
			model: `model
			schema 1.1
			type user
			
			type repo
			  relations
				define reader: [user]
				define can_read: reader`,
			assertions: []*openfgav1.Assertion{},
		},
		{
			_name: "writing_multiple_contextual_tuples_assertions_succeeds",
			model: `model
			schema 1.1
			type user
			
			type repo
			  relations
				define reader: [user with test_repo_name]
			
			condition test_repo_name(repo_name: string) {
			  repo_name == "testing"
			}`,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey: tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
					ContextualTuples: []*openfgav1.TupleKey{
						{
							User:     "user:smeadows",
							Object:   "repo:1",
							Relation: "reader",
							Condition: &openfgav1.RelationshipCondition{
								Name: "test_repo_name",
								Context: testutils.MustNewStruct(t, map[string]any{
									"repo_name": "testing",
								}),
							},
						},
					},
					Expectation: true,
				},
			},
		},
	}

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)
			modelID, err := commands.NewWriteAuthorizationModelCommand(datastore).Execute(ctx, &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         store,
				SchemaVersion:   typesystem.SchemaVersion1_1,
				TypeDefinitions: model.GetTypeDefinitions(),
				Conditions:      model.GetConditions(),
			})
			require.NoError(t, err)

			_, err = commands.NewWriteAssertionsCommand(datastore).Execute(ctx, &openfgav1.WriteAssertionsRequest{
				StoreId:              store,
				Assertions:           test.assertions,
				AuthorizationModelId: modelID.GetAuthorizationModelId(),
			})
			require.NoError(t, err)

			actualResponse, actualError := commands.NewReadAssertionsQuery(datastore).Execute(ctx, store, modelID.GetAuthorizationModelId())
			require.NoError(t, actualError)

			expectedResponse := &openfgav1.ReadAssertionsResponse{
				AuthorizationModelId: modelID.GetAuthorizationModelId(),
				Assertions:           test.assertions,
			}
			if diff := cmp.Diff(expectedResponse, actualResponse, protocmp.Transform()); diff != "" {
				t.Errorf("store mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestWriteAssertionsFailure(t *testing.T, datastore storage.OpenFGADatastore) {
	type writeAssertionsTestSettings struct {
		_name      string
		assertions []*openfgav1.Assertion
		modelID    string
		err        error
	}

	store := testutils.CreateRandomString(10)

	githubModelReq := &openfgav1.WriteAuthorizationModelRequest{
		StoreId: store,
		TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type repo
  relations
	define reader: [user]
	define can_read: reader`).GetTypeDefinitions(),
		SchemaVersion: typesystem.SchemaVersion1_1,
	}
	ctx := context.Background()

	writeAuthzModelCmd := commands.NewWriteAuthorizationModelCommand(datastore)
	modelID, err := writeAuthzModelCmd.Execute(ctx, githubModelReq)
	require.NoError(t, err)

	var tests = []writeAssertionsTestSettings{
		{
			_name: "writing_assertion_with_invalid_relation_fails",
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
			modelID: modelID.GetAuthorizationModelId(),
			err: serverErrors.ValidationError(
				fmt.Errorf("relation 'repo#invalidrelation' not found"),
			),
		},
		{
			_name: "writing_assertion_with_not_found_id",
			assertions: []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
					Expectation: false,
				},
			},
			modelID: "not_valid_id",
			err: serverErrors.AuthorizationModelNotFound(
				"not_valid_id",
			),
		},
		{
			_name: "write_conceptual_tuple_assertion_with_invalid_relation_fails",
			assertions: []*openfgav1.Assertion{
				{
					TupleKey: tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
					ContextualTuples: []*openfgav1.TupleKey{
						{
							User:     "user:jon",
							Object:   "repo:test",
							Relation: "invalidrelation",
						},
					},
					Expectation: false,
				},
			},
			modelID: modelID.GetAuthorizationModelId(),
			err: serverErrors.ValidationError(
				fmt.Errorf("relation 'repo#invalidrelation' not found"),
			),
		},
		{
			_name: "write_conceptual_tuple_assertion_with_invalid_object_fails",
			assertions: []*openfgav1.Assertion{
				{
					TupleKey: tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
					ContextualTuples: []*openfgav1.TupleKey{
						{
							User:     "user:jon",
							Object:   "invalidobject",
							Relation: "can_read",
						},
					},
					Expectation: false,
				},
			},
			modelID: modelID.GetAuthorizationModelId(),
			err: serverErrors.ValidationError(
				fmt.Errorf("invalid 'object' field format"),
			),
		},
		{
			_name: "write_conceptual_tuple_assertion_with_invalid_user_fails",
			assertions: []*openfgav1.Assertion{
				{
					TupleKey: tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
					ContextualTuples: []*openfgav1.TupleKey{
						{
							User:     "invaliduser",
							Object:   "repo:test",
							Relation: "can_read",
						},
					},
					Expectation: false,
				},
			},
			modelID: modelID.GetAuthorizationModelId(),
			err: serverErrors.ValidationError(
				fmt.Errorf("the 'user' field must be an object (e.g. document:1) or an 'object#relation' or a typed wildcard (e.g. group:*)"),
			),
		},
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			request := &openfgav1.WriteAssertionsRequest{
				StoreId:              store,
				Assertions:           test.assertions,
				AuthorizationModelId: test.modelID,
			}

			writeAssertionCmd := commands.NewWriteAssertionsCommand(datastore)
			_, err = writeAssertionCmd.Execute(ctx, request)
			require.ErrorIs(t, test.err, err)
		})
	}
}
