package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/openfga/openfga/pkg/server"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestWriteAndReadAssertions(t *testing.T, s *server.Server) {
	type writeAssertionsTestSettings struct {
		_name      string
		model      string
		assertions []*openfgav1.Assertion
	}

	ctx := context.Background()

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
			  define reader: [user]
			  define can_read: reader`,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey: tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
					ContextualTuples: []*openfgav1.TupleKey{
						{
							User: "user:smeadows", Object: "repo:test", Relation: "can_read",
						},
					},
					Expectation: false,
				},
				{
					TupleKey: tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
					ContextualTuples: []*openfgav1.TupleKey{
						{
							User: "user:maria", Object: "repo:test", Relation: "can_read",
						},
					},
					Expectation: false,
				},
				{
					TupleKey: tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
					ContextualTuples: []*openfgav1.TupleKey{
						{
							User: "user:jon", Object: "repo:test", Relation: "can_read",
						},
					},
					Expectation: true,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			model := parser.MustTransformDSLToProto(test.model)
			store := ulid.Make().String()

			modelID, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         store,
				TypeDefinitions: model.GetTypeDefinitions(),
				SchemaVersion:   typesystem.SchemaVersion1_1,
				Conditions:      model.GetConditions(),
			})
			require.NoError(t, err)

			_, err = s.WriteAssertions(ctx, &openfgav1.WriteAssertionsRequest{
				StoreId:              store,
				AuthorizationModelId: modelID.GetAuthorizationModelId(),
				Assertions:           test.assertions,
			})
			require.NoError(t, err)

			actualResponse, err := s.ReadAssertions(ctx, &openfgav1.ReadAssertionsRequest{
				StoreId:              store,
				AuthorizationModelId: modelID.GetAuthorizationModelId(),
			})
			require.NoError(t, err)

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

func TestWriteAssertionsFailure(t *testing.T, s *server.Server) {
	type writeAssertionsTestSettings struct {
		_name      string
		model      string
		modelID    string
		assertions []*openfgav1.Assertion
		err        error
	}

	ctx := context.Background()

	var tests = []writeAssertionsTestSettings{
		{
			_name: "writing_assertion_with_invalid_relation_fails",
			model: `model
			schema 1.1
			type user	
			type repo
			  relations
				define reader: [user]
				define can_read: reader`,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey: tuple.NewAssertionTupleKey(
						"repo:test", "invalidrelation", "user:elbuo",
					),
					Expectation: false,
				},
			},
			err: serverErrors.ValidationError(fmt.Errorf("relation 'repo#invalidrelation' not found")),
		},
		{
			_name: "writing_assertion_with_not_found_id",
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
			},
			modelID: "not_valid_id",
			err: serverErrors.InvalidArgumentError(
				fmt.Errorf(`invalid WriteAssertionsRequest.AuthorizationModelId: value does not match regex pattern "^[ABCDEFGHJKMNPQRSTVWXYZ0-9]{26}$"`)),
		},
		{
			_name: "write_conceptual_tuple_assertion_with_invalid_relation_fails",
			model: `model
			schema 1.1
			type user	
			type repo
			  relations
				define reader: [user]
				define can_read: reader`,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey: tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
					ContextualTuples: []*openfgav1.TupleKey{
						{
							User: "user:jon", Object: "repo:test", Relation: "invalidrelation",
						},
					},
					Expectation: false,
				},
			},
			err: serverErrors.ValidationError(fmt.Errorf("relation 'repo#invalidrelation' not found")),
		},
		{
			_name: "write_conceptual_tuple_assertion_with_invalid_object_fails",
			model: `model
			schema 1.1
			type user	
			type repo
			  relations
				define reader: [user]
				define can_read: reader`,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey: tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
					ContextualTuples: []*openfgav1.TupleKey{
						{
							User: "user:jon", Object: "invalidobject", Relation: "can_read",
						},
					},
					Expectation: false,
				},
			},
			err: serverErrors.ValidationError(
				fmt.Errorf("invalid 'object' field format")),
		},
		{
			_name: "write_conceptual_tuple_assertion_with_invalid_user_fails",
			model: `model
			schema 1.1
			type user	
			type repo
			  relations
				define reader: [user]
				define can_read: reader`,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey: tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
					ContextualTuples: []*openfgav1.TupleKey{
						{
							User: "invaliduser", Object: "repo:test", Relation: "can_read",
						},
					},
					Expectation: false,
				},
			},
			err: serverErrors.ValidationError(
				fmt.Errorf("the 'user' field must be an object (e.g. document:1) or an 'object#relation' or a typed wildcard (e.g. group:*)"),
			),
		},
		{
			_name: "writing_assertion_with_max_conceptual_tuples_fails",
			model: `model
			schema 1.1
			type user	
			type repo
			  relations
				define reader: [user]
				define can_read: reader`,
			assertions: []*openfgav1.Assertion{
				{
					TupleKey: tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
					ContextualTuples: []*openfgav1.TupleKey{
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
						{
							User: "user:test", Object: "repo:test", Relation: "can_read",
						},
					},
					Expectation: false,
				},
			},
			err: serverErrors.InvalidArgumentError(
				fmt.Errorf("invalid WriteAssertionsRequest.Assertions[0]: embedded message failed validation | caused by: invalid Assertion.ContextualTuples: value must contain no more than 20 item(s)")),
		},
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store := ulid.Make().String()
			model := parser.MustTransformDSLToProto(test.model)

			modelID, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         store,
				TypeDefinitions: model.GetTypeDefinitions(),
				SchemaVersion:   typesystem.SchemaVersion1_1,
				Conditions:      model.GetConditions(),
			})
			require.NoError(t, err)

			if test.modelID != "" {
				_, err = s.WriteAssertions(ctx, &openfgav1.WriteAssertionsRequest{
					StoreId:              store,
					AuthorizationModelId: test.modelID,
					Assertions:           test.assertions,
				})
			} else {
				_, err = s.WriteAssertions(ctx, &openfgav1.WriteAssertionsRequest{
					StoreId:              store,
					AuthorizationModelId: modelID.GetAuthorizationModelId(),
					Assertions:           test.assertions,
				})
			}
			require.ErrorIs(t, test.err, err)
		})
	}
}
