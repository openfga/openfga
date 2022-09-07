package test

import (
	"context"
	"errors"
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type writeCommandTest struct {
	_name           string
	typeDefinitions []*openfgapb.TypeDefinition
	tuples          []*openfgapb.TupleKey
	request         *openfgapb.WriteRequest
	err             error
	response        *openfgapb.WriteResponse
}

var tk = &openfgapb.TupleKey{
	Object:   "repository:openfga/openfga",
	Relation: "administrator",
	User:     "github|alice@openfga",
}

var writeCommandTests = []writeCommandTest{
	{
		_name: "ExecuteWithEmptyWritesAndDeletesReturnsZeroWrittenAndDeleted",
		// input
		request: &openfgapb.WriteRequest{},
		// output
		err: serverErrors.InvalidWriteInput,
	},
	{
		_name: "ExecuteWithSameTupleInWritesReturnsError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgapb.Userset{
					"administrator": {},
				},
			},
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{tk, tk}},
		},
		// output
		err: serverErrors.DuplicateTupleInWrite(tk),
	},
	{
		_name: "ExecuteWithWriteToIndirectUnionRelationshipReturnsError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgapb.Userset{
					"writer": {Userset: &openfgapb.Userset_This{}},
					"owner":  {Userset: &openfgapb.Userset_This{}},
					"viewer": {
						Userset: &openfgapb.Userset_Union{
							Union: &openfgapb.Usersets{
								Child: []*openfgapb.Userset{
									{Userset: &openfgapb.Userset_ComputedUserset{
										ComputedUserset: &openfgapb.ObjectRelation{
											Object:   "",
											Relation: "writer",
										},
									}},
									{Userset: &openfgapb.Userset_ComputedUserset{
										ComputedUserset: &openfgapb.ObjectRelation{
											Object:   "",
											Relation: "owner",
										},
									}},
								},
							},
						},
					},
				},
			},
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Object:   "repository:openfga/openfga",
				Relation: "viewer",
				User:     "github|alice@openfga.com",
			}}},
		},
		// output
		err: serverErrors.WriteToIndirectRelationError("Attempting to write directly to an indirect only relationship", &openfgapb.TupleKey{
			Object:   "repository:openfga/openfga",
			Relation: "viewer",
			User:     "github|alice@openfga.com",
		}),
	},
	{
		_name: "ExecuteWithWriteToIndirectIntersectionRelationshipReturnsError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgapb.Userset{
					"writer": {Userset: &openfgapb.Userset_This{}},
					"owner":  {Userset: &openfgapb.Userset_This{}},
					"viewer": {
						Userset: &openfgapb.Userset_Intersection{
							Intersection: &openfgapb.Usersets{
								Child: []*openfgapb.Userset{
									{Userset: &openfgapb.Userset_ComputedUserset{
										ComputedUserset: &openfgapb.ObjectRelation{
											Object:   "",
											Relation: "writer",
										},
									}},
									{Userset: &openfgapb.Userset_ComputedUserset{
										ComputedUserset: &openfgapb.ObjectRelation{
											Object:   "",
											Relation: "owner",
										},
									}},
								},
							},
						},
					},
				},
			},
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Object:   "repository:openfga/openfga",
				Relation: "viewer",
				User:     "github|alice@openfga.com",
			}}},
		},
		// output
		err: serverErrors.WriteToIndirectRelationError("Attempting to write directly to an indirect only relationship", &openfgapb.TupleKey{
			Object:   "repository:openfga/openfga",
			Relation: "viewer",
			User:     "github|alice@openfga.com",
		}),
	},
	{
		_name: "ExecuteWithWriteToIndirectDifferenceRelationshipReturnsError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgapb.Userset{
					"writer": {Userset: &openfgapb.Userset_This{}},
					"owner":  {Userset: &openfgapb.Userset_This{}},
					"banned": {Userset: &openfgapb.Userset_This{}},
					"viewer": {
						Userset: &openfgapb.Userset_Difference{
							Difference: &openfgapb.Difference{
								Base: &openfgapb.Userset{
									Userset: &openfgapb.Userset_Union{
										Union: &openfgapb.Usersets{
											Child: []*openfgapb.Userset{
												{Userset: &openfgapb.Userset_ComputedUserset{
													ComputedUserset: &openfgapb.ObjectRelation{
														Object:   "",
														Relation: "writer",
													},
												}},
												{Userset: &openfgapb.Userset_ComputedUserset{
													ComputedUserset: &openfgapb.ObjectRelation{
														Object:   "",
														Relation: "owner",
													},
												}},
											},
										},
									},
								},
								Subtract: &openfgapb.Userset{
									Userset: &openfgapb.Userset_ComputedUserset{
										ComputedUserset: &openfgapb.ObjectRelation{
											Relation: "banned",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Object:   "repository:openfga/openfga",
				Relation: "viewer",
				User:     "github|alice@openfga.com",
			}}},
		},
		// output
		err: serverErrors.WriteToIndirectRelationError("Attempting to write directly to an indirect only relationship", &openfgapb.TupleKey{
			Object:   "repository:openfga/openfga",
			Relation: "viewer",
			User:     "github|alice@openfga.com",
		}),
	},
	{
		_name: "ExecuteWithWriteToIndirectComputerUsersetRelationshipReturnsError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgapb.Userset{
					"writer": {Userset: &openfgapb.Userset_This{}},
					"owner":  {Userset: &openfgapb.Userset_This{}},
					"viewer": {
						Userset: &openfgapb.Userset_ComputedUserset{
							ComputedUserset: &openfgapb.ObjectRelation{
								Object:   "",
								Relation: "writer",
							},
						},
					},
				},
			},
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Object:   "repository:openfga/openfga",
				Relation: "viewer",
				User:     "github|alice@openfga.com",
			}}},
		},
		// output
		err: serverErrors.WriteToIndirectRelationError("Attempting to write directly to an indirect only relationship", &openfgapb.TupleKey{
			Object:   "repository:openfga/openfga",
			Relation: "viewer",
			User:     "github|alice@openfga.com",
		}),
	},
	{
		_name: "ExecuteWithWriteToIndirectTupleToUsersetRelationshipReturnsError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgapb.Userset{
					"writer": {Userset: &openfgapb.Userset_This{}},
					"owner":  {Userset: &openfgapb.Userset_This{}},
					"viewer": {
						Userset: &openfgapb.Userset_TupleToUserset{
							TupleToUserset: &openfgapb.TupleToUserset{
								Tupleset: &openfgapb.ObjectRelation{
									Object:   "",
									Relation: "writer",
								},
								ComputedUserset: &openfgapb.ObjectRelation{
									Object:   "",
									Relation: "writer",
								},
							},
						},
					},
				},
			},
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Object:   "repository:openfga/openfga",
				Relation: "viewer",
				User:     "github|alice@openfga.com",
			}}},
		},
		// output
		err: serverErrors.WriteToIndirectRelationError("Attempting to write directly to an indirect only relationship", &openfgapb.TupleKey{
			Object:   "repository:openfga/openfga",
			Relation: "viewer",
			User:     "github|alice@openfga.com",
		}),
	},
	{
		_name: "ExecuteWithSameTupleInDeletesReturnsError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgapb.Userset{
					"administrator": {},
				},
			},
		},
		// input
		request: &openfgapb.WriteRequest{
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{tk, tk}},
		},
		// output
		err: serverErrors.DuplicateTupleInWrite(tk),
	},
	{
		_name: "ExecuteWithSameTupleInWritesAndDeletesReturnsError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgapb.Userset{
					"administrator": {},
				},
			},
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes:  &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{tk}},
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{tk}},
		},
		// output
		err: serverErrors.DuplicateTupleInWrite(tk),
	},
	{
		_name: "ExecuteDeleteTupleWhichDoesNotExistReturnsError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgapb.Userset{
					"administrator": {},
				},
			},
		},
		// input
		request: &openfgapb.WriteRequest{
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{tk}},
		},
		// output
		err: serverErrors.WriteFailedDueToInvalidInput(storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_DELETE)),
	},
	{
		_name: "ExecuteWithWriteTupleWithInvalidAuthorizationModelReturnsError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgapb.Userset{},
		}},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{tk}},
		},
		// output
		err: serverErrors.TypeNotFound("repository"),
	},
	{
		_name: "ExecuteWithWriteTupleWithMissingUserError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgapb.Userset{
				"owner": {},
			},
		}},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Object:   "repo:openfga",
				Relation: "owner",
			}}},
		},
		// output
		err: serverErrors.InvalidTuple("the 'user' field must be a non-empty string", &openfgapb.TupleKey{Object: "repo:openfga", Relation: "owner"}),
	},
	{
		_name: "ExecuteWithWriteTupleWithMissingObjectError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgapb.Userset{
				"owner": {},
			},
		}},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Relation: "owner",
				User:     "elbuo@github.com",
			}}},
		},
		// output
		err: serverErrors.InvalidObjectFormat(&openfgapb.TupleKey{
			Relation: "owner",
			User:     "elbuo@github.com",
		}),
	},
	{
		_name: "ExecuteWithWriteTupleWithInvalidRelationError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgapb.Userset{
				"owner": {},
			},
		}},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Object: "repo:openfga",
				User:   "elbuo@github.com",
			}}},
		},
		// output
		err: serverErrors.InvalidTuple("invalid relation", &openfgapb.TupleKey{Object: "repo:openfga", User: "elbuo@github.com"}),
	},
	{
		_name: "ExecuteWithWriteTupleWithNotFoundRelationError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgapb.Userset{
				"owner": {},
			},
		}},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Object:   "repo:openfga",
				Relation: "BadRelation",
				User:     "elbuo@github.com",
			}}},
		},
		// output
		err: serverErrors.RelationNotFound("BadRelation", "repo",
			&openfgapb.TupleKey{Object: "repo:openfga", Relation: "BadRelation", User: "elbuo@github.com"}),
	},
	{
		_name: "ExecuteDeleteTupleWithInvalidAuthorizationModelIgnoresAuthorizationModelValidation",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgapb.Userset{},
		}},
		tuples: []*openfgapb.TupleKey{tk},
		// input
		request: &openfgapb.WriteRequest{
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{tk}},
		},
	},
	{
		_name: "ExecuteWithInvalidObjectFormatReturnsError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgapb.Userset{},
		}},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				// invalid because it has no :
				Object:   "openfga",
				Relation: "owner",
				User:     "github|jose@openfga",
			}}},
		},
		// output
		err: serverErrors.InvalidObjectFormat(&openfgapb.TupleKey{
			Object:   "openfga",
			Relation: "owner",
			User:     "github|jose@openfga",
		}),
	},
	{
		_name: "ExecuteReturnsErrorIfWriteRelationDoesNotExistInAuthorizationModel",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgapb.Userset{
				"admin": {},
			},
		},
			{
				Type: "org",
				Relations: map[string]*openfgapb.Userset{
					"manages": {},
				},
			}},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				{
					Object:   "repo:openfga/openfga",
					Relation: "writer",
					User:     "github|jose@openfga",
				},
			}},
		},
		// output
		err: serverErrors.RelationNotFound("writer", "repo", &openfgapb.TupleKey{
			Object:   "repo:openfga/openfga",
			Relation: "writer",
			User:     "github|jose@openfga",
		}),
	},
	{
		_name: "ExecuteReturnsSuccessIfDeleteRelationDoesNotExistInAuthorizationModel",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"admin": {},
				},
			},
			{
				Type: "org",
				Relations: map[string]*openfgapb.Userset{
					"manages": {},
				},
			}},
		tuples: []*openfgapb.TupleKey{
			{
				Object:   "org:openfga",
				Relation: "owner",
				User:     "github|jose@openfga",
			},
		},
		// input
		request: &openfgapb.WriteRequest{
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				{
					Object:   "org:openfga",
					Relation: "owner",
					User:     "github|jose@openfga",
				},
			}},
		},
	},
	{
		_name: "ExecuteSucceedsForWriteOnly",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"admin":  {},
					"writer": {},
				},
			},
			{
				Type: "org",
				Relations: map[string]*openfgapb.Userset{
					"owner": {},
				},
			},
			{
				Type: "team",
				Relations: map[string]*openfgapb.Userset{
					"member": {},
				},
			}},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				{
					Object:   "org:openfga",
					Relation: "owner",
					User:     "github|jose@openfga",
				},
				{
					Object:   "repo:openfga/openfga",
					Relation: "admin",
					User:     "github|jose@openfga",
				},
				{
					Object:   "repo:openfga/openfga",
					Relation: "writer",
					User:     "team:openfga/iam#member",
				},
				{
					Object:   "team:openfga/iam",
					Relation: "member",
					User:     "iaco@openfga",
				},
			}},
		},
	},
	{
		_name: "ExecuteSucceedsForDeleteOnly",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"admin":  {},
					"writer": {},
				},
			},
			{
				Type: "org",
				Relations: map[string]*openfgapb.Userset{
					"owner": {},
				},
			},
			{
				Type: "team",
				Relations: map[string]*openfgapb.Userset{
					"member": {},
				},
			}},
		tuples: []*openfgapb.TupleKey{
			{
				Object:   "org:openfga",
				Relation: "owner",
				User:     "github|jose@openfga",
			},
			{
				Object:   "repo:openfga/openfga",
				Relation: "admin",
				User:     "github|jose@openfga",
			},
			{
				Object:   "repo:openfga/openfga",
				Relation: "writer",
				User:     "team:openfga/iam#member",
			},
			{
				Object:   "team:openfga/iam",
				Relation: "member",
				User:     "iaco@openfga",
			},
		},
		// input
		request: &openfgapb.WriteRequest{
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				{
					Object:   "org:openfga",
					Relation: "owner",
					User:     "github|jose@openfga",
				},
				{
					Object:   "repo:openfga/openfga",
					Relation: "admin",
					User:     "github|jose@openfga",
				},
				{
					Object:   "repo:openfga/openfga",
					Relation: "writer",
					User:     "team:openfga/iam#member",
				},
				{
					Object:   "team:openfga/iam",
					Relation: "member",
					User:     "iaco@openfga",
				},
			}},
		},
	},
	{
		_name: "ExecuteSucceedsForWriteAndDelete",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"admin":  {},
					"writer": {},
				},
			},
			{
				Type: "org",
				Relations: map[string]*openfgapb.Userset{
					"owner": {},
				},
			},
			{
				Type: "team",
				Relations: map[string]*openfgapb.Userset{
					"member": {},
				},
			}},
		tuples: []*openfgapb.TupleKey{
			{
				Object:   "org:openfga",
				Relation: "owner",
				User:     "github|yenkel@openfga",
			},
			{
				Object:   "repo:openfga/openfga",
				Relation: "reader",
				User:     "team:openfga/platform#member",
			},
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				{
					Object:   "org:openfga",
					Relation: "owner",
					User:     "github|jose@openfga",
				},
				{
					Object:   "repo:openfga/openfga",
					Relation: "admin",
					User:     "github|jose@openfga",
				},
				{
					Object:   "repo:openfga/openfga",
					Relation: "writer",
					User:     "team:openfga/iam#member",
				},
				{
					Object:   "team:openfga/iam",
					Relation: "member",
					User:     "iaco@openfga",
				},
			}},
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				{
					Object:   "org:openfga",
					Relation: "owner",
					User:     "github|yenkel@openfga",
				},
				{
					Object:   "repo:openfga/openfga",
					Relation: "reader",
					User:     "team:openfga/platform#member",
				},
			}},
		},
	},
}

func TestWriteCommand(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	for _, test := range writeCommandTests {
		t.Run(test._name, func(t *testing.T) {
			store := testutils.CreateRandomString(10)
			modelID, err := id.NewString()
			require.NoError(err)

			if test.typeDefinitions != nil {
				err = datastore.WriteAuthorizationModel(ctx, store, modelID, test.typeDefinitions)
				require.NoError(err)
			}

			if test.tuples != nil {
				err := datastore.Write(ctx, store, []*openfgapb.TupleKey{}, test.tuples)
				require.NoError(err)
			}

			cmd := commands.NewWriteCommand(datastore, tracer, logger)
			test.request.StoreId = store
			test.request.AuthorizationModelId = modelID
			resp, gotErr := cmd.Execute(ctx, test.request)

			if test.err != nil {
				if gotErr == nil {
					t.Errorf("[%s] Expected error '%s', but got none", test._name, test.err)
				}
				if !errors.Is(gotErr, test.err) {
					t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.err, gotErr)
				}
			}

			if test.err == nil && gotErr != nil {
				t.Errorf("[%s] Did not expect an error but got one: %v", test._name, gotErr)
			}

			if test.response != nil {
				if resp == nil {
					t.Error("Expected non nil response, got nil")
				}
			}
		})
	}
}
