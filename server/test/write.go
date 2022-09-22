package test

import (
	"context"
	"errors"
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type writeCommandTest struct {
	_name           string
	typeDefinitions []*openfgapb.TypeDefinition
	schemaVersion   typesystem.SchemaVersion
	tuples          []*openfgapb.TupleKey
	request         *openfgapb.WriteRequest
	err             error
	response        *openfgapb.WriteResponse
}

var tk = tuple.NewTupleKey("repository:openfga/openfga", "administrator", "github|alice@openfga")

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
		schemaVersion: typesystem.SchemaVersion1_0,
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgapb.Userset{
					"administrator": {Userset: &openfgapb.Userset_This{}},
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgapb.Userset{
					"administrator": {Userset: &openfgapb.Userset_This{}},
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"admin":  {Userset: &openfgapb.Userset_This{}},
					"writer": {Userset: &openfgapb.Userset_This{}},
				},
			},
			{
				Type: "org",
				Relations: map[string]*openfgapb.Userset{
					"owner": {Userset: &openfgapb.Userset_This{}},
				},
			},
			{
				Type: "team",
				Relations: map[string]*openfgapb.Userset{
					"member": {Userset: &openfgapb.Userset_This{}},
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
		schemaVersion: typesystem.SchemaVersion1_0,
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
		schemaVersion: typesystem.SchemaVersion1_0,
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"admin":  {Userset: &openfgapb.Userset_This{}},
					"writer": {Userset: &openfgapb.Userset_This{}},
				},
			},
			{
				Type: "org",
				Relations: map[string]*openfgapb.Userset{
					"owner": {Userset: &openfgapb.Userset_This{}},
				},
			},
			{
				Type: "team",
				Relations: map[string]*openfgapb.Userset{
					"member": {Userset: &openfgapb.Userset_This{}},
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
	{
		_name: "Delete succeeds even if user field contains a type that is not allowed by the current authorization model",
		// state
		schemaVersion: typesystem.SchemaVersion1_1,
		tuples: []*openfgapb.TupleKey{
			{
				Object:   "org:openfga",
				Relation: "owner",
				User:     "impossible:1",
			},
		},
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type:      "user",
				Relations: map[string]*openfgapb.Userset{},
			},
			{
				Type: "org",
				Relations: map[string]*openfgapb.Userset{
					"owner": {Userset: &openfgapb.Userset_This{}},
				},
				Metadata: &openfgapb.Metadata{
					Relations: map[string]*openfgapb.RelationMetadata{
						"owner": {
							DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
								{
									Type: "user",
								},
							},
						},
					},
				},
			}},
		request: &openfgapb.WriteRequest{
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("org:openfga", "owner", "impossible:1"),
			},
			},
		},
	},
	{
		_name: "Write fails if user field contains a type that does not exist",
		// state
		schemaVersion: typesystem.SchemaVersion1_1,
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "org",
				Relations: map[string]*openfgapb.Userset{
					"owner": {Userset: &openfgapb.Userset_This{}},
				},
				Metadata: &openfgapb.Metadata{
					Relations: map[string]*openfgapb.RelationMetadata{
						"owner": {
							DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
								{
									Type: "user",
								},
							},
						},
					},
				},
			}},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("org:openfga", "owner", "impossible:1"),
			},
			},
		},
		err: serverErrors.InvalidWriteInput,
	},
	{
		_name: "Write fails if user field contains a type that is not allowed by the authorization model (which only allows group:...)",
		// state
		schemaVersion: typesystem.SchemaVersion1_1,
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type:      "user",
				Relations: map[string]*openfgapb.Userset{},
			},
			{
				Type: "group",
				Relations: map[string]*openfgapb.Userset{
					"member": {Userset: &openfgapb.Userset_This{}},
				},
			},
			{
				Type: "document",
				Relations: map[string]*openfgapb.Userset{
					"reader": {Userset: &openfgapb.Userset_This{}},
				},
				Metadata: &openfgapb.Metadata{
					Relations: map[string]*openfgapb.RelationMetadata{
						"reader": {
							DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
								{
									Type: "group",
								},
							},
						},
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "reader", "user:abc"),
			}},
		},
		err: serverErrors.InvalidTuple("Object of type user is not allowed to have relation reader with document:budget",
			tuple.NewTupleKey("document:budget", "reader", "user:abc"),
		),
	},
	{
		_name: "Write succeeds if user field contains a type that is allowed by the authorization model (which only allows user:...)",
		// state
		schemaVersion: typesystem.SchemaVersion1_1,
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type:      "user",
				Relations: map[string]*openfgapb.Userset{},
			},
			{
				Type: "document",
				Relations: map[string]*openfgapb.Userset{
					"reader": {Userset: &openfgapb.Userset_This{}},
				},
				Metadata: &openfgapb.Metadata{
					Relations: map[string]*openfgapb.RelationMetadata{
						"reader": {
							DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
								{
									Type: "user",
								},
							},
						},
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "reader", "user:bob"),
			}},
		},
	},
	{
		_name: "Write fails if user field contains a type that is not allowed by the authorization model (which only allows group:...#member)",
		// state
		schemaVersion: typesystem.SchemaVersion1_1,
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type:      "user",
				Relations: map[string]*openfgapb.Userset{},
			},
			{
				Type: "group",
				Relations: map[string]*openfgapb.Userset{
					"member": {Userset: &openfgapb.Userset_This{}},
				},
			},
			{
				Type: "document",
				Relations: map[string]*openfgapb.Userset{
					"reader": {Userset: &openfgapb.Userset_This{}},
				},
				Metadata: &openfgapb.Metadata{
					Relations: map[string]*openfgapb.RelationMetadata{
						"reader": {
							DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
								{
									Type:     "group",
									Relation: "member",
								},
							},
						},
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "reader", "user:abc"),
			}},
		},
		err: serverErrors.InvalidTuple("Object of type user is not allowed to have relation reader with document:budget",
			tuple.NewTupleKey("document:budget", "reader", "user:abc"),
		),
	},
	{
		_name: "Write succeeds if user field contains a type that is allowed by the authorization model (which only allows group:...#member)",
		// state
		schemaVersion: typesystem.SchemaVersion1_1,
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type:      "user",
				Relations: map[string]*openfgapb.Userset{},
			},
			{
				Type: "group",
				Relations: map[string]*openfgapb.Userset{
					"member": {Userset: &openfgapb.Userset_This{}},
				},
			},
			{
				Type: "document",
				Relations: map[string]*openfgapb.Userset{
					"reader": {Userset: &openfgapb.Userset_This{}},
				},
				Metadata: &openfgapb.Metadata{
					Relations: map[string]*openfgapb.RelationMetadata{
						"reader": {
							DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
								{
									Type:     "group",
									Relation: "member",
								},
							},
						},
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "reader", "group:abc#member"),
			}},
		},
	},
	{
		_name: "Write succeeds if user is * and type references a specific type",
		// state
		schemaVersion: typesystem.SchemaVersion1_1,
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type:      "user",
				Relations: map[string]*openfgapb.Userset{},
			},
			{
				Type: "group",
				Relations: map[string]*openfgapb.Userset{
					"member": {Userset: &openfgapb.Userset_This{}},
				},
			},
			{
				Type: "document",
				Relations: map[string]*openfgapb.Userset{
					"reader": {Userset: &openfgapb.Userset_This{}},
				},
				Metadata: &openfgapb.Metadata{
					Relations: map[string]*openfgapb.RelationMetadata{
						"reader": {
							DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
								{
									Type: "group",
								},
							},
						},
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "reader", "*"),
			}},
		},
	},
	{
		_name: "Write fails if user is * and type does not reference a specific type",
		// state
		schemaVersion: typesystem.SchemaVersion1_1,
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type:      "user",
				Relations: map[string]*openfgapb.Userset{},
			},
			{
				Type: "group",
				Relations: map[string]*openfgapb.Userset{
					"member": {Userset: &openfgapb.Userset_This{}},
				},
			},
			{
				Type: "document",
				Relations: map[string]*openfgapb.Userset{
					"reader": {Userset: &openfgapb.Userset_This{}},
				},
				Metadata: &openfgapb.Metadata{
					Relations: map[string]*openfgapb.RelationMetadata{
						"reader": {
							DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
								{
									Type:     "group",
									Relation: "member",
								},
							},
						},
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "reader", "*"),
			}},
		},
		err: serverErrors.InvalidTuple("User=* is not allowed to have relation reader with document:budget",
			tuple.NewTupleKey("document:budget", "reader", "*"),
		),
	},
	{
		_name: "Write fails if schema version is 1.1 but type definitions are lacking metadata",
		// state
		schemaVersion: typesystem.SchemaVersion1_1,
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "document",
				Relations: map[string]*openfgapb.Userset{
					"reader": {Userset: &openfgapb.Userset_This{}},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "reader", "*"),
			}},
		},
		err: serverErrors.NewInternalError("", errors.New("invalid authorization model")),
	},
}

func TestWriteCommand(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	for _, test := range writeCommandTests {
		t.Run(test._name, func(t *testing.T) {
			store := id.Must(id.New()).String()
			modelID := id.Must(id.New()).String()

			if test.typeDefinitions != nil {
				err := datastore.WriteAuthorizationModel(ctx, store, modelID, test.typeDefinitions)
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

			require.ErrorIs(gotErr, test.err)

			if test.response != nil {
				if resp == nil {
					t.Error("Expected non nil response, got nil")
				}
			}
		})
	}
}
