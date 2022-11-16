package test

import (
	"context"
	"errors"
	"testing"

	"github.com/oklog/ulid/v2"
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
	_name    string
	model    *openfgapb.AuthorizationModel
	tuples   []*openfgapb.TupleKey
	request  *openfgapb.WriteRequest
	err      error
	response *openfgapb.WriteResponse
}

var tk = tuple.NewTupleKey("repository:openfga/openfga", "administrator", "github|alice@openfga")

var writeCommandTests = []writeCommandTest{
	{
		_name: "ExecuteWithEmptyWritesAndDeletesReturnsZeroWrittenAndDeleted",
		// input
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repository",
				},
			},
		},
		request: &openfgapb.WriteRequest{},
		// output
		err: serverErrors.InvalidWriteInput,
	},
	{
		_name: "ExecuteWithSameTupleInWritesReturnsError",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repository",
					Relations: map[string]*openfgapb.Userset{
						"administrator": {Userset: &openfgapb.Userset_This{}},
					},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repository",
					Relations: map[string]*openfgapb.Userset{
						"administrator": {},
					},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repository",
					Relations: map[string]*openfgapb.Userset{
						"administrator": {Userset: &openfgapb.Userset_This{}},
					},
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
		_name: "ExecuteDeleteTupleWhichDoesNotExistSucceeds",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repository",
					Relations: map[string]*openfgapb.Userset{
						"administrator": {},
					},
				},
			},
		},
		// input
		request: &openfgapb.WriteRequest{
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{tk}},
		},
		// output
		err: nil,
	},
	{
		_name: "ExecuteWithWriteTupleWithInvalidAuthorizationModelReturnsError",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type:      "repo",
					Relations: map[string]*openfgapb.Userset{},
				},
			},
		},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"owner": {},
					},
				},
			},
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Object:   "repo:openfga",
				Relation: "owner",
			}}},
		},
		// output
		err: serverErrors.InvalidTuple("the 'user' field is invalid", &openfgapb.TupleKey{Object: "repo:openfga", Relation: "owner"}),
	},
	{
		_name: "ExecuteWithWriteTupleWithMissingObjectError",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"owner": {},
					},
				},
			},
		},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"owner": {},
					},
				},
			},
		},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"owner": {},
					},
				},
			},
		},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type:      "repo",
					Relations: map[string]*openfgapb.Userset{},
				},
			},
		},
		tuples: []*openfgapb.TupleKey{tk},
		// input
		request: &openfgapb.WriteRequest{
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{tk}},
		},
	},
	{
		_name: "ExecuteWithInvalidObjectFormatReturnsError",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type:      "repo",
					Relations: map[string]*openfgapb.Userset{},
				},
			},
		},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
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
				},
			},
		},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
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
				},
			},
		},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
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
				},
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
		},
	},
	{
		_name: "ExecuteSucceedsForDeleteOnly",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
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
				},
			},
		},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
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
				},
			},
		},
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
	// Begin section with tests for schema version 1.1
	{
		_name: "Delete succeeds even if user field contains a type that is not allowed by the current authorization model",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "user",
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
				},
			},
		},
		tuples: []*openfgapb.TupleKey{
			{
				Object:   "org:openfga",
				Relation: "owner",
				User:     "impossible:1",
			},
		},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "user",
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
				},
			},
		},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "group",
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
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "reader", "user:abc"),
			}},
		},
		err: serverErrors.InvalidTuple("User 'user:abc' is not allowed to have relation reader with document:budget",
			tuple.NewTupleKey("document:budget", "reader", "user:abc"),
		),
	},
	{
		_name: "Write fails if user field is a userset that is not allowed by the authorization model (which only allows group:...)",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "group",
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
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "reader", "group:abc#member"),
			}},
		},
		err: serverErrors.InvalidTuple("User 'group:abc#member' is not allowed to have relation reader with document:budget",
			tuple.NewTupleKey("document:budget", "reader", "group:abc#member"),
		),
	},
	{
		_name: "Write succeeds if user field contains a type that is allowed by the authorization model (which only allows user:...)",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "user",
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "group",
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
										Type:               "group",
										RelationOrWildcard: &openfgapb.RelationReference_Relation{Relation: "member"},
									},
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
		err: serverErrors.InvalidTuple("User 'user:abc' is not allowed to have relation reader with document:budget",
			tuple.NewTupleKey("document:budget", "reader", "user:abc"),
		),
	},
	{
		_name: "Write succeeds if user field contains a type that is allowed by the authorization model (which only allows group:...#member)",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "group",
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
										Type:               "group",
										RelationOrWildcard: &openfgapb.RelationReference_Relation{Relation: "member"},
									},
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
		_name: "Multiple writes succeed if user fields contain a type that is allowed by the authorization model",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "group",
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
									{
										Type:               "group",
										RelationOrWildcard: &openfgapb.RelationReference_Relation{Relation: "member"},
									},
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
				tuple.NewTupleKey("document:budget", "reader", "user:def"),
			}},
		},
	},
	{
		_name: "Write succeeds if user is * and type references a specific type",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "group",
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "group",
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
										Type:               "group",
										RelationOrWildcard: &openfgapb.RelationReference_Relation{Relation: "member"},
									},
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
		err: serverErrors.InvalidTuple("User '*' is not allowed to have relation reader with document:budget",
			tuple.NewTupleKey("document:budget", "reader", "*"),
		),
	},
	{
		_name: "Write fails if schema version is 1.1 but type definitions are lacking metadata",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"reader": {Userset: &openfgapb.Userset_This{}},
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "reader", "*"),
			}},
		},
		err: serverErrors.NewInternalError("invalid authorization model", errors.New("invalid authorization model")),
	},

	{
		_name: "Write fails if a. schema version is 1.0 b. user is a userset c. relation is referenced in a tupleset of a tupleToUserset relation",
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "folder",
					Relations: map[string]*openfgapb.Userset{
						"owner": typesystem.This(),
						"admin": typesystem.This(),
					},
				},
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"parent":   typesystem.This(),
						"can_view": typesystem.TupleToUserset("parent", "owner"), //owner from parent
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "parent", "folder:budgets#admin"),
			}},
		},
		err: serverErrors.InvalidTuple("Userset 'folder:budgets#admin' is not allowed to have relation 'parent' with 'document:budget'",
			tuple.NewTupleKey("document:budget", "parent", "folder:budgets#admin"),
		),
	},
	{
		_name: "Write fails if a. schema version is 1.0 b. user is a userset c. relation is referenced in a tupleset of a tupleToUserset relation (defined as union)",
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "folder",
					Relations: map[string]*openfgapb.Userset{
						"owner": typesystem.This(),
						"admin": typesystem.This(),
					},
				},
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"parent": typesystem.This(),
						"can_view": typesystem.Union(
							typesystem.TupleToUserset("parent", "owner"), //owner from parent
							typesystem.TupleToUserset("parent", "admin"), //admin from parent
						),
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "parent", "folder:budgets#admin"),
			}},
		},
		err: serverErrors.InvalidTuple("Userset 'folder:budgets#admin' is not allowed to have relation 'parent' with 'document:budget'",
			tuple.NewTupleKey("document:budget", "parent", "folder:budgets#admin"),
		),
	},
	{
		_name: "Write fails if a. schema version is 1.0 b. user is a userset c. relation is referenced in a tupleset of a tupleToUserset relation (defined as intersection)",
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "folder",
					Relations: map[string]*openfgapb.Userset{
						"owner": typesystem.This(),
						"admin": typesystem.This(),
					},
				},
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"parent": typesystem.This(),
						"can_view": typesystem.Intersection(
							typesystem.TupleToUserset("parent", "owner"), //owner from parent
							typesystem.TupleToUserset("parent", "admin"), //admin from parent
						),
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "parent", "folder:budgets#admin"),
			}},
		},
		err: serverErrors.InvalidTuple("Userset 'folder:budgets#admin' is not allowed to have relation 'parent' with 'document:budget'",
			tuple.NewTupleKey("document:budget", "parent", "folder:budgets#admin"),
		),
	},
	{
		_name: "Write fails if a. schema version is 1.0 b. user is a userset c. relation is referenced in a tupleset of a tupleToUserset relation (defined as difference)",
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "folder",
					Relations: map[string]*openfgapb.Userset{
						"owner": typesystem.This(),
						"admin": typesystem.This(),
					},
				},
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"parent": typesystem.This(),
						"can_view": typesystem.Difference(
							typesystem.TupleToUserset("parent", "owner"), //owner from parent
							typesystem.TupleToUserset("parent", "admin"), //admin from parent
						),
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "parent", "folder:budgets#admin"),
			}},
		},
		err: serverErrors.InvalidTuple("Userset 'folder:budgets#admin' is not allowed to have relation 'parent' with 'document:budget'",
			tuple.NewTupleKey("document:budget", "parent", "folder:budgets#admin"),
		),
	},
	{
		_name: "Write succeeds if a. schema version is 1.0 b. user is a userset c. relation is referenced in a tupleset of a tupleToUserset relation of another type",
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "folder",
					Relations: map[string]*openfgapb.Userset{
						"owner": typesystem.This(),
						"parent": typesystem.Union( // let's confuse the code. if this were defined in 'document' type, it would fail
							typesystem.TupleToUserset("parent", "owner"),
						),
					},
				},
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"owner":  typesystem.This(),
						"parent": typesystem.This(),
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "parent", "folder:budgets#admin"),
			}},
		},
	},
}

func WriteCommandTest(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	for _, test := range writeCommandTests {
		t.Run(test._name, func(t *testing.T) {
			store := ulid.Make().String()

			err := datastore.WriteAuthorizationModel(ctx, store, test.model)
			require.NoError(err)

			if test.tuples != nil {
				err := datastore.Write(ctx, store, []*openfgapb.TupleKey{}, test.tuples)
				require.NoError(err)
			}

			cmd := commands.NewWriteCommand(datastore, tracer, logger)
			test.request.StoreId = store
			test.request.AuthorizationModelId = test.model.Id
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
