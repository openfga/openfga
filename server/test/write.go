package test

import (
	"context"
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
		_name: "ExecuteDeleteTupleWhichDoesNotExistReturnsError",
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
		err: serverErrors.WriteFailedDueToInvalidInput(storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_DELETE)),
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
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga", "owner", ""),
			}},
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
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("", "owner", "elbuo@github.com"),
			}},
		},
		// output
		err: serverErrors.InvalidObjectFormat(tuple.NewTupleKey("", "owner", "elbuo@github.com")),
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
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga", "", "elbuo@github.com"),
			}},
		},
		// output
		err: serverErrors.InvalidTuple("invalid relation", tuple.NewTupleKey("repo:openfga", "", "elbuo@github.com")),
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
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga", "undefined", "elbuo@github.com"),
			}},
		},
		// output
		err: serverErrors.RelationNotFound(
			"undefined",
			"repo",
			tuple.NewTupleKey("repo:openfga", "undefined", "elbuo@github.com"),
		),
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
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				// invalid because object has no :
				tuple.NewTupleKey("openfga", "owner", "github|jose@openfga"),
			}},
		},
		// output
		err: serverErrors.InvalidObjectFormat(tuple.NewTupleKey("openfga", "owner", "github|jose@openfga")),
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
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "github|jose@openfga"),
			}},
		},
		// output
		err: serverErrors.RelationNotFound(
			"writer",
			"repo",
			tuple.NewTupleKey("repo:openfga/openfga", "writer", "github|jose@openfga"),
		),
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
			tuple.NewTupleKey("org:openfga", "owner", "github|jose@openfga"),
		},
		// input
		request: &openfgapb.WriteRequest{
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("org:openfga", "owner", "github|jose@openfga"),
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
				tuple.NewTupleKey("org:openfga", "owner", "github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "team:openfga/iam#member"),
				tuple.NewTupleKey("team:openfga/iam", "member", "iaco@openfga"),
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
			tuple.NewTupleKey("org:openfga", "owner", "github|jose@openfga"),
			tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
			tuple.NewTupleKey("repo:openfga/openfga", "writer", "team:openfga/iam#member"),
			tuple.NewTupleKey("team:openfga/iam", "member", "iaco@openfga"),
		},
		// input
		request: &openfgapb.WriteRequest{
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("org:openfga", "owner", "github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "team:openfga/iam#member"),
				tuple.NewTupleKey("team:openfga/iam", "member", "iaco@openfga"),
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
			tuple.NewTupleKey("org:openfga", "owner", "github|yenkel@openfga"),
			tuple.NewTupleKey("repo:openfga/openfga", "reader", "team:openfga/platform#member"),
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("org:openfga", "owner", "github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "team:openfga/iam#member"),
				tuple.NewTupleKey("team:openfga/iam", "member", "iaco@openfga"),
			}},
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("org:openfga", "owner", "github|yenkel@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "reader", "team:openfga/platform#member"),
			}},
		},
	},
	{
		_name: "1.0_Execute_fails_if_type_in_userset_value_was_not_found",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"viewer": typesystem.This(),
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{
				TupleKeys: []*openfgapb.TupleKey{
					tuple.NewTupleKey("document:doc1", "viewer", "group:engineering#member"),
				},
			},
		},
		err: serverErrors.TypeNotFound("group"),
	},
	{
		_name: "1.0_Execute_fails_if_relation_in_userset_value_was_not_found",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"viewer": typesystem.This(),
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{
				TupleKeys: []*openfgapb.TupleKey{
					tuple.NewTupleKey("document:doc1", "viewer", "document:doc1#editor"),
				},
			},
		},
		err: serverErrors.RelationNotFound("editor", "document", tuple.NewTupleKey("document:doc1", "viewer", "document:doc1#editor")),
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
		err: serverErrors.TypeNotFound("impossible"),
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
		_name: "1.1_Execute_fails_if_relation_in_userset_value_was_not_found",
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
						"reader": typesystem.This(),
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
		err: serverErrors.RelationNotFound("member", "group", tuple.NewTupleKey("document:budget", "reader", "group:abc#member")),
	},
	{
		_name: "1.1_Execute_fails_if_type_in_userset_value_was_not_found",
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
						"reader": typesystem.This(),
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
				tuple.NewTupleKey("document:budget", "reader", "notallowed:abc#member"),
			}},
		},
		err: serverErrors.TypeNotFound("notallowed"),
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
					Relations: map[string]*openfgapb.Userset{
						"member": typesystem.This(),
					},
					Metadata: &openfgapb.Metadata{
						Relations: map[string]*openfgapb.RelationMetadata{
							"member": {
								DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
									{Type: "user"},
								},
							},
						},
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
					Relations: map[string]*openfgapb.Userset{
						"member": typesystem.This(),
					},
					Metadata: &openfgapb.Metadata{
						Relations: map[string]*openfgapb.RelationMetadata{
							"member": {
								DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
									{Type: "user"},
								},
							},
						},
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
		_name: "Write succeeds if user is wildcard and type references a specific type",
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
									typesystem.WildcardRelationReference("user"),
								},
							},
						},
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "reader", "user:*"),
			}},
		},
	},
	{
		_name: "Write fails if user is a typed wildcard and the type restrictions don't permit it",
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
									typesystem.DirectRelationReference("group", "member"),
								},
							},
						},
					},
				},
			},
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:budget", "reader", "group:*"),
			}},
		},
		err: serverErrors.InvalidTuple("User 'group:*' is not allowed to have relation reader with document:budget",
			tuple.NewTupleKey("document:budget", "reader", "group:*"),
		),
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
		err: serverErrors.InvalidTuple("unexpected user 'folder:budgets#admin' with tupleset relation 'document#parent'",
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
		err: serverErrors.InvalidTuple("unexpected user 'folder:budgets#admin' with tupleset relation 'document#parent'",
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
		err: serverErrors.InvalidTuple("unexpected user 'folder:budgets#admin' with tupleset relation 'document#parent'",
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
		err: serverErrors.InvalidTuple("unexpected user 'folder:budgets#admin' with tupleset relation 'document#parent'",
			tuple.NewTupleKey("document:budget", "parent", "folder:budgets#admin"),
		),
	},
}

func TestWriteCommand(t *testing.T, datastore storage.OpenFGADatastore) {
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
				require.Equal(test.response, resp)
			}
		})
	}
}
