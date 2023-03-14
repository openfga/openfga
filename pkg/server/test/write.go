package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type writeCommandTest struct {
	_name         string
	model         *openfgapb.AuthorizationModel
	tuples        []*openfgapb.TupleKey
	request       *openfgapb.WriteRequest
	allowSchema10 bool
	err           error
	response      *openfgapb.WriteResponse
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
		request:       &openfgapb.WriteRequest{},
		allowSchema10: true,
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
		allowSchema10: true,

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
		allowSchema10: true,

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
		allowSchema10: true,
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
		allowSchema10: true,
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
		allowSchema10: true,
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
		allowSchema10: true,
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
		allowSchema10: true,
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
		allowSchema10: true,
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
		allowSchema10: true,
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
		allowSchema10: true,
		// output
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    &tuple.TypeNotFoundError{TypeName: "repository"},
				TupleKey: tk,
			},
		),
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
		allowSchema10: true,
		// output
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'user' field is malformed"),
				TupleKey: tuple.NewTupleKey("repo:openfga", "owner", ""),
			},
		),
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
		allowSchema10: true,
		// output
		err: serverErrors.ValidationError(&tuple.InvalidTupleError{
			Cause:    fmt.Errorf("invalid 'object' field format"),
			TupleKey: tuple.NewTupleKey("", "owner", "elbuo@github.com"),
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
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga", "", "elbuo@github.com"),
			}},
		},
		allowSchema10: true,
		// output
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'relation' field is malformed"),
				TupleKey: tuple.NewTupleKey("repo:openfga", "", "elbuo@github.com"),
			},
		),
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
		allowSchema10: true,
		// output
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause: &tuple.RelationNotFoundError{
					TypeName: "repo",
					Relation: "undefined",
				},
				TupleKey: tuple.NewTupleKey("repo:openfga", "undefined", "elbuo@github.com"),
			},
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
		allowSchema10: true,
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
		allowSchema10: true,
		// output
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    fmt.Errorf("invalid 'object' field format"),
				TupleKey: tuple.NewTupleKey("openfga", "owner", "github|jose@openfga"),
			},
		),
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
		allowSchema10: true,
		// output
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause: &tuple.RelationNotFoundError{
					TypeName: "repo",
					Relation: "writer",
				},
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "writer", "github|jose@openfga"),
			},
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
		allowSchema10: true,
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
		allowSchema10: true,
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
		allowSchema10: true,
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
		allowSchema10: true,
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
		allowSchema10: true,
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    &tuple.TypeNotFoundError{TypeName: "group"},
				TupleKey: tuple.NewTupleKey("document:doc1", "viewer", "group:engineering#member"),
			},
		),
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
		allowSchema10: true,

		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause: &tuple.RelationNotFoundError{
					TypeName: "document",
					Relation: "editor",
				},
				TupleKey: tuple.NewTupleKey("document:doc1", "viewer", "document:doc1#editor"),
			},
		),
	},
	{
		_name: "Obsolete_model_1_0",
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
		allowSchema10: false,
		err:           serverErrors.ValidationError(commands.ErrObsoleteAuthorizationModel),
	},
	// Begin section with tests for schema version 1.1
	{
		_name: "Delete_succeeds_even_if_user_field_contains_a_type_that_is_not_allowed_by_the_current_authorization_model",
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
		allowSchema10: true,
	},
	{
		_name: "Write_fails_if_user_field_contains_a_type_that_does_not_exist",
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
				tuple.NewTupleKey("org:openfga", "owner", "undefined:1"),
			},
			},
		},
		allowSchema10: true,
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    &tuple.TypeNotFoundError{TypeName: "undefined"},
				TupleKey: tuple.NewTupleKey("org:openfga", "owner", "undefined:1"),
			},
		),
	},
	{
		_name: "Write_fails_if_user_field_contains_a_type_that_is_not_allowed_by_the_authorization_model_(which_only_allows_group:...)",
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
		allowSchema10: true,
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    fmt.Errorf("type 'user' is not an allowed type restriction for 'document#reader'"),
				TupleKey: tuple.NewTupleKey("document:budget", "reader", "user:abc"),
			},
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
		allowSchema10: true,
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause: &tuple.RelationNotFoundError{
					TypeName: "group",
					Relation: "member",
				},
				TupleKey: tuple.NewTupleKey("document:budget", "reader", "group:abc#member"),
			},
		),
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
				tuple.NewTupleKey("document:budget", "reader", "undefined:abc#member"),
			}},
		},
		allowSchema10: true,
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    &tuple.TypeNotFoundError{TypeName: "undefined"},
				TupleKey: tuple.NewTupleKey("document:budget", "reader", "undefined:abc#member"),
			},
		),
	},
	{
		_name: "Write_succeeds_if_user_field_contains_a_type_that_is_allowed_by_the_authorization_model_(which_only_allows_user:...)",
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
		allowSchema10: true,
	},
	{
		_name: "Write_fails_if_user_field_contains_a_type_that_is_not_allowed_by_the_authorization_model_(which_only_allows_group:...#member)",
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
				tuple.NewTupleKey("document:budget", "reader", "user:abc"),
			}},
		},
		allowSchema10: true,
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    fmt.Errorf("type 'user' is not an allowed type restriction for 'document#reader'"),
				TupleKey: tuple.NewTupleKey("document:budget", "reader", "user:abc"),
			},
		),
	},
	{
		_name: "Write_succeeds_if_user_field_contains_a_type_that_is_allowed_by_the_authorization_model_(which_only_allows_group:...#member)",
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
		allowSchema10: true,
	},
	{
		_name: "Multiple_writes_succeed_if_user_fields_contain_a_type_that_is_allowed_by_the_authorization_model",
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
		allowSchema10: true,
	},
	{
		_name: "Write_succeeds_if_user_is_wildcard_and_type_references_a_specific_type",
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
		allowSchema10: true,
	},
	{
		_name: "Write_fails_if_user_is_a_typed_wildcard_and_the_type_restrictions_do_not_permit_it",
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
		allowSchema10: true,
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the typed wildcard 'group:*' is not an allowed type restriction for 'document#reader'"),
				TupleKey: tuple.NewTupleKey("document:budget", "reader", "group:*"),
			},
		),
	},
	{
		_name: "Write_fails_if_a._schema_version_is_1.0_b._user_is_a_userset_c._relation_is_referenced_in_a_tupleset_of_a_tupleToUserset_relation",
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
		allowSchema10: true,
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    fmt.Errorf("unexpected user 'folder:budgets#admin' with tupleset relation 'document#parent'"),
				TupleKey: tuple.NewTupleKey("document:budget", "parent", "folder:budgets#admin"),
			},
		),
	},
	{
		_name: "Write_fails_if_a._schema_version_is_1.0_b._user_is_a_userset_c._relation_is_referenced_in_a_tupleset_of_a_tupleToUserset_relation_(defined_as_union)",
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
		allowSchema10: true,
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    fmt.Errorf("unexpected user 'folder:budgets#admin' with tupleset relation 'document#parent'"),
				TupleKey: tuple.NewTupleKey("document:budget", "parent", "folder:budgets#admin"),
			},
		),
	},
	{
		_name: "Write_fails_if_a._schema_version_is_1.0_b._user_is_a_userset_c._relation_is_referenced_in_a_tupleset_of_a_tupleToUserset_relation_(defined_as_intersection)",
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
		allowSchema10: true,
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    fmt.Errorf("unexpected user 'folder:budgets#admin' with tupleset relation 'document#parent'"),
				TupleKey: tuple.NewTupleKey("document:budget", "parent", "folder:budgets#admin"),
			},
		),
	},
	{
		_name: "Write_fails_if_a._schema_version_is_1.0_b._user_is_a_userset_c._relation_is_referenced_in_a_tupleset_of_a_tupleToUserset_relation_(defined_as_difference)",
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
		allowSchema10: true,
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    fmt.Errorf("unexpected user 'folder:budgets#admin' with tupleset relation 'document#parent'"),
				TupleKey: tuple.NewTupleKey("document:budget", "parent", "folder:budgets#admin"),
			},
		),
	},
	{
		_name: "Write_succeeds_if_a._schema_version_is_1.0_b._user_is_a_userset_c._relation_is_referenced_in_a_tupleset_of_a_tupleToUserset_relation_of_another_type",
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "folder",
					Relations: map[string]*openfgapb.Userset{
						"owner": typesystem.This(),
						"parent": typesystem.Union(
							typesystem.TupleToUserset("parent", "owner"), // 'folder#parent' is a tupleset relation but 'document#parent' is not
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
				tuple.NewTupleKey("document:budget", "parent", "folder:budgets#owner"), // the 'document#parent' relation isn't a tupleset so this is fine
			}},
		},
		allowSchema10: true,
	},
	{
		_name: "invalid_type_restriction_in_write_body",
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
									typesystem.DirectRelationReference("user", ""),
								},
							},
						},
					},
				},
				{
					Type: "resource",
					Relations: map[string]*openfgapb.Userset{
						"writer": typesystem.This(),
					},
					Metadata: &openfgapb.Metadata{
						Relations: map[string]*openfgapb.RelationMetadata{
							"writer": {
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
				tuple.NewTupleKey("resource:bad", "writer", "group:fga"),
			}},
		},
		allowSchema10: true,
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    fmt.Errorf("type 'group' is not an allowed type restriction for 'resource#writer'"),
				TupleKey: tuple.NewTupleKey("resource:bad", "writer", "group:fga"),
			},
		),
	},
}

func TestWriteCommand(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()
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

			cmd := commands.NewWriteCommand(datastore, logger, test.allowSchema10)
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
