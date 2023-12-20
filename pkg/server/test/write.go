package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type writeCommandTest struct {
	_name    string
	model    *openfgav1.AuthorizationModel
	tuples   []*openfgav1.TupleKey
	request  *openfgav1.WriteRequest
	err      error
	response *openfgav1.WriteResponse
}

var tk = tuple.NewTupleKey("repo:openfga/openfga", "admin", "user:github|alice@openfga")
var unconditionedTK = &openfgav1.TupleKeyWithoutCondition{
	Object:   tk.GetObject(),
	Relation: tk.GetRelation(),
	User:     tk.GetUser(),
}

func TestWriteCommand(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	var tests = []writeCommandTest{
		{
			_name: "ExecuteWithEmptyWritesAndDeletesReturnsZeroWrittenAndDeleted",
			// input
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type repo`).TypeDefinitions,
			},
			request: &openfgav1.WriteRequest{},
			// output
			err: serverErrors.InvalidWriteInput,
		},
		{
			_name: "ExecuteWithSameTupleInWritesReturnsError",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
  schema 1.1
type user

type repo
  relations
    define admin: [user]`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{tk, tk},
				},
			},

			// output
			err: serverErrors.DuplicateTupleInWrite(tk),
		},
		{
			_name: "ExecuteWithWriteToIndirectUnionRelationshipReturnsError",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
  schema 1.1
type user

type repo
  relations
	define writer: [user]
	define owner: [user]
	define viewer: writer or owner`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{{
						Object:   "repo:openfga/openfga",
						Relation: "viewer",
						User:     "user:github|alice@openfga.com",
					}},
				},
			},
			// output
			err: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("type 'user' is not an allowed type restriction for 'repo#viewer'"),
					TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "viewer", "user:github|alice@openfga.com"),
				},
			),
		},
		{
			_name: "ExecuteWithWriteToIndirectIntersectionRelationshipReturnsError",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
  schema 1.1
type user

type repo
  relations
	define writer: [user]
	define owner: [user]
	define viewer: writer and owner`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{{
						Object:   "repo:openfga/openfga",
						Relation: "viewer",
						User:     "user:github|alice@openfga.com",
					}},
				},
			},
			// output
			err: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("type 'user' is not an allowed type restriction for 'repo#viewer'"),
					TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "viewer", "user:github|alice@openfga.com"),
				},
			),
		},
		{
			_name: "ExecuteWithWriteToIndirectDifferenceRelationshipReturnsError",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"writer": typesystem.This(),
							"owner":  typesystem.This(),
							"banned": typesystem.This(),
							"viewer": typesystem.Difference(
								typesystem.Union(
									typesystem.ComputedUserset("writer"),
									typesystem.ComputedUserset("owner")),
								typesystem.ComputedUserset("banned")),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"writer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
								"owner": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
								"banned": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
				},
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{{
						Object:   "repo:openfga/openfga",
						Relation: "viewer",
						User:     "user:github|alice@openfga.com",
					}},
				},
			},
			// output
			err: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("type 'user' is not an allowed type restriction for 'repo#viewer'"),
					TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "viewer", "user:github|alice@openfga.com"),
				},
			),
		},
		{
			_name: "ExecuteWithWriteToIndirectComputerUsersetRelationshipReturnsError",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
  schema 1.1
type user

type repo
  relations
	define writer: [user]
	define owner: [user]
	define viewer: writer`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{{
						Object:   "repo:openfga/openfga",
						Relation: "viewer",
						User:     "user:github|alice@openfga.com",
					}},
				},
			},
			// output
			err: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("type 'user' is not an allowed type restriction for 'repo#viewer'"),
					TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "viewer", "user:github|alice@openfga.com"),
				},
			),
		},
		{
			_name: "ExecuteWithWriteToIndirectTupleToUsersetRelationshipReturnsError",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
  schema 1.1
type user

type org
  relations
	define viewer: [user]

type repo
  relations
	define owner: [org]
	define viewer: viewer from owner`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{{
						Object:   "repo:openfga/openfga",
						Relation: "viewer",
						User:     "user:github|alice@openfga.com",
					}},
				},
			},
			// output
			err: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("type 'user' is not an allowed type restriction for 'repo#viewer'"),
					TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "viewer", "user:github|alice@openfga.com"),
				},
			),
		},
		{
			_name: "ExecuteWithSameTupleInDeletesReturnsError",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
  schema 1.1
type user

type repo
  relations
	define admin: [user]`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{unconditionedTK, unconditionedTK},
				},
			},
			// output
			err: serverErrors.DuplicateTupleInWrite(tk),
		},
		{
			_name: "ExecuteWithSameTupleInWritesAndDeletesReturnsError",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type repo
  relations
	define admin: [user]`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{tk},
				},
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{unconditionedTK},
				},
			},
			// output
			err: serverErrors.DuplicateTupleInWrite(tk),
		},
		{
			_name: "ExecuteDeleteTupleWhichDoesNotExistReturnsError",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
  schema 1.1
type user

type repo
  relations
	define admin: [user]`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{unconditionedTK},
				},
			},
			// output
			err: serverErrors.WriteFailedDueToInvalidInput(storage.InvalidWriteInputError(tk, openfgav1.TupleOperation_TUPLE_OPERATION_DELETE)),
		},
		{
			_name: "ExecuteWithWriteTupleWithInvalidAuthorizationModelReturnsError",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
  schema 1.1
type user
type repository`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{tk},
				},
			},
			// output
			err: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    &tuple.TypeNotFoundError{TypeName: "repo"},
					TupleKey: tk,
				},
			),
		},
		{
			_name: "ExecuteWithWriteTupleWithMissingUserError",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
  schema 1.1
type user

type repo
  relations
	define owner: [user]`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "repo:openfga", Relation: "owner", User: ""},
					},
				},
			},
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
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type repo
  relations
	define owner: [user]`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{{
						Object: "", Relation: "owner", User: "user:elbuo@github.com"},
					},
				},
			},
			// output
			err: serverErrors.ValidationError(&tuple.InvalidTupleError{
				Cause:    fmt.Errorf("invalid 'object' field format"),
				TupleKey: tuple.NewTupleKey("", "owner", "user:elbuo@github.com"),
			}),
		},
		{
			_name: "ExecuteWithWriteTupleWithInvalidRelationError",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type repo
  relations
	define owner: [user]`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{{
						Object: "repo:openfga", Relation: "", User: "user:elbuo@github.com"},
					},
				},
			},
			// output
			err: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("the 'relation' field is malformed"),
					TupleKey: tuple.NewTupleKey("repo:openfga", "", "user:elbuo@github.com"),
				},
			),
		},
		{
			_name: "ExecuteWithWriteTupleWithNotFoundRelationError",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type repo
  relations
	define owner: [user]`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{{
						Object: "repo:openfga", Relation: "undefined", User: "user:elbuo@github.com"},
					},
				},
			},
			// output
			err: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: &tuple.RelationNotFoundError{
						TypeName: "repo",
						Relation: "undefined",
					},
					TupleKey: tuple.NewTupleKey("repo:openfga", "undefined", "user:elbuo@github.com"),
				},
			),
		},
		{
			_name: "ExecuteDeleteTupleWithInvalidAuthorizationModelIgnoresAuthorizationModelValidation",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type:      "repo",
						Relations: map[string]*openfgav1.Userset{},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{tk},
			// input
			request: &openfgav1.WriteRequest{
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{unconditionedTK},
				},
			},
		},
		{
			_name: "ExecuteWithInvalidObjectFormatReturnsError",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user
type repo`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{{
						// invalid because object has no :
						Object: "openfga", Relation: "owner", User: "user:github|jose@openfga"},
					},
				},
			},
			// output
			err: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("invalid 'object' field format"),
					TupleKey: tuple.NewTupleKey("openfga", "owner", "user:github|jose@openfga"),
				},
			),
		},
		{
			_name: "ExecuteReturnsErrorIfAuthModelNotFound",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type repo
  relations
	define admin: [user]
	define writer: [user]`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				AuthorizationModelId: "01GZFXJ2XPAF8FBHDKJ83XAJQP",
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "repo:openfga/openfga", Relation: "admin", User: "user:github|jose@openfga"},
						{Object: "repo:openfga/openfga", Relation: "writer", User: "user:github|jon@openfga"},
					},
				},
			},
			err: serverErrors.AuthorizationModelNotFound("01GZFXJ2XPAF8FBHDKJ83XAJQP"),
		},
		{
			_name: "ExecuteReturnsSuccessIfDeleteRelationDoesNotExistInAuthorizationModel",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type org
  relations
	define manager: [user]`).TypeDefinitions,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("org:openfga", "owner", "user:github|jose@openfga"),
			},
			// input
			request: &openfgav1.WriteRequest{
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
						Object: "org:openfga", Relation: "owner", User: "user:github|jose@openfga"},
					},
				},
			},
		},
		{
			_name: "ExecuteSucceedsForWriteOnly",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type repo
  relations
	define admin: [user]
	define writer: [user, team#member]

type org
  relations
	define owner: [user]

type team
  relations
	define member: [user]`).TypeDefinitions,
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "org:openfga", Relation: "owner", User: "user:github|jose@openfga"},
						{Object: "repo:openfga/openfga", Relation: "admin", User: "user:github|jose@openfga"},
						{Object: "repo:openfga/openfga", Relation: "writer", User: "team:openfga/iam#member"},
						{Object: "team:openfga/iam", Relation: "member", User: "user:iaco@openfga"},
					},
				},
			},
		},
		{
			_name: "ExecuteSucceedsForDeleteOnly",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type repo
  relations
	define admin: [user]
	define writer: [user, team#member]

type org
  relations
	define owner: [user]

type team
  relations
	define member: [user]`).TypeDefinitions,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("org:openfga", "owner", "user:github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "user:github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "team:openfga/iam#member"),
				tuple.NewTupleKey("team:openfga/iam", "member", "user:iaco@openfga"),
			},
			// input
			request: &openfgav1.WriteRequest{
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
						{Object: "org:openfga", Relation: "owner", User: "user:github|jose@openfga"},
						{Object: "repo:openfga/openfga", Relation: "admin", User: "user:github|jose@openfga"},
						{Object: "repo:openfga/openfga", Relation: "writer", User: "team:openfga/iam#member"},
						{Object: "team:openfga/iam", Relation: "member", User: "user:iaco@openfga"},
					},
				},
			},
		},
		{
			_name: "ExecuteSucceedsForWriteAndDelete",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type repo
  relations
	define admin: [user]
	define writer: [user, team#member]

type org
  relations
	define owner: [user]

type team
  relations
	define member: [user]`).TypeDefinitions,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("org:openfga", "owner", "user:github|yenkel@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "reader", "team:openfga/platform#member"),
			},
			// input
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "org:openfga", Relation: "owner", User: "user:github|jose@openfga"},
						{Object: "repo:openfga/openfga", Relation: "admin", User: "user:github|jose@openfga"},
						{Object: "repo:openfga/openfga", Relation: "writer", User: "team:openfga/iam#member"},
						{Object: "team:openfga/iam", Relation: "member", User: "user:iaco@openfga"},
					},
				},
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
						{Object: "org:openfga", Relation: "owner", User: "user:github|yenkel@openfga"},
						{Object: "repo:openfga/openfga", Relation: "reader", User: "team:openfga/platform#member"},
					},
				},
			},
		},
		{
			_name: "Execute_fails_if_type_in_userset_value_was_not_found",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type document
  relations
	define viewer: [user]`).TypeDefinitions,
			},
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "document:doc1", Relation: "viewer", User: "group:engineering#member"},
					},
				},
			},
			err: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    &tuple.TypeNotFoundError{TypeName: "group"},
					TupleKey: tuple.NewTupleKey("document:doc1", "viewer", "group:engineering#member"),
				},
			),
		},
		{
			_name: "Execute_fails_if_relation_in_userset_value_was_not_found",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type document
  relations
	define viewer: [user]`).TypeDefinitions,
			},
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "document:doc1", Relation: "viewer", User: "document:doc1#editor"},
					},
				},
			},
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
		// Begin section with tests for schema version 1.1
		{
			_name: "Delete_succeeds_even_if_user_field_contains_a_type_that_is_not_allowed_by_the_current_authorization_model",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type org
  relations
	define owner: [user]`).TypeDefinitions,
			},
			tuples: []*openfgav1.TupleKey{
				{
					Object:   "org:openfga",
					Relation: "owner",
					User:     "impossible:1",
				},
			},
			request: &openfgav1.WriteRequest{
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
						{Object: "org:openfga", Relation: "owner", User: "impossible:1"},
					},
				},
			},
		},
		{
			_name: "Write_fails_if_user_field_contains_a_type_that_does_not_exist",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "org",
						Relations: map[string]*openfgav1.Userset{
							"owner": {Userset: &openfgav1.Userset_This{}},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"owner": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
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
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "org:openfga", Relation: "owner", User: "undefined:1"},
					},
				},
			},
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
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {Userset: &openfgav1.Userset_This{}},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
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
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "document:budget", Relation: "reader", User: "user:abc"},
					},
				},
			},
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
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
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
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "document:budget", Relation: "reader", User: "group:abc#member"},
					},
				},
			},
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
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
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
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "document:budget", Relation: "reader", User: "undefined:abc#member"},
					},
				},
			},
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
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {Userset: &openfgav1.Userset_This{}},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
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
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "document:budget", Relation: "reader", User: "user:bob"},
					},
				},
			},
		},
		{
			_name: "Write_fails_if_user_field_contains_a_type_that_is_not_allowed_by_the_authorization_model_(which_only_allows_group:...#member)",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {Userset: &openfgav1.Userset_This{}},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
				},
			},
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "document:budget", Relation: "reader", User: "user:abc"},
					},
				},
			},
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
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgav1.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {Userset: &openfgav1.Userset_This{}},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type:               "group",
											RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"},
										},
									},
								},
							},
						},
					},
				},
			},
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "document:budget", Relation: "reader", User: "group:abc#member"},
					},
				},
			},
		},
		{
			_name: "Multiple_writes_succeed_if_user_fields_contain_a_type_that_is_allowed_by_the_authorization_model",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgav1.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {Userset: &openfgav1.Userset_This{}},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
										{
											Type:               "group",
											RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"},
										},
									},
								},
							},
						},
					},
				},
			},
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "document:budget", Relation: "reader", User: "group:abc#member"},
						{Object: "document:budget", Relation: "reader", User: "user:def"},
					},
				},
			},
		},
		{
			_name: "Write_succeeds_if_user_is_wildcard_and_type_references_a_specific_type",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {Userset: &openfgav1.Userset_This{}},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
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
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "document:budget", Relation: "reader", User: "user:*"},
					},
				},
			},
		},
		{
			_name: "Write_fails_if_user_is_a_typed_wildcard_and_the_type_restrictions_do_not_permit_it",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {Userset: &openfgav1.Userset_This{}},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
				},
			},
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "document:budget", Relation: "reader", User: "group:*"},
					},
				},
			},
			err: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("the typed wildcard 'group:*' is not an allowed type restriction for 'document#reader'"),
					TupleKey: tuple.NewTupleKey("document:budget", "reader", "group:*"),
				},
			),
		},
		{
			_name: "invalid_type_restriction_in_write_body",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgav1.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
					{
						Type: "resource",
						Relations: map[string]*openfgav1.Userset{
							"writer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"writer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
				},
			},
			request: &openfgav1.WriteRequest{
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "resource:bad", Relation: "writer", User: "group:fga"},
					},
				},
			},
			err: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("type 'group' is not an allowed type restriction for 'resource#writer'"),
					TupleKey: tuple.NewTupleKey("resource:bad", "writer", "group:fga"),
				},
			),
		},
	}
	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store := ulid.Make().String()

			err := datastore.WriteAuthorizationModel(ctx, store, test.model)
			require.NoError(t, err)

			if test.tuples != nil {
				err := datastore.Write(
					ctx,
					store,
					[]*openfgav1.TupleKeyWithoutCondition{},
					test.tuples,
				)
				require.NoError(t, err)
			}

			cmd := commands.NewWriteCommand(datastore)
			test.request.StoreId = store
			if test.request.AuthorizationModelId == "" {
				test.request.AuthorizationModelId = test.model.Id
			}
			resp, gotErr := cmd.Execute(ctx, test.request)

			if test.err != nil {
				require.ErrorIs(t, gotErr, test.err)
				require.ErrorContains(t, gotErr, test.err.Error())
			}

			if test.response != nil {
				require.NoError(t, gotErr)
				require.Equal(t, test.response, resp)
			}
		})
	}
}
