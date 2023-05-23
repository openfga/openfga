package test

import (
	"context"
	"fmt"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
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
	_name    string
	model    *openfgapb.AuthorizationModel
	tuples   []*openfgapb.TupleKey
	request  *openfgapb.WriteRequest
	err      error
	response *openfgapb.WriteResponse
}

var tk = tuple.NewTupleKey("repo:openfga/openfga", "admin", "user:github|alice@openfga")

var writeCommandTests = []writeCommandTest{
	{
		_name: "invalid_schema_version",
		model: &openfgapb.AuthorizationModel{
			Id:              ulid.Make().String(),
			SchemaVersion:   typesystem.SchemaVersion1_0,
			TypeDefinitions: parser.MustParse(`type repo`),
		},
		request: &openfgapb.WriteRequest{Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{tk}}},
		err:     serverErrors.ValidationError(typesystem.ErrInvalidSchemaVersion),
	},
	{
		_name: "ExecuteWithEmptyWritesAndDeletesReturnsZeroWrittenAndDeleted",
		// input
		model: &openfgapb.AuthorizationModel{
			Id:              ulid.Make().String(),
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`type repo`),
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
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type repo
			  relations
			    define admin: [user] as self
			`),
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
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type repo
			  relations
			    define writer: [user] as self
				define owner: [user] as self
				define viewer as writer or owner
			`),
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Object:   "repo:openfga/openfga",
				Relation: "viewer",
				User:     "user:github|alice@openfga.com",
			}}},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type repo
			  relations
			    define writer: [user] as self
				define owner: [user] as self
				define viewer as writer and owner
			`),
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Object:   "repo:openfga/openfga",
				Relation: "viewer",
				User:     "user:github|alice@openfga.com",
			}}},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type repo
			  relations
			    define writer: [user] as self
				define owner: [user] as self
				define banned: [user] as self
				define viewer as (writer or owner) but not banned
			`),
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Object:   "repo:openfga/openfga",
				Relation: "viewer",
				User:     "user:github|alice@openfga.com",
			}}},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type repo
			  relations
			    define writer: [user] as self
				define owner: [user] as self
				define viewer as writer
			`),
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Object:   "repo:openfga/openfga",
				Relation: "viewer",
				User:     "user:github|alice@openfga.com",
			}}},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type org
			  relations
			    define viewer: [user] as self
			
			type repo
			  relations
			    define owner: [org] as self
				define viewer as viewer from owner
			`),
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				Object:   "repo:openfga/openfga",
				Relation: "viewer",
				User:     "user:github|alice@openfga.com",
			}}},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user
			
			type repo
			  relations
			    define admin: [user] as self
			`),
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
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type repo
			  relations
			    define admin: [user] as self
			`),
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
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type repo
			  relations
			    define admin: [user] as self
			`),
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
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user
			type repository
			`),
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{tk}},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type repo
			  relations
			    define owner: [user] as self
			`),
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga", "owner", ""),
			}},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type repo
			  relations
			    define owner: [user] as self
			`),
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("", "owner", "user:elbuo@github.com"),
			}},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type repo
			  relations
			    define owner: [user] as self
			`),
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga", "", "user:elbuo@github.com"),
			}},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type repo
			  relations
			    define owner: [user] as self
			`),
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga", "undefined", "user:elbuo@github.com"),
			}},
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
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user
			type repo
			`),
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				// invalid because object has no :
				tuple.NewTupleKey("openfga", "owner", "user:github|jose@openfga"),
			}},
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user
			
			type repo
			  relations
			    define admin: [user] as self
				define writer: [user] as self
			`),
		},
		// input
		request: &openfgapb.WriteRequest{
			AuthorizationModelId: "01GZFXJ2XPAF8FBHDKJ83XAJQP",
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "user:github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "user:github|jon@openfga"),
			}},
		},
		err: serverErrors.AuthorizationModelNotFound("01GZFXJ2XPAF8FBHDKJ83XAJQP"),
	},
	{
		_name: "ExecuteReturnsSuccessIfDeleteRelationDoesNotExistInAuthorizationModel",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type org
			  relations
			    define manager: [user] as self
			`),
		},
		tuples: []*openfgapb.TupleKey{
			tuple.NewTupleKey("org:openfga", "owner", "user:github|jose@openfga"),
		},
		// input
		request: &openfgapb.WriteRequest{
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("org:openfga", "owner", "user:github|jose@openfga"),
			}},
		},
	},
	{
		_name: "ExecuteSucceedsForWriteOnly",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type repo
			  relations
			    define admin: [user] as self
				define writer: [user, team#member] as self

			type org
			  relations
			    define owner: [user] as self

			type team
			  relations
			    define member: [user] as self
			`),
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("org:openfga", "owner", "user:github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "user:github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "team:openfga/iam#member"),
				tuple.NewTupleKey("team:openfga/iam", "member", "user:iaco@openfga"),
			}},
		},
	},
	{
		_name: "ExecuteSucceedsForDeleteOnly",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type repo
			  relations
			    define admin: [user] as self
				define writer: [user, team#member] as self

			type org
			  relations
			    define owner: [user] as self

			type team
			  relations
			    define member: [user] as self
			`),
		},
		tuples: []*openfgapb.TupleKey{
			tuple.NewTupleKey("org:openfga", "owner", "user:github|jose@openfga"),
			tuple.NewTupleKey("repo:openfga/openfga", "admin", "user:github|jose@openfga"),
			tuple.NewTupleKey("repo:openfga/openfga", "writer", "team:openfga/iam#member"),
			tuple.NewTupleKey("team:openfga/iam", "member", "user:iaco@openfga"),
		},
		// input
		request: &openfgapb.WriteRequest{
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("org:openfga", "owner", "user:github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "user:github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "team:openfga/iam#member"),
				tuple.NewTupleKey("team:openfga/iam", "member", "user:iaco@openfga"),
			}},
		},
	},
	{
		_name: "ExecuteSucceedsForWriteAndDelete",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type repo
			  relations
			    define admin: [user] as self
				define writer: [user, team#member] as self

			type org
			  relations
			    define owner: [user] as self

			type team
			  relations
			    define member: [user] as self
			`),
		},
		tuples: []*openfgapb.TupleKey{
			tuple.NewTupleKey("org:openfga", "owner", "user:github|yenkel@openfga"),
			tuple.NewTupleKey("repo:openfga/openfga", "reader", "team:openfga/platform#member"),
		},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("org:openfga", "owner", "user:github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "user:github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "team:openfga/iam#member"),
				tuple.NewTupleKey("team:openfga/iam", "member", "user:iaco@openfga"),
			}},
			Deletes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("org:openfga", "owner", "user:github|yenkel@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "reader", "team:openfga/platform#member"),
			}},
		},
	},
	{
		_name: "Execute_fails_if_type_in_userset_value_was_not_found",
		// state
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type document
			  relations
			    define viewer: [user] as self
			`),
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{
				TupleKeys: []*openfgapb.TupleKey{
					tuple.NewTupleKey("document:doc1", "viewer", "group:engineering#member"),
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type document
			  relations
			    define viewer: [user] as self
			`),
		},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{
				TupleKeys: []*openfgapb.TupleKey{
					tuple.NewTupleKey("document:doc1", "viewer", "document:doc1#editor"),
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
		model: &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user
			
			type org
			  relations
			    define owner: [user] as self
			`),
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
		err: serverErrors.ValidationError(
			&tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the typed wildcard 'group:*' is not an allowed type restriction for 'document#reader'"),
				TupleKey: tuple.NewTupleKey("document:budget", "reader", "group:*"),
			},
		),
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

			cmd := commands.NewWriteCommand(
				datastore,
				logger,
				typesystem.MemoizedTypesystemResolverFunc(datastore),
			)
			test.request.StoreId = store
			if test.request.AuthorizationModelId == "" {
				test.request.AuthorizationModelId = test.model.Id
			}
			resp, gotErr := cmd.Execute(ctx, test.request)

			require.ErrorIs(gotErr, test.err)

			if test.response != nil {
				require.Equal(test.response, resp)
			}
		})
	}
}
