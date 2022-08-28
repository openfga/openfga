package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type writeCommandTest struct {
	name            string
	typeDefinitions []*openfgapb.TypeDefinition
	tuples          []*openfgapb.TupleKey
	request         *openfgapb.WriteRequest
	err             error
	response        *openfgapb.WriteResponse
}

var tk = tuple.NewTupleKey("repository:openfga/openfga", "administrator", "github|alice@openfga")

var writeCommandTests = []writeCommandTest{
	{
		name: "ExecuteWithEmptyWritesAndDeletesReturnsZeroWrittenAndDeleted",
		// input
		request: &openfgapb.WriteRequest{},
		// output
		err: serverErrors.InvalidWriteInput,
	},
	{
		name: "ExecuteWithSameTupleInWritesReturnsError",
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
		name: "ExecuteWithSameTupleInDeletesReturnsError",
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
		name: "ExecuteWithSameTupleInWritesAndDeletesReturnsError",
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
		name: "ExecuteDeleteTupleWhichDoesNotExistReturnsError",
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
		name: "ExecuteWithWriteTupleWithInvalidAuthorizationModelReturnsError",
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
		name: "ExecuteWithWriteTupleWithMissingUserError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgapb.Userset{
				"owner": {},
			},
		}},
		// input
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga", "owner", ""),
			}},
		},
		// output
		err: serverErrors.InvalidTuple("the 'user' field must be a non-empty string", tuple.NewTupleKey("repo:openfga", "owner", "")),
	},
	{
		name: "ExecuteWithWriteTupleWithMissingObjectError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgapb.Userset{
				"owner": {},
			},
		}},
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
		name: "ExecuteWithWriteTupleWithInvalidRelationError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgapb.Userset{
				"owner": {},
			},
		}},
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
		name: "ExecuteWithWriteTupleWithNotFoundRelationError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgapb.Userset{
				"owner": {},
			},
		}},
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
		name: "ExecuteDeleteTupleWithInvalidAuthorizationModelIgnoresAuthorizationModelValidation",
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
		name: "ExecuteWithInvalidObjectFormatReturnsError",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgapb.Userset{},
		}},
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
		name: "ExecuteReturnsErrorIfWriteRelationDoesNotExistInAuthorizationModel",
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
		name: "ExecuteReturnsSuccessIfDeleteRelationDoesNotExistInAuthorizationModel",
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
		name: "ExecuteSucceedsForWriteOnly",
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
				tuple.NewTupleKey("org:openfga", "owner", "github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "team:openfga/iam#member"),
				tuple.NewTupleKey("team:openfga/iam", "member", "iaco@openfga"),
			}},
		},
	},
	{
		name: "ExecuteSucceedsForDeleteOnly",
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
		name: "ExecuteSucceedsForWriteAndDelete",
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
		name: "undefined type in userset value",
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "document",
				Relations: map[string]*openfgapb.Userset{
					"viewer": {
						Userset: &openfgapb.Userset_This{},
					},
				},
			},
		},
		tuples: []*openfgapb.TupleKey{},
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
		name: "undefined type relation in userset value",
		typeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "document",
				Relations: map[string]*openfgapb.Userset{
					"viewer": {
						Userset: &openfgapb.Userset_This{},
					},
				},
			},
		},
		tuples: []*openfgapb.TupleKey{},
		request: &openfgapb.WriteRequest{
			Writes: &openfgapb.TupleKeys{
				TupleKeys: []*openfgapb.TupleKey{
					tuple.NewTupleKey("document:doc1", "viewer", "document:doc1#editor"),
				},
			},
		},
		err: serverErrors.RelationNotFound("editor", "document", tuple.NewTupleKey("document:doc1", "viewer", "document:doc1#editor")),
	},
}

func TestWriteCommand(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	for _, test := range writeCommandTests {
		t.Run(test.name, func(t *testing.T) {
			store := testutils.CreateRandomString(10)
			modelID, err := id.NewString()
			require.NoError(err)

			if test.typeDefinitions != nil {
				err = datastore.WriteAuthorizationModel(ctx, store, modelID, &openfgapb.TypeDefinitions{TypeDefinitions: test.typeDefinitions})
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

			// todo: validate the response bodies are equivalent
			if test.response != nil {
				require.NotNil(resp)
			}
		})
	}
}
