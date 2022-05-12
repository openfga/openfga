package commands

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type writeCommandTest struct {
	_name           string
	typeDefinitions []*openfgav1pb.TypeDefinition
	tuples          []*openfga.TupleKey
	request         *openfgav1pb.WriteRequest
	err             error
	response        *openfgav1pb.WriteResponse
}

var tk = &openfga.TupleKey{
	Object:   "repository:auth0/express-jwt",
	Relation: "administrator",
	User:     "github|alice@auth0.com",
}

var writeCommandTests = []writeCommandTest{
	{
		_name: "ExecuteWithEmptyWritesAndDeletesReturnsZeroWrittenAndDeleted",
		// input
		request: &openfgav1pb.WriteRequest{},
		// output
		err: serverErrors.InvalidWriteInput,
	},
	{
		_name: "ExecuteWithSameTupleInWritesReturnsError",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgav1pb.Userset{
					"administrator": {},
				},
			},
		},
		// input
		request: &openfgav1pb.WriteRequest{
			Writes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{tk, tk}},
		},
		// output
		err: serverErrors.DuplicateTupleInWrite(tk),
	},
	{
		_name: "ExecuteWithSameTupleInDeletesReturnsError",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgav1pb.Userset{
					"administrator": {},
				},
			},
		},
		// input
		request: &openfgav1pb.WriteRequest{
			Deletes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{tk, tk}},
		},
		// output
		err: serverErrors.DuplicateTupleInWrite(tk),
	},
	{
		_name: "ExecuteWithSameTupleInWritesAndDeletesReturnsError",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgav1pb.Userset{
					"administrator": {},
				},
			},
		},
		// input
		request: &openfgav1pb.WriteRequest{
			Writes:  &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{tk}},
			Deletes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{tk}},
		},
		// output
		err: serverErrors.DuplicateTupleInWrite(tk),
	},
	{
		_name: "ExecuteDeleteTupleWhichDoesNotExistReturnsError",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repository",
				Relations: map[string]*openfgav1pb.Userset{
					"administrator": {},
				},
			},
		},
		// input
		request: &openfgav1pb.WriteRequest{
			Deletes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{tk}},
		},
		// output
		err: serverErrors.WriteFailedDueToInvalidInput(storage.InvalidWriteInputError(tk, openfga.TupleOperation_DELETE)),
	},
	{
		_name: "ExecuteWithWriteTupleWithInvalidAuthorizationModelReturnsError",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgav1pb.Userset{},
		}},
		// input
		request: &openfgav1pb.WriteRequest{
			Writes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{tk}},
		},
		// output
		err: serverErrors.TypeNotFound("repository"),
	},
	{
		_name: "ExecuteWithWriteTupleWithMissingUserError",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"owner": {},
			},
		}},
		// input
		request: &openfgav1pb.WriteRequest{
			Writes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{{
				Object:   "repo:auth0",
				Relation: "owner",
			}}},
		},
		// output
		err: serverErrors.InvalidTuple("missing user", &openfga.TupleKey{Object: "repo:auth0", Relation: "owner"}),
	},
	{
		_name: "ExecuteWithWriteTupleWithMissingObjectError",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"owner": {},
			},
		}},
		// input
		request: &openfgav1pb.WriteRequest{
			Writes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{{
				Relation: "owner",
				User:     "elbuo@github.com",
			}}},
		},
		// output
		err: serverErrors.InvalidObjectFormat(&openfga.TupleKey{
			Relation: "owner",
			User:     "elbuo@github.com",
		}),
	},
	{
		_name: "ExecuteWithWriteTupleWithInvalidRelationError",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"owner": {},
			},
		}},
		// input
		request: &openfgav1pb.WriteRequest{
			Writes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{{
				Object: "repo:auth0",
				User:   "elbuo@github.com",
			}}},
		},
		// output
		err: serverErrors.InvalidTuple("invalid relation", &openfga.TupleKey{Object: "repo:auth0", User: "elbuo@github.com"}),
	},
	{
		_name: "ExecuteWithWriteTupleWithNotFoundRelationError",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"owner": {},
			},
		}},
		// input
		request: &openfgav1pb.WriteRequest{
			Writes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{{
				Object:   "repo:auth0",
				Relation: "BadRelation",
				User:     "elbuo@github.com",
			}}},
		},
		// output
		err: serverErrors.RelationNotFound("BadRelation", "repo",
			&openfga.TupleKey{Object: "repo:auth0", Relation: "BadRelation", User: "elbuo@github.com"}),
	},
	{
		_name: "ExecuteDeleteTupleWithInvalidAuthorizationModelIgnoresAuthorizationModelValidation",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgav1pb.Userset{},
		}},
		tuples: []*openfga.TupleKey{tk},
		// input
		request: &openfgav1pb.WriteRequest{
			Deletes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{tk}},
		},
	},
	{
		_name: "ExecuteWithInvalidObjectFormatReturnsError",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgav1pb.Userset{},
		}},
		// input
		request: &openfgav1pb.WriteRequest{
			Writes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{{
				// invalid because it has no :
				Object:   "auth0",
				Relation: "owner",
				User:     "github|jose@auth0.com",
			}}},
		},
		// output
		err: serverErrors.InvalidObjectFormat(&openfga.TupleKey{
			Object:   "auth0",
			Relation: "owner",
			User:     "github|jose@auth0.com",
		}),
	},
	{
		_name: "ExecuteReturnsErrorIfWriteRelationDoesNotExistInAuthorizationModel",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"admin": {},
			},
		},
			{
				Type: "org",
				Relations: map[string]*openfgav1pb.Userset{
					"manages": {},
				},
			}},
		// input
		request: &openfgav1pb.WriteRequest{
			Writes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{
				{
					Object:   "repo:auth0/express-jwt",
					Relation: "writer",
					User:     "github|jose@auth0.com",
				},
			}},
		},
		// output
		err: serverErrors.RelationNotFound("writer", "repo", &openfga.TupleKey{
			Object:   "repo:auth0/express-jwt",
			Relation: "writer",
			User:     "github|jose@auth0.com",
		}),
	},
	{
		_name: "ExecuteReturnsSuccessIfDeleteRelationDoesNotExistInAuthorizationModel",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {},
				},
			},
			{
				Type: "org",
				Relations: map[string]*openfgav1pb.Userset{
					"manages": {},
				},
			}},
		tuples: []*openfga.TupleKey{
			{
				Object:   "org:auth0",
				Relation: "owner",
				User:     "github|jose@auth0.com",
			},
		},
		// input
		request: &openfgav1pb.WriteRequest{
			Deletes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{
				{
					Object:   "org:auth0",
					Relation: "owner",
					User:     "github|jose@auth0.com",
				},
			}},
		},
	},
	{
		_name: "ExecuteSucceedsForWriteOnly",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin":  {},
					"writer": {},
				},
			},
			{
				Type: "org",
				Relations: map[string]*openfgav1pb.Userset{
					"owner": {},
				},
			},
			{
				Type: "team",
				Relations: map[string]*openfgav1pb.Userset{
					"member": {},
				},
			}},
		// input
		request: &openfgav1pb.WriteRequest{
			Writes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{
				{
					Object:   "org:auth0",
					Relation: "owner",
					User:     "github|jose@auth0.com",
				},
				{
					Object:   "repo:auth0/express-jwt",
					Relation: "admin",
					User:     "github|jose@auth0.com",
				},
				{
					Object:   "repo:auth0/express-jwt",
					Relation: "writer",
					User:     "team:auth0/iam#member",
				},
				{
					Object:   "team:auth0/iam",
					Relation: "member",
					User:     "iaco@auth0.com",
				},
			}},
		},
	},
	{
		_name: "ExecuteSucceedsForDeleteOnly",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin":  {},
					"writer": {},
				},
			},
			{
				Type: "org",
				Relations: map[string]*openfgav1pb.Userset{
					"owner": {},
				},
			},
			{
				Type: "team",
				Relations: map[string]*openfgav1pb.Userset{
					"member": {},
				},
			}},
		tuples: []*openfga.TupleKey{
			{
				Object:   "org:auth0",
				Relation: "owner",
				User:     "github|jose@auth0.com",
			},
			{
				Object:   "repo:auth0/express-jwt",
				Relation: "admin",
				User:     "github|jose@auth0.com",
			},
			{
				Object:   "repo:auth0/express-jwt",
				Relation: "writer",
				User:     "team:auth0/iam#member",
			},
			{
				Object:   "team:auth0/iam",
				Relation: "member",
				User:     "iaco@auth0.com",
			},
		},
		// input
		request: &openfgav1pb.WriteRequest{
			Deletes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{
				{
					Object:   "org:auth0",
					Relation: "owner",
					User:     "github|jose@auth0.com",
				},
				{
					Object:   "repo:auth0/express-jwt",
					Relation: "admin",
					User:     "github|jose@auth0.com",
				},
				{
					Object:   "repo:auth0/express-jwt",
					Relation: "writer",
					User:     "team:auth0/iam#member",
				},
				{
					Object:   "team:auth0/iam",
					Relation: "member",
					User:     "iaco@auth0.com",
				},
			}},
		},
	},
	{
		_name: "ExecuteSucceedsForWriteAndDelete",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin":  {},
					"writer": {},
				},
			},
			{
				Type: "org",
				Relations: map[string]*openfgav1pb.Userset{
					"owner": {},
				},
			},
			{
				Type: "team",
				Relations: map[string]*openfgav1pb.Userset{
					"member": {},
				},
			}},
		tuples: []*openfga.TupleKey{
			{
				Object:   "org:auth0",
				Relation: "owner",
				User:     "github|yenkel@auth0.com",
			},
			{
				Object:   "repo:auth0/express-jwt",
				Relation: "reader",
				User:     "team:auth0/platform#member",
			},
		},
		// input
		request: &openfgav1pb.WriteRequest{
			Writes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{
				{
					Object:   "org:auth0",
					Relation: "owner",
					User:     "github|jose@auth0.com",
				},
				{
					Object:   "repo:auth0/express-jwt",
					Relation: "admin",
					User:     "github|jose@auth0.com",
				},
				{
					Object:   "repo:auth0/express-jwt",
					Relation: "writer",
					User:     "team:auth0/iam#member",
				},
				{
					Object:   "team:auth0/iam",
					Relation: "member",
					User:     "iaco@auth0.com",
				},
			}},
			Deletes: &openfga.TupleKeys{TupleKeys: []*openfga.TupleKey{
				{
					Object:   "org:auth0",
					Relation: "owner",
					User:     "github|yenkel@auth0.com",
				},
				{
					Object:   "repo:auth0/express-jwt",
					Relation: "reader",
					User:     "team:auth0/platform#member",
				},
			}},
		},
	},
}

func TestWriteCommand(t *testing.T) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	backends, err := testutils.BuildAllBackends(ctx, tracer, logger)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range writeCommandTests {
		t.Run(test._name, func(t *testing.T) {
			store := testutils.CreateRandomString(10)
			modelID, err := id.NewString()
			if err != nil {
				t.Fatal(err)
			}
			if test.typeDefinitions != nil {
				if err := backends.AuthorizationModelBackend.WriteAuthorizationModel(ctx, store, modelID, &openfgav1pb.TypeDefinitions{TypeDefinitions: test.typeDefinitions}); err != nil {
					t.Fatalf("%s: WriteAuthorizationModel: got '%v', want nil", test._name, err)
				}
			}

			if test.tuples != nil {
				if err := backends.TupleBackend.Write(ctx, store, []*openfga.TupleKey{}, test.tuples); err != nil {
					t.Fatalf("error writing test tuples: %v", err)
				}
			}

			cmd := NewWriteCommand(backends.TupleBackend, backends.AuthorizationModelBackend, tracer, logger)
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

func TestValidateWriteTuples(t *testing.T) {
	type test struct {
		name          string
		deletes       []*openfga.TupleKey
		writes        []*openfga.TupleKey
		expectedError error
	}

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	backends, err := testutils.BuildAllBackends(ctx, tracer, logger)
	if err != nil {
		t.Fatal(err)
	}

	items := make([]*openfga.TupleKey, backends.TupleBackend.MaxTuplesInWriteOperation()+1)
	for i := 0; i < backends.TupleBackend.MaxTuplesInWriteOperation()+1; i++ {
		items[i] = &openfga.TupleKey{
			Object:   fmt.Sprintf("%s:1", testutils.CreateRandomString(459)),
			Relation: testutils.CreateRandomString(50),
			User:     testutils.CreateRandomString(512),
		}
	}

	cmd := NewWriteCommand(backends.TupleBackend, backends.AuthorizationModelBackend, tracer, logger)

	tests := []test{
		{
			name:    "empty deletes and writes",
			deletes: []*openfga.TupleKey{},
			writes:  []*openfga.TupleKey{},
		},
		{
			name:    "good deletes and writes",
			deletes: []*openfga.TupleKey{items[0], items[1]},
			writes:  []*openfga.TupleKey{items[2], items[3]},
		},
		{
			name:          "duplicate deletes",
			deletes:       []*openfga.TupleKey{items[0], items[1], items[0]},
			writes:        []*openfga.TupleKey{},
			expectedError: serverErrors.DuplicateTupleInWrite(items[0]),
		},
		{
			name:          "duplicate writes",
			deletes:       []*openfga.TupleKey{},
			writes:        []*openfga.TupleKey{items[0], items[1], items[0]},
			expectedError: serverErrors.DuplicateTupleInWrite(items[0]),
		},
		{
			name:          "same item appeared in writes and deletes",
			deletes:       []*openfga.TupleKey{items[2], items[1]},
			writes:        []*openfga.TupleKey{items[0], items[1]},
			expectedError: serverErrors.DuplicateTupleInWrite(items[1]),
		},
		{
			name:          "too many items writes and deletes",
			deletes:       items[:5],
			writes:        items[5:],
			expectedError: serverErrors.ExceededEntityLimit("write operations", cmd.tupleBackend.MaxTuplesInWriteOperation()),
		},
	}

	for _, test := range tests {
		err := cmd.validateWriteTuples(test.deletes, test.writes)
		if !reflect.DeepEqual(err, test.expectedError) {
			t.Errorf("%s: Expected error %v, got %v", test.name, test.expectedError, err)
		}
	}

}
