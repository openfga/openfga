package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestWriteAssertions(t *testing.T, datastore storage.OpenFGADatastore) {
	type writeAssertionsTestSettings struct {
		_name   string
		request *openfgapb.WriteAssertionsRequest
		err     error
	}

	store := testutils.CreateRandomString(10)

	githubModelReq := &openfgapb.WriteAuthorizationModelRequest{
		StoreId: store,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"reader": {Userset: &openfgapb.Userset_This{}},
				},
			},
		},
	}

	var tests = []writeAssertionsTestSettings{
		{
			_name: "writing assertions succeeds",
			request: &openfgapb.WriteAssertionsRequest{
				StoreId: store,
				Assertions: []*openfgapb.Assertion{{
					TupleKey: &openfgapb.TupleKey{
						Object:   "repo:test",
						Relation: "reader",
						User:     "elbuo",
					},
					Expectation: false,
				}},
			},
		},
		{
			_name: "writing empty assertions succeeds",
			request: &openfgapb.WriteAssertionsRequest{
				StoreId:    store,
				Assertions: []*openfgapb.Assertion{},
			},
		},
		{
			_name: "writing assertion with invalid relation fails",
			request: &openfgapb.WriteAssertionsRequest{
				StoreId: store,
				Assertions: []*openfgapb.Assertion{
					{
						TupleKey: &openfgapb.TupleKey{
							Object:   "repo:test",
							Relation: "invalidrelation",
							User:     "elbuo",
						},
						Expectation: false,
					},
				},
			},
			err: serverErrors.RelationNotFound("invalidrelation", "repo", &openfgapb.TupleKey{
				Object:   "repo:test",
				Relation: "invalidrelation",
				User:     "elbuo",
			}),
		},
	}

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	modelID, err := commands.NewWriteAuthorizationModelCommand(datastore, logger).Execute(ctx, githubModelReq)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			cmd := commands.NewWriteAssertionsCommand(datastore, logger)
			test.request.AuthorizationModelId = modelID.AuthorizationModelId

			_, err := cmd.Execute(ctx, test.request)
			if diff := cmp.Diff(err, test.err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("mismatch (-got +want):\n%s", diff)
			}
		})
	}
}
