package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestWriteAssertions(t *testing.T, dbTester teststorage.DatastoreTester) {
	type writeAssertionsTestSettings struct {
		_name   string
		request *openfgav1pb.WriteAssertionsRequest
		err     error
	}

	store := testutils.CreateRandomString(10)

	githubModelReq := &openfgav1pb.WriteAuthorizationModelRequest{
		StoreId: store,
		TypeDefinitions: &openfgav1pb.TypeDefinitions{
			TypeDefinitions: []*openfgav1pb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgav1pb.Userset{
						"reader": {Userset: &openfgav1pb.Userset_This{}},
					},
				},
			},
		},
	}

	var tests = []writeAssertionsTestSettings{
		{
			_name: "writing assertions succeeds",
			request: &openfgav1pb.WriteAssertionsRequest{
				StoreId: store,
				Assertions: []*openfga.Assertion{{
					TupleKey: &openfga.TupleKey{
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
			request: &openfgav1pb.WriteAssertionsRequest{
				StoreId: store,
				Assertions: []*openfga.Assertion{{
					TupleKey: &openfga.TupleKey{
						Object:   "repo:test",
						Relation: "reader",
						User:     "elbuo",
					},
					Expectation: false,
				},
				},
			},
		},
	}

	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	modelID, err := commands.NewWriteAuthorizationModelCommand(datastore, logger).Execute(ctx, githubModelReq)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			cmd := commands.NewWriteAssertionsCommand(datastore, datastore, logger)
			test.request.AuthorizationModelId = modelID.AuthorizationModelId

			_, err := cmd.Execute(ctx, test.request)
			if err != test.err {
				t.Fatalf("got '%v', expected '%v'", err, test.err)
			}
		})
	}
}
