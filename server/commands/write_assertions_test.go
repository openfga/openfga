package commands

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const writeTestStore = "auth0"

func TestWriteAssertions(t *testing.T) {
	type writeAssertionsTestSettings struct {
		_name   string
		request *openfgav1pb.WriteAssertionsRequest
		err     error
	}

	githubModelReq := &openfgav1pb.WriteAuthorizationModelRequest{
		StoreId: writeTestStore,
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
				StoreId: writeTestStore,
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
				StoreId: writeTestStore,
			},
		},
	}

	for _, test := range tests {
		ctx := context.Background()
		tracer := telemetry.NewNoopTracer()
		logger := logger.NewNoopLogger()

		t.Run(test._name, func(t *testing.T) {
			storage, err := testutils.BuildAllBackends(tracer)
			if err != nil {
				t.Fatalf("Error building backend: %s", err)
			}

			modelID, err := NewWriteAuthorizationModelCommand(storage.AuthorizationModelBackend, logger).Execute(ctx, githubModelReq)
			if err != nil {
				t.Fatalf("Error storing model: %s", err)
			}

			cmd := NewWriteAssertionsCommand(storage.AssertionsBackend, storage.AuthorizationModelBackend, logger)
			test.request.AuthorizationModelId = modelID.AuthorizationModelId
			_, err = cmd.Execute(ctx, test.request)
			if err != test.err {
				t.Fatalf("Expected error to be '%v', actual '%v'", test.err, err)
			}
		})
	}
}
