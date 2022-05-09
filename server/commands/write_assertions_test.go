package commands

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel"
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
			_name: "ExecuteWriteSucceeds",
			request: &openfgav1pb.WriteAssertionsRequest{
				StoreId: writeTestStore,
				Params: &openfgav1pb.WriteAssertionsRequestParams{
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
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			tracer := otel.Tracer("noop")
			storage, err := testutils.BuildAllBackends(tracer)
			if err != nil {
				t.Fatalf("Error building backend: %s", err)
			}
			ctx := context.Background()

			logger := logger.NewNoopLogger()

			authzModelId, err := NewWriteAuthorizationModelCommand(storage.AuthorizationModelBackend, logger).Execute(ctx, githubModelReq)
			if err != nil {
				t.Fatalf("Error storing model: %s", err)
			}
			cmd := NewWriteAssertionsCommand(storage.AssertionsBackend, storage.AuthorizationModelBackend, logger)
			test.request.AuthorizationModelId = authzModelId.AuthorizationModelId
			_, actualError := cmd.Execute(ctx, test.request)

			if actualError != test.err {
				t.Fatalf("Expected error to be nil, actual %s", actualError)
			}
		})
	}
}
