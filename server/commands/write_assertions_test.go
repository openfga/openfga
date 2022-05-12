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

func TestWriteAssertions(t *testing.T) {
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

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	backends, err := testutils.BuildAllBackends(ctx, tracer, logger)
	if err != nil {
		t.Fatal(err)
	}

	modelID, err := NewWriteAuthorizationModelCommand(backends.AuthorizationModelBackend, logger).Execute(ctx, githubModelReq)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			cmd := NewWriteAssertionsCommand(backends.AssertionsBackend, backends.AuthorizationModelBackend, logger)
			test.request.AuthorizationModelId = modelID.AuthorizationModelId

			_, err := cmd.Execute(ctx, test.request)
			if err != test.err {
				t.Fatalf("got '%v', expected '%v'", err, test.err)
			}
		})
	}
}
