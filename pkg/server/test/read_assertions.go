package test

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/server"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReadAssertionQuery(t *testing.T, s *server.Server) {
	type readAssertionsQueryTest struct {
		_name            string
		request          *openfgav1.ReadAssertionsRequest
		expectedResponse *openfgav1.ReadAssertionsResponse
	}

	var tests = []readAssertionsQueryTest{
		{
			_name:   "ReturnsAssertionModelNotFound",
			request: &openfgav1.ReadAssertionsRequest{StoreId: "store", AuthorizationModelId: "test"},
			expectedResponse: &openfgav1.ReadAssertionsResponse{
				AuthorizationModelId: "test",
				Assertions: []*openfgav1.Assertion{
					{
						TupleKey: tuple.NewAssertionTupleKey("repo:test", "reader", "user:test"),
					},
				},
			},
		},
	}

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store := ulid.Make().String()
			model := parser.MustTransformDSLToProto(string(`model
				schema 1.1
				  type user
				  type repo
				relations
				  define reader: [user]
				  define can_read: reader`))

			modelID, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         store,
				TypeDefinitions: model.GetTypeDefinitions(),
				SchemaVersion:   typesystem.SchemaVersion1_1,
				Conditions:      model.GetConditions(),
			})
			require.NoError(t, err)

			test.expectedResponse.AuthorizationModelId = modelID.GetAuthorizationModelId()

			_, err = s.WriteAssertions(ctx, &openfgav1.WriteAssertionsRequest{
				StoreId:              store,
				AuthorizationModelId: modelID.GetAuthorizationModelId(),
				Assertions: []*openfgav1.Assertion{
					{
						TupleKey: tuple.NewAssertionTupleKey("repo:test", "reader", "user:test"),
					},
				},
			})
			require.NoError(t, err)

			actualResponse, err := s.ReadAssertions(ctx, &openfgav1.ReadAssertionsRequest{
				StoreId:              store,
				AuthorizationModelId: modelID.GetAuthorizationModelId(),
			})
			require.NoError(t, err)
			require.Equal(t, test.expectedResponse, actualResponse)
		})
	}
}
