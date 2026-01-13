package authzen_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/tests"
)

// testContext holds shared test state.
type testContext struct {
	t             *testing.T
	storeID       string
	modelID       string
	grpcAddr      string
	openfgaClient openfgav1.OpenFGAServiceClient
	authzenClient authzenv1.AuthZenServiceClient
}

// setupTestContext creates a new test server and returns a testContext.
// The server is automatically configured with AuthZEN experimental flag enabled.
func setupTestContext(t *testing.T) *testContext {
	t.Helper()

	cfg := serverconfig.MustDefaultConfig()
	cfg.Experimentals = []string{serverconfig.ExperimentalEnableAuthZen}
	cfg.Log.Level = "error"
	cfg.Datastore.Engine = "memory"

	tests.StartServer(t, cfg)

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	tc := &testContext{
		t:             t,
		grpcAddr:      cfg.GRPC.Addr,
		openfgaClient: openfgav1.NewOpenFGAServiceClient(conn),
		authzenClient: authzenv1.NewAuthZenServiceClient(conn),
	}

	return tc
}

// createStore creates a new store.
func (tc *testContext) createStore(name string) {
	tc.t.Helper()

	resp, err := tc.openfgaClient.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: name,
	})
	require.NoError(tc.t, err)
	tc.storeID = resp.GetId()
}

// writeModel writes an authorization model.
func (tc *testContext) writeModel(model string) {
	tc.t.Helper()

	modelProto := testutils.MustTransformDSLToProtoWithID(model)

	resp, err := tc.openfgaClient.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         tc.storeID,
		TypeDefinitions: modelProto.GetTypeDefinitions(),
		SchemaVersion:   modelProto.GetSchemaVersion(),
		Conditions:      modelProto.GetConditions(),
	})
	require.NoError(tc.t, err)
	tc.modelID = resp.GetAuthorizationModelId()
}

// writeTuples writes relationship tuples to the store.
func (tc *testContext) writeTuples(tuples []*openfgav1.TupleKey) {
	tc.t.Helper()

	_, err := tc.openfgaClient.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId: tc.storeID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: tuples,
		},
	})
	require.NoError(tc.t, err)
}

// evaluate calls the AuthZEN Evaluation endpoint.
func (tc *testContext) evaluate(subject, resource, action string) (*authzenv1.EvaluationResponse, error) {
	return tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
		StoreId: tc.storeID,
		Subject: &authzenv1.Subject{
			Type: parseType(subject),
			Id:   parseID(subject),
		},
		Resource: &authzenv1.Resource{
			Type: parseType(resource),
			Id:   parseID(resource),
		},
		Action: &authzenv1.Action{
			Name: action,
		},
	})
}

// parseType extracts the type from "type:id" format.
func parseType(s string) string {
	for i, c := range s {
		if c == ':' {
			return s[:i]
		}
	}
	return s
}

// parseID extracts the id from "type:id" format.
func parseID(s string) string {
	for i, c := range s {
		if c == ':' {
			return s[i+1:]
		}
	}
	return s
}
