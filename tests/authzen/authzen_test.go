package authzen_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/tests"
)

const (
	testStoreName = "test-store"

	simpleReaderModel = `
		model
			schema 1.1
		type user
		type document
			relations
				define reader: [user]
	`

	simpleReaderWriterModel = `
		model
			schema 1.1
		type user
		type document
			relations
				define reader: [user]
				define writer: [user]
	`
)

// testContext holds shared test state.
type testContext struct {
	t             *testing.T
	storeID       string
	modelID       string
	grpcAddr      string
	httpAddr      string
	openfgaClient openfgav1.OpenFGAServiceClient
	authzenClient authzenv1.AuthZenServiceClient
}

// setupTestContextWithExperimentals creates a new test server with specified experimental flags.
func setupTestContextWithExperimentals(t *testing.T, experimentals []string) *testContext {
	t.Helper()

	cfg := testutils.MustDefaultConfig()
	cfg.Experimentals = experimentals
	cfg.Log.Level = "error"
	cfg.Datastore.Engine = "memory"
	cfg.Authzen.BaseURL = "http://openfga.example"

	tests.StartServer(t, cfg)

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	tc := &testContext{
		t:             t,
		grpcAddr:      cfg.GRPC.Addr,
		httpAddr:      cfg.HTTP.Addr,
		openfgaClient: openfgav1.NewOpenFGAServiceClient(conn),
		authzenClient: authzenv1.NewAuthZenServiceClient(conn),
	}

	return tc
}

// setupTestContext creates a new test server and returns a testContext.
// The server is automatically configured with AuthZEN experimental flag enabled.
func setupTestContext(t *testing.T) *testContext {
	t.Helper()
	return setupTestContextWithExperimentals(t, []string{serverconfig.ExperimentalAuthZen})
}

// setupTestContextWithStore creates a test context and store.
func setupTestContextWithStore(t *testing.T) *testContext {
	t.Helper()

	tc := setupTestContext(t)
	tc.createStore(testStoreName)

	return tc
}

// setupTestContextWithStoreAndModel creates a test context, store, and model.
func setupTestContextWithStoreAndModel(t *testing.T, model string) *testContext {
	t.Helper()

	tc := setupTestContextWithStore(t)
	tc.writeModel(model)

	return tc
}

// createStore creates a new store.
//
//nolint:unparam // name parameter kept for test flexibility
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
//
//nolint:unparam // resource parameter kept for test flexibility
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
	idx := strings.Index(s, ":")
	if idx == -1 {
		return s
	}
	return s[:idx]
}

// parseID extracts the id from "type:id" format.
func parseID(s string) string {
	idx := strings.Index(s, ":")
	if idx == -1 {
		return s
	}
	return s[idx+1:]
}
