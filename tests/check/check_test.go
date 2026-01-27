package check

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
)

func TestMatrixMemory(t *testing.T) {
	runMatrixWithEngine(t, "memory")
}

func TestMatrixPostgres(t *testing.T) {
	runMatrixWithEngine(t, "postgres")
}

func TestMatrixMysql(t *testing.T) {
	runMatrixWithEngine(t, "mysql")
}

func TestMatrixDSQL(t *testing.T) {
	runMatrixWithEngine(t, "dsql")
}

// TODO: re-enable after investigating write contention in test
// func TestMatrixSqlite(t *testing.T) {
//	runMatrixWithEngine(t, "sqlite")
//}

func runMatrixWithEngine(t *testing.T, engine string) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	clientWithExperimentals := tests.BuildClientInterface(t, engine, []string{config.ExperimentalCheckOptimizations})
	RunMatrixTests(t, engine, true, clientWithExperimentals)

	clientWithoutExperimentals := tests.BuildClientInterface(t, engine, []string{})
	RunMatrixTests(t, engine, false, clientWithoutExperimentals)
}

func TestCheckMemory(t *testing.T) {
	testRunAll(t, "memory")
}

func TestCheckPostgres(t *testing.T) {
	testRunAll(t, "postgres")
}

func TestCheckMySQL(t *testing.T) {
	testRunAll(t, "mysql")
}

func TestCheckSQLite(t *testing.T) {
	testRunAll(t, "sqlite")
}

func TestCheckDSQL(t *testing.T) {
	testRunAll(t, "dsql")
}

// TODO move elsewhere as this isn't asserting on just Check API logs.
func TestServerLogs(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	// create mock OTLP server so we can assert that field "trace_id" is populated
	otlpServerPort, otlpServerPortReleaser := testutils.TCPRandomPort()
	localOTLPServerURL := fmt.Sprintf("localhost:%d", otlpServerPort)
	otlpServerPortReleaser()
	_ = mocks.NewMockTracingServer(t, otlpServerPort)

	cfg := config.MustDefaultConfig()
	cfg.Experimentals = append(cfg.Experimentals, "enable-check-optimizations")
	cfg.Trace.Enabled = true
	cfg.Trace.OTLP.Endpoint = localOTLPServerURL
	cfg.Datastore.Engine = "memory"

	observerLogger, logs := observer.New(zap.DebugLevel)
	serverCtx := &run.ServerContext{
		Logger: &logger.ZapLogger{
			Logger: zap.New(observerLogger),
		},
	}

	// We're starting a full fledged server because the logs we
	// want to observe are emitted on the interceptors/middleware layer.
	tests.StartServerWithContext(t, cfg, serverCtx)

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr,
		grpc.WithUserAgent("test-user-agent"),
	)
	client := openfgav1.NewOpenFGAServiceClient(conn)

	createStoreResp, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)

	storeID := createStoreResp.GetId()

	model := parser.MustTransformDSLToProto(`
		model
			schema 1.1
		type user

		type document
			relations
				define viewer: [user]`)

	writeModelResp, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: model.GetTypeDefinitions(),
		Conditions:      model.GetConditions(),
	})
	require.NoError(t, err)

	authorizationModelID := writeModelResp.GetAuthorizationModelId()

	_, err = client.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId: storeID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{Object: "document:1", Relation: "viewer", User: "user:anne"},
			},
		},
	})
	require.NoError(t, err)

	// clear all write logs
	logs.TakeAll()

	type test struct {
		_name           string
		endpoint        string
		grpcReq         *openfgav1.CheckRequest
		httpReqBody     io.Reader
		expectedError   bool
		expectedContext map[string]interface{}
	}

	tests := []test{
		{
			_name:    "grpc_check_success",
			endpoint: "check",
			grpcReq: &openfgav1.CheckRequest{
				AuthorizationModelId: authorizationModelID,
				StoreId:              storeID,
				TupleKey:             tuple.NewCheckRequestTupleKey("document:1", "viewer", "user:anne"),
			},
			expectedContext: map[string]interface{}{
				"grpc_service":                "openfga.v1.OpenFGAService",
				"grpc_method":                 "Check",
				"grpc_type":                   "unary",
				"grpc_code":                   int32(0),
				"raw_request":                 fmt.Sprintf(`{"store_id":"%s","tuple_key":{"object":"document:1","relation":"viewer","user":"user:anne"},"contextual_tuples":null,"authorization_model_id":"%s","trace":false,"context":null, "consistency":"UNSPECIFIED"}`, storeID, authorizationModelID),
				"raw_response":                `{"allowed":true,"resolution":""}`,
				"authorization_model_id":      authorizationModelID,
				"store_id":                    storeID,
				"user_agent":                  "test-user-agent" + " grpc-go/" + grpc.Version,
				"request.dispatch_throttled":  false,
				"request.datastore_throttled": false,
			},
		},
		{
			_name:    "http_check_success",
			endpoint: "check",
			httpReqBody: bytes.NewBufferString(`{
  "tuple_key": {
    "user": "user:anne",
    "relation": "viewer",
    "object": "document:1"
  },
  "authorization_model_id": "` + authorizationModelID + `"
}`),
			expectedContext: map[string]interface{}{
				"grpc_service":                "openfga.v1.OpenFGAService",
				"grpc_method":                 "Check",
				"grpc_type":                   "unary",
				"grpc_code":                   int32(0),
				"raw_request":                 fmt.Sprintf(`{"store_id":"%s","tuple_key":{"object":"document:1","relation":"viewer","user":"user:anne"},"contextual_tuples":null,"authorization_model_id":"%s","trace":false,"context":null, "consistency":"UNSPECIFIED"}`, storeID, authorizationModelID),
				"raw_response":                `{"allowed":true,"resolution":""}`,
				"authorization_model_id":      authorizationModelID,
				"store_id":                    storeID,
				"user_agent":                  "test-user-agent",
				"request.dispatch_throttled":  false,
				"request.datastore_throttled": false,
			},
		},
		{
			_name:    "grpc_check_error",
			endpoint: "check",
			grpcReq: &openfgav1.CheckRequest{
				AuthorizationModelId: authorizationModelID,
				StoreId:              storeID,
				TupleKey:             tuple.NewCheckRequestTupleKey("", "viewer", "user:anne"),
			},
			expectedError: true,
			expectedContext: map[string]interface{}{
				"grpc_service": "openfga.v1.OpenFGAService",
				"grpc_method":  "Check",
				"grpc_type":    "unary",
				"grpc_code":    int32(2000),
				"raw_request":  fmt.Sprintf(`{"store_id":"%s","tuple_key":{"object":"","relation":"viewer","user":"user:anne"},"contextual_tuples":null,"authorization_model_id":"%s","trace":false,"context":null,"consistency":"UNSPECIFIED"}`, storeID, authorizationModelID),
				"raw_response": `{"code":"validation_error", "message":"invalid CheckRequestTupleKey.Object: value does not match regex pattern \"^[^\\\\s]{2,256}$\""}`,
				"store_id":     storeID,
				"user_agent":   "test-user-agent" + " grpc-go/" + grpc.Version,
			},
		},
		{
			_name:    "http_check_error",
			endpoint: "check",
			httpReqBody: bytes.NewBufferString(`{
  "tuple_key": {
    "user": "user:anne",
    "relation": "viewer"
  },
  "authorization_model_id": "` + authorizationModelID + `"
}`),
			expectedError: true,
			expectedContext: map[string]interface{}{
				"grpc_service": "openfga.v1.OpenFGAService",
				"grpc_method":  "Check",
				"grpc_type":    "unary",
				"grpc_code":    int32(2000),
				"raw_request":  fmt.Sprintf(`{"store_id":"%s","tuple_key":{"object":"","relation":"viewer","user":"user:anne"},"contextual_tuples":null,"authorization_model_id":"%s","trace":false,"context":null,"consistency":"UNSPECIFIED"}`, storeID, authorizationModelID),
				"raw_response": `{"code":"validation_error", "message":"invalid CheckRequestTupleKey.Object: value does not match regex pattern \"^[^\\\\s]{2,256}$\""}`,
				"store_id":     storeID,
				"user_agent":   "test-user-agent",
			},
		},
		{
			_name:    "streamed_list_objects_success",
			endpoint: "streamed-list-objects",
			httpReqBody: bytes.NewBufferString(`{
  "type": "document",
  "relation": "viewer",
  "user": "user:anne",
  "authorization_model_id": "` + authorizationModelID + `"
}`),
			expectedError: false,
			expectedContext: map[string]interface{}{
				"grpc_service":                "openfga.v1.OpenFGAService",
				"grpc_method":                 "StreamedListObjects",
				"grpc_type":                   "server_stream",
				"grpc_code":                   int32(0),
				"raw_request":                 fmt.Sprintf(`{"authorization_model_id":"%s","context":null,"contextual_tuples":null,"relation":"viewer","store_id":"%s","type":"document","user":"user:anne","consistency":"UNSPECIFIED"}`, authorizationModelID, storeID),
				"raw_response":                `{"object":"document:1"}`,
				"store_id":                    storeID,
				"authorization_model_id":      authorizationModelID,
				"user_agent":                  "test-user-agent",
				"request.dispatch_throttled":  false,
				"request.datastore_throttled": false,
			},
		},
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			// clear observed logs after each run. We expect each test to log one line
			defer logs.TakeAll()

			if test.grpcReq != nil {
				_, err = client.Check(context.Background(), test.grpcReq)
			} else if test.httpReqBody != nil {
				var httpReq *http.Request
				httpReq, err = http.NewRequest("POST", "http://"+cfg.HTTP.Addr+"/stores/"+storeID+"/"+test.endpoint, test.httpReqBody)
				require.NoError(t, err)

				httpReq.Header.Set("User-Agent", "test-user-agent")
				client := &http.Client{}
				var resp *http.Response

				resp, err = client.Do(httpReq)
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
			if test.expectedError && test.grpcReq != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			actualLogs := logs.All()
			require.Len(t, actualLogs, 1)

			fields := actualLogs[0].ContextMap()
			require.Equal(t, test.expectedContext["grpc_service"], fields["grpc_service"])
			require.Equal(t, test.expectedContext["grpc_method"], fields["grpc_method"])
			require.Equal(t, test.expectedContext["grpc_type"], fields["grpc_type"])
			require.Equal(t, test.expectedContext["grpc_code"], fields["grpc_code"])
			require.JSONEq(t, test.expectedContext["raw_request"].(string), string(fields["raw_request"].(json.RawMessage)))
			require.JSONEq(t, test.expectedContext["raw_response"].(string), string(fields["raw_response"].(json.RawMessage)))
			require.Equal(t, test.expectedContext["authorization_model_id"], fields["authorization_model_id"])
			require.Equal(t, test.expectedContext["store_id"], fields["store_id"])
			require.Equal(t, test.expectedContext["user_agent"], fields["user_agent"])
			require.NotEmpty(t, fields["peer.address"])
			require.NotEmpty(t, fields["request_id"])
			require.NotEmpty(t, fields["trace_id"])
			require.Equal(t, fields["request_id"], fields["trace_id"])
			require.Contains(t, fields, "query_duration_ms")
			if !test.expectedError {
				require.NotEmpty(t, fields["datastore_query_count"])
				require.GreaterOrEqual(t, fields["dispatch_count"], float64(0))
				require.Equal(t, test.expectedContext["request.dispatch_throttled"], fields["request.dispatch_throttled"])
				require.Equal(t, test.expectedContext["request.datastore_throttled"], fields["request.datastore_throttled"])
				require.GreaterOrEqual(t, len(fields), 15)
			} else {
				require.Len(t, fields, 13)
			}
		})
	}
}

func testRunAll(t *testing.T, engine string) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	cfg := config.MustDefaultConfig()
	cfg.Experimentals = append(cfg.Experimentals, config.ExperimentalCheckOptimizations)
	cfg.Log.Level = "error"
	cfg.Datastore.Engine = engine
	// extend the timeout for the tests, coverage makes them slower
	cfg.RequestTimeout = 10 * time.Second
	cfg.SharedIterator.Enabled = true

	cfg.CheckIteratorCache.Enabled = true

	tests.StartServer(t, cfg)

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	RunAllTests(t, openfgav1.NewOpenFGAServiceClient(conn))
}
