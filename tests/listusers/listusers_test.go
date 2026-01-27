package listusers

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
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
)

func TestListUsersMemory(t *testing.T) {
	testRunAll(t, "memory")
}

func TestListUsersPostgres(t *testing.T) {
	testRunAll(t, "postgres")
}

func TestListUsersMySQL(t *testing.T) {
	testRunAll(t, "mysql")
}

func TestListUsersSQLite(t *testing.T) {
	testRunAll(t, "sqlite")
}

func TestListUsersDSQL(t *testing.T) {
	testRunAll(t, "dsql")
}

func testRunAll(t *testing.T, engine string) {
	t.Cleanup(func() {
		// [Goroutine 60101 in state select, with github.com/go-sql-driver/mysql.(*mysqlConn).startWatcher.func1 on top of the stack:
		// github.com/go-sql-driver/mysql.(*mysqlConn).startWatcher.func1()
		// 	/home/runner/go/pkg/mod/github.com/go-sql-driver/mysql@v1.8.1/connection.go:628 +0x105
		// created by github.com/go-sql-driver/mysql.(*mysqlConn).startWatcher in goroutine 60029
		// 	/home/runner/go/pkg/mod/github.com/go-sql-driver/mysql@v1.8.1/connection.go:625 +0x1dd
		// ]
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/go-sql-driver/mysql.(*mysqlConn).startWatcher.func1"))
	})
	cfg := config.MustDefaultConfig()
	cfg.Experimentals = append(cfg.Experimentals, "enable-check-optimizations", "enable-list-objects-optimizations")
	cfg.Log.Level = "error"
	cfg.Datastore.Engine = engine
	cfg.ListUsersDeadline = 0 // no deadline
	// extend the timeout for the tests, coverage makes them slower
	cfg.RequestTimeout = 10 * time.Second

	tests.StartServer(t, cfg)

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	RunAllTests(t, openfgav1.NewOpenFGAServiceClient(conn))
}

func TestListUsersLogs(t *testing.T) {
	// uncomment after https://github.com/openfga/openfga/pull/1199 is done. the span exporter needs to be closed properly
	// defer goleak.VerifyNone(t)

	// create mock OTLP server
	otlpServerPort, otlpServerPortReleaser := testutils.TCPRandomPort()
	localOTLPServerURL := fmt.Sprintf("localhost:%d", otlpServerPort)
	otlpServerPortReleaser()

	cfg := config.MustDefaultConfig()
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

		type group
			relations
				define member: [user]

		type document
			relations
				define viewer: [group#member]`)

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
				tuple.NewTupleKey("group:fga", "member", "user:anne"),
				tuple.NewTupleKey("document:1", "viewer", "group:fga#member"),
			},
		},
	})
	require.NoError(t, err)

	logs.TakeAll()

	type test struct {
		_name           string
		grpcReq         *openfgav1.ListUsersRequest
		httpReqBody     io.Reader
		expectedError   bool
		expectedContext map[string]interface{}
	}

	tests := []test{
		{
			_name: "grpc_list_users_success",
			grpcReq: &openfgav1.ListUsersRequest{
				StoreId:              storeID,
				AuthorizationModelId: authorizationModelID,
				Relation:             "viewer",
				Object: &openfgav1.Object{
					Type: "document",
					Id:   "1",
				},
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			expectedContext: map[string]interface{}{
				"grpc_service":           "openfga.v1.OpenFGAService",
				"grpc_method":            "ListUsers",
				"grpc_type":              "unary",
				"grpc_code":              int32(0),
				"raw_request":            fmt.Sprintf(`{"store_id":"%s","relation":"viewer","object":{"type":"document","id":"1"},"user_filters":[{"type":"user","relation":""}], "contextual_tuples":[],"authorization_model_id":"%s","context":null,"consistency":"UNSPECIFIED"}`, storeID, authorizationModelID),
				"raw_response":           `{"users":[{"object":{"type":"user","id":"anne"}}]}`,
				"authorization_model_id": authorizationModelID,
				"store_id":               storeID,
				"user_agent":             "test-user-agent" + " grpc-go/" + grpc.Version,
			},
		},
		{
			_name: "http_list_users_success",
			httpReqBody: bytes.NewBufferString(`{
		  "authorization_model_id": "` + authorizationModelID + `",
		  "relation":"viewer",
		  "object":{"type":"document","id":"1"},
		  "user_filters":[{"type":"user"}]
		}`),
			expectedContext: map[string]interface{}{
				"grpc_service":           "openfga.v1.OpenFGAService",
				"grpc_method":            "ListUsers",
				"grpc_type":              "unary",
				"grpc_code":              int32(0),
				"raw_request":            fmt.Sprintf(`{"store_id":"%s","relation":"viewer","object":{"type":"document","id":"1"},"user_filters":[{"type":"user","relation":""}], "contextual_tuples":[],"authorization_model_id":"%s","context":null,"consistency":"UNSPECIFIED"}`, storeID, authorizationModelID),
				"raw_response":           `{"users":[{"object":{"type":"user","id":"anne"}}]}`,
				"authorization_model_id": authorizationModelID,
				"store_id":               storeID,
				"user_agent":             "test-user-agent",
			},
		},
		{
			_name: "grpc_list_users_error",
			grpcReq: &openfgav1.ListUsersRequest{
				StoreId:              storeID,
				AuthorizationModelId: authorizationModelID,
				Relation:             "viewer",
				// Object field is missing
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			expectedError: true,
			expectedContext: map[string]interface{}{
				"grpc_service": "openfga.v1.OpenFGAService",
				"grpc_method":  "ListUsers",
				"grpc_type":    "unary",
				"grpc_code":    int32(2000),
				"raw_request":  fmt.Sprintf(`{"store_id":"%s","relation":"viewer","object":null,"user_filters":[{"type":"user","relation":""}], "contextual_tuples":[],"authorization_model_id":"%s","context":null,"consistency":"UNSPECIFIED"}`, storeID, authorizationModelID),
				"raw_response": `{"code":"validation_error", "message":"invalid ListUsersRequest.Object: value is required"}`,
				"store_id":     storeID,
				"user_agent":   "test-user-agent" + " grpc-go/" + grpc.Version,
			},
		},
		{
			_name: "http_list_users_error",
			httpReqBody: bytes.NewBufferString(`{
				"authorization_model_id": "` + authorizationModelID + `",
				"relation":"viewer",
				"user_filters":[{"type":"user"}]
			  }`),
			expectedError: true,
			expectedContext: map[string]interface{}{
				"grpc_service": "openfga.v1.OpenFGAService",
				"grpc_method":  "ListUsers",
				"grpc_type":    "unary",
				"grpc_code":    int32(2000),
				"raw_request":  fmt.Sprintf(`{"store_id":"%s","relation":"viewer","object":null,"user_filters":[{"type":"user","relation":""}], "contextual_tuples":[],"authorization_model_id":"%s","context":null,"consistency":"UNSPECIFIED"}`, storeID, authorizationModelID),
				"raw_response": `{"code":"validation_error", "message":"invalid ListUsersRequest.Object: value is required"}`,
				"store_id":     storeID,
				"user_agent":   "test-user-agent",
			},
		},
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			// clear observed logs after each run. We expect each test to log one line
			defer logs.TakeAll()

			if test.grpcReq != nil {
				_, err = client.ListUsers(context.Background(), test.grpcReq)
			} else if test.httpReqBody != nil {
				var httpReq *http.Request
				httpReq, err = http.NewRequest("POST", "http://"+cfg.HTTP.Addr+"/stores/"+storeID+"/list-users", test.httpReqBody)
				require.NoError(t, err)

				httpReq.Header.Set("User-Agent", "test-user-agent")
				client := &http.Client{}

				_, err = client.Do(httpReq)
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
			require.NotEmpty(t, fields["query_duration_ms"])
			if !test.expectedError {
				require.GreaterOrEqual(t, fields["datastore_query_count"], float64(1))
				require.GreaterOrEqual(t, fields["datastore_item_count"], float64(1))
				require.GreaterOrEqual(t, fields["dispatch_count"], float64(1))
				require.Len(t, fields, 16)
			} else {
				require.Len(t, fields, 13)
			}
		})
	}
}
