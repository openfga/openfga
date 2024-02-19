package check

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openfga/openfga/pkg/testutils"

	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
)

var tuples = []*openfgav1.TupleKey{
	tuple.NewTupleKey("repo:openfga/openfga", "reader", "team:openfga#member"),
	tuple.NewTupleKey("team:openfga", "member", "user:github|iaco@openfga"),
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

func TestCheckLogs(t *testing.T) {
	// uncomment after https://github.com/openfga/openfga/pull/1199 is done. the span exporter needs to be closed properly
	// defer goleak.VerifyNone(t)

	// create mock OTLP server
	otlpServerPort, otlpServerPortReleaser := run.TCPRandomPort()
	localOTLPServerURL := fmt.Sprintf("localhost:%d", otlpServerPort)
	otlpServerPortReleaser()
	_ = mocks.NewMockTracingServer(t, otlpServerPort)

	cfg := run.MustDefaultConfigWithRandomPorts()
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

	model := parser.MustTransformDSLToProto(`model
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
		grpcReq         *openfgav1.CheckRequest
		httpReqBody     io.Reader
		expectedError   bool
		expectedContext map[string]interface{}
	}

	tests := []test{
		{
			_name: "grpc_check_success",
			grpcReq: &openfgav1.CheckRequest{
				AuthorizationModelId: authorizationModelID,
				StoreId:              storeID,
				TupleKey:             tuple.NewCheckRequestTupleKey("document:1", "viewer", "user:anne"),
			},
			expectedContext: map[string]interface{}{
				"grpc_service":           "openfga.v1.OpenFGAService",
				"grpc_method":            "Check",
				"grpc_type":              "unary",
				"grpc_code":              int32(0),
				"raw_request":            fmt.Sprintf(`{"store_id":"%s","tuple_key":{"object":"document:1","relation":"viewer","user":"user:anne"},"contextual_tuples":null,"authorization_model_id":"%s","trace":false,"context":null}`, storeID, authorizationModelID),
				"raw_response":           `{"allowed":true,"resolution":""}`,
				"authorization_model_id": authorizationModelID,
				"store_id":               storeID,
				"user_agent":             "test-user-agent" + " grpc-go/" + grpc.Version,
			},
		},
		{
			_name: "http_check_success",
			httpReqBody: bytes.NewBufferString(`{
  "tuple_key": {
    "user": "user:anne",
    "relation": "viewer",
    "object": "document:1"
  },
  "authorization_model_id": "` + authorizationModelID + `"
}`),
			expectedContext: map[string]interface{}{
				"grpc_service":           "openfga.v1.OpenFGAService",
				"grpc_method":            "Check",
				"grpc_type":              "unary",
				"grpc_code":              int32(0),
				"raw_request":            fmt.Sprintf(`{"store_id":"%s","tuple_key":{"object":"document:1","relation":"viewer","user":"user:anne"},"contextual_tuples":null,"authorization_model_id":"%s","trace":false,"context":null}`, storeID, authorizationModelID),
				"raw_response":           `{"allowed":true,"resolution":""}`,
				"authorization_model_id": authorizationModelID,
				"store_id":               storeID,
				"user_agent":             "test-user-agent",
			},
		},
		{
			_name: "grpc_check_error",
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
				"raw_request":  fmt.Sprintf(`{"store_id":"%s","tuple_key":{"object":"","relation":"viewer","user":"user:anne"},"contextual_tuples":null,"authorization_model_id":"%s","trace":false,"context":null}`, storeID, authorizationModelID),
				"raw_response": `{"code":"validation_error", "message":"invalid CheckRequestTupleKey.Object: value does not match regex pattern \"^[^\\\\s]{2,256}$\""}`,
				"store_id":     storeID,
				"user_agent":   "test-user-agent" + " grpc-go/" + grpc.Version,
			},
		},
		{
			_name: "http_check_error",
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
				"raw_request":  fmt.Sprintf(`{"store_id":"%s","tuple_key":{"object":"","relation":"viewer","user":"user:anne"},"contextual_tuples":null,"authorization_model_id":"%s","trace":false,"context":null}`, storeID, authorizationModelID),
				"raw_response": `{"code":"validation_error", "message":"invalid CheckRequestTupleKey.Object: value does not match regex pattern \"^[^\\\\s]{2,256}$\""}`,
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
				_, err = client.Check(context.Background(), test.grpcReq)
			} else if test.httpReqBody != nil {
				var httpReq *http.Request
				httpReq, err = http.NewRequest("POST", "http://"+cfg.HTTP.Addr+"/stores/"+storeID+"/check", test.httpReqBody)
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
			if !test.expectedError {
				require.NotEmpty(t, fields["datastore_query_count"])
				require.Len(t, fields, 13)
			} else {
				require.Len(t, fields, 12)
			}
		})
	}
}

func testRunAll(t *testing.T, engine string) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "error"
	cfg.Datastore.Engine = engine

	tests.StartServer(t, cfg)

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	RunAllTests(t, openfgav1.NewOpenFGAServiceClient(conn))
}

func BenchmarkCheckMemory(b *testing.B) {
	benchmarkAll(b, "memory")
}

func BenchmarkCheckPostgres(b *testing.B) {
	benchmarkAll(b, "postgres")
}

func BenchmarkCheckMySQL(b *testing.B) {
	benchmarkAll(b, "mysql")
}

func benchmarkAll(b *testing.B, engine string) {
	b.Run("BenchmarkCheckWithoutTrace", func(b *testing.B) { benchmarkCheckWithoutTrace(b, engine) })
	b.Run("BenchmarkCheckWithTrace", func(b *testing.B) { benchmarkCheckWithTrace(b, engine) })
	b.Run("BenchmarkCheckWithDirectResolution", func(b *testing.B) { benchmarkCheckWithDirectResolution(b, engine) })
	b.Run("BenchmarkCheckWithBypassDirectRead", func(b *testing.B) { benchmarkCheckWithBypassDirectRead(b, engine) })
	b.Run("BenchmarkCheckWithBypassUsersetRead", func(b *testing.B) { benchmarkCheckWithBypassUsersetRead(b, engine) })
	b.Run("BenchmarkCheckWithOneCondition", func(b *testing.B) { benchmarkCheckWithOneCondition(b, engine) })
	b.Run("BenchmarkCheckWithOneConditionWithManyParameters", func(b *testing.B) { benchmarkCheckWithOneConditionWithManyParameters(b, engine) })
}

const githubModel = `model
  schema 1.1
type user
type team
  relations
    define member: [user,team#member]
type repo
  relations
    define admin: [user,team#member] or repo_admin from owner
    define maintainer: [user,team#member] or admin
    define owner: [organization]
    define reader: [user,team#member] or triager or repo_reader from owner
    define triager: [user,team#member] or writer
    define writer: [user,team#member] or maintainer or repo_writer from owner
type organization
  relations
    define member: [user] or owner
    define owner: [user]
    define repo_admin: [user,organization#member]
    define repo_reader: [user,organization#member]
    define repo_writer: [user,organization#member]`

// setupBenchmarkTest spins a new server and a backing datastore, and returns a client to the server
// and a cancellation function that stops the benchmark timer.
func setupBenchmarkTest(b *testing.B, engine string) (openfgav1.OpenFGAServiceClient, context.CancelFunc) {
	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = engine

	tests.StartServer(b, cfg)

	conn, err := grpc.Dial(cfg.GRPC.Addr,
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: grpcbackoff.DefaultConfig}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(b, err)
	b.Cleanup(func() {
		conn.Close()
	})

	client := openfgav1.NewOpenFGAServiceClient(conn)
	return client, func() {
		// so we don't steal time from the benchmark itself
		b.StopTimer()
	}
}

func benchmarkCheckWithoutTrace(b *testing.B, engine string) {
	client, cancel := setupBenchmarkTest(b, engine)
	defer cancel()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "check benchmark without trace"})
	require.NoError(b, err)

	storeID := resp.GetId()
	model := parser.MustTransformDSLToProto(githubModel)
	writeAuthModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: model.GetTypeDefinitions(),
		Conditions:      model.GetConditions(),
	})
	require.NoError(b, err)
	_, err = client.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: tuples,
		},
	})
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = client.Check(ctx, &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
			TupleKey:             tuple.NewCheckRequestTupleKey("repo:openfga/openfga", "reader", "user:github|iaco@openfga"),
		})

		require.NoError(b, err)
	}
}

func benchmarkCheckWithTrace(b *testing.B, engine string) {
	client, cancel := setupBenchmarkTest(b, engine)
	defer cancel()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "check benchmark with trace"})
	require.NoError(b, err)

	storeID := resp.GetId()
	model := parser.MustTransformDSLToProto(githubModel)
	writeAuthModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: model.GetTypeDefinitions(),
		Conditions:      model.GetConditions(),
	})
	require.NoError(b, err)
	_, err = client.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: tuples,
		},
	})
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = client.Check(ctx, &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
			TupleKey:             tuple.NewCheckRequestTupleKey("repo:openfga/openfga", "reader", "user:github|iaco@openfga"),
			Trace:                true,
		})

		require.NoError(b, err)
	}
}

func benchmarkCheckWithDirectResolution(b *testing.B, engine string) {
	client, cancel := setupBenchmarkTest(b, engine)
	defer cancel()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "check benchmark with direct resolution"})
	require.NoError(b, err)

	storeID := resp.GetId()
	model := parser.MustTransformDSLToProto(githubModel)
	writeAuthModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: model.GetTypeDefinitions(),
		Conditions:      model.GetConditions(),
	})
	require.NoError(b, err)

	// add user to many usersets
	for i := 0; i < 1000; i++ {
		_, err = client.Write(ctx, &openfgav1.WriteRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					{Object: fmt.Sprintf("team:%d", i), Relation: "member", User: "user:anne"},
				},
			},
		})
		require.NoError(b, err)
	}

	// one of those usersets gives access to the repo
	_, err = client.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{Object: "repo:openfga", Relation: "admin", User: "team:999#member"},
			},
		},
	})
	require.NoError(b, err)

	// add direct access to the repo
	_, err = client.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{Object: "repo:openfga", Relation: "admin", User: "user:anne"},
			},
		},
	})
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = client.Check(ctx, &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
			TupleKey:             tuple.NewCheckRequestTupleKey("repo:openfga/openfga", "admin", "user:anne"),
		})

		require.NoError(b, err)
	}
}

func benchmarkCheckWithBypassDirectRead(b *testing.B, engine string) {
	client, cancel := setupBenchmarkTest(b, engine)
	defer cancel()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "check benchmark with bypass direct read"})
	require.NoError(b, err)

	storeID := resp.GetId()
	model := parser.MustTransformDSLToProto(githubModel)
	writeAuthModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: model.GetTypeDefinitions(),
		Conditions:      model.GetConditions(),
	})
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		check, err := client.Check(ctx, &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
			// users can't be direct owners of repos
			TupleKey: tuple.NewCheckRequestTupleKey("repo:openfga/openfga", "owner", "user:anne"),
		})

		require.False(b, check.GetAllowed())
		require.NoError(b, err)
	}
}

func benchmarkCheckWithBypassUsersetRead(b *testing.B, engine string) {
	client, cancel := setupBenchmarkTest(b, engine)
	defer cancel()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "check benchmark with bypass direct read"})
	require.NoError(b, err)

	storeID := resp.GetId()
	// model A
	writeAuthModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:       storeID,
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user
type group
  relations
    define member: [user]
type document
  relations
    define viewer: [user:*, group#member]`).TypeDefinitions,
	})
	require.NoError(b, err)

	// add user to many usersets according to model A
	for i := 0; i < 1000; i++ {
		_, err = client.Write(ctx, &openfgav1.WriteRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					{Object: fmt.Sprintf("group:%d", i), Relation: "member", User: "user:anne"},
				},
			},
		})
		require.NoError(b, err)
	}

	// one of those usersets gives access to document:budget
	_, err = client.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{Object: "document:budget", Relation: "viewer", User: "group:999#member"},
			},
		},
	})
	require.NoError(b, err)

	// model B
	writeAuthModelResponse, err = client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:       storeID,
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user
type user2
type group
  relations
    define member: [user2]
type document
  relations
    define viewer: [user:*, group#member]`).TypeDefinitions,
	})
	require.NoError(b, err)

	// all the usersets added above are now invalid and should be skipped!

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		check, err := client.Check(ctx, &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
			TupleKey:             tuple.NewCheckRequestTupleKey("document:budget", "viewer", "user:anne"),
		})

		require.False(b, check.GetAllowed())
		require.NoError(b, err)
	}
}

func benchmarkCheckWithOneCondition(b *testing.B, engine string) {
	client, cancel := setupBenchmarkTest(b, engine)
	defer cancel()

	storeID := ulid.Make().String()
	model := parser.MustTransformDSLToProto(`model
	schema 1.1
type user
type doc
  relations
    define viewer: [user with password]
condition password(p: string) {
  p == "secret"
}`)
	writeAuthModelResponse, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   model.GetSchemaVersion(),
		TypeDefinitions: model.GetTypeDefinitions(),
		Conditions:      model.GetConditions(),
	})
	require.NoError(b, err)
	_, err = client.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("doc:x", "viewer", "user:maria", "password", nil),
			},
		},
	})
	require.NoError(b, err)

	contextStruct, err := structpb.NewStruct(map[string]interface{}{
		"p": "secret",
	})
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, err := client.Check(context.Background(), &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
			TupleKey:             tuple.NewCheckRequestTupleKey("doc:x", "viewer", "user:maria"),
			Context:              contextStruct,
		})
		require.NoError(b, err)
		require.True(b, resp.GetAllowed())
	}
}

func benchmarkCheckWithOneConditionWithManyParameters(b *testing.B, engine string) {
	client, cancel := setupBenchmarkTest(b, engine)
	defer cancel()

	storeID := ulid.Make().String()
	model := parser.MustTransformDSLToProto(`model
	schema 1.1
type user
type doc
  relations
    define viewer: [user with complex]
condition complex(b: bool, s:string, i: int, u: uint, d: double, du: duration, t:timestamp, ip:ipaddress) {
  b == true && s == "s" && i == 1 && u == uint(1) && d == 0.1 && du == duration("1h") && t == timestamp("1972-01-01T10:00:20.021Z") && ip == ipaddress("127.0.0.1")
}`)
	writeAuthModelResponse, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   model.GetSchemaVersion(),
		TypeDefinitions: model.GetTypeDefinitions(),
		Conditions:      model.GetConditions(),
	})
	require.NoError(b, err)
	_, err = client.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("doc:x", "viewer", "user:maria", "complex", nil),
			},
		},
	})
	require.NoError(b, err)

	contextStruct, err := structpb.NewStruct(map[string]interface{}{
		"b":  true,
		"s":  "s",
		"i":  1,
		"u":  1,
		"d":  0.1,
		"du": "1h",
		"t":  "1972-01-01T10:00:20.021Z",
		"ip": "127.0.0.1",
	})
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, err := client.Check(context.Background(), &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
			TupleKey:             tuple.NewCheckRequestTupleKey("doc:x", "viewer", "user:maria"),
			Context:              contextStruct,
		})
		require.NoError(b, err)
		require.True(b, resp.GetAllowed())
	}
}
