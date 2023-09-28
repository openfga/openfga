package check

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	// create mock OTLP server
	otlpServerPort, otlpServerPortReleaser := run.TCPRandomPort()
	localOTLPServerURL := fmt.Sprintf("localhost:%d", otlpServerPort)
	otlpServerPortReleaser()
	_, serverStopFunc, err := mocks.NewMockTracingServer(otlpServerPort)
	defer serverStopFunc()
	require.NoError(t, err)

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
	cancel := tests.StartServerWithContext(t, cfg, serverCtx)
	defer cancel()

	conn, err := grpc.Dial(cfg.GRPC.Addr,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUserAgent("test-user-agent"),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := openfgav1.NewOpenFGAServiceClient(conn)

	createStoreResp, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)

	storeID := createStoreResp.GetId()

	typedefs := parser.MustParse(`
	type user

	type document
	  relations
	    define viewer: [user] as self
	`)

	writeModelResp, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: typedefs,
	})
	require.NoError(t, err)

	authorizationModelID := writeModelResp.GetAuthorizationModelId()

	_, err = client.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId: storeID,
		Writes: &openfgav1.TupleKeys{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:anne"),
			},
		},
	})
	require.NoError(t, err)

	type test struct {
		_name           string
		grpcReq         *openfgav1.CheckRequest
		httpReqBody     io.Reader
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
			_name: "check_http_success",
			httpReqBody: bytes.NewBuffer([]byte(`{
  "tuple_key": {
    "user": "user:anne",
    "relation": "viewer",
    "object": "document:1"
  },
  "authorization_model_id": "` + authorizationModelID + `"
}`)),
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
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			// clear observed logs after each run
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
			require.NoError(t, err)

			filteredLogs := logs.Filter(func(e observer.LoggedEntry) bool {
				if e.Message == "grpc_req_complete" {
					for _, ctxField := range e.Context {
						if ctxField.Equals(zap.String("grpc_method", "Check")) {
							return true
						}
					}
				}

				return false
			})

			expectedLogs := filteredLogs.All()
			require.Len(t, expectedLogs, 1)

			fields := expectedLogs[len(expectedLogs)-1].ContextMap()
			require.Equal(t, test.expectedContext["grpc_service"], fields["grpc_service"])
			require.Equal(t, test.expectedContext["grpc_method"], fields["grpc_method"])
			require.Equal(t, test.expectedContext["grpc_type"], fields["grpc_type"])
			require.Equal(t, test.expectedContext["grpc_code"], fields["grpc_code"])
			require.JSONEq(t, test.expectedContext["raw_request"].(string), string(fields["raw_request"].(json.RawMessage)))
			require.JSONEq(t, test.expectedContext["raw_response"].(string), string(fields["raw_response"].(json.RawMessage)))
			require.Equal(t, test.expectedContext["authorization_model_id"], fields["authorization_model_id"])
			require.Equal(t, test.expectedContext["store_id"], fields["store_id"])
			require.Equal(t, test.expectedContext["user_agent"], fields["user_agent"])
			require.NotEmpty(t, fields["datastore_query_count"])
			require.NotEmpty(t, fields["peer.address"])
			require.NotEmpty(t, fields["request_id"])
			require.NotEmpty(t, fields["trace_id"])
			require.Len(t, fields, 13)
		})
	}
}

func testRunAll(t *testing.T, engine string) {
	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = engine

	cancel := tests.StartServer(t, cfg)
	defer cancel()

	conn, err := grpc.Dial(cfg.GRPC.Addr,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

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
}

const githubModel = `
type user
type team
  relations
    define member: [user,team#member] as self
type repo
  relations
    define admin: [user,team#member] as self or repo_admin from owner
    define maintainer: [user,team#member] as self or admin
    define owner: [organization] as self
    define reader: [user,team#member] as self or triager or repo_reader from owner
    define triager: [user,team#member] as self or writer
    define writer: [user,team#member] as self or maintainer or repo_writer from owner
type organization
  relations
    define member: [user] as self or owner
    define owner: [user] as self
    define repo_admin: [user,organization#member] as self
    define repo_reader: [user,organization#member] as self
    define repo_writer: [user,organization#member] as self
`

func setupBenchmarkTest(b *testing.B, engine string) (context.CancelFunc, *grpc.ClientConn, openfgav1.OpenFGAServiceClient) {
	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = engine

	cancel := tests.StartServer(b, cfg)

	conn, err := grpc.Dial(cfg.GRPC.Addr,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(b, err)

	client := openfgav1.NewOpenFGAServiceClient(conn)
	return cancel, conn, client
}

func benchmarkCheckWithoutTrace(b *testing.B, engine string) {
	cancel, conn, client := setupBenchmarkTest(b, engine)
	defer cancel()
	defer conn.Close()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "check benchmark without trace"})
	require.NoError(b, err)

	storeID := resp.GetId()
	_, err = client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(githubModel),
	})
	require.NoError(b, err)
	_, err = client.Write(ctx, &openfgav1.WriteRequest{
		StoreId: storeID,
		Writes:  &openfgav1.TupleKeys{TupleKeys: tuples},
	})
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = client.Check(ctx, &openfgav1.CheckRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewCheckRequestTupleKey("repo:openfga/openfga", "reader", "user:github|iaco@openfga"),
		})

		require.NoError(b, err)
	}
}

func benchmarkCheckWithTrace(b *testing.B, engine string) {
	cancel, conn, client := setupBenchmarkTest(b, engine)
	defer cancel()
	defer conn.Close()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "check benchmark with trace"})
	require.NoError(b, err)

	storeID := resp.GetId()
	_, err = client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(githubModel),
	})
	require.NoError(b, err)
	_, err = client.Write(ctx, &openfgav1.WriteRequest{
		StoreId: storeID,
		Writes:  &openfgav1.TupleKeys{TupleKeys: tuples},
	})
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = client.Check(ctx, &openfgav1.CheckRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewCheckRequestTupleKey("repo:openfga/openfga", "reader", "user:github|iaco@openfga"),
			Trace:    true,
		})

		require.NoError(b, err)
	}
}

func benchmarkCheckWithDirectResolution(b *testing.B, engine string) {
	cancel, conn, client := setupBenchmarkTest(b, engine)
	defer cancel()
	defer conn.Close()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "check benchmark with direct resolution"})
	require.NoError(b, err)

	storeID := resp.GetId()
	_, err = client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(githubModel),
	})
	require.NoError(b, err)

	// add user to many usersets
	for i := 0; i < 1000; i++ {
		_, err = client.Write(ctx, &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.TupleKeys{TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey(fmt.Sprintf("team:%d", i), "member", "user:anne"),
			}},
		})
		require.NoError(b, err)
	}

	// one of those usersets gives access to the repo
	_, err = client.Write(ctx, &openfgav1.WriteRequest{
		StoreId: storeID,
		Writes: &openfgav1.TupleKeys{TupleKeys: []*openfgav1.TupleKey{
			tuple.NewTupleKey("repo:openfga", "admin", "team:999#member"),
		}},
	})
	require.NoError(b, err)

	// add direct access to the repo
	_, err = client.Write(ctx, &openfgav1.WriteRequest{
		StoreId: storeID,
		Writes: &openfgav1.TupleKeys{TupleKeys: []*openfgav1.TupleKey{
			tuple.NewTupleKey("repo:openfga", "admin", "user:anne"),
		}},
	})
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = client.Check(ctx, &openfgav1.CheckRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewCheckRequestTupleKey("repo:openfga/openfga", "admin", "user:anne"),
		})

		require.NoError(b, err)
	}
}

func benchmarkCheckWithBypassDirectRead(b *testing.B, engine string) {
	cancel, conn, client := setupBenchmarkTest(b, engine)
	defer cancel()
	defer conn.Close()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "check benchmark with bypass direct read"})
	require.NoError(b, err)

	storeID := resp.GetId()
	writeAuthModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(githubModel),
	})
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		check, err := client.Check(ctx, &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.AuthorizationModelId,
			// users can't be direct owners of repos
			TupleKey: tuple.NewCheckRequestTupleKey("repo:openfga/openfga", "owner", "user:anne"),
		})

		require.False(b, check.Allowed)
		require.NoError(b, err)
	}
}

func benchmarkCheckWithBypassUsersetRead(b *testing.B, engine string) {
	cancel, conn, client := setupBenchmarkTest(b, engine)
	defer cancel()
	defer conn.Close()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "check benchmark with bypass direct read"})
	require.NoError(b, err)

	storeID := resp.GetId()
	// model A
	writeAuthModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:       storeID,
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(`type user
          type group
            relations
              define member: [user] as self
          type document
            relations
              define viewer: [user:*, group#member] as self`),
	})
	require.NoError(b, err)

	// add user to many usersets according to model A
	for i := 0; i < 1000; i++ {
		_, err = client.Write(ctx, &openfgav1.WriteRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.AuthorizationModelId,
			Writes: &openfgav1.TupleKeys{TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey(fmt.Sprintf("group:%d", i), "member", "user:anne"),
			}},
		})
		require.NoError(b, err)
	}

	// one of those usersets gives access to document:budget
	_, err = client.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: writeAuthModelResponse.AuthorizationModelId,
		Writes: &openfgav1.TupleKeys{TupleKeys: []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:budget", "viewer", "group:999#member"),
		}},
	})
	require.NoError(b, err)

	// model B
	writeAuthModelResponse, err = client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:       storeID,
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(`type user
          type user2
          type group
            relations
              define member: [user2] as self
          type document
            relations
              define viewer: [user:*, group#member] as self`),
	})
	require.NoError(b, err)

	// all the usersets added above are now invalid and should be skipped!

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		check, err := client.Check(ctx, &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.AuthorizationModelId,
			TupleKey:             tuple.NewCheckRequestTupleKey("document:budget", "viewer", "user:anne"),
		})

		require.False(b, check.Allowed)
		require.NoError(b, err)
	}
}
