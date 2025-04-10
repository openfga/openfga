package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/cmd/run"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
)

// TestClientBootstrapper defines a client interface definition that can be used by tests
// to bootstrap OpenFGA resources (stores, models, relationship tuples, etc.), needed to
// execute tests.
type TestClientBootstrapper interface {
	CreateStore(ctx context.Context, in *openfgav1.CreateStoreRequest, opts ...grpc.CallOption) (*openfgav1.CreateStoreResponse, error)
	WriteAuthorizationModel(ctx context.Context, in *openfgav1.WriteAuthorizationModelRequest, opts ...grpc.CallOption) (*openfgav1.WriteAuthorizationModelResponse, error)
	Write(ctx context.Context, in *openfgav1.WriteRequest, opts ...grpc.CallOption) (*openfgav1.WriteResponse, error)
}

// ClientInterface defines client interface for running tests.
type ClientInterface interface {
	TestClientBootstrapper
	Check(ctx context.Context, in *openfgav1.CheckRequest, opts ...grpc.CallOption) (*openfgav1.CheckResponse, error)
	ListUsers(ctx context.Context, in *openfgav1.ListUsersRequest, opts ...grpc.CallOption) (*openfgav1.ListUsersResponse, error)
	ListObjects(ctx context.Context, in *openfgav1.ListObjectsRequest, opts ...grpc.CallOption) (*openfgav1.ListObjectsResponse, error)
	StreamedListObjects(ctx context.Context, in *openfgav1.StreamedListObjectsRequest, opts ...grpc.CallOption) (openfgav1.OpenFGAService_StreamedListObjectsClient, error)
}

// StartServer calls StartServerWithContext. See the docs for that.
func StartServer(t testing.TB, cfg *serverconfig.Config) {
	logger := logger.MustNewLogger(cfg.Log.Format, cfg.Log.Level, cfg.Log.TimestampFormat)
	serverCtx := &run.ServerContext{Logger: logger}
	StartServerWithContext(t, cfg, serverCtx)
}

// StartServerWithContext starts a server in random ports and with a specific ServerContext and waits until it is healthy.
// When the test ends, all resources are cleaned.
func StartServerWithContext(t testing.TB, cfg *serverconfig.Config, serverCtx *run.ServerContext) {
	container := storage.RunDatastoreTestContainer(t, cfg.Datastore.Engine)
	cfg.Datastore.URI = container.GetConnectionURI(true)

	ctx, cancel := context.WithCancel(context.Background())

	httpPort, httpPortReleaser := testutils.TCPRandomPort()
	cfg.HTTP.Addr = fmt.Sprintf("localhost:%d", httpPort)
	grpcPort, grpcPortReleaser := testutils.TCPRandomPort()
	cfg.GRPC.Addr = fmt.Sprintf("localhost:%d", grpcPort)

	// these two functions release the ports so that the server can start listening on them
	httpPortReleaser()
	grpcPortReleaser()

	serverDone := make(chan error)
	go func() {
		serverDone <- serverCtx.Run(ctx, cfg)
	}()
	t.Cleanup(func() {
		t.Log("waiting for server to stop")
		cancel()
		serverErr := <-serverDone
		t.Log("server stopped with error: ", serverErr)
	})

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil)
}

func buildAndStartServer(t *testing.T, engine string, experimentals []string) *serverconfig.Config {
	cfg := serverconfig.MustDefaultConfig()
	if len(experimentals) > 0 {
		cfg.Experimentals = append(cfg.Experimentals, experimentals...)
	}
	cfg.Log.Level = "error"
	cfg.Datastore.Engine = engine
	cfg.ListUsersDeadline = 0   // no deadline
	cfg.ListObjectsDeadline = 0 // no deadline
	// extend the timeout for the tests, coverage makes them slower
	cfg.RequestTimeout = 10 * time.Second

	cfg.CheckIteratorCache.Enabled = true
	cfg.ContextPropagationToDatastore = true

	StartServer(t, cfg)
	return cfg
}

// BuildClientInterface sets up test client interface to be used for matrix test.
func BuildClientInterface(t *testing.T, engine string, experimentals []string) ClientInterface {
	cfg := buildAndStartServer(t, engine, experimentals)
	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)
	return openfgav1.NewOpenFGAServiceClient(conn)
}

// BuildServer sets up a server to use for tests that use the SDK client.
func BuildServer(t *testing.T, engine string, experimentals []string) string {
	cfg := buildAndStartServer(t, engine, experimentals)
	return cfg.HTTP.Addr
}
