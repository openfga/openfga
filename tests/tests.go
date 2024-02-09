package tests

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/openfga/openfga/pkg/testfixtures/storage"

	"github.com/openfga/openfga/pkg/testutils"

	"github.com/openfga/openfga/cmd/run"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/logger"
)

// TestClientBootstrapper defines a client interface definition that can be used by tests
// to bootstrap OpenFGA resources (stores, models, relationship tuples, etc..) needed to
// execute tests.
type TestClientBootstrapper interface {
	CreateStore(ctx context.Context, in *openfgav1.CreateStoreRequest, opts ...grpc.CallOption) (*openfgav1.CreateStoreResponse, error)
	WriteAuthorizationModel(ctx context.Context, in *openfgav1.WriteAuthorizationModelRequest, opts ...grpc.CallOption) (*openfgav1.WriteAuthorizationModelResponse, error)
	Write(ctx context.Context, in *openfgav1.WriteRequest, opts ...grpc.CallOption) (*openfgav1.WriteResponse, error)
}

// StartServer calls StartServerWithContext. See the docs for that.
func StartServer(t testing.TB, cfg *serverconfig.Config) context.CancelFunc {
	logger := logger.MustNewLogger(cfg.Log.Format, cfg.Log.Level)
	serverCtx := &run.ServerContext{Logger: logger}
	return StartServerWithContext(t, cfg, serverCtx)
}

// StartServerWithContext starts a server with a specific ServerContext, waits until it is healthy, and returns a cancel function
// that callers must call when they want to stop the server and the backing datastore.
// If the server never becomes healthy, the cancel function must still be called, and any subsequent attempts to connect
// to it will fail.
func StartServerWithContext(t testing.TB, cfg *serverconfig.Config, serverCtx *run.ServerContext) context.CancelFunc {
	container, stopFunc := storage.RunDatastoreTestContainer(t, cfg.Datastore.Engine)
	cfg.Datastore.URI = container.GetConnectionURI(true)

	ctx, cancel := context.WithCancel(context.Background())

	serverDone := make(chan error)
	go func() {
		serverDone <- serverCtx.Run(ctx, cfg)
	}()

	err := testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil, false)
	if err != nil {
		return func() {
			stopFunc()
			cancel()
			err := <-serverDone
			t.Log(err)
		}
	}

	return func() {
		stopFunc()
		cancel()
		serverErr := <-serverDone
		require.NoError(t, serverErr)
	}
}
