package tests

import (
	"context"
	"testing"

	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
)

// TestClientBootstrapper defines a client interface definition that can be used by tests
// to bootstrap OpenFGA resources (stores, models, relationship tuples, etc..) needed to
// execute tests.
type TestClientBootstrapper interface {
	CreateStore(ctx context.Context, in *openfgapb.CreateStoreRequest, opts ...grpc.CallOption) (*openfgapb.CreateStoreResponse, error)
	WriteAuthorizationModel(ctx context.Context, in *openfgapb.WriteAuthorizationModelRequest, opts ...grpc.CallOption) (*openfgapb.WriteAuthorizationModelResponse, error)
	Write(ctx context.Context, in *openfgapb.WriteRequest, opts ...grpc.CallOption) (*openfgapb.WriteResponse, error)
}

func StartServer(t testing.TB, cfg *run.Config) context.CancelFunc {
	container := storage.RunDatastoreTestContainer(t, cfg.Datastore.Engine)
	cfg.Datastore.URI = container.GetConnectionURI(true)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		err := run.RunServer(ctx, cfg)
		require.NoError(t, err)
	}()

	return cancel
}
