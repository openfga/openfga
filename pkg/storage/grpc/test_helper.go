package grpc

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/openfga/openfga/pkg/storage"
	storagev1 "github.com/openfga/openfga/pkg/storage/grpc/proto/storage/v1"
	"github.com/openfga/openfga/pkg/storage/memory"
)

const testBufSize = 1024 * 1024

// SetupTestClientServer sets up a test gRPC server and client using an in-memory datastore.
// This is exported for use in other test packages.
func SetupTestClientServer(t testing.TB) (*Client, storage.OpenFGADatastore, func()) {
	// Create in-memory datastore
	datastore := memory.New()

	// Set up gRPC server with bufconn
	lis := bufconn.Listen(testBufSize)
	grpcServer := grpc.NewServer()
	storageServer := NewServer(datastore)
	storagev1.RegisterStorageServiceServer(grpcServer, storageServer)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()

	// Create client
	bufDialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client := &Client{
		conn:                   conn,
		client:                 storagev1.NewStorageServiceClient(conn),
		maxTuplesPerWriteField: storage.DefaultMaxTuplesPerWrite,
		maxTypesPerModelField:  storage.DefaultMaxTypesPerAuthorizationModel,
	}

	cleanup := func() {
		client.Close()
		datastore.Close()
		grpcServer.Stop()
	}

	return client, datastore, cleanup
}
