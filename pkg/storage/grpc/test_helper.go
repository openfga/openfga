package grpc

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	storagev1 "github.com/openfga/api/proto/storage/v1beta1"
	"github.com/openfga/openfga/pkg/storage"
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

// SetupTestClientServerOverNetwork sets up a test gRPC server and client using a real TCP network listener.
// This mimics real network overhead better than bufconn and is useful for benchmarks.
func SetupTestClientServerOverTCP(t testing.TB) (*Client, func()) {
	// Create in-memory datastore
	datastore := memory.New()

	// Set up gRPC server with real network listener
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	storageServer := NewServer(datastore)
	storagev1.RegisterStorageServiceServer(grpcServer, storageServer)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()

	// Create client
	config := ClientConfig{
		Addr: lis.Addr().String(),
	}
	client, err := NewClient(config)
	require.NoError(t, err)

	cleanup := func() {
		client.Close()
		datastore.Close()
		grpcServer.Stop()
	}

	return client, cleanup
}

// SetupTestClientServerOverUnixSocket sets up a test gRPC server and client using a Unix domain socket.
// This is useful for testing Unix socket connectivity and for scenarios where same-machine
// performance is critical (e.g., sidecar deployments).
func SetupTestClientServerOverUnixSocket(t testing.TB) (*Client, func()) {
	// Create in-memory datastore
	datastore := memory.New()

	// Create a temporary directory for the socket
	tmpDir, err := os.MkdirTemp("", "grpc-test-*")
	require.NoError(t, err)

	socketPath := filepath.Join(tmpDir, "test.sock")

	// Set up gRPC server with Unix socket listener
	lis, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	storageServer := NewServer(datastore)
	storagev1.RegisterStorageServiceServer(grpcServer, storageServer)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()

	// Create client using unix:// scheme
	config := ClientConfig{
		Addr: "unix://" + socketPath,
	}
	client, err := NewClient(config)
	require.NoError(t, err)

	cleanup := func() {
		client.Close()
		datastore.Close()
		grpcServer.Stop()
		os.RemoveAll(tmpDir)
	}

	return client, cleanup
}
