package recovery

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
)

func TestPanic(t *testing.T) {
	logger := logger.MustNewLogger("text", "info", "unix")
	handlerFunc := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		panic("Unexpected error!")
	})

	t.Run("With panic Recovery", func(t *testing.T) {
		handler := Panic(handlerFunc, logger)

		req, err := http.NewRequest(http.MethodGet, "/", nil)
		if err != nil {
			require.NoError(t, err)
		}

		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)

		require.Equal(t, http.StatusInternalServerError, resp.Code)
		require.Equal(t, http.StatusText(http.StatusInternalServerError), strings.Trim(resp.Body.String(), "\n"))
	})
}

func TestUnaryPanicInterceptor(t *testing.T) {
	listner := bufconn.Listen(1024 * 1024)

	serverOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			[]grpc.UnaryServerInterceptor{
				UnaryPanicInterceptor(logger.MustNewLogger("text", "info", "unix")),
			}...,
		),
	}

	t.Run("With Unary Panic Interceptor", func(t *testing.T) {
		srv := grpc.NewServer(serverOpts...)
		t.Cleanup(func() {
			srv.Stop()
		})

		openfgav1.RegisterOpenFGAServiceServer(srv, &unimplementedOpenFGAServiceServer{})

		go func() {
			err := srv.Serve(listner)
			if err != nil {
				t.Fail()
			}
		}()

		dialer := func(context.Context, string) (net.Conn, error) {
			return listner.Dial()
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
		})

		opts := []grpc.DialOption{
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials())}

		conn, err := grpc.DialContext(ctx, "", opts...)
		require.NoError(t, err)

		t.Cleanup(func() {
			conn.Close()
		})

		cli := openfgav1.NewOpenFGAServiceClient(conn)

		_, err = cli.Read(ctx, &openfgav1.ReadRequest{})
		require.ErrorContains(t, err, "Read not implemented")

		_, err = cli.Check(ctx, &openfgav1.CheckRequest{})
		require.ErrorContains(t, err, http.StatusText(http.StatusInternalServerError))

		_, err = cli.ReadAuthorizationModels(ctx, &openfgav1.ReadAuthorizationModelsRequest{})
		require.ErrorContains(t, err, "method ReadAuthorizationModels not implemented")
	})
}

func TestStreamPanicInterceptor(t *testing.T) {
	listner := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() {
		listner.Close()
	})

	serverOpts := []grpc.ServerOption{
		grpc.ChainStreamInterceptor(
			[]grpc.StreamServerInterceptor{
				StreamPanicInterceptor(logger.MustNewLogger("text", "info", "unix")),
			}...,
		),
	}

	srv := grpc.NewServer(serverOpts...)
	t.Cleanup(func() {
		srv.Stop()
	})

	openfgav1.RegisterOpenFGAServiceServer(srv, &unimplementedOpenFGAServiceServer{})

	go func() {
		_ = srv.Serve(listner)
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return listner.Dial()
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	opts := []grpc.DialOption{
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials())}

	conn, err := grpc.DialContext(ctx, "", opts...)
	require.NoError(t, err)

	cli := openfgav1.NewOpenFGAServiceClient(conn)
	stream, err := cli.StreamedListObjects(ctx, &openfgav1.StreamedListObjectsRequest{})
	require.NoError(t, err)

	_, err = stream.Recv()
	require.ErrorContains(t, err, http.StatusText(http.StatusInternalServerError))
}

type unimplementedOpenFGAServiceServer struct {
	openfgav1.UnimplementedOpenFGAServiceServer
}

func (unimplementedOpenFGAServiceServer) Check(context.Context, *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
	panic("Unexpected error!")
}
func (unimplementedOpenFGAServiceServer) CreateStore(context.Context, *openfgav1.CreateStoreRequest) (*openfgav1.CreateStoreResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateStore not implemented")
}
func (unimplementedOpenFGAServiceServer) Read(context.Context, *openfgav1.ReadRequest) (*openfgav1.ReadResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Read not implemented")
}
func (unimplementedOpenFGAServiceServer) Write(context.Context, *openfgav1.WriteRequest) (*openfgav1.WriteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Write not implemented")
}
func (unimplementedOpenFGAServiceServer) Expand(context.Context, *openfgav1.ExpandRequest) (*openfgav1.ExpandResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Expand not implemented")
}
func (unimplementedOpenFGAServiceServer) ReadAuthorizationModels(context.Context, *openfgav1.ReadAuthorizationModelsRequest) (*openfgav1.ReadAuthorizationModelsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadAuthorizationModels not implemented")
}
func (unimplementedOpenFGAServiceServer) ReadAuthorizationModel(context.Context, *openfgav1.ReadAuthorizationModelRequest) (*openfgav1.ReadAuthorizationModelResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadAuthorizationModel not implemented")
}
func (unimplementedOpenFGAServiceServer) WriteAuthorizationModel(context.Context, *openfgav1.WriteAuthorizationModelRequest) (*openfgav1.WriteAuthorizationModelResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteAuthorizationModel not implemented")
}
func (unimplementedOpenFGAServiceServer) WriteAssertions(context.Context, *openfgav1.WriteAssertionsRequest) (*openfgav1.WriteAssertionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteAssertions not implemented")
}
func (unimplementedOpenFGAServiceServer) ReadAssertions(context.Context, *openfgav1.ReadAssertionsRequest) (*openfgav1.ReadAssertionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadAssertions not implemented")
}
func (unimplementedOpenFGAServiceServer) ReadChanges(context.Context, *openfgav1.ReadChangesRequest) (*openfgav1.ReadChangesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadChanges not implemented")
}

func (unimplementedOpenFGAServiceServer) UpdateStore(context.Context, *openfgav1.UpdateStoreRequest) (*openfgav1.UpdateStoreResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateStore not implemented")
}
func (unimplementedOpenFGAServiceServer) DeleteStore(context.Context, *openfgav1.DeleteStoreRequest) (*openfgav1.DeleteStoreResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteStore not implemented")
}
func (unimplementedOpenFGAServiceServer) GetStore(context.Context, *openfgav1.GetStoreRequest) (*openfgav1.GetStoreResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetStore not implemented")
}
func (unimplementedOpenFGAServiceServer) ListStores(context.Context, *openfgav1.ListStoresRequest) (*openfgav1.ListStoresResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListStores not implemented")
}
func (unimplementedOpenFGAServiceServer) StreamedListObjects(m *openfgav1.StreamedListObjectsRequest, stream openfgav1.OpenFGAService_StreamedListObjectsServer) error {
	_ = stream.RecvMsg(m)

	panic("Unexpected error!")
}
func (unimplementedOpenFGAServiceServer) ListObjects(context.Context, *openfgav1.ListObjectsRequest) (*openfgav1.ListObjectsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListObjects not implemented")
}
