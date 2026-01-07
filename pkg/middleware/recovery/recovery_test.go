package recovery

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
)

func TestPanic(t *testing.T) {
	panicHandlerFunc := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		panic("Unexpected error!")
	})

	handler := HTTPPanicRecoveryHandler(panicHandlerFunc, logger.MustNewLogger("text", "info", "unix"))

	req, err := http.NewRequest(http.MethodGet, "/", nil)
	require.NoError(t, err)

	resp := httptest.NewRecorder()
	require.NotPanics(t, func() {
		handler.ServeHTTP(resp, req)
	})

	require.Equal(t, http.StatusInternalServerError, resp.Code)
}

func TestUnaryPanicInterceptor(t *testing.T) {
	listner := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() {
		listner.Close()
		goleak.VerifyNone(t)
	})

	serverOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			[]grpc.UnaryServerInterceptor{
				grpc_recovery.UnaryServerInterceptor(
					grpc_recovery.WithRecoveryHandlerContext(
						PanicRecoveryHandler(logger.MustNewLogger("text", "info", "unix")),
					),
				),
			}...,
		),
	}

	srv := grpc.NewServer(serverOpts...)
	t.Cleanup(srv.Stop)

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
	t.Cleanup(cancel)

	opts := []grpc.DialOption{
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials())}

	// nolint:staticcheck // ignoring gRPC deprecations
	conn, err := grpc.DialContext(ctx, "", opts...)
	require.NoError(t, err)

	t.Cleanup(func() {
		conn.Close()
	})

	cli := openfgav1.NewOpenFGAServiceClient(conn)

	_, err = cli.Check(ctx, &openfgav1.CheckRequest{})
	st, ok := status.FromError(err)
	require.True(t, ok)

	require.Equal(t, codes.Internal, st.Code())
}

func TestStreamPanicInterceptor(t *testing.T) {
	listner := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() {
		listner.Close()
	})

	serverOpts := []grpc.ServerOption{
		grpc.ChainStreamInterceptor(
			[]grpc.StreamServerInterceptor{
				grpc_recovery.StreamServerInterceptor(
					grpc_recovery.WithRecoveryHandlerContext(
						PanicRecoveryHandler(logger.MustNewLogger("text", "info", "unix")),
					),
				)}...,
		),
	}

	srv := grpc.NewServer(serverOpts...)
	t.Cleanup(srv.Stop)

	openfgav1.RegisterOpenFGAServiceServer(srv, &unimplementedOpenFGAServiceServer{})

	go func() {
		_ = srv.Serve(listner)
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return listner.Dial()
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	opts := []grpc.DialOption{
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials())}

	// nolint:staticcheck // ignoring gRPC deprecations
	conn, err := grpc.DialContext(ctx, "", opts...)
	require.NoError(t, err)

	cli := openfgav1.NewOpenFGAServiceClient(conn)
	stream, err := cli.StreamedListObjects(ctx, &openfgav1.StreamedListObjectsRequest{})
	require.NoError(t, err)

	_, err = stream.Recv()
	st, ok := status.FromError(err)
	require.True(t, ok)

	require.Equal(t, codes.Internal, st.Code())
}

type unimplementedOpenFGAServiceServer struct {
	openfgav1.UnimplementedOpenFGAServiceServer
}

func (unimplementedOpenFGAServiceServer) Check(context.Context, *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
	panic("Unexpected error!")
}

func (unimplementedOpenFGAServiceServer) StreamedListObjects(m *openfgav1.StreamedListObjectsRequest, stream openfgav1.OpenFGAService_StreamedListObjectsServer) error {
	_ = stream.RecvMsg(m)

	panic("Unexpected error!")
}
