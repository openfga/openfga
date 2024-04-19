package recovery

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server"
	"github.com/openfga/openfga/pkg/storage/memory"
)

func TestPanic(t *testing.T) {
	logger := logger.MustNewLogger("text", "info", "unix")
	handlerFunc := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		panic("Unexpected error!")
	})

	t.Run("Without print stack", func(t *testing.T) {
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
	logger := logger.MustNewLogger("text", "info", "unix")

	t.Run("Without print stack", func(t *testing.T) {
		//	handler := UnaryPanicInterceptor(logger)

		serverOpts := []grpc.ServerOption{
			grpc.ChainUnaryInterceptor(
				[]grpc.UnaryServerInterceptor{
					UnaryPanicInterceptor(logger),
				}...,
			),
			grpc.ChainStreamInterceptor(
				[]grpc.StreamServerInterceptor{
					StreamPanicInterceptor(logger),
				}...,
			),
		}

		srv := server.MustNewServerWithOpts(
			server.WithLogger(logger),
			server.WithDatastore(memory.New()),
		)

		grpcServer := grpc.NewServer(serverOpts...)
		openfgav1.RegisterOpenFGAServiceServer(grpcServer, srv)
		req, err := http.NewRequest(http.MethodGet, "/", nil)
		if err != nil {
			require.NoError(t, err)
		}

		resp := httptest.NewRecorder()
		grpcServer.ServeHTTP(resp, req)

		require.Equal(t, http.StatusInternalServerError, resp.Code)
		require.Equal(t, http.StatusText(http.StatusInternalServerError), strings.Trim(resp.Body.String(), "\n"))
	})
}
