package mocks

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	otlpcollector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

type mockTracingServer struct {
	otlpcollector.UnimplementedTraceServiceServer
	exportCount int
	serviceMu   sync.Mutex
	server      *grpc.Server
}

var _ otlpcollector.TraceServiceServer = (*mockTracingServer)(nil)

func (s *mockTracingServer) Export(context.Context, *otlpcollector.ExportTraceServiceRequest) (*otlpcollector.ExportTraceServiceResponse, error) {
	s.serviceMu.Lock()
	defer s.serviceMu.Unlock()
	s.exportCount++
	return &otlpcollector.ExportTraceServiceResponse{}, nil
}

func NewMockTracingServer(t testing.TB, port int) *mockTracingServer {
	mockServer := &mockTracingServer{exportCount: 0, server: grpc.NewServer()}
	otlpcollector.RegisterTraceServiceServer(mockServer.server, mockServer)
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	require.NoError(t, err)
	t.Cleanup(mockServer.server.GracefulStop)

	go func() {
		if err := mockServer.server.Serve(listener); err != nil {
			if !errors.Is(err, grpc.ErrServerStopped) {
				t.Log("mock tracing server failed to serve", err)
			}
		}
	}()

	return mockServer
}

func (s *mockTracingServer) GetExportCount() int {
	s.serviceMu.Lock()
	defer s.serviceMu.Unlock()
	return s.exportCount
}
