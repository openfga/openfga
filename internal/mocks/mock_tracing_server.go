package mocks

import (
	"context"
	"fmt"
	"log"
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
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	require.NoError(t, err)
	t.Cleanup(mockServer.server.Stop)

	go func() {
		if err := mockServer.server.Serve(listener); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
		log.Println("server closed")
	}()

	return mockServer
}

func (s *mockTracingServer) GetExportCount() int {
	s.serviceMu.Lock()
	defer s.serviceMu.Unlock()
	return s.exportCount
}
