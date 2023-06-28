package mocks

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	otlpcollector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

type mockTracingServer struct {
	otlpcollector.UnimplementedTraceServiceServer
	exportCount int
	serviceMu   sync.Mutex
}

var _ otlpcollector.TraceServiceServer = (*mockTracingServer)(nil)

func (s *mockTracingServer) Export(context.Context, *otlpcollector.ExportTraceServiceRequest) (*otlpcollector.ExportTraceServiceResponse, error) {
	s.serviceMu.Lock()
	defer s.serviceMu.Unlock()
	s.exportCount++
	return &otlpcollector.ExportTraceServiceResponse{}, nil
}

func NewMockTracingServer(port int) (*mockTracingServer, error) {
	mockServer := &mockTracingServer{exportCount: 0}

	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		server := grpc.NewServer()
		otlpcollector.RegisterTraceServiceServer(server, mockServer)
		if err := server.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	return mockServer, nil
}

func (s *mockTracingServer) GetExportCount() int {
	s.serviceMu.Lock()
	defer s.serviceMu.Unlock()
	return s.exportCount
}
