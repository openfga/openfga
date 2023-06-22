package mocks

import (
	"context"
	"fmt"
	"log"
	"net"

	otlpcollector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

type MockTracingServer struct {
	otlpcollector.UnimplementedTraceServiceServer
	exportCount int
}

func (s *MockTracingServer) Export(context.Context, *otlpcollector.ExportTraceServiceRequest) (*otlpcollector.ExportTraceServiceResponse, error) {
	s.exportCount++
	return &otlpcollector.ExportTraceServiceResponse{}, nil
}

func NewMockTracingServer(port int) (*MockTracingServer, error) {
	mockServer := &MockTracingServer{exportCount: 0}

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

func (s *MockTracingServer) GetExportCount() int {
	return s.exportCount
}
