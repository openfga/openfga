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
	server      *grpc.Server
}

var _ otlpcollector.TraceServiceServer = (*mockTracingServer)(nil)

func (s *mockTracingServer) Export(context.Context, *otlpcollector.ExportTraceServiceRequest) (*otlpcollector.ExportTraceServiceResponse, error) {
	s.serviceMu.Lock()
	defer s.serviceMu.Unlock()
	s.exportCount++
	return &otlpcollector.ExportTraceServiceResponse{}, nil
}

func NewMockTracingServer(port int) (*mockTracingServer, func(), error) {
	mockServer := &mockTracingServer{exportCount: 0, server: grpc.NewServer()}
	otlpcollector.RegisterTraceServiceServer(mockServer.server, mockServer)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		if err := mockServer.server.Serve(listener); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
		log.Println("server closed")
	}()

	return mockServer, mockServer.server.Stop, nil
}

func (s *mockTracingServer) GetExportCount() int {
	s.serviceMu.Lock()
	defer s.serviceMu.Unlock()
	return s.exportCount
}
