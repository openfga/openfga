package middleware

import (
	"context"
	"testing"

	grpc_testing "github.com/grpc-ecosystem/go-grpc-middleware/testing"
	pb_testproto "github.com/grpc-ecosystem/go-grpc-middleware/testing/testproto"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

var pingReq = &pb_testproto.PingRequest{Value: "ping"}

type pingService struct {
	pb_testproto.TestServiceServer
	T *testing.T
}

func (s *pingService) Ping(ctx context.Context, req *pb_testproto.PingRequest) (*pb_testproto.PingResponse, error) {
	_, ok := RequestIDFromContext(ctx)
	require.True(s.T, ok)

	return s.TestServiceServer.Ping(ctx, req)
}

func (s *pingService) PingStream(ss pb_testproto.TestService_PingStreamServer) error {
	_, ok := RequestIDFromContext(ss.Context())
	require.True(s.T, ok)

	return s.TestServiceServer.PingStream(ss)
}

func TestRequestIDTestSuite(t *testing.T) {
	logr := logger.NewNoopLogger()

	s := &RequestIDTestSuite{
		InterceptorTestSuite: &grpc_testing.InterceptorTestSuite{
			TestService: &pingService{&grpc_testing.TestPingService{T: t}, t},
			ServerOpts: []grpc.ServerOption{
				grpc.UnaryInterceptor(NewRequestIDInterceptor(logr)),
				grpc.StreamInterceptor(NewStreamingRequestIDInterceptor(logr)),
			},
		},
	}

	suite.Run(t, s)
}

type RequestIDTestSuite struct {
	*grpc_testing.InterceptorTestSuite
}

func (s *RequestIDTestSuite) TestPing() {
	_, err := s.Client.Ping(s.SimpleCtx(), pingReq)
	require.NoError(s.T(), err)
}

func (s *RequestIDTestSuite) TestStreamingPing() {
	_, err := s.Client.PingStream(s.SimpleCtx())
	require.NoError(s.T(), err)
}
