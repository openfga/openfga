package requestid

import (
	"context"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

var pingReq = &testpb.PingRequest{Value: "ping"}

type pingService struct {
	testpb.TestServiceServer
	T *testing.T
}

func (s *pingService) Ping(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
	_, ok := FromContext(ctx)
	require.True(s.T, ok)

	return s.TestServiceServer.Ping(ctx, req)
}

func (s *pingService) PingStream(ss testpb.TestService_PingStreamServer) error {
	_, ok := FromContext(ss.Context())
	require.True(s.T, ok)

	return s.TestServiceServer.PingStream(ss)
}

func TestRequestIDTestSuite(t *testing.T) {
	s := &RequestIDTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &pingService{&testpb.TestPingService{}, t},
			ServerOpts: []grpc.ServerOption{
				grpc.UnaryInterceptor(NewUnaryInterceptor()),
				grpc.StreamInterceptor(NewStreamingInterceptor()),
			},
		},
	}

	suite.Run(t, s)
}

type RequestIDTestSuite struct {
	*testpb.InterceptorTestSuite
}

func (s *RequestIDTestSuite) TestPing() {
	_, err := s.Client.Ping(s.SimpleCtx(), pingReq)
	s.Require().NoError(err)
}

func (s *RequestIDTestSuite) TestStreamingPing() {
	_, err := s.Client.PingStream(s.SimpleCtx())
	s.Require().NoError(err)
}
