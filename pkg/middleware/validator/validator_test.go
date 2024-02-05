package validator

import (
	"context"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

type pingService struct {
	testpb.TestServiceServer
	T *testing.T
}

func (s *pingService) Ping(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
	require.True(s.T, RequestIsValidatedFromContext(ctx))
	return s.TestServiceServer.Ping(ctx, req)
}

func (s *pingService) PingStream(ss testpb.TestService_PingStreamServer) error {
	require.True(s.T, RequestIsValidatedFromContext(ss.Context()))
	return s.TestServiceServer.PingStream(ss)
}

func TestValidator(t *testing.T) {
	s := &ValidatorTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &pingService{&testpb.TestPingService{}, t},
			ServerOpts: []grpc.ServerOption{
				grpc.UnaryInterceptor(UnaryServerInterceptor()),
				grpc.StreamInterceptor(StreamServerInterceptor()),
			},
		},
	}

	suite.Run(t, s)
}

type ValidatorTestSuite struct {
	*testpb.InterceptorTestSuite
}

func (s *ValidatorTestSuite) TestPing() {
	_, err := s.Client.Ping(s.SimpleCtx(), &testpb.PingRequest{Value: "ping"})
	s.Require().NoError(err)
}

func (s *ValidatorTestSuite) TestStreamingPing() {
	_, err := s.Client.PingStream(s.SimpleCtx())
	s.Require().NoError(err)
}
