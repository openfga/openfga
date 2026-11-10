package queryduration

import (
	"context"
	"strconv"
	"testing"
	"time"

	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const slowPingDuration = 20 * time.Millisecond

// slowPingService delays PingError so the reported duration can be checked
// against a known lower bound.
type slowPingService struct {
	testpb.TestServiceServer
}

func (s *slowPingService) PingError(ctx context.Context, req *testpb.PingErrorRequest) (*testpb.PingErrorResponse, error) {
	time.Sleep(slowPingDuration)
	return s.TestServiceServer.PingError(ctx, req)
}

// PingEmpty panics so the header can be checked when an outer recovery
// interceptor turns the panic into an error.
func (s *slowPingService) PingEmpty(context.Context, *testpb.PingEmptyRequest) (*testpb.PingEmptyResponse, error) {
	panic("boom")
}

func TestQueryDurationTestSuite(t *testing.T) {
	s := &QueryDurationTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &slowPingService{&testpb.TestPingService{}},
			ServerOpts: []grpc.ServerOption{
				grpc.ChainUnaryInterceptor(
					grpc_recovery.UnaryServerInterceptor(),
					NewUnaryInterceptor(),
				),
			},
		},
	}

	suite.Run(t, s)
}

type QueryDurationTestSuite struct {
	*testpb.InterceptorTestSuite
}

func (s *QueryDurationTestSuite) requireDurationHeader(header metadata.MD, minDurationMs int64) {
	values := header.Get(QueryDurationHeader)
	s.Require().Len(values, 1)

	durationMs, err := strconv.ParseInt(values[0], 10, 64)
	s.Require().NoError(err)
	s.Require().GreaterOrEqual(durationMs, minDurationMs)
}

func (s *QueryDurationTestSuite) TestPingSetsHeader() {
	var header metadata.MD
	_, err := s.Client.Ping(s.SimpleCtx(), &testpb.PingRequest{Value: "ping"}, grpc.Header(&header))
	s.Require().NoError(err)

	s.requireDurationHeader(header, 0)
}

func (s *QueryDurationTestSuite) TestPingErrorSetsHeader() {
	var header metadata.MD
	_, err := s.Client.PingError(s.SimpleCtx(), &testpb.PingErrorRequest{
		Value:             "ping",
		ErrorCodeReturned: uint32(codes.InvalidArgument),
	}, grpc.Header(&header))
	s.Require().Equal(codes.InvalidArgument, status.Code(err))

	s.requireDurationHeader(header, slowPingDuration.Milliseconds())
}

func (s *QueryDurationTestSuite) TestPanicSetsHeader() {
	var header metadata.MD
	_, err := s.Client.PingEmpty(s.SimpleCtx(), &testpb.PingEmptyRequest{}, grpc.Header(&header))
	s.Require().Error(err)

	s.requireDurationHeader(header, 0)
}

func (s *QueryDurationTestSuite) TestStreamingPingHasNoHeader() {
	stream, err := s.Client.PingStream(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(stream.Send(&testpb.PingStreamRequest{Value: "ping"}))
	_, err = stream.Recv()
	s.Require().NoError(err)

	header, err := stream.Header()
	s.Require().NoError(err)
	s.Require().Empty(header.Get(QueryDurationHeader))
	s.Require().NoError(stream.CloseSend())
}
