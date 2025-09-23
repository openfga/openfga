package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"testing"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/middleware/requestid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type outputCapture struct {
	Level           string          `json:"level"`
	Ts              float64         `json:"ts"`
	Msg             string          `json:"msg"`
	GrpcService     string          `json:"grpc_service"`
	GrpcMethod      string          `json:"grpc_method"`
	GrpcType        string          `json:"grpc_type"`
	UserAgent       string          `json:"user_agent"`
	RawRequest      json.RawMessage `json:"raw_request"`
	RawResponse     json.RawMessage `json:"raw_response"`
	QueryDurationMs string          `json:"query_duration_ms"`
	PeerAddress     string          `json:"peer.address"`
	RequestId       string          `json:"request_id"`
	GrpcCode        int             `json:"grpc_code"`
}

func TestNewLoggingInterceptor_concrete(t *testing.T) {

	gotBuffer := new(bytes.Buffer)

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(gotBuffer),
		zap.InfoLevel,
	)
	argLogger := &logger.ZapLogger{Logger: zap.New(core)}

	serverOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(grpc_ctxtags.UnaryServerInterceptor(), requestid.NewUnaryInterceptor(), NewLoggingInterceptor(argLogger)),
	}

	listner := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(serverOpts...)
	t.Cleanup(srv.Stop)

	openfgav1.RegisterOpenFGAServiceServer(srv, &fgaServer{})

	go func() {
		err := srv.Serve(listner)
		require.NoError(t, err)
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return listner.Dial()
	}
	opts := []grpc.DialOption{
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient("passthrough://buffcon", opts...)
	require.NoError(t, err)

	client := openfgav1.NewOpenFGAServiceClient(conn)

	_, err = client.Check(context.Background(), &openfgav1.CheckRequest{})
	require.NoError(t, err)

	var output outputCapture
	err = json.NewDecoder(gotBuffer).Decode(&output)
	require.NoError(t, err)

	assert.Equal(t, "info", output.Level)
	assert.NotEmpty(t, output.Ts)
	assert.Equal(t, "grpc_req_complete", output.Msg)
	assert.Equal(t, "openfga.v1.OpenFGAService", output.GrpcService)
	assert.Equal(t, "Check", output.GrpcMethod)
	assert.Equal(t, "unary", output.GrpcType)
	assert.NotEmpty(t, output.UserAgent)
	assert.NotEmpty(t, output.RawRequest)
	assert.NotEmpty(t, output.RawResponse)
	assert.NotEmpty(t, output.QueryDurationMs)
	assert.NotEmpty(t, output.PeerAddress)
	assert.NotEmpty(t, output.RequestId)
	assert.Equal(t, 0, output.GrpcCode)
}

type fgaServer struct {
	openfgav1.UnimplementedOpenFGAServiceServer
}

func (fgaServer) Check(context.Context, *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
	return &openfgav1.CheckResponse{}, nil
}
