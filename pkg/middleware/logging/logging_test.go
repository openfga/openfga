package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/middleware/requestid"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
)

type outputCapture struct {
	Level           string          `json:"level"`
	TS              float64         `json:"ts"`
	Msg             string          `json:"msg"`
	GrpcService     string          `json:"grpc_service"`
	GrpcMethod      string          `json:"grpc_method"`
	GrpcType        string          `json:"grpc_type"`
	UserAgent       string          `json:"user_agent"`
	RawRequest      json.RawMessage `json:"raw_request"`
	RawResponse     json.RawMessage `json:"raw_response"`
	QueryDurationMs string          `json:"query_duration_ms"`
	PeerAddress     string          `json:"peer.address"`
	RequestID       string          `json:"request_id"`
	TraceID         string          `json:"trace_id"`
	InternalError   string          `json:"internal_error"`
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

	openfgav1.RegisterOpenFGAServiceServer(srv, &fgaServer{})

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := srv.Serve(listner)
		if err != nil {
			t.Errorf("failed to serve: %v", err)
		}
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
	assert.NotEmpty(t, output.TS)
	assert.Equal(t, "grpc_req_complete", output.Msg)
	assert.Equal(t, "openfga.v1.OpenFGAService", output.GrpcService)
	assert.Equal(t, "Check", output.GrpcMethod)
	assert.Equal(t, "unary", output.GrpcType)
	assert.NotEmpty(t, output.UserAgent)
	assert.NotEmpty(t, output.RawRequest)
	assert.NotEmpty(t, output.RawResponse)
	assert.NotEmpty(t, output.QueryDurationMs)
	assert.NotEmpty(t, output.PeerAddress)
	assert.NotEmpty(t, output.RequestID)
	assert.Equal(t, 0, output.GrpcCode)

	srv.Stop()
	wg.Wait()
}

type fgaServer struct {
	openfgav1.UnimplementedOpenFGAServiceServer

	checkErr error
}

func (s fgaServer) Check(context.Context, *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
	return &openfgav1.CheckResponse{}, s.checkErr
}

type recordingProcessor struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (p *recordingProcessor) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }

func (p *recordingProcessor) OnEmit(_ context.Context, record *sdklog.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, record.Clone())
	return nil
}

func (p *recordingProcessor) Shutdown(context.Context) error   { return nil }
func (p *recordingProcessor) ForceFlush(context.Context) error { return nil }

func (p *recordingProcessor) snapshot() []sdklog.Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]sdklog.Record(nil), p.records...)
}

// runCheckWithCorrelation drives one request through the logging interceptor
// with a span in context and captures both the stdout and exported streams.
func runCheckWithCorrelation(t *testing.T, level string, register func(*grpc.Server), invoke func(context.Context, *grpc.ClientConn) error) (trace.SpanContext, []byte, []sdklog.Record) {
	t.Helper()

	processor := &recordingProcessor{}
	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(processor),
		// The SDK deduplicates attributes by default, which would hide duplicated tags.
		sdklog.WithAllowKeyDuplication(),
	)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	core := otelzap.NewCore("openfga", otelzap.WithLoggerProvider(provider))

	outPath := filepath.Join(t.TempDir(), "out.log")
	log, err := logger.NewLogger(
		logger.WithFormat("json"),
		logger.WithLevel(level),
		logger.WithOTELCore(core),
		logger.WithOutputPaths(outPath),
	)
	require.NoError(t, err)

	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x0f, 0x1e, 0x2d, 0x3c},
		SpanID:     trace.SpanID{0x4b, 0x5a},
		TraceFlags: trace.FlagsSampled,
	})
	withSpan := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(trace.ContextWithSpanContext(ctx, spanCtx), req)
	}

	listener := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(
		grpc_ctxtags.UnaryServerInterceptor(),
		withSpan,
		requestid.NewUnaryInterceptor(),
		NewLoggingInterceptor(log),
	))
	register(srv)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if serveErr := srv.Serve(listener); serveErr != nil {
			t.Errorf("failed to serve: %v", serveErr)
		}
	}()
	t.Cleanup(func() {
		srv.Stop()
		wg.Wait()
	})

	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	conn, err := grpc.NewClient("passthrough://buffcon",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_ = invoke(context.Background(), conn)

	stdout, err := os.ReadFile(outPath)
	require.NoError(t, err)

	return spanCtx, stdout, processor.snapshot()
}

func countRecordAttr(record sdklog.Record, key string) int {
	count := 0
	record.WalkAttributes(func(kv attribute.KeyValue) bool {
		if string(kv.Key) == key {
			count++
		}
		return true
	})
	return count
}

func recordAttrString(record sdklog.Record, key string) string {
	value := ""
	record.WalkAttributes(func(kv attribute.KeyValue) bool {
		if string(kv.Key) == key {
			value = kv.Value.AsString()
		}
		return true
	})
	return value
}

func TestLoggingInterceptor_Correlation(t *testing.T) {
	internalErr := serverErrors.NewInternalError("internal", errors.New("boom"))

	registerFGA := func(handlerErr error) func(*grpc.Server) {
		return func(srv *grpc.Server) {
			openfgav1.RegisterOpenFGAServiceServer(srv, &fgaServer{checkErr: handlerErr})
		}
	}
	invokeCheck := func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := openfgav1.NewOpenFGAServiceClient(conn).Check(ctx, &openfgav1.CheckRequest{})
		return err
	}

	for _, tc := range []struct {
		name        string
		handlerErr  error
		wantLevel   string
		wantMessage string
	}{
		{
			name:        "success",
			handlerErr:  nil,
			wantLevel:   "info",
			wantMessage: grpcReqCompleteKey,
		},
		{
			name:        "customer error",
			handlerErr:  status.Error(codes.InvalidArgument, "invalid argument"),
			wantLevel:   "info",
			wantMessage: grpcReqCompleteKey,
		},
		{
			name:        "internal error",
			handlerErr:  internalErr,
			wantLevel:   "error",
			wantMessage: internalErr.Error(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spanCtx, stdout, records := runCheckWithCorrelation(t, "info", registerFGA(tc.handlerErr), invokeCheck)

			require.Len(t, records, 1)
			assert.Equal(t, spanCtx.TraceID(), records[0].TraceID())
			assert.Equal(t, spanCtx.SpanID(), records[0].SpanID())

			line := strings.TrimSpace(string(stdout))
			var output outputCapture
			require.NoError(t, json.Unmarshal([]byte(line), &output))
			assert.Equal(t, tc.wantLevel, output.Level)
			assert.Equal(t, tc.wantMessage, output.Msg)
			// stdout keeps the trace_id string field, matching the active span.
			assert.Equal(t, spanCtx.TraceID().String(), output.TraceID)

			// Tags must appear once: Info/Debug append in the interceptor, Error in the logger.
			assert.Equal(t, 1, strings.Count(line, `"request_id"`))
			assert.Equal(t, 1, strings.Count(line, `"trace_id"`))
			assert.NotContains(t, line, `"ctx"`)
			assert.Equal(t, 1, countRecordAttr(records[0], "request_id"))
			assert.Equal(t, 1, countRecordAttr(records[0], "trace_id"))
			assert.Equal(t, spanCtx.TraceID().String(), recordAttrString(records[0], "trace_id"))

			if tc.name == "internal error" {
				// ErrorWithContext appends the tags itself; the interceptor must not repeat them.
				assert.Equal(t, "boom", output.InternalError)
				assert.Equal(t, "boom", recordAttrString(records[0], internalErrorKey))
			}
		})
	}
}

func TestLoggingInterceptor_HealthCheckUsesDebug(t *testing.T) {
	register := func(srv *grpc.Server) {
		healthpb.RegisterHealthServer(srv, health.NewServer())
	}
	invoke := func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		return err
	}

	spanCtx, stdout, records := runCheckWithCorrelation(t, "debug", register, invoke)

	require.Len(t, records, 1)
	assert.Equal(t, spanCtx.TraceID(), records[0].TraceID())
	assert.Equal(t, spanCtx.SpanID(), records[0].SpanID())

	line := strings.TrimSpace(string(stdout))
	var output outputCapture
	require.NoError(t, json.Unmarshal([]byte(line), &output))
	assert.Equal(t, "debug", output.Level)
	assert.Equal(t, grpcReqCompleteKey, output.Msg)
	assert.Equal(t, spanCtx.TraceID().String(), output.TraceID)
	assert.Equal(t, 1, strings.Count(line, `"request_id"`))
	assert.Equal(t, 1, countRecordAttr(records[0], "request_id"))
	assert.NotContains(t, line, `"ctx"`)
}
