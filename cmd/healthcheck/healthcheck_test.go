package healthcheck

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type blockingHealthServer struct {
	healthv1pb.UnimplementedHealthServer
}

func (blockingHealthServer) Check(ctx context.Context, _ *healthv1pb.HealthCheckRequest) (*healthv1pb.HealthCheckResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// startServer starts a gRPC server on the loopback interface and returns its port.
func startServer(t *testing.T, healthServer healthv1pb.HealthServer, opts ...grpc.ServerOption) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer(opts...)
	healthv1pb.RegisterHealthServer(srv, healthServer)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	_, port, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(t, err)

	return port
}

func newHealthServer(status healthv1pb.HealthCheckResponse_ServingStatus) *health.Server {
	srv := health.NewServer()
	srv.SetServingStatus("", status)
	return srv
}

// selfSignedTLSCreds returns server credentials for a certificate that is only valid for a name
// that is never dialed, so a client that verified it would fail.
func selfSignedTLSCreds(t *testing.T) credentials.TransportCredentials {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "openfga.invalid"},
		DNSNames:     []string{"openfga.invalid"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS12,
	})
}

func execute(t *testing.T, args ...string) (string, error) {
	t.Helper()

	cmd := NewHealthcheckCommand()
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs(args)

	err := cmd.Execute()
	return out.String(), err
}

func TestHealthcheck(t *testing.T) {
	t.Run("serving", func(t *testing.T) {
		port := startServer(t, newHealthServer(healthv1pb.HealthCheckResponse_SERVING))

		out, err := execute(t, "--grpc-addr", "127.0.0.1:"+port)
		require.NoError(t, err)
		require.Equal(t, "status: SERVING\n", out)
	})

	t.Run("unspecified_host_is_checked_on_loopback", func(t *testing.T) {
		port := startServer(t, newHealthServer(healthv1pb.HealthCheckResponse_SERVING))

		for _, addr := range []string{"0.0.0.0:" + port, ":" + port} {
			_, err := execute(t, "--grpc-addr", addr)
			require.NoError(t, err, addr)
		}
	})

	t.Run("not_serving", func(t *testing.T) {
		port := startServer(t, newHealthServer(healthv1pb.HealthCheckResponse_NOT_SERVING))

		_, err := execute(t, "--grpc-addr", "127.0.0.1:"+port)
		require.ErrorContains(t, err, "not serving: NOT_SERVING")
	})

	t.Run("server_unreachable", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addr := lis.Addr().String()
		require.NoError(t, lis.Close())

		_, err = execute(t, "--grpc-addr", addr)
		require.ErrorContains(t, err, "health check against")
	})

	t.Run("timeout", func(t *testing.T) {
		port := startServer(t, blockingHealthServer{})

		start := time.Now()
		_, err := execute(t, "--grpc-addr", "127.0.0.1:"+port, "--timeout", "200ms")
		require.ErrorContains(t, err, "DeadlineExceeded")
		require.Less(t, time.Since(start), 5*time.Second)
	})

	t.Run("tls_without_certificate_verification", func(t *testing.T) {
		port := startServer(t, newHealthServer(healthv1pb.HealthCheckResponse_SERVING), grpc.Creds(selfSignedTLSCreds(t)))

		_, err := execute(t, "--grpc-addr", "127.0.0.1:"+port, "--grpc-tls-enabled")
		require.NoError(t, err)

		_, err = execute(t, "--grpc-addr", "127.0.0.1:"+port)
		require.Error(t, err)
	})

	t.Run("reads_server_env_vars", func(t *testing.T) {
		port := startServer(t, newHealthServer(healthv1pb.HealthCheckResponse_SERVING), grpc.Creds(selfSignedTLSCreds(t)))

		t.Setenv("OPENFGA_GRPC_ADDR", "0.0.0.0:"+port)
		t.Setenv("OPENFGA_GRPC_TLS_ENABLED", "true")

		_, err := execute(t)
		require.NoError(t, err)
	})

	t.Run("invalid_address", func(t *testing.T) {
		_, err := execute(t, "--grpc-addr", "openfga")
		require.ErrorContains(t, err, `invalid gRPC address "openfga"`)
	})

	t.Run("rejects_args", func(t *testing.T) {
		_, err := execute(t, "extra")
		require.Error(t, err)
	})
}

func TestDialTarget(t *testing.T) {
	tests := map[string]string{
		"0.0.0.0:8081":      "127.0.0.1:8081",
		":8081":             "127.0.0.1:8081",
		"[::]:8081":         "[::1]:8081",
		"localhost:8081":    "localhost:8081",
		"10.0.0.7:8081":     "10.0.0.7:8081",
		"openfga.svc:8081":  "openfga.svc:8081",
		"[2001:db8::1]:443": "[2001:db8::1]:443",
	}

	for addr, expected := range tests {
		t.Run(addr, func(t *testing.T) {
			target, err := dialTarget(addr)
			require.NoError(t, err)
			require.Equal(t, expected, target)
		})
	}
}
