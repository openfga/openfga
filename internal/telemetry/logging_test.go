package telemetry

import (
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
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// startGRPCServer starts a bare gRPC server on a random loopback port and
// returns its address. It is enough for the startup connectivity probe to
// reach READY without any registered service.
func startGRPCServer(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	go func() {
		_ = srv.Serve(ln)
	}()
	t.Cleanup(srv.Stop)

	return ln.Addr().String()
}

func TestMustNewLoggerProvider(t *testing.T) {
	lp := MustNewLoggerProvider(
		WithLogOTLPEndpoint(startGRPCServer(t)),
		WithLogOTLPInsecure(),
		WithLogAttributes(attribute.String("service.name", "test")),
	)
	require.NotNil(t, lp)

	require.NoError(t, lp.Shutdown(context.Background()))
}

func TestMustNewLoggerProviderPanicsOnUnreachableEndpoint(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	endpoint := ln.Addr().String()
	require.NoError(t, ln.Close())

	recovered := func() (r any) {
		defer func() { r = recover() }()
		MustNewLoggerProvider(
			WithLogOTLPEndpoint(endpoint),
			WithLogOTLPInsecure(),
		)
		return nil
	}()

	require.NotNil(t, recovered)
	msg, ok := recovered.(string)
	require.True(t, ok, "panic value should be a string, got %T", recovered)
	require.Contains(t, msg, "failed to connect to OTLP log collector")
	require.Contains(t, msg, endpoint)
}

// TestMustNewLoggerProviderHTTPSchemeEnablesTLS verifies that an https://
// endpoint forces TLS even when WithLogOTLPInsecure is passed. The target is a
// plaintext server, so the TLS handshake fails and construction panics; if the
// scheme were ignored the explicit insecure option would make it succeed. This
// is deliberately non-tautological: without the scheme, secure would be false.
func TestMustNewLoggerProviderHTTPSchemeEnablesTLS(t *testing.T) {
	addr := startGRPCServer(t)

	recovered := func() (r any) {
		defer func() { r = recover() }()
		MustNewLoggerProvider(
			WithLogOTLPEndpoint("https://"+addr),
			WithLogOTLPInsecure(),
		)
		return nil
	}()

	require.NotNil(t, recovered)
	require.Contains(t, recovered, "failed to connect to OTLP log collector")
}

// selfSignedCert generates a certificate for 127.0.0.1 that is not signed by
// any CA trusted by the system roots.
func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func TestProbeCollectorFailsOnUntrustedTLS(t *testing.T) {
	cert := selfSignedCert(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}})))
	go func() {
		_ = srv.Serve(ln)
	}()
	t.Cleanup(srv.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// secure=true makes the probe verify the collector certificate, which is
	// self-signed and therefore not trusted.
	require.Error(t, probeCollector(ctx, ln.Addr().String(), true))
}
