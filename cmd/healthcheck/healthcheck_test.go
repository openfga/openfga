package healthcheck

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
)

// certFixture holds a self-signed server certificate usable both as the
// server's leaf/key and as the CA the probe verifies against.
type certFixture struct {
	certPEM  []byte
	keyPEM   []byte
	certPath string
	tlsCert  tls.Certificate
}

// newCertFixture generates a self-signed certificate valid for localhost and
// 127.0.0.1 and writes the certificate PEM to a temp file for the probe to read.
func newCertFixture(t *testing.T) certFixture {
	t.Helper()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"OpenFGA Test"}},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	certPath := filepath.Join(t.TempDir(), "cert.pem")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))

	return certFixture{certPEM: certPEM, keyPEM: keyPEM, certPath: certPath, tlsCert: tlsCert}
}

// startGRPCHealthServer starts an in-process gRPC server exposing the standard
// health service, serving the given status for the empty (overall) service.
// When tlsCert is non-nil the server is served over TLS. It returns the address
// and registers cleanup.
func startGRPCHealthServer(t *testing.T, status healthv1pb.HealthCheckResponse_ServingStatus, tlsCert *tls.Certificate) string {
	t.Helper()

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	var opts []grpc.ServerOption
	if tlsCert != nil {
		opts = append(opts, grpc.Creds(credentials.NewServerTLSFromCert(tlsCert)))
	}

	srv := grpc.NewServer(opts...)
	hs := health.NewServer()
	hs.SetServingStatus("", status)
	healthv1pb.RegisterHealthServer(srv, hs)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

func TestCheckGRPC(t *testing.T) {
	t.Run("serving_plaintext_succeeds", func(t *testing.T) {
		addr := startGRPCHealthServer(t, healthv1pb.HealthCheckResponse_SERVING, nil)
		require.NoError(t, checkGRPC(context.Background(), options{addr: addr}))
	})

	t.Run("not_serving_fails", func(t *testing.T) {
		addr := startGRPCHealthServer(t, healthv1pb.HealthCheckResponse_NOT_SERVING, nil)
		err := checkGRPC(context.Background(), options{addr: addr})
		require.ErrorContains(t, err, "not serving")
	})

	t.Run("unknown_service_fails", func(t *testing.T) {
		addr := startGRPCHealthServer(t, healthv1pb.HealthCheckResponse_SERVING, nil)
		err := checkGRPC(context.Background(), options{addr: addr, service: "does.not.Exist"})
		require.ErrorContains(t, err, "grpc health check failed")
	})

	t.Run("connection_refused_fails", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		// Reserve then release a port so the dial target is closed.
		lis, err := net.Listen("tcp", "localhost:0")
		require.NoError(t, err)
		addr := lis.Addr().String()
		require.NoError(t, lis.Close())

		err = checkGRPC(ctx, options{addr: addr})
		require.Error(t, err)
	})

	t.Run("tls_verifies_against_configured_cert", func(t *testing.T) {
		fixture := newCertFixture(t)
		addr := startGRPCHealthServer(t, healthv1pb.HealthCheckResponse_SERVING, &fixture.tlsCert)

		err := checkGRPC(context.Background(), options{
			addr:       addr,
			tlsEnabled: true,
			tlsCert:    fixture.certPath,
		})
		require.NoError(t, err)
	})

	t.Run("tls_enabled_without_cert_fails", func(t *testing.T) {
		err := checkGRPC(context.Background(), options{addr: "localhost:8081", tlsEnabled: true})
		require.ErrorContains(t, err, "no certificate path was provided")
	})

	t.Run("tls_with_bad_address_fails", func(t *testing.T) {
		fixture := newCertFixture(t)
		err := checkGRPC(context.Background(), options{addr: "no-port", tlsEnabled: true, tlsCert: fixture.certPath})
		require.ErrorContains(t, err, "invalid grpc address")
	})
}

func TestCheckHTTP(t *testing.T) {
	t.Run("200_succeeds", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)

		require.NoError(t, checkHTTP(context.Background(), options{addr: hostPort(t, srv.URL)}))
	})

	t.Run("503_fails", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		t.Cleanup(srv.Close)

		err := checkHTTP(context.Background(), options{addr: hostPort(t, srv.URL)})
		require.ErrorContains(t, err, "HTTP 503")
	})

	t.Run("tls_verifies_against_configured_cert", func(t *testing.T) {
		fixture := newCertFixture(t)
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		srv.TLS = &tls.Config{Certificates: []tls.Certificate{fixture.tlsCert}}
		srv.StartTLS()
		t.Cleanup(srv.Close)

		err := checkHTTP(context.Background(), options{
			addr:       hostPort(t, srv.URL),
			tlsEnabled: true,
			tlsCert:    fixture.certPath,
		})
		require.NoError(t, err)
	})

	t.Run("connection_refused_fails", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		lis, err := net.Listen("tcp", "localhost:0")
		require.NoError(t, err)
		addr := lis.Addr().String()
		require.NoError(t, lis.Close())

		err = checkHTTP(ctx, options{addr: addr})
		require.ErrorContains(t, err, "http health check failed")
	})
}

func TestTLSConfigFromCert(t *testing.T) {
	t.Run("empty_path", func(t *testing.T) {
		_, err := tlsConfigFromCert("", "localhost")
		require.ErrorContains(t, err, "no certificate path was provided")
	})

	t.Run("missing_file", func(t *testing.T) {
		_, err := tlsConfigFromCert(filepath.Join(t.TempDir(), "nope.pem"), "localhost")
		require.ErrorContains(t, err, "failed to read TLS certificate")
	})

	t.Run("invalid_pem", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "bad.pem")
		require.NoError(t, os.WriteFile(path, []byte("not a cert"), 0o600))
		_, err := tlsConfigFromCert(path, "localhost")
		require.ErrorContains(t, err, "failed to parse TLS certificate")
	})

	t.Run("valid_cert", func(t *testing.T) {
		fixture := newCertFixture(t)
		cfg, err := tlsConfigFromCert(fixture.certPath, "localhost")
		require.NoError(t, err)
		require.Equal(t, "localhost", cfg.ServerName)
		require.NotNil(t, cfg.RootCAs)
	})
}

func TestNewHealthCheckCommand(t *testing.T) {
	cmd := NewHealthCheckCommand()
	require.Equal(t, "healthcheck", cmd.Name())

	target, err := cmd.Flags().GetString(targetFlag)
	require.NoError(t, err)
	require.Equal(t, targetGRPC, target)

	timeout, err := cmd.Flags().GetDuration(timeoutFlag)
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, timeout)

	// grpc-addr is mirrored from the run command's default.
	addr, err := cmd.Flags().GetString("grpc-addr")
	require.NoError(t, err)
	require.NotEmpty(t, addr)
}

func TestRunHealthCheckInvalidTarget(t *testing.T) {
	cmd := NewHealthCheckCommand()
	cmd.SetArgs([]string{"--target", "bogus"})
	err := cmd.Execute()
	require.ErrorContains(t, err, "invalid --target")
}

// hostPort extracts host:port from an httptest server URL (http://host:port).
func hostPort(t *testing.T, rawURL string) string {
	t.Helper()
	// Strip scheme.
	for _, prefix := range []string{"https://", "http://"} {
		if len(rawURL) > len(prefix) && rawURL[:len(prefix)] == prefix {
			return rawURL[len(prefix):]
		}
	}
	t.Fatalf("unexpected URL %q", rawURL)
	return ""
}
