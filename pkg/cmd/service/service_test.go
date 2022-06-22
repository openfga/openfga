package service

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/server/authentication/mocks"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	openFGAServerURL      = "http://localhost:8080"
	grpcTLSEnabledEnvVar  = "OPENFGA_GRPC_TLS_ENABLED"
	grpcTLSCertPathEnvVar = "OPENFGA_GRPC_TLS_CERT_PATH"
	grpcTLSKeyPathEnvVar  = "OPENFGA_GRPC_TLS_KEY_PATH"
	httpTLSEnabledEnvVar  = "OPENFGA_HTTP_TLS_ENABLED"
	httpTLSCertPathEnvVar = "OPENFGA_HTTP_TLS_CERT_PATH"
	httpTLSKeyPathEnvVar  = "OPENFGA_HTTP_TLS_KEY_PATH"
)

func genCert(template, parent *x509.Certificate, pub *rsa.PublicKey, priv *rsa.PrivateKey) (*x509.Certificate, []byte, error) {
	certBytes, err := x509.CreateCertificate(rand.Reader, template, parent, pub, priv)
	if err != nil {
		return nil, nil, err
	}

	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		return nil, nil, err
	}

	block := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	}

	return cert, pem.EncodeToMemory(block), nil
}

func genCACert() (*x509.Certificate, []byte, *rsa.PrivateKey, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, nil, err
	}

	var rootTemplate = &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            2,
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		Subject: pkix.Name{
			Country:      []string{"Earth"},
			Organization: []string{"Starfleet"},
		},
		DNSNames: []string{"localhost"},
	}

	rootCert, rootPEM, err := genCert(rootTemplate, rootTemplate, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, nil, err
	}

	return rootCert, rootPEM, priv, nil
}

func genServerCert(caCert *x509.Certificate, caKey *rsa.PrivateKey) (*x509.Certificate, []byte, *rsa.PrivateKey, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, nil, err
	}

	var template = &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		KeyUsage:              x509.KeyUsageCRLSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		Subject: pkix.Name{
			Country:      []string{"Earth"},
			Organization: []string{"Starfleet"},
		},
		DNSNames: []string{"localhost"},
	}

	serverCert, serverPEM, err := genCert(template, caCert, &priv.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, err
	}

	return serverCert, serverPEM, priv, nil
}

func writeToTempFile(data []byte) (*os.File, error) {
	file, err := os.CreateTemp("", "openfga_tls_test")
	if err != nil {
		return nil, err
	}

	_, err = file.Write(data)
	if err != nil {
		return nil, err
	}

	return file, nil
}

type certHandle struct {
	caCert         *x509.Certificate
	serverCertFile string
	serverKeyFile  string
}

func (c certHandle) Clean() {
	os.Remove(c.serverCertFile)
	os.Remove(c.serverKeyFile)
}

// createKeys generates a self-signed root CA certificate and a server certificate and server key. It will write
// the PEM encoded server certificate and server key to temporary files. It is the responsibility of the caller
// to delete these files by calling `Clean` on the returned `certHandle`.
func createKeys(t *testing.T) certHandle {
	caCert, _, caKey, err := genCACert()
	require.NoError(t, err)

	_, serverPEM, serverKey, err := genServerCert(caCert, caKey)
	require.NoError(t, err)

	serverCertFile, err := writeToTempFile(serverPEM)
	require.NoError(t, err)

	serverKeyFile, err := writeToTempFile(pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(serverKey),
		},
	))
	require.NoError(t, err)

	require.NoError(t, os.Setenv(httpTLSEnabledEnvVar, "true"), "failed to set env var")
	require.NoError(t, os.Setenv(httpTLSCertPathEnvVar, serverCertFile.Name()), "failed to set env var")
	require.NoError(t, os.Setenv(httpTLSKeyPathEnvVar, serverKeyFile.Name()), "failed to set env var")

	require.NoError(t, os.Setenv(grpcTLSEnabledEnvVar, "true"), "failed to set env var")
	require.NoError(t, os.Setenv(grpcTLSCertPathEnvVar, serverCertFile.Name()), "failed to set env var")
	require.NoError(t, os.Setenv(grpcTLSKeyPathEnvVar, serverKeyFile.Name()), "failed to set env var")

	return certHandle{
		caCert:         caCert,
		serverCertFile: serverCertFile.Name(),
		serverKeyFile:  serverKeyFile.Name(),
	}
}

type authTest struct {
	_name         string
	authHeader    string
	expectedError string
}

func TestBuildServerWithNoAuth(t *testing.T) {
	service, err := BuildService(GetServiceConfig(), logger.NewNoopLogger())
	require.NoError(t, err, "Failed to build server and/or datastore")
	service.Close(context.Background())
}

func TestBuildServerWithPresharedKeyAuthenticationFailsIfZeroKeys(t *testing.T) {
	os.Setenv("OPENFGA_AUTH_METHOD", "preshared")
	os.Setenv("OPENFGA_AUTH_PRESHARED_KEYS", "")

	_, err := BuildService(GetServiceConfig(), logger.NewNoopLogger())
	require.EqualError(t, err, "failed to initialize authenticator: invalid auth configuration, please specify at least one key")
}

func TestBuildServerWithPresharedKeyAuthentication(t *testing.T) {
	os.Setenv("OPENFGA_AUTH_METHOD", "preshared")
	os.Setenv("OPENFGA_AUTH_PRESHARED_KEYS", "KEYONE,KEYTWO")

	ctx, cancel := context.WithCancel(context.Background())

	service, err := BuildService(GetServiceConfig(), logger.NewNoopLogger())
	require.NoError(t, err)

	g := new(errgroup.Group)
	g.Go(func() error {
		return service.Run(ctx)
	})

	ensureServiceUp(t)

	tests := []authTest{{
		_name:         "Header with incorrect key fails",
		authHeader:    "Bearer incorrectkey",
		expectedError: "unauthorized",
	}, {
		_name:         "Missing header fails",
		authHeader:    "",
		expectedError: "missing bearer token",
	}, {
		_name:         "Correct key one succeeds",
		authHeader:    "Bearer KEYONE",
		expectedError: "",
	}, {
		_name:         "Correct key two succeeds",
		authHeader:    "Bearer KEYTWO",
		expectedError: "",
	}}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			payload := strings.NewReader(`{"name": "some-store-name"}`)
			req, err := http.NewRequest("POST", fmt.Sprintf("%s/stores", openFGAServerURL), payload)
			require.NoError(t, err, "Failed to construct request")
			req.Header.Set("content-type", "application/json")
			req.Header.Set("authorization", test.authHeader)
			retryClient := http.Client{}
			res, err := retryClient.Do(req)
			require.NoError(t, err, "Failed to execute request")

			defer res.Body.Close()
			body, err := ioutil.ReadAll(res.Body)
			require.NoError(t, err, "Failed to read response")

			stringBody := string(body)

			if test.expectedError == "" && strings.Contains(stringBody, "code") {
				t.Fatalf("Expected no error but got %v", stringBody)
			}

			if !strings.Contains(stringBody, test.expectedError) && test.expectedError != "" {
				t.Fatalf("Expected %v to contain %v", stringBody, test.expectedError)
			}
		})
	}

	cancel()
	require.NoError(t, g.Wait())
	require.NoError(t, service.Close(ctx))
}

func TestBuildServerWithOidcAuthentication(t *testing.T) {
	const localOidcServerURL = "http://localhost:8083"
	os.Setenv("OPENFGA_AUTH_METHOD", "oidc")
	os.Setenv("OPENFGA_AUTH_OIDC_ISSUER", localOidcServerURL)
	os.Setenv("OPENFGA_AUTH_OIDC_AUDIENCE", openFGAServerURL)

	trustedIssuerServer, err := mocks.NewMockOidcServer(localOidcServerURL)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	service, err := BuildService(GetServiceConfig(), logger.NewNoopLogger())
	require.NoError(t, err)

	g := new(errgroup.Group)
	g.Go(func() error {
		return service.Run(ctx)
	})

	ensureServiceUp(t)

	trustedToken, err := trustedIssuerServer.GetToken(openFGAServerURL, "some-user")
	if err != nil {
		t.Fatal(err)
	}

	tests := []authTest{{
		_name:         "Header with invalid token fails",
		authHeader:    "Bearer incorrecttoken",
		expectedError: "error parsing token",
	}, {
		_name:         "Missing header fails",
		authHeader:    "",
		expectedError: "missing bearer token",
	}, {
		_name:         "Correct token succeeds",
		authHeader:    "Bearer " + trustedToken,
		expectedError: "",
	}}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			payload := strings.NewReader(`{"name": "some-store-name"}`)
			req, err := http.NewRequest("POST", fmt.Sprintf("%s/stores", openFGAServerURL), payload)
			require.NoError(t, err, "Failed to construct request")
			req.Header.Set("content-type", "application/json")
			req.Header.Set("authorization", test.authHeader)
			retryClient := http.Client{}
			res, err := retryClient.Do(req)
			require.NoError(t, err, "Failed to execute request")

			defer res.Body.Close()
			body, err := ioutil.ReadAll(res.Body)
			require.NoError(t, err, "Failed to read response")

			stringBody := string(body)
			if test.expectedError == "" && strings.Contains(stringBody, "code") {
				t.Fatalf("Expected no error but got %v", stringBody)
			}

			if !strings.Contains(stringBody, test.expectedError) && test.expectedError != "" {
				t.Fatalf("Expected %v to contain %v", stringBody, test.expectedError)
			}
		})
	}

	cancel()
	require.NoError(t, g.Wait())
	require.NoError(t, service.Close(ctx))
}

func ensureServiceUp(t *testing.T) {
	t.Helper()

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 2 * time.Second

	err := backoff.Retry(
		func() error {
			resp, err := http.Get(fmt.Sprintf("%s/healthz", openFGAServerURL))
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return errors.New("waiting for OK status")
			}

			return nil
		},
		backoffPolicy,
	)
	require.NoError(t, err)
}

func TestTLSFailureSettings(t *testing.T) {
	logger := logger.NewNoopLogger()

	t.Run("failing to set http cert path will not allow server to start", func(t *testing.T) {
		require.NoError(t, os.Setenv(httpTLSEnabledEnvVar, "true"), "failed to set env var")
		require.NoError(t, os.Setenv(httpTLSKeyPathEnvVar, "some/path"), "failed to set env var")
		defer os.Clearenv()

		_, err := BuildService(GetServiceConfig(), logger)
		require.ErrorIs(t, err, ErrInvalidHTTPTLSConfig)
	})

	t.Run("failing to set grpc cert path will not allow server to start", func(t *testing.T) {
		require.NoError(t, os.Setenv(grpcTLSEnabledEnvVar, "true"), "failed to set env var")
		require.NoError(t, os.Setenv(grpcTLSKeyPathEnvVar, "some/path"), "failed to set env var")
		defer os.Clearenv()

		_, err := BuildService(GetServiceConfig(), logger)
		require.ErrorIs(t, err, ErrInvalidGRPCTLSConfig)
	})

	t.Run("failing to set http key path will not allow server to start", func(t *testing.T) {
		require.NoError(t, os.Setenv(httpTLSEnabledEnvVar, "true"), "failed to set env var")
		require.NoError(t, os.Setenv(httpTLSCertPathEnvVar, "some/path"), "failed to set env var")
		defer os.Clearenv()

		_, err := BuildService(GetServiceConfig(), logger)
		require.ErrorIs(t, err, ErrInvalidHTTPTLSConfig)
	})

	t.Run("failing to set grpc key path will not allow server to start", func(t *testing.T) {
		require.NoError(t, os.Setenv(grpcTLSEnabledEnvVar, "true"), "failed to set env var")
		require.NoError(t, os.Setenv(grpcTLSCertPathEnvVar, "some/path"), "failed to set env var")
		defer os.Clearenv()

		_, err := BuildService(GetServiceConfig(), logger)
		require.ErrorIs(t, err, ErrInvalidGRPCTLSConfig)
	})
}

func TestHTTPServingTLS(t *testing.T) {
	logger := logger.NewNoopLogger()

	t.Run("enable HTTP TLS is false, even with keys set, will serve plaintext", func(t *testing.T) {
		chain := createKeys(t)
		defer chain.Clean()
		defer os.Clearenv()

		config := GetServiceConfig()
		config.HTTPTLSEnabled = false
		service, err := BuildService(config, logger)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		g := new(errgroup.Group)
		g.Go(func() error {
			return service.Run(ctx)
		})

		ensureServiceUp(t)

		cancel()
		require.NoError(t, g.Wait())
		require.NoError(t, service.Close(ctx))
	})

	t.Run("enable HTTP TLS is true will serve HTTP TLS", func(t *testing.T) {
		chain := createKeys(t)
		defer chain.Clean()
		defer os.Clearenv()

		service, err := BuildService(GetServiceConfig(), logger)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		g := new(errgroup.Group)
		g.Go(func() error {
			return service.Run(ctx)
		})

		certPool := x509.NewCertPool()
		certPool.AddCert(chain.caCert)
		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs: certPool,
				},
			},
		}

		backoffPolicy := backoff.NewExponentialBackOff()
		backoffPolicy.MaxElapsedTime = 2 * time.Second
		err = backoff.Retry(
			func() error {
				resp, err := client.Get("https://localhost:8080/healthz")
				if err != nil {
					return err
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					return errors.New("waiting for OK status")
				}

				return nil
			},
			backoffPolicy,
		)
		require.NoError(t, err)

		cancel()
		require.NoError(t, g.Wait())
		require.NoError(t, service.Close(ctx))
	})

}

func TestGRPCServingTLS(t *testing.T) {
	logger := logger.NewNoopLogger()

	t.Run("enable grpc TLS is false, even with keys set, will serve plaintext", func(t *testing.T) {
		chain := createKeys(t)
		defer chain.Clean()
		defer os.Clearenv()

		config := GetServiceConfig()
		config.GRPCTLSEnabled = false
		service, err := BuildService(config, logger)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		g := new(errgroup.Group)
		g.Go(func() error {
			return service.Run(ctx)
		})

		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithConnectParams(grpc.ConnectParams{Backoff: grpcbackoff.DefaultConfig}),
		}
		conn, err := grpc.Dial("localhost:8081", opts...)
		require.NoError(t, err)

		client := openfgapb.NewOpenFGAServiceClient(conn)
		_, err = client.ListStores(ctx, &openfgapb.ListStoresRequest{})
		require.NoError(t, err)

		conn.Close()
		cancel()
		require.NoError(t, g.Wait())
		require.NoError(t, service.Close(ctx))
	})

	t.Run("enable grpc TLS is true will serve grpc TLS", func(t *testing.T) {
		chain := createKeys(t)
		defer chain.Clean()
		defer os.Clearenv()

		service, err := BuildService(GetServiceConfig(), logger)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		g := new(errgroup.Group)
		g.Go(func() error {
			return service.Run(ctx)
		})

		certPool := x509.NewCertPool()
		certPool.AddCert(chain.caCert)
		creds := credentials.NewClientTLSFromCert(certPool, "")

		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(creds),
			grpc.WithConnectParams(grpc.ConnectParams{Backoff: grpcbackoff.DefaultConfig}),
		}
		conn, err := grpc.Dial("localhost:8081", opts...)
		require.NoError(t, err)

		client := openfgapb.NewOpenFGAServiceClient(conn)
		_, err = client.ListStores(ctx, &openfgapb.ListStoresRequest{})
		require.NoError(t, err)

		conn.Close()
		cancel()
		require.NoError(t, g.Wait())
		require.NoError(t, service.Close(ctx))
	})
}
