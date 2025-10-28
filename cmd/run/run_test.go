package run

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/goleak"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/encoding/protojson"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/middleware/requestid"
	"github.com/openfga/openfga/pkg/middleware/storeid"
	"github.com/openfga/openfga/pkg/server"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/sqlite"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestMain(m *testing.M) {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "../../..")
	err := os.Chdir(dir)
	if err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

func genCert(t *testing.T, template, parent *x509.Certificate, pub *rsa.PublicKey, priv *rsa.PrivateKey) (*x509.Certificate, []byte) {
	certBytes, err := x509.CreateCertificate(rand.Reader, template, parent, pub, priv)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(certBytes)
	require.NoError(t, err)

	block := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	}

	return cert, pem.EncodeToMemory(block)
}

func genCACert(t *testing.T) (*x509.Certificate, []byte, *rsa.PrivateKey) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

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

	rootCert, rootPEM := genCert(t, rootTemplate, rootTemplate, &priv.PublicKey, priv)

	return rootCert, rootPEM, priv
}

func genServerCert(t *testing.T, caCert *x509.Certificate, caKey *rsa.PrivateKey) (*x509.Certificate, []byte, *rsa.PrivateKey) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

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

	serverCert, serverPEM := genCert(t, template, caCert, &priv.PublicKey, caKey)

	return serverCert, serverPEM, priv
}

func writeToTempFile(t *testing.T, data []byte) *os.File {
	file, err := os.CreateTemp("", "openfga_tls_test")
	require.NoError(t, err)

	_, err = file.Write(data)
	require.NoError(t, err)

	return file
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

// createCertsAndKeys generates a self-signed root CA certificate and a server certificate and server key. It will write
// the PEM encoded server certificate and server key to temporary files. It is the responsibility of the caller
// to delete these files by calling `Clean` on the returned `certHandle`.
func createCertsAndKeys(t *testing.T) certHandle {
	caCert, _, caKey := genCACert(t)
	_, serverPEM, serverKey := genServerCert(t, caCert, caKey)
	serverCertFile := writeToTempFile(t, serverPEM)
	serverKeyFile := writeToTempFile(t, pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(serverKey),
		},
	))

	return certHandle{
		caCert:         caCert,
		serverCertFile: serverCertFile.Name(),
		serverKeyFile:  serverKeyFile.Name(),
	}
}

type authTest struct {
	_name                 string
	authHeader            string
	expectedErrorResponse *serverErrors.ErrorResponse
	expectedStatusCode    int
}

func runServer(ctx context.Context, cfg *serverconfig.Config) error {
	if err := cfg.Verify(); err != nil {
		return err
	}

	logger := logger.MustNewLogger(cfg.Log.Format, cfg.Log.Level, cfg.Log.TimestampFormat)
	serverCtx := &ServerContext{Logger: logger}
	return serverCtx.Run(ctx, cfg)
}

func TestBuildServiceWithPresharedKeyAuthenticationFailsIfZeroKeys(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	cfg := testutils.MustDefaultConfigWithRandomPorts()
	cfg.Authn.Method = "preshared"
	cfg.Authn.AuthnPresharedKeyConfig = &serverconfig.AuthnPresharedKeyConfig{}

	err := runServer(context.Background(), cfg)
	require.EqualError(t, err, "failed to initialize authenticator: invalid auth configuration, please specify at least one key")
}

func TestBuildServiceWithNoAuth(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	cfg := testutils.MustDefaultConfigWithRandomPorts()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil)

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	client := openfgav1.NewOpenFGAServiceClient(conn)

	// Just checking we can create a store with no authentication.
	_, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "store"})
	require.NoError(t, err)
}

func TestBuildServiceWithPresharedKeyAuthentication(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	cfg := testutils.MustDefaultConfigWithRandomPorts()
	cfg.Authn.Method = "preshared"
	cfg.Authn.AuthnPresharedKeyConfig = &serverconfig.AuthnPresharedKeyConfig{
		Keys: []string{"KEYONE", "KEYTWO"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil)

	tests := []authTest{{
		_name:      "Header_with_incorrect_key_fails",
		authHeader: "Bearer incorrectkey",
		expectedErrorResponse: &serverErrors.ErrorResponse{
			Code:    "unauthenticated",
			Message: "unauthenticated",
		},
		expectedStatusCode: 401,
	}, {
		_name:      "Missing_header_fails",
		authHeader: "",
		expectedErrorResponse: &serverErrors.ErrorResponse{
			Code:    "bearer_token_missing",
			Message: "missing bearer token",
		},
		expectedStatusCode: 401,
	}, {
		_name:              "Correct_key_one_succeeds",
		authHeader:         fmt.Sprintf("Bearer %s", cfg.Authn.Keys[0]),
		expectedStatusCode: 200,
	}, {
		_name:              "Correct_key_two_succeeds",
		authHeader:         fmt.Sprintf("Bearer %s", cfg.Authn.Keys[1]),
		expectedStatusCode: 200,
	}}

	retryClient := retryablehttp.NewClient()
	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			tryGetStores(t, test, cfg.HTTP.Addr, retryClient)
		})

		t.Run(test._name+"/streaming", func(t *testing.T) {
			tryStreamingListObjects(t, test, cfg.HTTP.Addr, retryClient, cfg.Authn.Keys[0])
		})
	}
}

func TestBuildServiceWithTracingEnabled(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	// create mock OTLP server
	otlpServerPort, otlpServerPortReleaser := testutils.TCPRandomPort()
	localOTLPServerURL := fmt.Sprintf("localhost:%d", otlpServerPort)
	otlpServerPortReleaser()
	otlpServer := mocks.NewMockTracingServer(t, otlpServerPort)

	// create OpenFGA server with tracing enabled
	cfg := testutils.MustDefaultConfigWithRandomPorts()
	cfg.Trace.Enabled = true
	cfg.Trace.SampleRatio = 1
	cfg.Trace.OTLP.Endpoint = localOTLPServerURL
	cfg.Trace.OTLP.TLS.Enabled = false

	ctx, cancel := context.WithCancel(context.Background())

	serverDone := make(chan error)
	go func() {
		serverDone <- runServer(ctx, cfg)
	}()
	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil)

	// attempt a random request
	client := retryablehttp.NewClient()
	t.Cleanup(client.HTTPClient.CloseIdleConnections)
	response, err := client.Get(fmt.Sprintf("http://%s/healthz", cfg.HTTP.Addr))
	require.NoError(t, err)

	t.Cleanup(func() {
		err := response.Body.Close()
		require.NoError(t, err)
	})

	cancel()
	select {
	case <-time.After(5 * time.Second):
		require.Fail(t, "timed out")
	case err := <-serverDone:
		require.NoError(t, err)
	}

	// at this point, all spans should have been forcefully exported

	require.Equal(t, 1, otlpServer.GetExportCount())
}

func tryStreamingListObjects(t *testing.T, test authTest, httpAddr string, retryClient *retryablehttp.Client, validToken string) {
	// create a store
	createStorePayload := strings.NewReader(`{"name": "some-store-name"}`)
	req, err := retryablehttp.NewRequest("POST", fmt.Sprintf("http://%s/stores", httpAddr), createStorePayload)
	require.NoError(t, err, "Failed to construct create store request")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", fmt.Sprintf("Bearer %s", validToken))
	res, err := retryClient.Do(req)
	require.NoError(t, err, "Failed to execute create store request")
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err, "Failed to read create store response")
	var createStoreResponse openfgav1.CreateStoreResponse
	err = protojson.Unmarshal(body, &createStoreResponse)
	require.NoError(t, err, "Failed to unmarshal create store response")

	// create an authorization model
	authModelPayload := strings.NewReader(`{
  "type_definitions": [
    {
      "type": "user",
      "relations": {}
    },
    {
      "type": "document",
      "relations": {
        "owner": {
          "this": {}
        }
      },
      "metadata": {
        "relations": {
          "owner": {
            "directly_related_user_types": [
              {
                "type": "user"
              }
            ]
          }
        }
      }
    }
  ],
  "schema_version": "1.1"
}`)
	req, err = retryablehttp.NewRequest("POST", fmt.Sprintf("http://%s/stores/%s/authorization-models", httpAddr, createStoreResponse.GetId()), authModelPayload)
	require.NoError(t, err, "Failed to construct create authorization model request")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", fmt.Sprintf("Bearer %s", validToken))
	response, err := retryClient.Do(req)
	require.NoError(t, err, "Failed to execute create authorization model request")

	t.Cleanup(func() {
		err := response.Body.Close()
		require.NoError(t, err)
	})

	// call one streaming endpoint
	listObjectsPayload := strings.NewReader(`{"type": "document", "user": "user:anne", "relation": "owner"}`)
	req, err = retryablehttp.NewRequest("POST", fmt.Sprintf("http://%s/stores/%s/streamed-list-objects", httpAddr, createStoreResponse.GetId()), listObjectsPayload)
	require.NoError(t, err, "Failed to construct request")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", test.authHeader)
	res, err = retryClient.Do(req)
	require.NoError(t, err, "Failed to execute streaming request")
	defer res.Body.Close()
	require.Equal(t, test.expectedStatusCode, res.StatusCode)
	body, err = io.ReadAll(res.Body)
	require.NoError(t, err, "Failed to read response")

	if test.expectedErrorResponse != nil {
		require.Contains(t, string(body), fmt.Sprintf(",\"message\":\"%s\"", test.expectedErrorResponse.Message))
	}
}

func tryGetStores(t *testing.T, test authTest, httpAddr string, retryClient *retryablehttp.Client) {
	req, err := retryablehttp.NewRequest("GET", fmt.Sprintf("http://%s/stores", httpAddr), nil)
	require.NoError(t, err, "Failed to construct request")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", test.authHeader)

	res, err := retryClient.Do(req)
	require.NoError(t, err, "Failed to execute request")
	defer res.Body.Close()
	require.Equal(t, test.expectedStatusCode, res.StatusCode)
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err, "Failed to read response")

	if test.expectedErrorResponse != nil {
		var actualErrorResponse serverErrors.ErrorResponse
		err = json.Unmarshal(body, &actualErrorResponse)

		require.NoError(t, err, "Failed to unmarshal response")

		require.Equal(t, test.expectedErrorResponse, &actualErrorResponse)
	}
}

func TestHTTPServerWithCORS(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	cfg := testutils.MustDefaultConfigWithRandomPorts()
	cfg.Authn.Method = "preshared"
	cfg.Authn.AuthnPresharedKeyConfig = &serverconfig.AuthnPresharedKeyConfig{
		Keys: []string{"KEYONE", "KEYTWO"},
	}
	cfg.HTTP.CORSAllowedOrigins = []string{"http://openfga.dev", "http://localhost"}
	cfg.HTTP.CORSAllowedHeaders = []string{"Origin", "Accept", "Content-Type", "X-Requested-With", "Authorization", "X-Custom-Header"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil)

	type args struct {
		origin string
		header string
	}
	type want struct {
		origin string
		header string
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "origin_allowed",
			args: args{
				origin: "http://localhost",
				// must be lowercase, see https://github.com/rs/cors/issues/174#issuecomment-2082098921
				header: "authorization,x-custom-header",
			},
			want: want{
				origin: "http://localhost",
				header: "authorization,x-custom-header",
			},
		},
		{
			name: "origin_forbidden",
			args: args{
				origin: "http://openfga.example",
				header: "X-Custom-Header",
			},
			want: want{
				origin: "",
				header: "",
			},
		},
		{
			name: "origin_allowed_but_header_forbidden",
			args: args{
				origin: "http://localhost",
				header: "Bad-Custom-Header",
			},
			want: want{
				origin: "",
				header: "",
			},
		},
	}

	client := retryablehttp.NewClient()
	t.Cleanup(client.HTTPClient.CloseIdleConnections)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := strings.NewReader(`{"name": "some-store-name"}`)
			req, err := retryablehttp.NewRequest("OPTIONS", fmt.Sprintf("http://%s/stores", cfg.HTTP.Addr), payload)
			require.NoError(t, err, "Failed to construct request")
			req.Header.Set("content-type", "application/json")
			req.Header.Set("Origin", test.args.origin)
			req.Header.Set("Access-Control-Request-Method", "OPTIONS")
			req.Header.Set("Access-Control-Request-Headers", test.args.header)

			res, err := client.Do(req)
			require.NoError(t, err, "Failed to execute request")
			defer res.Body.Close()

			origin := res.Header.Get("Access-Control-Allow-Origin")
			acceptedHeader := res.Header.Get("Access-Control-Allow-Headers")
			require.Equal(t, test.want.origin, origin)

			require.Equal(t, test.want.header, acceptedHeader)

			_, err = io.ReadAll(res.Body)
			require.NoError(t, err, "Failed to read response")
		})
	}
}

func TestBuildServerWithOIDCAuthentication(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	oidcServerPort, oidcServerPortReleaser := testutils.TCPRandomPort()
	localOIDCServerURL := fmt.Sprintf("http://localhost:%d", oidcServerPort)

	cfg := testutils.MustDefaultConfigWithRandomPorts()
	cfg.Authn.Method = "oidc"
	cfg.Authn.AuthnOIDCConfig = &serverconfig.AuthnOIDCConfig{
		Audience: "openfga.dev",
		Issuer:   localOIDCServerURL,
	}

	oidcServerPortReleaser()

	trustedIssuerServer, err := mocks.NewMockOidcServer(localOIDCServerURL)
	require.NoError(t, err)
	t.Cleanup(trustedIssuerServer.Stop)

	trustedToken, err := trustedIssuerServer.GetToken("openfga.dev", "some-user")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil)

	tests := []authTest{
		{
			_name:      "Header_with_invalid_token_fails",
			authHeader: "Bearer incorrecttoken",
			expectedErrorResponse: &serverErrors.ErrorResponse{
				Code:    "invalid_claims",
				Message: "invalid claims",
			},
			expectedStatusCode: 401,
		},
		{
			_name:      "Missing_header_fails",
			authHeader: "",
			expectedErrorResponse: &serverErrors.ErrorResponse{
				Code:    "bearer_token_missing",
				Message: "missing bearer token",
			},
			expectedStatusCode: 401,
		},
		{
			_name:              "Correct_token_succeeds",
			authHeader:         "Bearer " + trustedToken,
			expectedStatusCode: 200,
		},
	}

	retryClient := retryablehttp.NewClient()
	t.Cleanup(retryClient.HTTPClient.CloseIdleConnections)

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			tryGetStores(t, test, cfg.HTTP.Addr, retryClient)
		})

		t.Run(test._name+"/streaming", func(t *testing.T) {
			tryStreamingListObjects(t, test, cfg.HTTP.Addr, retryClient, trustedToken)
		})
	}
}

func TestBuildServerWithOIDCAuthenticationAlias(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	oidcServerPort1, oidcServerPortReleaser1 := testutils.TCPRandomPort()
	oidcServerPort2, oidcServerPortReleaser2 := testutils.TCPRandomPort()
	oidcServerURL1 := fmt.Sprintf("http://localhost:%d", oidcServerPort1)
	oidcServerURL2 := fmt.Sprintf("http://localhost:%d", oidcServerPort2)

	cfg := testutils.MustDefaultConfigWithRandomPorts()
	cfg.Authn.Method = "oidc"
	cfg.Authn.AuthnOIDCConfig = &serverconfig.AuthnOIDCConfig{
		Audience:      "openfga.dev",
		Issuer:        oidcServerURL1,
		IssuerAliases: []string{oidcServerURL2},
	}

	oidcServerPortReleaser1()
	oidcServerPortReleaser2()

	trustedIssuerServer1, err := mocks.NewMockOidcServer(oidcServerURL1)
	require.NoError(t, err)
	t.Cleanup(trustedIssuerServer1.Stop)

	trustedIssuerServer2 := trustedIssuerServer1.NewAliasMockServer(oidcServerURL2)
	t.Cleanup(trustedIssuerServer2.Stop)

	trustedTokenFromAlias, err := trustedIssuerServer2.GetToken("openfga.dev", "some-user")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil)

	retryClient := retryablehttp.NewClient()
	t.Cleanup(retryClient.HTTPClient.CloseIdleConnections)

	test := authTest{
		_name:              "Token_with_issuer_equal_to_alias_is_accepted",
		authHeader:         "Bearer " + trustedTokenFromAlias,
		expectedStatusCode: 200,
	}
	t.Run(test._name, func(t *testing.T) {
		tryGetStores(t, test, cfg.HTTP.Addr, retryClient)
	})

	t.Run(test._name+"/streaming", func(t *testing.T) {
		tryStreamingListObjects(t, test, cfg.HTTP.Addr, retryClient, trustedTokenFromAlias)
	})
}

func TestHTTPServingTLS(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("enable_HTTP_TLS_is_false,_even_with_keys_set,_will_serve_plaintext", func(t *testing.T) {
		certsAndKeys := createCertsAndKeys(t)
		defer certsAndKeys.Clean()

		cfg := testutils.MustDefaultConfigWithRandomPorts()
		cfg.HTTP.TLS = &serverconfig.TLSConfig{
			CertPath: certsAndKeys.serverCertFile,
			KeyPath:  certsAndKeys.serverKeyFile,
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			if err := runServer(ctx, cfg); err != nil {
				log.Fatal(err)
			}
		}()

		testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil)
	})

	t.Run("enable_HTTP_TLS_is_true_will_serve_HTTP_TLS", func(t *testing.T) {
		certsAndKeys := createCertsAndKeys(t)
		defer certsAndKeys.Clean()

		cfg := testutils.MustDefaultConfigWithRandomPorts()
		cfg.HTTP.TLS = &serverconfig.TLSConfig{
			Enabled:  true,
			CertPath: certsAndKeys.serverCertFile,
			KeyPath:  certsAndKeys.serverKeyFile,
		}
		// Port for TLS cannot be 0.0.0.0
		cfg.HTTP.Addr = strings.ReplaceAll(cfg.HTTP.Addr, "0.0.0.0", "localhost")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			if err := runServer(ctx, cfg); err != nil {
				log.Fatal(err)
			}
		}()

		certPool := x509.NewCertPool()
		certPool.AddCert(certsAndKeys.caCert)
		client := retryablehttp.NewClient()
		t.Cleanup(client.HTTPClient.CloseIdleConnections)
		client.HTTPClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: certPool,
			},
		}

		resp, err := client.Get(fmt.Sprintf("https://%s/healthz", cfg.HTTP.Addr))
		require.NoError(t, err)
		resp.Body.Close()
	})
}

func TestGRPCServingTLS(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("enable_grpc_TLS_is_false,_even_with_keys_set,_will_serve_plaintext", func(t *testing.T) {
		certsAndKeys := createCertsAndKeys(t)
		defer certsAndKeys.Clean()

		cfg := testutils.MustDefaultConfigWithRandomPorts()
		cfg.HTTP.Enabled = false
		cfg.GRPC.TLS = &serverconfig.TLSConfig{
			CertPath: certsAndKeys.serverCertFile,
			KeyPath:  certsAndKeys.serverKeyFile,
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			if err := runServer(ctx, cfg); err != nil {
				log.Fatal(err)
			}
		}()

		testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, "", nil)
	})

	t.Run("enable_grpc_TLS_is_true_will_serve_grpc_TLS", func(t *testing.T) {
		certsAndKeys := createCertsAndKeys(t)
		defer certsAndKeys.Clean()

		cfg := testutils.MustDefaultConfigWithRandomPorts()
		cfg.HTTP.Enabled = false
		cfg.GRPC.TLS = &serverconfig.TLSConfig{
			Enabled:  true,
			CertPath: certsAndKeys.serverCertFile,
			KeyPath:  certsAndKeys.serverKeyFile,
		}
		// Port for TLS cannot be 0.0.0.0
		cfg.GRPC.Addr = strings.ReplaceAll(cfg.GRPC.Addr, "0.0.0.0", "localhost")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			if err := runServer(ctx, cfg); err != nil {
				log.Fatal(err)
			}
		}()

		certPool := x509.NewCertPool()
		certPool.AddCert(certsAndKeys.caCert)
		creds := credentials.NewClientTLSFromCert(certPool, "")

		testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, "", creds)
	})
}

func TestServerMetricsReporting(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	const nonPgxPrometheusMetrics = "go_sql_idle_connections"
	const pgxPrometheusMetrics = "pgxpool_idle_conns"
	t.Run("mysql", func(t *testing.T) {
		testServerMetricsReporting(t, "mysql", nonPgxPrometheusMetrics)
	})
	t.Run("postgres", func(t *testing.T) {
		testServerMetricsReporting(t, "postgres", pgxPrometheusMetrics)
	})
	t.Run("sqlite", func(t *testing.T) {
		testServerMetricsReporting(t, "sqlite", nonPgxPrometheusMetrics)
	})
}

func testServerMetricsReporting(t *testing.T, engine string, connectionMetricName string) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, engine)

	cfg := testutils.MustDefaultConfigWithRandomPorts()
	cfg.Datastore.Engine = engine
	cfg.Datastore.URI = testDatastore.GetConnectionURI(true)
	cfg.Datastore.Metrics.Enabled = true
	cfg.Metrics.Enabled = true
	cfg.Metrics.EnableRPCHistograms = true
	metricsPort, metricsPortReleaser := testutils.TCPRandomPort()
	metricsPortReleaser()

	cfg.Metrics.Addr = fmt.Sprintf("localhost:%d", metricsPort)

	cfg.MaxConcurrentReadsForCheck = 30
	cfg.MaxConcurrentReadsForListObjects = 30

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil)

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	client := openfgav1.NewOpenFGAServiceClient(conn)

	createStoreResp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{
		Name: "test-store",
	})
	require.NoError(t, err)

	storeID := createStoreResp.GetId()

	_, err = client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId: storeID,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"allowed": typesystem.This(),
					"editor":  typesystem.This(),
					"viewer": typesystem.Union(
						typesystem.This(),
						typesystem.Intersection(
							typesystem.ComputedUserset("editor"),
							typesystem.ComputedUserset("allowed"))),
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"allowed": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
							},
						},
						"editor": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
							},
						},
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user", Condition: "condx"},
							},
						},
					},
				},
			},
		},
		Conditions: map[string]*openfgav1.Condition{
			"condx": {
				Name: "condx",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"x": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT,
					},
				},
				Expression: "x < 100",
			},
		},
		SchemaVersion: typesystem.SchemaVersion1_1,
	})
	require.NoError(t, err)

	_, err = client.Write(ctx, &openfgav1.WriteRequest{
		StoreId: storeID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:jon", "condx", nil),
				{Object: "document:2", Relation: "editor", User: "user:jon"},
				{Object: "document:2", Relation: "allowed", User: "user:jon"},
			},
		},
	})
	require.NoError(t, err)

	checkResp, err := client.Check(ctx, &openfgav1.CheckRequest{
		StoreId:  storeID,
		TupleKey: tuple.NewCheckRequestTupleKey("document:1", "viewer", "user:jon"),
		Context: testutils.MustNewStruct(t, map[string]interface{}{
			"x": 10,
		}),
	})
	require.NoError(t, err)
	require.True(t, checkResp.GetAllowed())

	listObjectsResp, err := client.ListObjects(ctx, &openfgav1.ListObjectsRequest{
		StoreId:  storeID,
		Type:     "document",
		Relation: "viewer",
		User:     "user:jon",
		Context: testutils.MustNewStruct(t, map[string]interface{}{
			"x": 10,
		}),
	})
	require.NoError(t, err)
	require.Len(t, listObjectsResp.GetObjects(), 2)
	require.Contains(t, listObjectsResp.GetObjects(), "document:1")
	require.Contains(t, listObjectsResp.GetObjects(), "document:2")

	resp, err := retryablehttp.Get(fmt.Sprintf("http://%s/metrics", cfg.Metrics.Addr))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()

	expectedMetrics := []string{
		"openfga_datastore_query_count",
		"openfga_request_duration_ms",
		"grpc_server_handling_seconds",
		"openfga_list_objects_further_eval_required_count",
		"openfga_list_objects_no_further_eval_required_count",
		"openfga_condition_evaluation_cost",
		"openfga_condition_compilation_duration_ms",
		"openfga_condition_evaluation_duration_ms",
	}

	expectedMetrics = append(expectedMetrics, connectionMetricName)

	for _, metric := range expectedMetrics {
		count, err := testutil.GatherAndCount(prometheus.DefaultGatherer, metric)
		require.NoError(t, err)
		require.GreaterOrEqualf(t, count, 1, "expected at least 1 reported value for '%s'", metric)
	}
}

func TestHTTPServerDisabled(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	cfg := testutils.MustDefaultConfigWithRandomPorts()
	cfg.HTTP.Enabled = false

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, "", nil)

	_, err := http.Get(fmt.Sprintf("http://%s/healthz", cfg.HTTP.Addr))
	require.Error(t, err)
	require.ErrorContains(t, err, "connect: connection refused")
}

func TestHTTPServerEnabled(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	cfg := testutils.MustDefaultConfigWithRandomPorts()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil)
}

func TestDefaultConfig(t *testing.T) {
	cfg, err := ReadConfig()
	require.NoError(t, err)

	_, basepath, _, _ := runtime.Caller(0)
	jsonSchema, err := os.ReadFile(path.Join(filepath.Dir(basepath), "..", "..", ".config-schema.json"))
	require.NoError(t, err)

	res := gjson.ParseBytes(jsonSchema)

	val := res.Get("properties.datastore.properties.engine.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Datastore.Engine)

	val = res.Get("properties.datastore.properties.maxCacheSize.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.Datastore.MaxCacheSize)

	val = res.Get("properties.datastore.properties.maxIdleConns.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.Datastore.MaxIdleConns)

	val = res.Get("properties.datastore.properties.minIdleConns.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.Datastore.MinIdleConns)

	val = res.Get("properties.datastore.properties.maxOpenConns.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.Datastore.MaxOpenConns)

	val = res.Get("properties.datastore.properties.minOpenConns.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.Datastore.MinOpenConns)

	val = res.Get("properties.datastore.properties.connMaxIdleTime.default")
	require.True(t, val.Exists())

	val = res.Get("properties.datastore.properties.connMaxLifetime.default")
	require.True(t, val.Exists())

	val = res.Get("properties.datastore.properties.metrics.properties.enabled.default")
	require.True(t, val.Exists())
	require.False(t, val.Bool())

	val = res.Get("properties.grpc.properties.addr.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.GRPC.Addr)

	val = res.Get("properties.http.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.HTTP.Enabled)

	val = res.Get("properties.http.properties.addr.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.HTTP.Addr)

	val = res.Get("properties.playground.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.Playground.Enabled)

	val = res.Get("properties.playground.properties.port.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.Playground.Port)

	val = res.Get("properties.profiler.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.Profiler.Enabled)

	val = res.Get("properties.profiler.properties.addr.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Profiler.Addr)

	val = res.Get("properties.authn.properties.method.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Authn.Method)

	val = res.Get("properties.log.properties.format.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Log.Format)

	val = res.Get("properties.maxTuplesPerWrite.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.MaxTuplesPerWrite)

	val = res.Get("properties.maxTypesPerAuthorizationModel.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.MaxTypesPerAuthorizationModel)

	val = res.Get("properties.maxConcurrentReadsForListObjects.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.MaxConcurrentReadsForListObjects)

	val = res.Get("properties.maxConcurrentReadsForCheck.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.MaxConcurrentReadsForCheck)

	val = res.Get("properties.maxConcurrentChecksPerBatchCheck.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.MaxConcurrentChecksPerBatchCheck)

	val = res.Get("properties.maxChecksPerBatchCheck.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.MaxChecksPerBatchCheck)

	val = res.Get("properties.maxConditionEvaluationCost.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Uint(), cfg.MaxConditionEvaluationCost)

	val = res.Get("properties.maxConcurrentReadsForListUsers.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.MaxConcurrentReadsForListUsers)

	val = res.Get("properties.changelogHorizonOffset.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ChangelogHorizonOffset)

	val = res.Get("properties.resolveNodeBreadthLimit.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ResolveNodeBreadthLimit)

	val = res.Get("properties.resolveNodeLimit.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ResolveNodeLimit)

	val = res.Get("properties.grpc.properties.tls.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.GRPC.TLS.Enabled)

	val = res.Get("properties.http.properties.tls.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.HTTP.TLS.Enabled)

	val = res.Get("properties.listObjectsDeadline.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.ListObjectsDeadline.String())

	val = res.Get("properties.listObjectsMaxResults.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListObjectsMaxResults)

	val = res.Get("properties.listUsersDeadline.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.ListUsersDeadline.String())

	val = res.Get("properties.listUsersMaxResults.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListUsersMaxResults)

	val = res.Get("properties.experimentals.default")
	require.True(t, val.Exists())
	require.Len(t, cfg.Experimentals, len(val.Array()))

	val = res.Get("properties.metrics.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.Metrics.Enabled)

	val = res.Get("properties.metrics.properties.addr.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Metrics.Addr)

	val = res.Get("properties.metrics.properties.enableRPCHistograms.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.Metrics.EnableRPCHistograms)

	val = res.Get("properties.trace.properties.serviceName.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Trace.ServiceName)

	val = res.Get("properties.trace.properties.otlp.properties.endpoint.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Trace.OTLP.Endpoint)

	val = res.Get("properties.trace.properties.otlp.properties.tls.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.Trace.OTLP.TLS.Enabled)

	val = res.Get("properties.checkCache.properties.limit.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.CheckCache.Limit)

	val = res.Get("properties.checkQueryCache.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.CheckQueryCache.Enabled)

	val = res.Get("properties.checkQueryCache.properties.ttl.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.CheckQueryCache.TTL.String())

	val = res.Get("properties.checkIteratorCache.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.CheckIteratorCache.Enabled)

	val = res.Get("properties.checkIteratorCache.properties.maxResults.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.CheckIteratorCache.MaxResults)

	val = res.Get("properties.checkIteratorCache.properties.ttl.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.CheckIteratorCache.TTL.String())

	val = res.Get("properties.listObjectsIteratorCache.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.ListObjectsIteratorCache.Enabled)

	val = res.Get("properties.listObjectsIteratorCache.properties.maxResults.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListObjectsIteratorCache.MaxResults)

	val = res.Get("properties.listObjectsIteratorCache.properties.ttl.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.ListObjectsIteratorCache.TTL.String())

	val = res.Get("properties.cacheController.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.CacheController.Enabled)

	val = res.Get("properties.cacheController.properties.ttl.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.CacheController.TTL.String())

	val = res.Get("properties.sharedIterator.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.SharedIterator.Enabled)

	val = res.Get("properties.sharedIterator.properties.limit.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.SharedIterator.Limit)

	val = res.Get("properties.requestDurationDatastoreQueryCountBuckets.default")
	require.True(t, val.Exists())
	require.Len(t, cfg.RequestDurationDatastoreQueryCountBuckets, len(val.Array()))
	for index, arrayVal := range val.Array() {
		require.Equal(t, arrayVal.String(), cfg.RequestDurationDatastoreQueryCountBuckets[index])
	}

	val = res.Get("properties.requestDurationDispatchCountBuckets.default")
	require.True(t, val.Exists())
	require.Len(t, cfg.RequestDurationDispatchCountBuckets, len(val.Array()))
	for index, arrayVal := range val.Array() {
		require.Equal(t, arrayVal.String(), cfg.RequestDurationDispatchCountBuckets[index])
	}

	val = res.Get("properties.contextPropagationToDatastore.default")
	require.True(t, val.Exists())
	require.False(t, val.Bool())

	val = res.Get("properties.checkDispatchThrottling.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.CheckDispatchThrottling.Enabled)

	val = res.Get("properties.checkDispatchThrottling.properties.frequency.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.CheckDispatchThrottling.Frequency.String())

	val = res.Get("properties.checkDispatchThrottling.properties.threshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.CheckDispatchThrottling.Threshold)

	val = res.Get("properties.checkDispatchThrottling.properties.maxThreshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.CheckDispatchThrottling.MaxThreshold)

	val = res.Get("properties.listObjectsDispatchThrottling.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.ListObjectsDispatchThrottling.Enabled)

	val = res.Get("properties.listObjectsDispatchThrottling.properties.frequency.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.ListObjectsDispatchThrottling.Frequency.String())

	val = res.Get("properties.listObjectsDispatchThrottling.properties.threshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListObjectsDispatchThrottling.Threshold)

	val = res.Get("properties.listObjectsDispatchThrottling.properties.maxThreshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListObjectsDispatchThrottling.MaxThreshold)

	val = res.Get("properties.listUsersDispatchThrottling.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.ListUsersDispatchThrottling.Enabled)

	val = res.Get("properties.listUsersDispatchThrottling.properties.frequency.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.ListUsersDispatchThrottling.Frequency.String())

	val = res.Get("properties.listUsersDispatchThrottling.properties.threshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListUsersDispatchThrottling.Threshold)

	val = res.Get("properties.listUsersDispatchThrottling.properties.maxThreshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListUsersDispatchThrottling.MaxThreshold)

	val = res.Get("properties.checkDatastoreThrottle.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.CheckDatastoreThrottle.Enabled)

	val = res.Get("properties.checkDatastoreThrottle.properties.threshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.CheckDatastoreThrottle.Threshold)

	val = res.Get("properties.checkDatastoreThrottle.properties.duration.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.CheckDatastoreThrottle.Duration.String())

	val = res.Get("properties.listObjectsDatastoreThrottle.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.ListObjectsDatastoreThrottle.Enabled)

	val = res.Get("properties.listObjectsDatastoreThrottle.properties.threshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListObjectsDatastoreThrottle.Threshold)

	val = res.Get("properties.listObjectsDatastoreThrottle.properties.duration.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.ListObjectsDatastoreThrottle.Duration.String())

	val = res.Get("properties.listUsersDatastoreThrottle.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.ListUsersDatastoreThrottle.Enabled)

	val = res.Get("properties.listUsersDatastoreThrottle.properties.threshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListUsersDatastoreThrottle.Threshold)

	val = res.Get("properties.listUsersDatastoreThrottle.properties.duration.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.ListUsersDatastoreThrottle.Duration.String())

	val = res.Get("properties.requestTimeout.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.RequestTimeout.String())
}

func TestRunCommandNoConfigDefaultValues(t *testing.T) {
	util.PrepareTempConfigDir(t)
	runCmd := NewRunCommand()
	runCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Empty(t, viper.GetString(datastoreEngineFlag))
		require.Empty(t, viper.GetString(datastoreURIFlag))
		require.False(t, viper.GetBool("check-query-cache-enabled"))
		require.False(t, viper.GetBool("context-propagation-to-datastore"))
		require.Equal(t, uint32(0), viper.GetUint32("check-query-cache-limit"))
		require.Equal(t, 0*time.Second, viper.GetDuration("check-query-cache-ttl"))
		require.Empty(t, viper.GetIntSlice("request-duration-datastore-query-count-buckets"))
		return nil
	}

	rootCmd := cmd.NewRootCommand()
	rootCmd.AddCommand(runCmd)
	rootCmd.SetArgs([]string{"run"})
	require.NoError(t, rootCmd.Execute())
}

func TestRunCommandConfigFileValuesAreParsed(t *testing.T) {
	config := `datastore:
    engine: postgres
    uri: postgres://postgres:password@127.0.0.1:5432/postgres
`
	util.PrepareTempConfigFile(t, config)

	runCmd := NewRunCommand()
	runCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "postgres", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "postgres://postgres:password@127.0.0.1:5432/postgres", viper.GetString(datastoreURIFlag))
		return nil
	}

	rootCmd := cmd.NewRootCommand()
	rootCmd.AddCommand(runCmd)
	rootCmd.SetArgs([]string{"run"})
	require.NoError(t, rootCmd.Execute())
}

func TestParseConfig(t *testing.T) {
	config := `checkQueryCache:
    enabled: true
    TTL: 5s
requestDurationDatastoreQueryCountBuckets: [33,44]
requestDurationDispatchCountBuckets: [32,42]
`
	util.PrepareTempConfigFile(t, config)

	runCmd := NewRunCommand()
	runCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		return nil
	}
	rootCmd := cmd.NewRootCommand()
	rootCmd.AddCommand(runCmd)
	rootCmd.SetArgs([]string{"run"})
	require.NoError(t, rootCmd.Execute())

	cfg, err := ReadConfig()
	require.NoError(t, err)
	require.True(t, cfg.CheckQueryCache.Enabled)
	require.Equal(t, 5*time.Second, cfg.CheckQueryCache.TTL)
	require.Equal(t, []string{"33", "44"}, cfg.RequestDurationDatastoreQueryCountBuckets)
	require.Equal(t, []string{"32", "42"}, cfg.RequestDurationDispatchCountBuckets)
}

func TestRunCommandConfigIsMerged(t *testing.T) {
	config := `datastore:
    engine: postgres
`
	util.PrepareTempConfigFile(t, config)

	t.Setenv("OPENFGA_DATASTORE_URI", "postgres://postgres:PASS2@127.0.0.1:5432/postgres")
	t.Setenv("OPENFGA_MAX_TYPES_PER_AUTHORIZATION_MODEL", "1")
	t.Setenv("OPENFGA_CHECK_QUERY_CACHE_ENABLED", "true")
	t.Setenv("OPENFGA_CHECK_CACHE_LIMIT", "33")
	t.Setenv("OPENFGA_CHECK_QUERY_CACHE_TTL", "5s")
	t.Setenv("OPENFGA_CACHE_CONTROLLER_ENABLED", "true")
	t.Setenv("OPENFGA_CACHE_CONTROLLER_TTL", "4s")
	t.Setenv("OPENFGA_REQUEST_DURATION_DATASTORE_QUERY_COUNT_BUCKETS", "33 44")
	t.Setenv("OPENFGA_DISPATCH_THROTTLING_ENABLED", "true")
	t.Setenv("OPENFGA_DISPATCH_THROTTLING_FREQUENCY", "1ms")
	t.Setenv("OPENFGA_DISPATCH_THROTTLING_THRESHOLD", "120")
	t.Setenv("OPENFGA_DISPATCH_THROTTLING_MAX_THRESHOLD", "130")
	t.Setenv("OPENFGA_MAX_CONDITION_EVALUATION_COST", "120")
	t.Setenv("OPENFGA_ACCESS_CONTROL_ENABLED", "true")
	t.Setenv("OPENFGA_ACCESS_CONTROL_STORE_ID", "12345")
	t.Setenv("OPENFGA_ACCESS_CONTROL_MODEL_ID", "67891")
	t.Setenv("OPENFGA_CONTEXT_PROPAGATION_TO_DATASTORE", "true")
	t.Setenv("OPENFGA_SHARED_ITERATOR_ENABLED", "true")
	t.Setenv("OPENFGA_SHARED_ITERATOR_LIMIT", "950")

	runCmd := NewRunCommand()
	runCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "postgres", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "postgres://postgres:PASS2@127.0.0.1:5432/postgres", viper.GetString(datastoreURIFlag))
		require.Equal(t, "1", viper.GetString("max-types-per-authorization-model"))
		require.True(t, viper.GetBool("check-query-cache-enabled"))
		require.Equal(t, uint32(33), viper.GetUint32("check-cache-limit"))
		require.Equal(t, 5*time.Second, viper.GetDuration("check-query-cache-ttl"))
		require.True(t, viper.GetBool("cache-controller-enabled"))
		require.Equal(t, 4*time.Second, viper.GetDuration("cache-controller-ttl"))

		require.Equal(t, []string{"33", "44"}, viper.GetStringSlice("request-duration-datastore-query-count-buckets"))
		require.True(t, viper.GetBool("dispatch-throttling-enabled"))
		require.Equal(t, "1ms", viper.GetString("dispatch-throttling-frequency"))
		require.Equal(t, "120", viper.GetString("dispatch-throttling-threshold"))
		require.Equal(t, "130", viper.GetString("dispatch-throttling-max-threshold"))
		require.Equal(t, "120", viper.GetString("max-condition-evaluation-cost"))
		require.Equal(t, uint64(120), viper.GetUint64("max-condition-evaluation-cost"))
		require.True(t, viper.GetBool("access-control-enabled"))
		require.Equal(t, "12345", viper.GetString("access-control-store-id"))
		require.Equal(t, "67891", viper.GetString("access-control-model-id"))
		require.True(t, viper.GetBool("context-propagation-to-datastore"))
		require.True(t, viper.GetBool("shared-iterator-enabled"))
		require.Equal(t, uint32(950), viper.GetUint32("shared-iterator-limit"))

		return nil
	}

	rootCmd := cmd.NewRootCommand()
	rootCmd.AddCommand(runCmd)
	rootCmd.SetArgs([]string{"run"})
	require.NoError(t, rootCmd.Execute())
}

func TestHTTPHeaders(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	cfg := testutils.MustDefaultConfigWithRandomPorts()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil)

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	client := openfgav1.NewOpenFGAServiceClient(conn)

	createStoreResp, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)

	storeID := createStoreResp.GetId()

	writeModelResp, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:       storeID,
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustTransformDSLToProto(`
			model
				schema 1.1
			type user

			type document
				relations
					define viewer: [user]`).GetTypeDefinitions(),
	})
	require.NoError(t, err)

	authorizationModelID := writeModelResp.GetAuthorizationModelId()

	httpClient := retryablehttp.NewClient()
	t.Cleanup(httpClient.HTTPClient.CloseIdleConnections)

	var testCases = map[string]struct {
		httpVerb      string
		httpPath      string
		httpJSONBody  string
		expectedError bool
	}{
		`check`: {
			httpVerb:     "POST",
			httpPath:     fmt.Sprintf("http://%s/stores/%s/check", cfg.HTTP.Addr, storeID),
			httpJSONBody: `{"tuple_key": {"user": "user:anne",  "relation": "viewer", "object": "document:1"}}`,
		},
		`listobjects`: {
			httpVerb:     "POST",
			httpPath:     fmt.Sprintf("http://%s/stores/%s/list-objects", cfg.HTTP.Addr, storeID),
			httpJSONBody: `{"type": "document", "user": "user:anne", "relation": "viewer"}`,
		},
		`streamed-list-objects`: {
			httpVerb:     "POST",
			httpPath:     fmt.Sprintf("http://%s/stores/%s/streamed-list-objects", cfg.HTTP.Addr, storeID),
			httpJSONBody: `{"type": "document", "user": "user:anne", "relation": "viewer"}`,
		},
		`listusers`: {
			httpVerb:     "POST",
			httpPath:     fmt.Sprintf("http://%s/stores/%s/list-users", cfg.HTTP.Addr, storeID),
			httpJSONBody: `{"object": { "type": "document", "id": "1" } , "relation": "viewer", "user_filters": [ {"type":"user"} ]}`,
		},
		`expand`: {
			httpVerb:     "POST",
			httpPath:     fmt.Sprintf("http://%s/stores/%s/expand", cfg.HTTP.Addr, storeID),
			httpJSONBody: `{"tuple_key": {"user": "user:anne",  "relation": "viewer", "object": "document:1"}}`,
		},
		`write`: {
			httpVerb:     "POST",
			httpPath:     fmt.Sprintf("http://%s/stores/%s/write", cfg.HTTP.Addr, storeID),
			httpJSONBody: `{"writes": { "tuple_keys": [{"user": "user:anne",  "relation": "viewer", "object": "document:1"}]}}`,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			req, err := retryablehttp.NewRequest(test.httpVerb, test.httpPath, strings.NewReader(test.httpJSONBody))
			require.NoError(t, err, "Failed to construct request")

			httpResponse, err := httpClient.Do(req)
			require.NoError(t, err)
			defer httpResponse.Body.Close()

			// These are set in the server RPCs
			require.Len(t, httpResponse.Header[server.AuthorizationModelIDHeader], 1)
			require.Equal(t, authorizationModelID, httpResponse.Header[server.AuthorizationModelIDHeader][0])

			// These are set in middlewares
			require.Len(t, httpResponse.Header[storeid.StoreIDHeader], 1)
			require.Equal(t, storeID, httpResponse.Header[storeid.StoreIDHeader][0])
			require.Len(t, httpResponse.Header[requestid.RequestIDHeader], 1)
			require.NotEmpty(t, httpResponse.Header[requestid.RequestIDHeader][0])

			httpResponse.Body.Close()
		})
	}
}

func TestServerContext_datastoreConfig(t *testing.T) {
	tests := []struct {
		name           string
		config         *serverconfig.Config
		wantDSType     interface{}
		wantSerializer encoder.ContinuationTokenSerializer
		wantErr        error
	}{
		{
			name: "sqlite",
			config: &serverconfig.Config{
				Datastore: serverconfig.DatastoreConfig{
					Engine: "sqlite",
				},
			},
			wantDSType:     &sqlite.Datastore{},
			wantSerializer: &sqlcommon.SQLContinuationTokenSerializer{},
			wantErr:        nil,
		},
		{
			name: "sqlite_bad_uri",
			config: &serverconfig.Config{
				Datastore: serverconfig.DatastoreConfig{
					Engine: "sqlite",
					URI:    "uri?is;bad=true",
				},
			},
			wantDSType:     nil,
			wantSerializer: nil,
			wantErr:        errors.New("invalid semicolon separator in query"),
		},
		{
			name: "mysql_bad_uri",
			config: &serverconfig.Config{
				Datastore: serverconfig.DatastoreConfig{
					Engine:   "mysql",
					Username: "root",
					Password: "password",
					URI:      "uri?is;bad=true",
				},
			},
			wantDSType:     nil,
			wantSerializer: nil,
			wantErr:        errors.New("missing the slash separating the database name"),
		},
		{
			name: "postgres_bad_uri",
			config: &serverconfig.Config{
				Datastore: serverconfig.DatastoreConfig{
					Engine:   "postgres",
					Username: "root",
					Password: "password",
					URI:      "~!@#$%^&*()_+}{:<>?",
				},
			},
			wantDSType:     nil,
			wantSerializer: nil,
			wantErr:        errors.New("parse postgres connection uri"),
		},
		{
			name: "unsupported_engine",
			config: &serverconfig.Config{
				Datastore: serverconfig.DatastoreConfig{
					Engine: "unsupported",
				},
			},
			wantDSType:     nil,
			wantSerializer: nil,
			wantErr:        errors.New("storage engine 'unsupported' is unsupported"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &ServerContext{
				Logger: logger.NewNoopLogger(),
			}
			datastore, serializer, err := s.datastoreConfig(tt.config)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.Nil(t, datastore)
				assert.Nil(t, serializer)
				assert.ErrorContains(t, err, tt.wantErr.Error())
			} else {
				require.NoError(t, err)
				assert.IsType(t, tt.wantDSType, datastore)
				assert.Equal(t, tt.wantSerializer, serializer)
			}
		})
	}
}
