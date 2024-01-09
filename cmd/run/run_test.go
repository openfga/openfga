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
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/pkg/testutils"

	"github.com/openfga/openfga/pkg/middleware/requestid"
	"github.com/openfga/openfga/pkg/middleware/storeid"
	"github.com/openfga/openfga/pkg/server"

	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/internal/mocks"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
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

	logger := logger.MustNewLogger(cfg.Log.Format, cfg.Log.Level)
	serverCtx := &ServerContext{Logger: logger}
	return serverCtx.Run(ctx, cfg)
}

func TestBuildServiceWithPresharedKeyAuthenticationFailsIfZeroKeys(t *testing.T) {
	cfg := MustDefaultConfigWithRandomPorts()
	cfg.Authn.Method = "preshared"
	cfg.Authn.AuthnPresharedKeyConfig = &serverconfig.AuthnPresharedKeyConfig{}

	err := runServer(context.Background(), cfg)
	require.EqualError(t, err, "failed to initialize authenticator: invalid auth configuration, please specify at least one key")
}

func TestBuildServiceWithNoAuth(t *testing.T) {
	cfg := MustDefaultConfigWithRandomPorts()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil, true)

	conn, err := grpc.Dial(cfg.GRPC.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := openfgav1.NewOpenFGAServiceClient(conn)

	// Just checking we can create a store with no authentication.
	_, err = client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "store"})
	require.NoError(t, err)
}

func TestBuildServiceWithPresharedKeyAuthentication(t *testing.T) {
	cfg := MustDefaultConfigWithRandomPorts()
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

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil, true)

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
		authHeader:         fmt.Sprintf("Bearer %s", cfg.Authn.AuthnPresharedKeyConfig.Keys[0]),
		expectedStatusCode: 200,
	}, {
		_name:              "Correct_key_two_succeeds",
		authHeader:         fmt.Sprintf("Bearer %s", cfg.Authn.AuthnPresharedKeyConfig.Keys[1]),
		expectedStatusCode: 200,
	}}

	retryClient := retryablehttp.NewClient()
	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			tryGetStores(t, test, cfg.HTTP.Addr, retryClient)
		})

		t.Run(test._name+"/streaming", func(t *testing.T) {
			tryStreamingListObjects(t, test, cfg.HTTP.Addr, retryClient, cfg.Authn.AuthnPresharedKeyConfig.Keys[0])
		})
	}
}

func TestBuildServiceWithTracingEnabled(t *testing.T) {
	// create mock OTLP server
	otlpServerPort, otlpServerPortReleaser := TCPRandomPort()
	localOTLPServerURL := fmt.Sprintf("localhost:%d", otlpServerPort)
	otlpServerPortReleaser()
	otlpServer, serverStopFunc, err := mocks.NewMockTracingServer(otlpServerPort)
	defer serverStopFunc()
	require.NoError(t, err)

	// create OpenFGA server with tracing enabled
	cfg := MustDefaultConfigWithRandomPorts()
	cfg.Trace.Enabled = true
	cfg.Trace.SampleRatio = 1
	cfg.Trace.OTLP.Endpoint = localOTLPServerURL
	cfg.Trace.OTLP.TLS.Enabled = false

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil, true)

	// attempt a random request
	client := retryablehttp.NewClient()
	_, err = client.Get(fmt.Sprintf("http://%s/healthz", cfg.HTTP.Addr))
	require.NoError(t, err)

	// wait for trace exporting
	time.Sleep(sdktrace.DefaultScheduleDelay * time.Millisecond)

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
	req, err = retryablehttp.NewRequest("POST", fmt.Sprintf("http://%s/stores/%s/authorization-models", httpAddr, createStoreResponse.Id), authModelPayload)
	require.NoError(t, err, "Failed to construct create authorization model request")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", fmt.Sprintf("Bearer %s", validToken))
	_, err = retryClient.Do(req)
	require.NoError(t, err, "Failed to execute create authorization model request")

	// call one streaming endpoint
	listObjectsPayload := strings.NewReader(`{"type": "document", "user": "user:anne", "relation": "owner"}`)
	req, err = retryablehttp.NewRequest("POST", fmt.Sprintf("http://%s/stores/%s/streamed-list-objects", httpAddr, createStoreResponse.Id), listObjectsPayload)
	require.NoError(t, err, "Failed to construct request")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", test.authHeader)
	res, err = retryClient.Do(req)
	require.Equal(t, test.expectedStatusCode, res.StatusCode)
	require.NoError(t, err, "Failed to execute streaming request")
	defer res.Body.Close()
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
	require.Equal(t, test.expectedStatusCode, res.StatusCode)
	defer res.Body.Close()
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
	cfg := MustDefaultConfigWithRandomPorts()
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

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil, true)

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
			name: "Good_Origin",
			args: args{
				origin: "http://localhost",
				header: "Authorization, X-Custom-Header",
			},
			want: want{
				origin: "http://localhost",
				header: "Authorization, X-Custom-Header",
			},
		},
		{
			name: "Bad_Origin",
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
			name: "Bad_Header",
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
	oidcServerPort, oidcServerPortReleaser := TCPRandomPort()
	localOIDCServerURL := fmt.Sprintf("http://localhost:%d", oidcServerPort)

	cfg := MustDefaultConfigWithRandomPorts()
	cfg.Authn.Method = "oidc"
	cfg.Authn.AuthnOIDCConfig = &serverconfig.AuthnOIDCConfig{
		Audience: "openfga.dev",
		Issuer:   localOIDCServerURL,
	}

	oidcServerPortReleaser()

	trustedIssuerServer, err := mocks.NewMockOidcServer(localOIDCServerURL)
	require.NoError(t, err)

	trustedToken, err := trustedIssuerServer.GetToken("openfga.dev", "some-user")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil, true)

	tests := []authTest{
		{
			_name:      "Header_with_invalid_token_fails",
			authHeader: "Bearer incorrecttoken",
			expectedErrorResponse: &serverErrors.ErrorResponse{
				Code:    "auth_failed_invalid_bearer_token",
				Message: "invalid bearer token",
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
	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			tryGetStores(t, test, cfg.HTTP.Addr, retryClient)
		})

		t.Run(test._name+"/streaming", func(t *testing.T) {
			tryStreamingListObjects(t, test, cfg.HTTP.Addr, retryClient, trustedToken)
		})
	}
}

func TestHTTPServingTLS(t *testing.T) {
	t.Run("enable_HTTP_TLS_is_false,_even_with_keys_set,_will_serve_plaintext", func(t *testing.T) {
		certsAndKeys := createCertsAndKeys(t)
		defer certsAndKeys.Clean()

		cfg := MustDefaultConfigWithRandomPorts()
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

		testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil, true)
	})

	t.Run("enable_HTTP_TLS_is_true_will_serve_HTTP_TLS", func(t *testing.T) {
		certsAndKeys := createCertsAndKeys(t)
		defer certsAndKeys.Clean()

		cfg := MustDefaultConfigWithRandomPorts()
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
		client.HTTPClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: certPool,
			},
		}

		_, err := client.Get(fmt.Sprintf("https://%s/healthz", cfg.HTTP.Addr))
		require.NoError(t, err)
	})
}

func TestGRPCServingTLS(t *testing.T) {
	t.Run("enable_grpc_TLS_is_false,_even_with_keys_set,_will_serve_plaintext", func(t *testing.T) {
		certsAndKeys := createCertsAndKeys(t)
		defer certsAndKeys.Clean()

		cfg := MustDefaultConfigWithRandomPorts()
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

		testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil, false)
	})

	t.Run("enable_grpc_TLS_is_true_will_serve_grpc_TLS", func(t *testing.T) {
		certsAndKeys := createCertsAndKeys(t)
		defer certsAndKeys.Clean()

		cfg := MustDefaultConfigWithRandomPorts()
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

		testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, creds, false)
	})
}

func TestServerMetricsReporting(t *testing.T) {
	t.Run("mysql", func(t *testing.T) {
		testServerMetricsReporting(t, "mysql")
	})
	t.Run("postgres", func(t *testing.T) {
		testServerMetricsReporting(t, "postgres")
	})
}

func testServerMetricsReporting(t *testing.T, engine string) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, engine)

	cfg := MustDefaultConfigWithRandomPorts()
	cfg.Datastore.Engine = engine
	cfg.Datastore.URI = testDatastore.GetConnectionURI(true)
	cfg.Datastore.Metrics.Enabled = true
	cfg.Metrics.Enabled = true
	cfg.Metrics.EnableRPCHistograms = true
	metricsPort, metricsPortReleaser := TCPRandomPort()
	metricsPortReleaser()

	cfg.Metrics.Addr = fmt.Sprintf("0.0.0.0:%d", metricsPort)

	cfg.MaxConcurrentReadsForCheck = 30
	cfg.MaxConcurrentReadsForListObjects = 30

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil, false)

	conn, err := grpc.Dial(cfg.GRPC.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

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
								{Type: "user"},
							},
						},
					},
				},
			},
		},
		SchemaVersion: typesystem.SchemaVersion1_1,
	})
	require.NoError(t, err)

	_, err = client.Write(ctx, &openfgav1.WriteRequest{
		StoreId: storeID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{Object: "document:1", Relation: "viewer", User: "user:jon"},
				{Object: "document:2", Relation: "editor", User: "user:jon"},
				{Object: "document:2", Relation: "allowed", User: "user:jon"},
			},
		},
	})
	require.NoError(t, err)

	checkResp, err := client.Check(ctx, &openfgav1.CheckRequest{
		StoreId:  storeID,
		TupleKey: tuple.NewCheckRequestTupleKey("document:1", "viewer", "user:jon"),
	})
	require.NoError(t, err)
	require.True(t, checkResp.GetAllowed())

	listObjectsResp, err := client.ListObjects(ctx, &openfgav1.ListObjectsRequest{
		StoreId:  storeID,
		Type:     "document",
		Relation: "viewer",
		User:     "user:jon",
	})
	require.NoError(t, err)
	require.Len(t, listObjectsResp.GetObjects(), 2)
	require.Contains(t, listObjectsResp.GetObjects(), "document:1")
	require.Contains(t, listObjectsResp.GetObjects(), "document:2")

	resp, err := retryablehttp.Get(fmt.Sprintf("http://%s/metrics", cfg.Metrics.Addr))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()

	resBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	stringBody := string(resBody)
	require.Contains(t, stringBody, "datastore_query_count")
	require.Contains(t, stringBody, "request_duration_by_query_count_ms")
	require.Contains(t, stringBody, "grpc_server_handling_seconds")
	require.Contains(t, stringBody, "datastore_bounded_read_delay_ms")
	require.Contains(t, stringBody, "list_objects_further_eval_required_count")
	require.Contains(t, stringBody, "list_objects_no_further_eval_required_count")
	require.Contains(t, stringBody, "go_sql_idle_connections")
	require.Contains(t, stringBody, "condition_evaluation_cost")
}

func TestHTTPServerDisabled(t *testing.T) {
	cfg := MustDefaultConfigWithRandomPorts()
	cfg.HTTP.Enabled = false

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, "", nil, false)

	_, err := http.Get(fmt.Sprintf("http://%s/healthz", cfg.HTTP.Addr))
	require.Error(t, err)
	require.ErrorContains(t, err, "connect: connection refused")
}

func TestHTTPServerEnabled(t *testing.T) {
	cfg := MustDefaultConfigWithRandomPorts()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil, true)
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

	val = res.Get("properties.datastore.properties.maxOpenConns.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.Datastore.MaxOpenConns)

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

	val = res.Get("properties.experimentals.default")
	require.True(t, val.Exists())
	require.Equal(t, len(val.Array()), len(cfg.Experimentals))

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

	val = res.Get("properties.checkQueryCache.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.CheckQueryCache.Enabled)

	val = res.Get("properties.checkQueryCache.properties.limit.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.CheckQueryCache.Limit)

	val = res.Get("properties.checkQueryCache.properties.ttl.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.CheckQueryCache.TTL.String())

	val = res.Get("properties.requestDurationDatastoreQueryCountBuckets.default")
	require.True(t, val.Exists())
	require.Equal(t, len(val.Array()), len(cfg.RequestDurationDatastoreQueryCountBuckets))
	for index, arrayVal := range val.Array() {
		require.Equal(t, arrayVal.String(), cfg.RequestDurationDatastoreQueryCountBuckets[index])
	}
}

func TestRunCommandNoConfigDefaultValues(t *testing.T) {
	util.PrepareTempConfigDir(t)
	runCmd := NewRunCommand()
	runCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "", viper.GetString(datastoreURIFlag))
		require.False(t, viper.GetBool("check-query-cache-enabled"))
		require.Equal(t, uint32(0), viper.GetUint32("check-query-cache-limit"))
		require.Equal(t, 0*time.Second, viper.GetDuration("check-query-cache-ttl"))
		require.Equal(t, []int{}, viper.GetIntSlice("request-duration-datastore-query-count-buckets"))
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
    limit: 100
    TTL: 5s
requestDurationDatastoreQueryCountBuckets: [33,44]
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
	require.Equal(t, uint32(100), cfg.CheckQueryCache.Limit)
	require.Equal(t, 5*time.Second, cfg.CheckQueryCache.TTL)
	require.Equal(t, []string{"33", "44"}, cfg.RequestDurationDatastoreQueryCountBuckets)
}

func TestRunCommandConfigIsMerged(t *testing.T) {
	config := `datastore:
    engine: postgres
`
	util.PrepareTempConfigFile(t, config)

	t.Setenv("OPENFGA_DATASTORE_URI", "postgres://postgres:PASS2@127.0.0.1:5432/postgres")
	t.Setenv("OPENFGA_MAX_TYPES_PER_AUTHORIZATION_MODEL", "1")
	t.Setenv("OPENFGA_CHECK_QUERY_CACHE_ENABLED", "true")
	t.Setenv("OPENFGA_CHECK_QUERY_CACHE_LIMIT", "33")
	t.Setenv("OPENFGA_CHECK_QUERY_CACHE_TTL", "5s")
	t.Setenv("OPENFGA_REQUEST_DURATION_DATASTORE_QUERY_COUNT_BUCKETS", "33 44")

	runCmd := NewRunCommand()
	runCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "postgres", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "postgres://postgres:PASS2@127.0.0.1:5432/postgres", viper.GetString(datastoreURIFlag))
		require.Equal(t, "1", viper.GetString("max-types-per-authorization-model"))
		require.True(t, viper.GetBool("check-query-cache-enabled"))
		require.Equal(t, uint32(33), viper.GetUint32("check-query-cache-limit"))
		require.Equal(t, 5*time.Second, viper.GetDuration("check-query-cache-ttl"))

		require.Equal(t, []string{"33", "44"}, viper.GetStringSlice("request-duration-datastore-query-count-buckets"))
		return nil
	}

	rootCmd := cmd.NewRootCommand()
	rootCmd.AddCommand(runCmd)
	rootCmd.SetArgs([]string{"run"})
	require.NoError(t, rootCmd.Execute())
}

func TestHTTPHeaders(t *testing.T) {
	t.Parallel()
	cfg := MustDefaultConfigWithRandomPorts()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil, true)

	conn, err := grpc.Dial(cfg.GRPC.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
	})

	client := openfgav1.NewOpenFGAServiceClient(conn)

	createStoreResp, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)

	storeID := createStoreResp.GetId()

	writeModelResp, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:       storeID,
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type document
  relations
	define viewer: [user]`).TypeDefinitions,
	})
	require.NoError(t, err)

	authorizationModelID := writeModelResp.GetAuthorizationModelId()

	httpClient := retryablehttp.NewClient()

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
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			req, err := retryablehttp.NewRequest(test.httpVerb, test.httpPath, strings.NewReader(test.httpJSONBody))
			require.NoError(t, err, "Failed to construct request")

			httpResponse, err := httpClient.Do(req)
			require.NoError(t, err)

			// These are set in the server RPCs
			require.Len(t, httpResponse.Header[server.AuthorizationModelIDHeader], 1)
			require.Equal(t, authorizationModelID, httpResponse.Header[server.AuthorizationModelIDHeader][0])

			// These are set in middlewares
			require.Len(t, httpResponse.Header[storeid.StoreIDHeader], 1)
			require.Equal(t, storeID, httpResponse.Header[storeid.StoreIDHeader][0])
			require.Len(t, httpResponse.Header[requestid.RequestIDHeader], 1)
			require.NotEmpty(t, httpResponse.Header[requestid.RequestIDHeader][0])
		})
	}
}
