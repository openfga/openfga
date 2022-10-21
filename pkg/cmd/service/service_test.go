package service

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
	"math/big"
	"net/http"
	"os"
	"path"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-errors/errors"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/server/authn/mocks"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
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

func ensureServiceUp(t *testing.T, s *service, transportCredentials credentials.TransportCredentials, httpHealthCheck bool) {
	t.Helper()

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	creds := insecure.NewCredentials()
	if transportCredentials != nil {
		creds = transportCredentials
	}

	dialOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: grpcbackoff.DefaultConfig}),
	}

	conn, err := grpc.DialContext(
		timeoutCtx,
		fmt.Sprintf("localhost:%d", s.GetGRPCAddrPort().Port()),
		dialOpts...,
	)
	require.NoError(t, err)
	defer conn.Close()

	client := healthv1pb.NewHealthClient(conn)

	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 10 * time.Second

	err = backoff.Retry(func() error {
		resp, err := client.Check(timeoutCtx, &healthv1pb.HealthCheckRequest{
			Service: openfgapb.OpenFGAService_ServiceDesc.ServiceName,
		})
		if err != nil {
			return err
		}

		if resp.GetStatus() != healthv1pb.HealthCheckResponse_SERVING {
			return errors.Errorf("not serving")
		}

		return nil
	}, policy)
	require.NoError(t, err)

	if httpHealthCheck {
		_, err = retryablehttp.NewClient().Get(fmt.Sprintf("http://localhost:%d/healthz", s.GetHTTPAddrPort().Port()))
		require.NoError(t, err)
	}
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

func TestBuildServiceWithNoAuth(t *testing.T) {
	config, err := GetServiceConfig()
	require.NoError(t, err)

	service, err := BuildService(config, logger.NewNoopLogger())
	require.NoError(t, err, "Failed to build server and/or datastore")
	service.Close(context.Background())
}

func TestBuildServiceWithPresharedKeyAuthenticationFailsIfZeroKeys(t *testing.T) {
	config, err := GetServiceConfig()
	require.NoError(t, err)

	config.Authn.Method = "preshared"
	config.Authn.AuthnPresharedKeyConfig = &AuthnPresharedKeyConfig{}

	_, err = BuildService(config, logger.NewNoopLogger())
	require.EqualError(t, err, "failed to initialize authenticator: invalid auth configuration, please specify at least one key")
}

func TestBuildServiceWithPresharedKeyAuthentication(t *testing.T) {
	config, err := DefaultConfigWithRandomPorts()
	require.NoError(t, err)

	config.Authn.Method = "preshared"
	config.Authn.AuthnPresharedKeyConfig = &AuthnPresharedKeyConfig{
		Keys: []string{"KEYONE", "KEYTWO"},
	}

	service, err := BuildService(config, logger.NewNoopLogger())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	g := new(errgroup.Group)
	g.Go(func() error {
		return service.Run(ctx)
	})

	ensureServiceUp(t, service, nil, true)

	tests := []authTest{{
		_name:      "Header with incorrect key fails",
		authHeader: "Bearer incorrectkey",
		expectedErrorResponse: &serverErrors.ErrorResponse{
			Code:    "unauthenticated",
			Message: "unauthenticated",
		},
		expectedStatusCode: 401,
	}, {
		_name:      "Missing header fails",
		authHeader: "",
		expectedErrorResponse: &serverErrors.ErrorResponse{
			Code:    "bearer_token_missing",
			Message: "missing bearer token",
		},
		expectedStatusCode: 401,
	}, {
		_name:              "Correct key one succeeds",
		authHeader:         fmt.Sprintf("Bearer %s", config.Authn.AuthnPresharedKeyConfig.Keys[0]),
		expectedStatusCode: 200,
	}, {
		_name:              "Correct key two succeeds",
		authHeader:         fmt.Sprintf("Bearer %s", config.Authn.AuthnPresharedKeyConfig.Keys[1]),
		expectedStatusCode: 200,
	}}

	retryClient := retryablehttp.NewClient()
	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			tryGetStores(t, test, service, retryClient)
		})

		t.Run(test._name+"/streaming", func(t *testing.T) {
			tryStreamingListObjects(t, test, service, retryClient, config.Authn.AuthnPresharedKeyConfig.Keys[0])
		})
	}

	cancel()
	require.NoError(t, service.Close(ctx))
	require.NoError(t, g.Wait())
}

func tryStreamingListObjects(t *testing.T, test authTest, service *service, retryClient *retryablehttp.Client, validToken string) {
	// create a store
	createStorePayload := strings.NewReader(`{"name": "some-store-name"}`)
	req, err := retryablehttp.NewRequest("POST", fmt.Sprintf("http://localhost:%d/stores", service.GetHTTPAddrPort().Port()), createStorePayload)
	require.NoError(t, err, "Failed to construct create store request")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", fmt.Sprintf("Bearer %s", validToken))
	res, err := retryClient.Do(req)
	require.NoError(t, err, "Failed to execute create store request")
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err, "Failed to read create store response")
	var createStoreResponse openfgapb.CreateStoreResponse
	err = protojson.Unmarshal(body, &createStoreResponse)
	require.NoError(t, err, "Failed to unmarshal create store response")

	// create an authorization model
	authModelPayload := strings.NewReader(`{"type_definitions":[{"type":"document","relations":{"owner":{"this":{}}}}]}`)
	req, err = retryablehttp.NewRequest("POST", fmt.Sprintf("http://localhost:%d/stores/%s/authorization-models", service.GetHTTPAddrPort().Port(), createStoreResponse.Id), authModelPayload)
	require.NoError(t, err, "Failed to construct create authorization model request")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", fmt.Sprintf("Bearer %s", validToken))
	_, err = retryClient.Do(req)
	require.NoError(t, err, "Failed to execute create authorization model request")

	// call one streaming endpoint
	listObjectsPayload := strings.NewReader(`{"type": "document", "user": "anne", "relation": "owner"}`)
	req, err = retryablehttp.NewRequest("POST", fmt.Sprintf("http://localhost:%d/stores/%s/streamed-list-objects", service.GetHTTPAddrPort().Port(), createStoreResponse.Id), listObjectsPayload)
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

func tryGetStores(t *testing.T, test authTest, service *service, retryClient *retryablehttp.Client) {
	req, err := retryablehttp.NewRequest("GET", fmt.Sprintf("http://localhost:%d/stores", service.GetHTTPAddrPort().Port()), nil)
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
	config, err := DefaultConfigWithRandomPorts()
	require.NoError(t, err)

	config.Authn.Method = "preshared"
	config.Authn.AuthnPresharedKeyConfig = &AuthnPresharedKeyConfig{
		Keys: []string{"KEYONE", "KEYTWO"},
	}
	config.HTTP.CORSAllowedOrigins = []string{"http://openfga.dev", "http://localhost"}
	config.HTTP.CORSAllowedHeaders = []string{"Origin", "Accept", "Content-Type", "X-Requested-With", "Authorization", "X-Custom-Header"}

	ctx, cancel := context.WithCancel(context.Background())

	service, err := BuildService(config, logger.NewNoopLogger())
	require.NoError(t, err)

	g := new(errgroup.Group)
	g.Go(func() error {
		return service.Run(ctx)
	})

	ensureServiceUp(t, service, nil, true)

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
			name: "Good Origin",
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
			name: "Bad Origin",
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
			name: "Bad Header",
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
			req, err := retryablehttp.NewRequest("OPTIONS", fmt.Sprintf("http://localhost:%d/stores", service.GetHTTPAddrPort().Port()), payload)
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

	cancel()
	require.NoError(t, g.Wait())
	require.NoError(t, service.Close(ctx))

}

func TestBuildServerWithOIDCAuthentication(t *testing.T) {
	const localOIDCServerURL = "http://localhost:8083"

	config, err := DefaultConfigWithRandomPorts()
	require.NoError(t, err)

	config.Authn.Method = "oidc"
	config.Authn.AuthnOIDCConfig = &AuthnOIDCConfig{
		Audience: "openfga.dev",
		Issuer:   localOIDCServerURL,
	}

	trustedIssuerServer, err := mocks.NewMockOidcServer(localOIDCServerURL)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	service, err := BuildService(config, logger.NewNoopLogger())
	require.NoError(t, err)

	g := new(errgroup.Group)
	g.Go(func() error {
		return service.Run(ctx)
	})

	ensureServiceUp(t, service, nil, true)

	trustedToken, err := trustedIssuerServer.GetToken("openfga.dev", "some-user")
	require.NoError(t, err)

	tests := []authTest{{
		_name:      "Header with invalid token fails",
		authHeader: "Bearer incorrecttoken",
		expectedErrorResponse: &serverErrors.ErrorResponse{
			Code:    "auth_failed_invalid_bearer_token",
			Message: "invalid bearer token",
		},
		expectedStatusCode: 401,
	}, {
		_name:      "Missing header fails",
		authHeader: "",
		expectedErrorResponse: &serverErrors.ErrorResponse{
			Code:    "bearer_token_missing",
			Message: "missing bearer token",
		},
		expectedStatusCode: 401,
	}, {
		_name:              "Correct token succeeds",
		authHeader:         "Bearer " + trustedToken,
		expectedStatusCode: 200,
	}}

	retryClient := retryablehttp.NewClient()
	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			tryGetStores(t, test, service, retryClient)
		})

		t.Run(test._name+"/streaming", func(t *testing.T) {
			tryStreamingListObjects(t, test, service, retryClient, trustedToken)
		})
	}

	cancel()
	require.NoError(t, service.Close(ctx))
	require.NoError(t, g.Wait())
}

func TestTLSFailureSettings(t *testing.T) {
	logger := logger.NewNoopLogger()

	t.Run("failing to set http cert path will not allow server to start", func(t *testing.T) {
		config, err := DefaultConfigWithRandomPorts()
		require.NoError(t, err)

		config.HTTP.TLS = TLSConfig{
			Enabled: true,
			KeyPath: "some/path",
		}

		_, err = BuildService(config, logger)
		require.ErrorIs(t, err, ErrInvalidHTTPTLSConfig)
	})

	t.Run("failing to set grpc cert path will not allow server to start", func(t *testing.T) {
		config, err := GetServiceConfig()
		require.NoError(t, err)

		config.GRPC.TLS = TLSConfig{
			Enabled: true,
			KeyPath: "some/path",
		}

		_, err = BuildService(config, logger)
		require.ErrorIs(t, err, ErrInvalidGRPCTLSConfig)
	})

	t.Run("failing to set http key path will not allow server to start", func(t *testing.T) {
		config, err := GetServiceConfig()
		require.NoError(t, err)

		config.HTTP.TLS = TLSConfig{
			Enabled:  true,
			CertPath: "some/path",
		}

		_, err = BuildService(config, logger)
		require.ErrorIs(t, err, ErrInvalidHTTPTLSConfig)
	})

	t.Run("failing to set grpc key path will not allow server to start", func(t *testing.T) {
		config, err := GetServiceConfig()
		require.NoError(t, err)

		config.GRPC.TLS = TLSConfig{
			Enabled:  true,
			CertPath: "some/path",
		}

		_, err = BuildService(config, logger)
		require.ErrorIs(t, err, ErrInvalidGRPCTLSConfig)
	})
}

func TestHTTPServingTLS(t *testing.T) {
	logger := logger.NewNoopLogger()

	t.Run("enable HTTP TLS is false, even with keys set, will serve plaintext", func(t *testing.T) {
		certsAndKeys := createCertsAndKeys(t)
		defer certsAndKeys.Clean()

		config, err := DefaultConfigWithRandomPorts()
		require.NoError(t, err)

		config.HTTP.TLS = TLSConfig{
			CertPath: certsAndKeys.serverCertFile,
			KeyPath:  certsAndKeys.serverKeyFile,
		}

		service, err := BuildService(config, logger)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		g := new(errgroup.Group)
		g.Go(func() error {
			return service.Run(ctx)
		})

		ensureServiceUp(t, service, nil, true)

		cancel()
		require.NoError(t, g.Wait())
		require.NoError(t, service.Close(ctx))
	})

	t.Run("enable HTTP TLS is true will serve HTTP TLS", func(t *testing.T) {
		certsAndKeys := createCertsAndKeys(t)
		defer certsAndKeys.Clean()

		config, err := DefaultConfigWithRandomPorts()
		require.NoError(t, err)

		config.HTTP.TLS = TLSConfig{
			Enabled:  true,
			CertPath: certsAndKeys.serverCertFile,
			KeyPath:  certsAndKeys.serverKeyFile,
		}

		service, err := BuildService(config, logger)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		g := new(errgroup.Group)
		g.Go(func() error {
			return service.Run(ctx)
		})

		ensureServiceUp(t, service, nil, false)

		certPool := x509.NewCertPool()
		certPool.AddCert(certsAndKeys.caCert)
		client := retryablehttp.NewClient()
		client.HTTPClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: certPool,
			},
		}

		_, err = client.Get(fmt.Sprintf("https://localhost:%d/healthz", service.GetHTTPAddrPort().Port()))
		require.NoError(t, err)

		cancel()
		require.NoError(t, g.Wait())
		require.NoError(t, service.Close(ctx))
	})

}

func TestGRPCServingTLS(t *testing.T) {
	logger := logger.NewNoopLogger()

	t.Run("enable grpc TLS is false, even with keys set, will serve plaintext", func(t *testing.T) {
		certsAndKeys := createCertsAndKeys(t)
		defer certsAndKeys.Clean()

		config, err := DefaultConfigWithRandomPorts()
		require.NoError(t, err)

		config.GRPC.TLS = TLSConfig{
			CertPath: certsAndKeys.serverCertFile,
			KeyPath:  certsAndKeys.serverKeyFile,
		}
		config.HTTP.Enabled = false

		service, err := BuildService(config, logger)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		g := new(errgroup.Group)
		g.Go(func() error {
			return service.Run(ctx)
		})

		ensureServiceUp(t, service, nil, false)

		cancel()
		require.NoError(t, service.Close(ctx))
		require.NoError(t, g.Wait())
	})

	t.Run("enable grpc TLS is true will serve grpc TLS", func(t *testing.T) {
		certsAndKeys := createCertsAndKeys(t)
		defer certsAndKeys.Clean()

		config, err := DefaultConfigWithRandomPorts()
		require.NoError(t, err)

		config.GRPC.TLS = TLSConfig{
			Enabled:  true,
			CertPath: certsAndKeys.serverCertFile,
			KeyPath:  certsAndKeys.serverKeyFile,
		}
		config.HTTP.Enabled = false

		service, err := BuildService(config, logger)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		g := new(errgroup.Group)
		g.Go(func() error {
			return service.Run(ctx)
		})

		certPool := x509.NewCertPool()
		certPool.AddCert(certsAndKeys.caCert)
		creds := credentials.NewClientTLSFromCert(certPool, "")

		ensureServiceUp(t, service, creds, false)

		cancel()
		require.NoError(t, service.Close(ctx))
		require.NoError(t, g.Wait())
	})
}

func TestHTTPServerDisabled(t *testing.T) {
	config, err := DefaultConfigWithRandomPorts()
	require.NoError(t, err)

	config.HTTP.Enabled = false

	service, err := BuildService(config, logger.NewNoopLogger())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	g := new(errgroup.Group)
	g.Go(func() error {
		return service.Run(ctx)
	})

	ensureServiceUp(t, service, nil, false)

	_, err = http.Get("http://localhost:8080/healthz")
	require.Error(t, err)
	require.ErrorContains(t, err, "dial tcp [::1]:8080: connect: connection refused")

	cancel()
}

func TestHTTPServerEnabled(t *testing.T) {
	config, err := DefaultConfigWithRandomPorts()
	require.NoError(t, err)

	config.HTTP.Enabled = true

	service, err := BuildService(config, logger.NewNoopLogger())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	g := new(errgroup.Group)
	g.Go(func() error {
		return service.Run(ctx)
	})

	ensureServiceUp(t, service, nil, true)

	cancel()
}

func TestDefaultConfig(t *testing.T) {
	config, err := GetServiceConfig()
	require.NoError(t, err)

	jsonSchema, err := os.ReadFile(".config-schema.json")
	require.NoError(t, err)

	res := gjson.ParseBytes(jsonSchema)

	val := res.Get("properties.datastore.properties.engine.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), config.Datastore.Engine)

	val = res.Get("properties.datastore.properties.maxCacheSize.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), config.Datastore.MaxCacheSize)

	val = res.Get("properties.grpc.properties.addr.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), config.GRPC.Addr)

	val = res.Get("properties.http.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), config.HTTP.Enabled)

	val = res.Get("properties.http.properties.addr.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), config.HTTP.Addr)

	val = res.Get("properties.playground.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), config.Playground.Enabled)

	val = res.Get("properties.playground.properties.port.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), config.Playground.Port)

	val = res.Get("properties.profiler.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), config.Profiler.Enabled)

	val = res.Get("properties.profiler.properties.addr.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), config.Profiler.Addr)

	val = res.Get("properties.authn.properties.method.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), config.Authn.Method)

	val = res.Get("properties.log.properties.format.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), config.Log.Format)

	val = res.Get("properties.maxTuplesPerWrite.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), config.MaxTuplesPerWrite)

	val = res.Get("properties.maxTypesPerAuthorizationModel.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), config.MaxTypesPerAuthorizationModel)

	val = res.Get("properties.changelogHorizonOffset.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), config.ChangelogHorizonOffset)

	val = res.Get("properties.resolveNodeLimit.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), config.ResolveNodeLimit)

	val = res.Get("properties.grpc.properties.tls.$ref")
	require.True(t, val.Exists())
	require.Equal(t, "#/definitions/tls", val.String())

	val = res.Get("properties.http.properties.tls.$ref")
	require.True(t, val.Exists())
	require.Equal(t, "#/definitions/tls", val.String())

	val = res.Get("definitions.tls.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), config.GRPC.TLS.Enabled)
	require.Equal(t, val.Bool(), config.HTTP.TLS.Enabled)

	val = res.Get("properties.listObjectsDeadline.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), config.ListObjectsDeadline.String())

	val = res.Get("properties.listObjectsMaxResults.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), config.ListObjectsMaxResults)
}
