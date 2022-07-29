package service

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"path"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/retryablehttp"
	"github.com/openfga/openfga/server/authn/mocks"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
)

func init() {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "../../..")
	err := os.Chdir(dir)
	if err != nil {
		panic(err)
	}
}

const (
	openFGAServerAddr = "http://localhost:8080"
	openFGAGRPCAddr   = "localhost:8081"
)

func ensureServiceUp(t *testing.T, transportCredentials credentials.TransportCredentials) {
	t.Helper()

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
		openFGAGRPCAddr,
		dialOpts...,
	)
	require.NoError(t, err)
	defer conn.Close()

	client := healthv1pb.NewHealthClient(conn)

	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 3 * time.Second

	err = backoff.Retry(func() error {
		resp, err := client.Check(timeoutCtx, &healthv1pb.HealthCheckRequest{
			Service: openfgapb.OpenFGAService_ServiceDesc.ServiceName,
		})
		if err != nil {
			return err
		}

		if resp.GetStatus() != healthv1pb.HealthCheckResponse_SERVING {
			return fmt.Errorf("not serving")
		}

		return nil
	}, policy)
	require.NoError(t, err)
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
	_name         string
	authHeader    string
	expectedError string
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
	retryClient := retryablehttp.New().StandardClient()

	config, err := GetServiceConfig()
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

	ensureServiceUp(t, nil)

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
			req, err := http.NewRequest("POST", fmt.Sprintf("%s/stores", openFGAServerAddr), payload)
			require.NoError(t, err, "Failed to construct request")
			req.Header.Set("content-type", "application/json")
			req.Header.Set("authorization", test.authHeader)

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

func TestHTTPServerWithCORS(t *testing.T) {

	config, err := GetServiceConfig()
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

	ensureServiceUp(t, nil)

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

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := strings.NewReader(`{"name": "some-store-name"}`)
			req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/stores", openFGAServerAddr), payload)
			require.NoError(t, err, "Failed to construct request")
			req.Header.Set("content-type", "application/json")
			req.Header.Set("Origin", test.args.origin)
			req.Header.Set("Access-Control-Request-Method", "OPTIONS")
			req.Header.Set("Access-Control-Request-Headers", test.args.header)

			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err, "Failed to execute request")
			defer res.Body.Close()

			origin := res.Header.Get("Access-Control-Allow-Origin")
			acceptedHeader := res.Header.Get("Access-Control-Allow-Headers")
			require.Equal(t, test.want.origin, origin)

			require.Equal(t, test.want.header, acceptedHeader)

			_, err = ioutil.ReadAll(res.Body)
			require.NoError(t, err, "Failed to read response")
		})
	}

	cancel()
	require.NoError(t, g.Wait())
	require.NoError(t, service.Close(ctx))

}

func TestBuildServerWithOIDCAuthentication(t *testing.T) {
	retryClient := retryablehttp.New().StandardClient()

	const localOIDCServerURL = "http://localhost:8083"

	config, err := GetServiceConfig()
	require.NoError(t, err)

	config.Authn.Method = "oidc"
	config.Authn.AuthnOIDCConfig = &AuthnOIDCConfig{
		Audience: openFGAServerAddr,
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

	ensureServiceUp(t, nil)

	trustedToken, err := trustedIssuerServer.GetToken(openFGAServerAddr, "some-user")
	require.NoError(t, err)

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
			req, err := http.NewRequest("POST", fmt.Sprintf("%s/stores", openFGAServerAddr), payload)
			require.NoError(t, err, "Failed to construct request")
			req.Header.Set("content-type", "application/json")
			req.Header.Set("authorization", test.authHeader)

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

func TestTLSFailureSettings(t *testing.T) {
	logger := logger.NewNoopLogger()

	t.Run("failing to set http cert path will not allow server to start", func(t *testing.T) {
		config, err := GetServiceConfig()
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

		config, err := GetServiceConfig()
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

		ensureServiceUp(t, nil)

		cancel()
		require.NoError(t, g.Wait())
		require.NoError(t, service.Close(ctx))
	})

	t.Run("enable HTTP TLS is true will serve HTTP TLS", func(t *testing.T) {
		certsAndKeys := createCertsAndKeys(t)
		defer certsAndKeys.Clean()

		config, err := GetServiceConfig()
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

		ensureServiceUp(t, nil)

		certPool := x509.NewCertPool()
		certPool.AddCert(certsAndKeys.caCert)
		client := retryablehttp.NewWithClient(http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs: certPool,
				},
			},
		})

		_, err = client.Get("https://localhost:8080/healthz")
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

		config, err := GetServiceConfig()
		require.NoError(t, err)

		config.GRPC.TLS = TLSConfig{
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

		ensureServiceUp(t, nil)

		cancel()
		require.NoError(t, g.Wait())
		require.NoError(t, service.Close(ctx))
	})

	t.Run("enable grpc TLS is true will serve grpc TLS", func(t *testing.T) {
		certsAndKeys := createCertsAndKeys(t)
		defer certsAndKeys.Clean()

		config, err := GetServiceConfig()
		require.NoError(t, err)

		config.GRPC.TLS = TLSConfig{
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

		certPool := x509.NewCertPool()
		certPool.AddCert(certsAndKeys.caCert)
		creds := credentials.NewClientTLSFromCert(certPool, "")

		ensureServiceUp(t, creds)

		cancel()
		require.NoError(t, g.Wait())
		require.NoError(t, service.Close(ctx))
	})
}

func TestHTTPServerDisabled(t *testing.T) {
	config, err := GetServiceConfig()
	require.NoError(t, err)

	config.HTTP.Enabled = false

	service, err := BuildService(config, logger.NewNoopLogger())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	g := new(errgroup.Group)
	g.Go(func() error {
		return service.Run(ctx)
	})

	ensureServiceUp(t, nil)

	_, err = http.Get("http://localhost:8080/healthz")
	require.Error(t, err)
	require.ErrorContains(t, err, "dial tcp [::1]:8080: connect: connection refused")

	cancel()
}

func TestHTTPServerEnabled(t *testing.T) {
	config, err := GetServiceConfig()
	require.NoError(t, err)

	config.HTTP.Enabled = true

	service, err := BuildService(config, logger.NewNoopLogger())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	g := new(errgroup.Group)
	g.Go(func() error {
		return service.Run(ctx)
	})

	ensureServiceUp(t, nil)

	resp, err := http.Get("http://localhost:8080/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	cancel()
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	jsonSchema, err := ioutil.ReadFile(".config-schema.json")
	require.NoError(t, err)

	res := gjson.ParseBytes(jsonSchema)

	val := res.Get("properties.datastore.properties.engine.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), config.Datastore.Engine)

	val = res.Get("properties.datastore.properties.maxCacheSize.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), config.Datastore.MaxCacheSize)

	val = res.Get("properties.grpc.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), config.GRPC.Enabled)

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
}
