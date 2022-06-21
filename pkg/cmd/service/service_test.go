package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
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
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	grpcTLSEnabledEnvVar  = "OPENFGA_GRPC_TLS_ENABLED"
	grpcTLSCertPathEnvVar = "OPENFGA_GRPC_TLS_CERT_PATH"
	grpcTLSKeyPathEnvVar  = "OPENFGA_GRPC_TLS_KEY_PATH"
	httpTLSEnabledEnvVar  = "OPENFGA_HTTP_TLS_ENABLED"
	httpTLSCertPathEnvVar = "OPENFGA_HTTP_TLS_CERT_PATH"
	httpTLSKeyPathEnvVar  = "OPENFGA_HTTP_TLS_KEY_PATH"

	caCert = `-----BEGIN CERTIFICATE-----
MIIDpzCCAo+gAwIBAgIUayZ6IiyldzKDZIA42b1ByVu4Hd8wDQYJKoZIhvcNAQEL
BQAwWjELMAkGA1UEBhMCSlAxEDAOBgNVBAgTB0Z1a3Vva2ExEDAOBgNVBAcTB0Z1
a3Vva2ExEzARBgNVBAoTCmpvYi13b3JrZXIxEjAQBgNVBAMTCWxvY2FsaG9zdDAe
Fw0yMjA2MDYyMzQzMDBaFw0yNzA2MDUyMzQzMDBaMFoxCzAJBgNVBAYTAkpQMRAw
DgYDVQQIEwdGdWt1b2thMRAwDgYDVQQHEwdGdWt1b2thMRMwEQYDVQQKEwpqb2It
d29ya2VyMRIwEAYDVQQDEwlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IB
DwAwggEKAoIBAQDCgzzKv71AHNqdWA12wDlWY2zdlQoW0Flt+GyD9itEincSdllW
Bw9m5RvTZjScAZS0/veJ/ARttmvRRUcFTiL88SZl68heVXQbzGM2ks3mcnpJdg3T
Alq61h4gqSewYzgN9UTx7ftCc5ga5DEuzu8Sq//KKzSh08/7/ToXVYWxyiW1MQMh
DRB+l9OyDYP9sIQG0kiMgcSfsUOmy3BON3dILZ1W4Y7kVK9K4ES03LkAi98daIHb
MkBtJIbgijoOeOTf6R1zaS8vdqXvDvyqtg/lYOf2UdxwAoHhhYerA3+hzAnhptb/
WF3fKB3Yv7iXNCP00EhEvPczLrKkDYoErXD3AgMBAAGjZTBjMA4GA1UdDwEB/wQE
AwIBBjAPBgNVHRMBAf8EBTADAQH/MB0GA1UdDgQWBBTncJnA3mUMIQaMmzdgnZBI
+19t3zAhBgNVHREEGjAYhwR/AAABhxAAAAAAAAAAAAAAAAAAAAABMA0GCSqGSIb3
DQEBCwUAA4IBAQCeTdZwmfMhxG8HT7JipFjy5ZMM0Aj1qbdToy7FBuyJpLUuyMSp
pGkkpnvTcvEDtKsPtBY/tBTVRSq6y4bq8Wz1LC0qUy2bKXF1LnUv1DwSUC0Vl4IH
rgqsK8SGqVCXtTpSOtZNwi2hHsQy5r2cHaRXGa3D32qCav8HTMJg1VXphRq++QF/
AUcbaAeT+lg1swidlYf/ZnAlp8QE/pbbCbA0K2Kj8DQrKKeVPsILLOWZac66nhGm
GPUWzKp4A18yOcOaCL7XkhcWjIuIrTmMrnwhupSpCTJfPQ1yCtBHJuC72sOMt1Ps
5MMtvl22bRz2B/wGwcgsJ0tv/PkV5YDJY2MG
-----END CERTIFICATE-----`

	serverCert = `-----BEGIN CERTIFICATE-----
MIID0DCCArigAwIBAgIUMVe743fpJExbqklNX4ln2UOZ31wwDQYJKoZIhvcNAQEL
BQAwWjELMAkGA1UEBhMCSlAxEDAOBgNVBAgTB0Z1a3Vva2ExEDAOBgNVBAcTB0Z1
a3Vva2ExEzARBgNVBAoTCmpvYi13b3JrZXIxEjAQBgNVBAMTCWxvY2FsaG9zdDAe
Fw0yMjA2MDYyMzQzMDBaFw0yMzA2MDYyMzQzMDBaMFoxCzAJBgNVBAYTAkpQMRAw
DgYDVQQIEwdGdWt1b2thMRAwDgYDVQQHEwdGdWt1b2thMRMwEQYDVQQKEwpqb2It
d29ya2VyMRIwEAYDVQQDEwlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IB
DwAwggEKAoIBAQCr5BWTiPVu4hUlA1CsbA+wdMifQvmSlzuxDTel9lUyqnGzozxM
9Qi3DgsBKxLFyDlsEuBDe//xt73DXOrscu6qvddLju7jjggl4Xcr+gcd20ZdqRbL
79Mgidaq45FtZCmPgIYGZZ0PPC/YoUGEJHf/RFtXzBemeTu+aqaHZukqHflhOoI9
zH4wEth5Of31BmIr2GLIfJNyNPor7yMnAI++3CrsmS+lY5W8rqi++NFh70qz5meH
xVmslRtGZRCRtAqMXxZUnYwB/4YZI94B4EOxRsFotbKGXvJjBiGn08TQssx+trux
ln7gLCFIcW9pEfNZ9DhblbRtZjZqHOKfxs87AgMBAAGjgY0wgYowDgYDVR0PAQH/
BAQDAgWgMB0GA1UdJQQWMBQGCCsGAQUFBwMBBggrBgEFBQcDAjAMBgNVHRMBAf8E
AjAAMB0GA1UdDgQWBBSSa2txmZzKI7OBeCHIiTDMEXAu3DAsBgNVHREEJTAjggls
b2NhbGhvc3SHBH8AAAGHEAAAAAAAAAAAAAAAAAAAAAEwDQYJKoZIhvcNAQELBQAD
ggEBAG8e8Ga2wUlk6UUN5sg4dooPZgnAipHjkj8AYud9AExNf4o+2KOmA8EqQyYs
r8WmgDSB853osJ1RyrApAedsXj4V1vgqZmn6XBUbKupGWsIEUSQ/+BcviSICFQNM
NKe3yRA7FUrTVL+fou8QHtYyYIswqi75+TcgO3SKbmhHPU6I9zecPCYQAqEWvOP9
zhAaWrtwr07/2nt7eED+B6qNNsYRu1/A9qSDMhPK69MZ6ZumF97ypmgvi7M34yCK
EUnpHGvoDPbj4ZbaqXPTSC7DX4btrkYEHVy+5P+tKyoht7bPc8gMTxjQwVARNFnO
iDLb3/R0pIqILWhmcG6xg0ymD4Y=
-----END CERTIFICATE-----`

	serverKey = `-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAq+QVk4j1buIVJQNQrGwPsHTIn0L5kpc7sQ03pfZVMqpxs6M8
TPUItw4LASsSxcg5bBLgQ3v/8be9w1zq7HLuqr3XS47u444IJeF3K/oHHdtGXakW
y+/TIInWquORbWQpj4CGBmWdDzwv2KFBhCR3/0RbV8wXpnk7vmqmh2bpKh35YTqC
Pcx+MBLYeTn99QZiK9hiyHyTcjT6K+8jJwCPvtwq7JkvpWOVvK6ovvjRYe9Ks+Zn
h8VZrJUbRmUQkbQKjF8WVJ2MAf+GGSPeAeBDsUbBaLWyhl7yYwYhp9PE0LLMfra7
sZZ+4CwhSHFvaRHzWfQ4W5W0bWY2ahzin8bPOwIDAQABAoIBAHexHAEe1mB+12Bt
nYhius4Rk/2qQmT8IBmabYyIKi1cmE4RNZUU7xugkLMgjjLgyHNj9XuoZcGoQ2A9
XGyHX3/PL5KyldAof635AOXDdZ8pqCbh7jjV57r5oFxgmEyG+ZWuViUwLpyEOYDs
UNW2G0TKEZziRfmq45olY45Xb7beAjsJRiG9YICKMwbnZEwGFthRFUj7RqhZCFau
ZWLtrBcvevyKMo3rQYs7P0/3q558gSbFfLu8KNYMoAaab3LS0jbjYUdL1m0cJZce
AWnASFNu7QlsvOvPOKKmEpWjO0VTkkSesQjdM7KMGYXU9fRtIB/Z9UtlJgTJf/5g
/u7J90ECgYEA0R0pdX1rFhbFoiXlR+ZQ3vpOYWIvhZXYdJYdeHdiG8JeM6It7P6S
zOTc1p5EgV3tNYQnZGeqgwox3Cq4jaQdzGtV5IHbY+h1nSA7rrvDG084Fht51FKF
aqnWGWL63hXJGx2LR8U/V/2dcD2zDAE6QKFp7q2RlsZgFuRpTnTt9BsCgYEA0m5g
wexYwUyzGqnRwarR7S50Qj9qtB5ShXxhYFxjFz8+oCS4d27Tb3MDkgkPNwX/t60n
VtDSuN4wv0/DYztNdMwoSdWxELrcLxv4VQR7VV0KcUAQQBm4A+DvJ4t4HI1m6mED
TOHJhohDqqoBvtpwC0gjOCzn2gh6lRSXsllTA2ECgYEAmw/pz1KKFt4Z/QvmwfMa
Ys3vUy0wmfksgf9SqSK1oGn32ofXUFbR2peW3pqLp/ZTUIzHfR+WBAeKQ312Tqm0
4wFwtrpISgR1OmdNeluG6PhMWbBUHcp3XknEFh0cc5RqBO5aeoTcXM4WccV+wFck
sApBeBhCzjAZzr/fCquQS6MCgYAxA5az9LojpBrfrgh2hLRK+5QGzkCrXZi5EOSZ
jktiYc/Te1ogL4c+IVsGi+eoWFRc0w8jsJY0i0Rte0W2elyrRNZphEWu8OdSbcBl
BRs5IefJwzNFyvfKp3ztCBZdCC6djyU2pizLkje4q8qmSrjoV9AkSIlkhq8OxHIl
D5s/YQKBgQCzflz985M6k1KT5EG/dopUUqgWJh1h07A275q9DswBl+XmDBAVdmiB
JgSb5y84gQNFxuvXT1ZXV/lQytB6ZpCxYS1gZybQAUGlvPKQGW5NoXqDucimPpvm
p2CIcm9oUH1iG9P/ELWz/it0RUbfy7GuBsUJn9MOdiO58uTiDBE7Kg==
-----END RSA PRIVATE KEY-----`
)

func createKeys(t *testing.T) string {
	certFile, err := ioutil.TempFile("", "server_cert")
	require.NoError(t, err, "error creating pem file")
	defer certFile.Close()

	_, err = certFile.Write([]byte(serverCert))
	require.NoError(t, err, "failed to write pem")

	keyFile, err := ioutil.TempFile("", "server_key")
	require.NoError(t, err, "error creating pem file")
	defer keyFile.Close()

	_, err = keyFile.Write([]byte(serverKey))
	require.NoError(t, err, "failed to write pem")

	require.NoError(t, os.Setenv(httpTLSEnabledEnvVar, "true"), "failed to set env var")
	require.NoError(t, os.Setenv(httpTLSCertPathEnvVar, certFile.Name()), "failed to set env var")
	require.NoError(t, os.Setenv(httpTLSKeyPathEnvVar, keyFile.Name()), "failed to set env var")

	require.NoError(t, os.Setenv(grpcTLSEnabledEnvVar, "true"), "failed to set env var")
	require.NoError(t, os.Setenv(grpcTLSCertPathEnvVar, certFile.Name()), "failed to set env var")
	require.NoError(t, os.Setenv(grpcTLSKeyPathEnvVar, keyFile.Name()), "failed to set env var")

	return certFile.Name()
}

type authTest struct {
	_name         string
	authHeader    string
	expectedError string
}

const (
	openFgaServerURL = "http://localhost:8080"
)

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
			req, err := http.NewRequest("POST", fmt.Sprintf("%s/stores", openFgaServerURL), payload)
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

func TestSettingCORS(t *testing.T) {
	os.Setenv("OPENFGA_AUTH_METHOD", "preshared")
	os.Setenv("OPENFGA_AUTH_PRESHARED_KEYS", "KEYONE,KEYTWO")
	os.Setenv("OPENFGA_CORS_ALLOWED_ORIGINS", "http://openfga.dev,http://localhost")
	os.Setenv("OPENFGA_CORS_ALLOWED_HEADERS", "Origin,Accept,Content-Type,X-Requested-With,Authorization,X-Custom-Header")

	corsAllowedOrigins := GetServiceConfig().CORSAllowedOrigins
	expectedCORSAllowedOrigins := []string{"http://openfga.dev", "http://localhost"}
	if !reflect.DeepEqual(corsAllowedOrigins, expectedCORSAllowedOrigins) {
		t.Fatalf("Unexpected CORSAllowedOrigin expected %v, actual %v", expectedCORSAllowedOrigins, corsAllowedOrigins)
	}
	ctx, cancel := context.WithCancel(context.Background())

	service, err := BuildService(GetServiceConfig(), logger.NewNoopLogger())
	require.NoError(t, err)

	g := new(errgroup.Group)
	g.Go(func() error {
		return service.Run(ctx)
	})

	ensureServiceUp(t)

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
			req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/stores", openFgaServerURL), payload)
			require.NoError(t, err, "Failed to construct request")
			req.Header.Set("content-type", "application/json")
			req.Header.Set("authorization", "Bearer KEYTWO")
			req.Header.Set("Origin", test.args.origin)
			req.Header.Set("Access-Control-Request-Method", "OPTIONS")
			req.Header.Set("Access-Control-Request-Headers", test.args.header)
			retryClient := http.Client{}
			res, err := retryClient.Do(req)
			require.NoError(t, err, "Failed to execute request")

			origin := res.Header.Get("Access-Control-Allow-Origin")
			acceptedHeader := res.Header.Get("Access-Control-Allow-Headers")
			require.Equal(t, test.want.origin, origin,
				"Want Access-Control-Allow-Origin to be %v actual %v", test.want.origin, origin)

			require.Equal(t, test.want.header, acceptedHeader,
				"Want Access-Control-Allow-Headers to be %v actual %v", test.want.header, acceptedHeader)

			defer res.Body.Close()
			_, err = ioutil.ReadAll(res.Body)
			require.NoError(t, err, "Failed to read response")

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
	os.Setenv("OPENFGA_AUTH_OIDC_AUDIENCE", openFgaServerURL)

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

	trustedToken, err := trustedIssuerServer.GetToken(openFgaServerURL, "some-user")
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
			req, err := http.NewRequest("POST", fmt.Sprintf("%s/stores", openFgaServerURL), payload)
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
			resp, err := http.Get(fmt.Sprintf("%s/healthz", openFgaServerURL))
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
		createKeys(t)
		require.NoError(t, os.Setenv(httpTLSEnabledEnvVar, "false"), "failed to set env var") // override
		defer os.Clearenv()

		ctx, cancel := context.WithCancel(context.Background())

		service, err := BuildService(GetServiceConfig(), logger)
		require.NoError(t, err)

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
		createKeys(t)
		defer os.Clearenv()

		ctx, cancel := context.WithCancel(context.Background())

		service, err := BuildService(GetServiceConfig(), logger)
		require.NoError(t, err)

		g := new(errgroup.Group)
		g.Go(func() error {
			return service.Run(ctx)
		})

		certPool := x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM([]byte(caCert)); !ok {
			t.Error("failed to add ca cert to pool")
		}

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
		createKeys(t)
		require.NoError(t, os.Setenv(grpcTLSEnabledEnvVar, "false"), "failed to set env var") // override
		defer os.Clearenv()

		service, err := BuildService(GetServiceConfig(), logger)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		g := new(errgroup.Group)
		g.Go(func() error {
			return service.Run(ctx)
		})

		opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
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
		certFile := createKeys(t)
		defer os.Clearenv()

		service, err := BuildService(GetServiceConfig(), logger)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		g := new(errgroup.Group)
		g.Go(func() error {
			return service.Run(ctx)
		})

		creds, err := credentials.NewClientTLSFromFile(certFile, "")
		require.NoError(t, err)

		opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
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
