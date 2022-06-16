package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/server/authentication/mocks"
	"github.com/stretchr/testify/require"
)

type authTest struct {
	_name         string
	authHeader    string
	expectedError string
}

const (
	openFgaServerURL = "http://localhost:8080"
)

func TestBuildServerWithNoAuth(t *testing.T) {
	service, err := buildService(logger.NewNoopLogger())
	defer service.Close(context.Background())

	require.NoError(t, err, "Failed to build server and/or datastore")
}

func TestBuildServerWithPresharedKeyAuthenticationFailsIfZeroKeys(t *testing.T) {
	os.Setenv("OPENFGA_AUTH_METHOD", "preshared")
	os.Setenv("OPENFGA_AUTH_PRESHARED_KEYS", "")

	_, err := buildService(logger.NewNoopLogger())
	require.EqualError(t, err, "failed to initialize authenticator: invalid auth configuration, please specify at least one key")
}

func TestBuildServerWithPresharedKeyAuthentication(t *testing.T) {
	os.Setenv("OPENFGA_AUTH_METHOD", "preshared")
	os.Setenv("OPENFGA_AUTH_PRESHARED_KEYS", "KEYONE,KEYTWO")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service, err := buildService(logger.NewNoopLogger())
	require.NoError(t, err, "Failed to build server and/or datastore")
	defer service.Close(ctx)

	go func() {
		_ = service.server.Run(ctx)
	}()

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
}

func TestBuildServerWithOidcAuthentication(t *testing.T) {
	const localOidcServerURL = "http://localhost:8083"
	os.Setenv("OPENFGA_AUTH_METHOD", "oidc")
	os.Setenv("OPENFGA_AUTH_OIDC_ISSUER", localOidcServerURL)
	os.Setenv("OPENFGA_AUTH_OIDC_AUDIENCE", openFgaServerURL)

	trustedIssuerServer, err := mocks.NewMockOidcServer(localOidcServerURL)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service, err := buildService(logger.NewNoopLogger())
	require.NoError(t, err)
	defer service.Close(ctx)

	go func() {
		_ = service.server.Run(ctx)
	}()

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
