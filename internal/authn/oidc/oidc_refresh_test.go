package oidc

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

func rsaPublicKeyToJWK(kid string, pub *rsa.PublicKey) map[string]string {
	return map[string]string{
		"kty": "RSA",
		"use": "sig",
		"alg": "RS256",
		"kid": kid,
		"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
	}
}

// jwksTestServer serves /.well-known/openid-configuration and a JWKS endpoint
// backed by a mutable key set. It counts hits to the JWKS endpoint so tests
// can assert on refresh behavior.
type jwksTestServer struct {
	mu       sync.Mutex
	keys     map[string]*rsa.PublicKey
	jwksHits int32
	server   *httptest.Server
}

func newJWKSTestServer(initial map[string]*rsa.PublicKey) *jwksTestServer {
	j := &jwksTestServer{keys: make(map[string]*rsa.PublicKey)}
	for k, v := range initial {
		j.keys[k] = v
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&j.jwksHits, 1)
		j.mu.Lock()
		defer j.mu.Unlock()
		jwkList := make([]map[string]string, 0, len(j.keys))
		for kid, pk := range j.keys {
			jwkList = append(jwkList, rsaPublicKeyToJWK(kid, pk))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": jwkList})
	})
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":   j.server.URL,
			"jwks_uri": j.server.URL + "/jwks",
		})
	})

	j.server = httptest.NewServer(mux)
	return j
}

func (j *jwksTestServer) setKey(kid string, pk *rsa.PublicKey) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.keys[kid] = pk
}

func (j *jwksTestServer) hits() int32 {
	return atomic.LoadInt32(&j.jwksHits)
}

func (j *jwksTestServer) close() {
	j.server.Close()
}

// withRealFetchJWK ensures the package-level fetchJWKs is the real one for
// this test, even if a prior test mutated it.
func withRealFetchJWK(t *testing.T) {
	t.Helper()
	prev := fetchJWKs
	fetchJWKs = fetchJWK
	t.Cleanup(func() { fetchJWKs = prev })
}

// TestRemoteOidcAuthenticator_RefreshUnknownKID verifies that when the issuer
// rotates signing keys, a JWT signed by a kid that wasn't present at startup
// is accepted after the JWKS cache refreshes.
func TestRemoteOidcAuthenticator_RefreshUnknownKID(t *testing.T) {
	withRealFetchJWK(t)

	privKey1, pubKey1 := generateJWTSignatureKeys()
	privKey2, pubKey2 := generateJWTSignatureKeys()

	server := newJWKSTestServer(map[string]*rsa.PublicKey{"kid_1": pubKey1})
	defer server.close()

	oidc, err := NewRemoteOidcAuthenticator(server.server.URL, nil, "aud", nil, nil)
	require.NoError(t, err)
	defer oidc.Close()

	// Sanity: a token signed by the originally-known key validates.
	token1 := generateJWT(privKey1, "kid_1", jwt.MapClaims{
		"iss": server.server.URL,
		"aud": "aud",
		"sub": "some-user",
		"exp": time.Now().Add(10 * time.Minute).Unix(),
	})
	_, err = oidc.Authenticate(generateContext(token1))
	require.NoError(t, err)

	hitsBeforeRotation := server.hits()

	// Issuer rotates: add kid_2 to the JWKS endpoint.
	server.setKey("kid_2", pubKey2)

	token2 := generateJWT(privKey2, "kid_2", jwt.MapClaims{
		"iss": server.server.URL,
		"aud": "aud",
		"sub": "some-user",
		"exp": time.Now().Add(10 * time.Minute).Unix(),
	})

	// keyfunc refreshes asynchronously when an unknown kid is seen, so the
	// first Authenticate may still fail; subsequent calls should succeed
	// once the background refresh lands.
	require.Eventually(t, func() bool {
		_, err := oidc.Authenticate(generateContext(token2))
		return err == nil
	}, 5*time.Second, 50*time.Millisecond, "expected JWKS refresh to pick up rotated kid_2")

	require.Greater(t, server.hits(), hitsBeforeRotation,
		"JWKS endpoint should have been re-fetched after an unknown kid was presented")
}

// TestRemoteOidcAuthenticator_RefreshRateLimit verifies that a burst of JWTs
// with different unknown kids triggers at most one JWKS refresh within the
// rate-limit window.
func TestRemoteOidcAuthenticator_RefreshRateLimit(t *testing.T) {
	withRealFetchJWK(t)

	originalLimit := jwkRefreshRateLimit
	jwkRefreshRateLimit = 2 * time.Second
	t.Cleanup(func() { jwkRefreshRateLimit = originalLimit })

	_, pubKey1 := generateJWTSignatureKeys()

	server := newJWKSTestServer(map[string]*rsa.PublicKey{"kid_1": pubKey1})
	defer server.close()

	oidc, err := NewRemoteOidcAuthenticator(server.server.URL, nil, "aud", nil, nil)
	require.NoError(t, err)
	defer oidc.Close()

	hitsBeforeBurst := server.hits()

	// Burst several JWTs with distinct unknown kids in quick succession.
	const burst = 5
	for i := 0; i < burst; i++ {
		privKey, _ := generateJWTSignatureKeys()
		token := generateJWT(privKey, fmt.Sprintf("unknown_kid_%d", i), jwt.MapClaims{
			"iss": server.server.URL,
			"aud": "aud",
			"sub": "some-user",
			"exp": time.Now().Add(10 * time.Minute).Unix(),
		})
		_, _ = oidc.Authenticate(generateContext(token))
	}

	// Wait for the (single) in-flight refresh to land.
	require.Eventually(t, func() bool {
		return server.hits() > hitsBeforeBurst
	}, 2*time.Second, 25*time.Millisecond, "expected at least one refresh after burst of unknown kids")

	// Give a brief grace window to catch any extra refreshes that might fire.
	time.Sleep(200 * time.Millisecond)

	extras := server.hits() - hitsBeforeBurst
	require.Equal(t, int32(1), extras,
		"RefreshRateLimit should bound extra JWKS refreshes to one within the window, got %d", extras)
}
