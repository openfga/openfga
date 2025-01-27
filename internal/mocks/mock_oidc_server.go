package mocks

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"log"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
)

type mockOidcServer struct {
	issuerURL  string
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
	httpServer *http.Server
}

const kidHeader = "1"

// NewMockOidcServer creates a mock OIDC server with the given issuer URL and a random private key.
// You must call Stop afterward.
func NewMockOidcServer(issuerURL string) (*mockOidcServer, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, err
	}

	mockServer := &mockOidcServer{
		issuerURL:  issuerURL,
		privateKey: privateKey,
		publicKey:  privateKey.Public().(*rsa.PublicKey),
	}

	mockServer.httpServer = createHTTPServer(issuerURL, mockServer.publicKey)
	go mockServer.start()
	return mockServer, nil
}

// NewAliasMockServer creates an alias server of a mock OIDC server that was created by NewMockOidcServer.
// You must call Stop afterward.
func (server *mockOidcServer) NewAliasMockServer(aliasURL string) *mockOidcServer {
	mockServer := &mockOidcServer{
		issuerURL:  aliasURL,
		privateKey: server.privateKey,
		publicKey:  server.privateKey.Public().(*rsa.PublicKey),
	}

	mockServer.httpServer = createHTTPServer(aliasURL, mockServer.publicKey)
	go mockServer.start()
	return mockServer
}

func createHTTPServer(issuerURL string, publicKey *rsa.PublicKey) *http.Server {
	addr := strings.Split(issuerURL, "http://")[1]

	mockHandler := http.NewServeMux()

	mockHandler.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		err := json.NewEncoder(w).Encode(map[string]string{
			"issuer":   issuerURL,
			"jwks_uri": issuerURL + "/jwks.json",
		})
		if err != nil {
			log.Fatalf("failed to json encode the openid configurations: %v", err)
		}
	})

	mockHandler.HandleFunc("/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		err := json.NewEncoder(w).Encode(map[string]interface{}{
			"keys": []map[string]string{
				{
					"kid": kidHeader,
					"kty": "RSA",
					"n":   base64.RawURLEncoding.EncodeToString(publicKey.N.Bytes()),
					"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(publicKey.E)).Bytes()),
				},
			},
		})
		if err != nil {
			log.Fatalf("failed to json encode the jwks keys: %v", err)
		}
	})

	return &http.Server{Addr: addr, Handler: mockHandler}
}

func (server *mockOidcServer) start() {
	if err := server.httpServer.ListenAndServe(); err != nil {
		if err != http.ErrServerClosed {
			log.Fatal("failed to start mock OIDC server", zap.Error(err))
		}
	}
	log.Println("mock OIDC server shut down.")
}

func (server *mockOidcServer) Stop() {
	if server.httpServer != nil {
		_ = server.httpServer.Shutdown(context.Background())
	}
}

func (server *mockOidcServer) GetToken(audience, subject string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{
		Issuer:    server.issuerURL,
		Audience:  []string{audience},
		Subject:   subject,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(10 * time.Second)),
	})
	token.Header["kid"] = kidHeader
	return token.SignedString(server.privateKey)
}
