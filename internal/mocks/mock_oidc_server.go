package mocks

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v4"
)

type mockOidcServer struct {
	issuerURL  string
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
}

const kidHeader = "1"

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

	mockServer.start()
	return mockServer, nil
}

func (server mockOidcServer) start() {
	port := strings.Split(server.issuerURL, ":")[2]

	http.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		err := json.NewEncoder(w).Encode(map[string]string{
			"issuer":   server.issuerURL,
			"jwks_uri": fmt.Sprintf("%s/jwks.json", server.issuerURL),
		})
		if err != nil {
			log.Fatalf("failed to json encode the openid configurations: %v", err)
		}
	})

	http.HandleFunc("/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		err := json.NewEncoder(w).Encode(map[string]interface{}{
			"keys": []map[string]string{
				{
					"kid": kidHeader,
					"kty": "RSA",
					"n":   base64.RawURLEncoding.EncodeToString(server.publicKey.N.Bytes()),
					"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(server.publicKey.E)).Bytes()),
				},
			},
		})
		if err != nil {
			log.Fatalf("failed to json encode the jwks keys: %v", err)
		}
	})

	go func() {
		log.Fatal(http.ListenAndServe(":"+port, nil))
	}()
}

func (server mockOidcServer) GetToken(audience, subject string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{
		Issuer:   server.issuerURL,
		Audience: []string{audience},
		Subject:  subject,
	})
	token.Header["kid"] = kidHeader
	return token.SignedString(server.privateKey)
}
