package keyfunc

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"encoding/json"
)

// GivenKey represents a cryptographic key that resides in a JWKS. In conjuncture with Options.
type GivenKey struct {
	algorithm string
	inter     interface{}
}

// GivenKeyOptions represents the configuration options for a GivenKey.
type GivenKeyOptions struct {
	// Algorithm is the given key's signing algorithm. Its value will be compared to unverified tokens' "alg" header.
	//
	// See RFC 8725 Section 3.1 for details.
	// https://www.rfc-editor.org/rfc/rfc8725#section-3.1
	//
	// For a list of possible values, please see:
	// https://www.rfc-editor.org/rfc/rfc7518#section-3.1
	// https://www.iana.org/assignments/jose/jose.xhtml#web-signature-encryption-algorithms
	Algorithm string
}

// NewGiven creates a JWKS from a map of given keys.
func NewGiven(givenKeys map[string]GivenKey) (jwks *JWKS) {
	keys := make(map[string]parsedJWK)

	for kid, given := range givenKeys {
		keys[kid] = parsedJWK{
			algorithm: given.algorithm,
			public:    given.inter,
		}
	}

	return &JWKS{
		keys: keys,
	}
}

// NewGivenCustom creates a new GivenKey given an untyped variable. The key argument is expected to be a type supported
// by the jwt package used.
//
// Consider the options carefully as each field may have a security implication.
//
// See the https://pkg.go.dev/github.com/golang-jwt/jwt/v5#RegisterSigningMethod function for registering an unsupported
// signing method.
func NewGivenCustom(key interface{}, options GivenKeyOptions) (givenKey GivenKey) {
	return GivenKey{
		algorithm: options.Algorithm,
		inter:     key,
	}
}

// NewGivenECDSA creates a new GivenKey given an ECDSA public key.
//
// Consider the options carefully as each field may have a security implication.
func NewGivenECDSA(key *ecdsa.PublicKey, options GivenKeyOptions) (givenKey GivenKey) {
	return GivenKey{
		algorithm: options.Algorithm,
		inter:     key,
	}
}

// NewGivenEdDSA creates a new GivenKey given an EdDSA public key.
//
// Consider the options carefully as each field may have a security implication.
func NewGivenEdDSA(key ed25519.PublicKey, options GivenKeyOptions) (givenKey GivenKey) {
	return GivenKey{
		algorithm: options.Algorithm,
		inter:     key,
	}
}

// NewGivenHMAC creates a new GivenKey given an HMAC key in a byte slice.
//
// Consider the options carefully as each field may have a security implication.
func NewGivenHMAC(key []byte, options GivenKeyOptions) (givenKey GivenKey) {
	return GivenKey{
		algorithm: options.Algorithm,
		inter:     key,
	}
}

// NewGivenRSA creates a new GivenKey given an RSA public key.
//
// Consider the options carefully as each field may have a security implication.
func NewGivenRSA(key *rsa.PublicKey, options GivenKeyOptions) (givenKey GivenKey) {
	return GivenKey{
		algorithm: options.Algorithm,
		inter:     key,
	}
}

// NewGivenKeysFromJSON parses a raw JSON message into a map of key IDs (`kid`) to GivenKeys. The returned map is
// suitable for passing to `NewGiven()` or as `Options.GivenKeys` to `Get()`
func NewGivenKeysFromJSON(jwksBytes json.RawMessage) (map[string]GivenKey, error) {
	// Parse by making a temporary JWKS instance. No need to lock its map since it doesn't escape this function.
	j, err := NewJSON(jwksBytes)
	if err != nil {
		return nil, err
	}
	keys := make(map[string]GivenKey, len(j.keys))
	for kid, cryptoKey := range j.keys {
		keys[kid] = GivenKey{
			algorithm: cryptoKey.algorithm,
			inter:     cryptoKey.public,
		}
	}
	return keys, nil
}
