package encoder

import "testing"

func TestEncryptEncodeDecodeDecrypt(t *testing.T) {
	encrypter, err := NewTokenEncrypter("key")
	if err != nil {
		t.Fatalf("Error building encoder: %s", err)
	}
	s := []byte("any string")

	encoded, err := encrypter.Encode(s)
	if err != nil {
		t.Errorf("error %v", err)
	}
	decoded, err := encrypter.Decode(encoded)
	if err != nil {
		t.Errorf("error %v", err)
	}

	if string(s) != string(decoded) {
		t.Errorf("wanted '%s', got '%s'", string(s), string(decoded))
	}
}

func TestEncryptEncodeEmptyByteArrayReturnsEmptyString(t *testing.T) {
	encrypter, err := NewTokenEncrypter("key")
	if err != nil {
		t.Fatalf("Error building encoder: %s", err)
	}

	encoded, err := encrypter.Encode([]byte{})
	if err != nil {
		t.Errorf("error %v", err)
	}

	if encoded != "" {
		t.Errorf("wanted empty string, but got '%s'", encoded)
	}
}

func TestDecodeDecryptEmptyStringReturnsEmptyByteArray(t *testing.T) {
	encrypter, err := NewTokenEncrypter("key")
	if err != nil {
		t.Fatalf("Error building encoder: %s", err)
	}

	decoded, err := encrypter.Decode("")
	if err != nil {
		t.Errorf("error %v", err)
	}

	if len(decoded) != 0 {
		t.Errorf("wanted empty array, but got '%v'", decoded)
	}
}
