package fetcher

import (
	"testing"
	"filippo.io/age"
	"filippo.io/age/armor"
	"bytes"
)

func TestIsAgeArmored(t *testing.T) {
	if !IsAgeArmored([]byte("-----BEGIN AGE ENCRYPTED FILE-----\nabc\n")) {
		t.Fatal("armored payload not detected")
	}
	if IsAgeArmored([]byte("proxies:\n  - name: x\n")) {
		t.Fatal("plain clash yaml misdetected as age")
	}
	if IsAgeArmored([]byte("")) {
		t.Fatal("empty payload reported as age")
	}
}

func TestDecryptAgeRoundTrip(t *testing.T) {
	id, err := age.GenerateX25519Identity()
	if err != nil { t.Fatal(err) }
	plain := []byte("proxies:\n  - name: demo\n    type: http\n")
	var buf bytes.Buffer
	w := armor.NewWriter(&buf)
	enc, err := age.Encrypt(w, id.Recipient())
	if err != nil { t.Fatal(err) }
	if _, err := enc.Write(plain); err != nil { t.Fatal(err) }
	if err := enc.Close(); err != nil { t.Fatal(err) }
	if err := w.Close(); err != nil { t.Fatal(err) }
	got, err := DecryptAge(buf.Bytes(), id.String())
	if err != nil { t.Fatalf("decrypt: %v", err) }
	if !bytes.Equal(got, plain) { t.Fatalf("round trip mismatch: %q", got) }
}

func TestDecryptAgeBadKey(t *testing.T) {
	_, err := DecryptAge([]byte("-----BEGIN AGE ENCRYPTED FILE-----\nAA\n-----END AGE ENCRYPTED FILE-----\n"), "not-a-key")
	if err == nil { t.Fatal("expected error for malformed key") }
}