package fetcher

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"filippo.io/age"
	"filippo.io/age/armor"
)

// ageArmorMagic marks an ASCII-armored age file. Some public subscription
// providers (e.g. liqun) ship only this ciphertext plus the matching
// age-secret-key in a plaintext wrapper YAML, so nothing can be parsed until
// the payload is decrypted.
const ageArmorMagic = "-----BEGIN AGE ENCRYPTED FILE-----"

// IsAgeArmored reports whether the payload looks like an ASCII-armored age file.
// Only the head is inspected: armored files start with the marker line, and a
// subscription body can be hundreds of kilobytes.
func IsAgeArmored(b []byte) bool {
	head := b
	if len(head) > 256 {
		head = head[:256]
	}
	return bytes.Contains(head, []byte(ageArmorMagic))
}

// DecryptAge decrypts an ASCII-armored age file with an X25519 identity.
//
// The identity is the AGE-SECRET-KEY-1... string that providers publish inside
// their wrapper YAML as "age-secret-key". armor.NewReader is required: passing
// the armored bytes straight to age.Decrypt fails with
// `unexpected intro: "-----BEGIN AGE ENCRYPTED FILE-----"`.
func DecryptAge(ciphertext []byte, secretKey string) ([]byte, error) {
	key := strings.TrimSpace(secretKey)
	if key == "" {
		return nil, fmt.Errorf("age: empty secret key")
	}
	id, err := age.ParseX25519Identity(key)
	if err != nil {
		// A masked/placeholder key surfaces here; see the magpie note in AGENTS.md
		// before assuming the provider changed its format.
		return nil, fmt.Errorf("age: parse identity: %w", err)
	}
	r, err := age.Decrypt(armor.NewReader(bytes.NewReader(ciphertext)), id)
	if err != nil {
		return nil, fmt.Errorf("age: decrypt: %w", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("age: read plaintext: %w", err)
	}
	return out, nil
}