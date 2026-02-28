package strata_test

import (
"testing"

"github.com/AndrewDonelson/strata"
"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"
)

func TestAES256GCM_RoundTrip(t *testing.T) {
key := make([]byte, 32)
for i := range key {
key[i] = byte(i)
}
enc, err := strata.NewAES256GCM(key)
require.NoError(t, err)

plain := []byte("Hello, Strata!")
cipher, err := enc.Encrypt(plain)
require.NoError(t, err)
assert.NotEqual(t, plain, cipher)

decrypted, err := enc.Decrypt(cipher)
require.NoError(t, err)
assert.Equal(t, plain, decrypted)
}

func TestAES256GCM_InvalidKeyLength(t *testing.T) {
_, err := strata.NewAES256GCM([]byte("short"))
assert.Error(t, err)
}

func TestAES256GCM_TamperDetection(t *testing.T) {
key := make([]byte, 32)
enc, _ := strata.NewAES256GCM(key)
cipher, _ := enc.Encrypt([]byte("secret"))
// Tamper
cipher[len(cipher)-1] ^= 0xFF
_, err := enc.Decrypt(cipher)
assert.Error(t, err)
}
