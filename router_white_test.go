package strata

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── noopLogger ───────────────────────────────────────────────────────────────

func TestNoopLogger_AllMethods(t *testing.T) {
	l := noopLogger{}
	l.Info("info message", "key", "val")
	l.Warn("warn message", "key", 1)
	l.Error("error message", "err", errors.New("oops"))
	l.Debug("debug message", "k1", "v1", "k2", 2)
}

// ── test models ───────────────────────────────────────────────────────────────

// encWBModel has an encrypted field for encryption round-trip tests.
type encWBModel struct {
	ID    string `strata:"primary_key"`
	Token string `strata:"encrypted"`
	Name  string
}

// scanWBModel has an omit_cache field to test buildScanDest filtering.
type scanWBModel struct {
	ID    string `strata:"primary_key"`
	Name  string
	Notes string `strata:"omit_cache,nullable"`
}

// ── buildScanDest ─────────────────────────────────────────────────────────────

func TestBuildScanDest_Basic(t *testing.T) {
	cs := mustCompileSchema(t, Schema{Name: "scan_basic", Model: &scanWBModel{}})
	m := &scanWBModel{}
	val := reflect.ValueOf(m).Elem()
	dests := buildScanDest(val, cs)
	// scanWBModel: ID, Name are normal; Notes is omit_cache → excluded
	assert.Len(t, dests, 2)
}

func TestBuildScanDest_WithOmitCache(t *testing.T) {
	// whiteBoxModel.Secret is omit_cache; 5 fields total → 4 dests
	cs := mustCompileSchema(t, Schema{Name: "scan_omit", Model: &whiteBoxModel{}})
	m := &whiteBoxModel{}
	val := reflect.ValueOf(m).Elem()
	dests := buildScanDest(val, cs)
	assert.Equal(t, 4, len(dests), "omit_cache field should be excluded")
}

// ── isNoRowsError ─────────────────────────────────────────────────────────────

func TestIsNoRowsError_Nil(t *testing.T) {
	assert.False(t, isNoRowsError(nil))
}

func TestIsNoRowsError_NoRows(t *testing.T) {
	assert.True(t, isNoRowsError(errors.New("no rows in result set")))
	assert.True(t, isNoRowsError(errors.New("query returned no rows")))
}

func TestIsNoRowsError_OtherError(t *testing.T) {
	assert.False(t, isNoRowsError(errors.New("connection refused")))
}

// ── populateDest ──────────────────────────────────────────────────────────────

type popModel struct {
	ID   string
	Val  int
}

func TestPopulateDest_Nil(t *testing.T) {
	var dest popModel
	ok, err := populateDest(nil, &dest)
	assert.False(t, ok)
	assert.NoError(t, err)
}

func TestPopulateDest_AssignableValue(t *testing.T) {
	src := popModel{ID: "x", Val: 42}
	var dest popModel
	ok, err := populateDest(src, &dest)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "x", dest.ID)
}

func TestPopulateDest_Pointer(t *testing.T) {
	src := &popModel{ID: "ptr", Val: 7}
	var dest popModel
	ok, err := populateDest(src, &dest)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "ptr", dest.ID)
}

func TestPopulateDest_NonAssignable(t *testing.T) {
	var dest popModel
	ok, err := populateDest("not a popModel", &dest)
	assert.False(t, ok)
	assert.NoError(t, err)
}

func TestPopulateDest_NonPointerDest(t *testing.T) {
	src := popModel{ID: "y"}
	var dest popModel
	ok, err := populateDest(src, dest) // not a pointer
	assert.False(t, ok)
	assert.NoError(t, err)
}

// ── encryptFields / decryptFields ─────────────────────────────────────────────

func TestEncryptDecryptFields_RoundTrip(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	ds, err := NewDataStore(Config{EncryptionKey: key})
	require.NoError(t, err)
	defer ds.Close()

	require.NoError(t, ds.Register(Schema{Name: "enc_wb", Model: &encWBModel{}}))
	cs, err := ds.registry.get("enc_wb")
	require.NoError(t, err)

	m := &encWBModel{ID: "1", Token: "mysecret", Name: "Alice"}
	val := reflect.ValueOf(m).Elem()

	// Encrypt
	require.NoError(t, ds.encryptFields(cs, val))
	encrypted := val.FieldByName("Token").String()
	assert.NotEqual(t, "mysecret", encrypted, "token should be ciphertext after encrypt")

	// Name (not encrypted) should be unchanged
	assert.Equal(t, "Alice", val.FieldByName("Name").String())

	// Decrypt
	require.NoError(t, ds.decryptFields(cs, val))
	assert.Equal(t, "mysecret", val.FieldByName("Token").String(), "token should be restored after decrypt")
}

func TestEncryptFields_NoEncryptor(t *testing.T) {
	ds, err := NewDataStore(Config{}) // no EncryptionKey → encryptor == nil
	require.NoError(t, err)
	defer ds.Close()

	require.NoError(t, ds.Register(Schema{Name: "noenc_wb", Model: &encWBModel{}}))
	cs, err := ds.registry.get("noenc_wb")
	require.NoError(t, err)

	m := &encWBModel{Token: "plain"}
	val := reflect.ValueOf(m).Elem()

	require.NoError(t, ds.encryptFields(cs, val))
	assert.Equal(t, "plain", val.FieldByName("Token").String(), "no encryptor → field unchanged")
}

func TestDecryptFields_InvalidCiphertext_SkippedSilently(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	ds, err := NewDataStore(Config{EncryptionKey: key})
	require.NoError(t, err)
	defer ds.Close()

	require.NoError(t, ds.Register(Schema{Name: "dec_skip", Model: &encWBModel{}}))
	cs, err := ds.registry.get("dec_skip")
	require.NoError(t, err)

	// Token holds something that is NOT valid ciphertext → decryptFields skips it silently
	m := &encWBModel{Token: "plaintext-not-encrypted"}
	val := reflect.ValueOf(m).Elem()
	require.NoError(t, ds.decryptFields(cs, val))
	assert.Equal(t, "plaintext-not-encrypted", val.FieldByName("Token").String())
}

func TestEncryptFields_EmptyToken_Skipped(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	ds, err := NewDataStore(Config{EncryptionKey: key})
	require.NoError(t, err)
	defer ds.Close()

	require.NoError(t, ds.Register(Schema{Name: "enc_empty", Model: &encWBModel{}}))
	cs, err := ds.registry.get("enc_empty")
	require.NoError(t, err)

	// Empty token should be skipped (no encryption attempt)
	m := &encWBModel{Token: ""}
	val := reflect.ValueOf(m).Elem()
	require.NoError(t, ds.encryptFields(cs, val))
	assert.Equal(t, "", val.FieldByName("Token").String())
}

// ── handleInvalidation ────────────────────────────────────────────────────────

func TestHandleInvalidation_MalformedJSON(t *testing.T) {
	// Malformed JSON → json.Unmarshal error → se.ds.logger.Warn → noopLogger.Warn
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	defer ds.Close()

	require.NotNil(t, ds.sync)
	// Should not panic; noopLogger.Warn absorbs the log call
	ds.sync.handleInvalidation("{ not : valid json }")
}

func TestHandleInvalidation_InvalidateAll(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	defer ds.Close()

	require.NotNil(t, ds.l1)
	require.NotNil(t, ds.sync)

	// Pre-populate L1 with two schemas
	ds.l1.Set("myschema:k1", "v1", 0)
	ds.l1.Set("myschema:k2", "v2", 0)
	ds.l1.Set("other:k1", "v3", 0)

	// Directly invoke handleInvalidation with "invalidate_all"
	msg, _ := json.Marshal(invalidationMsg{Schema: "myschema", Op: "invalidate_all"})
	ds.sync.handleInvalidation(string(msg))

	_, ok1 := ds.l1.Get("myschema:k1")
	_, ok2 := ds.l1.Get("myschema:k2")
	_, ok3 := ds.l1.Get("other:k1")

	assert.False(t, ok1, "myschema:k1 should be evicted by invalidate_all")
	assert.False(t, ok2, "myschema:k2 should be evicted by invalidate_all")
	assert.True(t, ok3, "other:k1 should remain untouched")
}

func TestHandleInvalidation_NilL1(t *testing.T) {
	// Without L1, handleInvalidation should be a no-op after JSON decode
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	defer ds.Close()

	// Temporarily disable L1
	origL1 := ds.l1
	ds.l1 = nil
	defer func() { ds.l1 = origL1 }()

	msg, _ := json.Marshal(invalidationMsg{Schema: "s", ID: "1", Op: "set"})
	ds.sync.handleInvalidation(string(msg)) // should not panic
}

// ── failEncryptor – mock that always errors ──────────────────────────────────

type failEncryptor struct{}

func (failEncryptor) Encrypt(_ []byte) ([]byte, error) {
return nil, errors.New("forced encrypt error")
}
func (failEncryptor) Decrypt(_ []byte) ([]byte, error) {
return nil, errors.New("forced decrypt error")
}

// ── decryptFields nil-encryptor path ─────────────────────────────────────────

func TestDecryptFields_NilEncryptor(t *testing.T) {
// ds has no EncryptionKey → ds.encryptor == nil → decryptFields returns nil immediately
ds, err := NewDataStore(Config{})
require.NoError(t, err)
defer ds.Close()

cs := mustCompileSchema(t, Schema{Name: "df_noop", Model: &encWBModel{}})
m := encWBModel{ID: "1", Token: "unchanged", Name: "n"}
val := reflect.ValueOf(&m).Elem()
err = ds.decryptFields(cs, val)
assert.NoError(t, err)
assert.Equal(t, "unchanged", m.Token) // no encryptor → field untouched
}

// ── encryptFields / decryptFields with missing struct field ──────────────────

// partialModel has only ID (no Token / Name) — used to provoke invalid FieldByName.
type partialModel struct {
ID string `strata:"primary_key"`
}

func TestEncryptFields_FieldMissingInValue(t *testing.T) {
// cs is compiled from encWBModel (has Token, Name).
// val is from partialModel (only has ID) → FieldByName("Token") is invalid → continue.
key := make([]byte, 32)
ds, err := NewDataStore(Config{EncryptionKey: key})
require.NoError(t, err)
defer ds.Close()

cs := mustCompileSchema(t, Schema{Name: "ef_partial", Model: &encWBModel{}})
m := partialModel{ID: "1"}
val := reflect.ValueOf(&m).Elem()
// Should not panic; invalid fields are silently skipped.
err = ds.encryptFields(cs, val)
assert.NoError(t, err)
}

func TestDecryptFields_FieldMissingInValue(t *testing.T) {
key := make([]byte, 32)
ds, err := NewDataStore(Config{EncryptionKey: key})
require.NoError(t, err)
defer ds.Close()

cs := mustCompileSchema(t, Schema{Name: "df_partial", Model: &encWBModel{}})
m := partialModel{ID: "1"}
val := reflect.ValueOf(&m).Elem()
err = ds.decryptFields(cs, val)
assert.NoError(t, err)
}

// ── encryptFields Encrypt() error path ───────────────────────────────────────

func TestEncryptFields_EncryptError(t *testing.T) {
// Set up ds with a mock encryptor that always fails.
ds, err := NewDataStore(Config{})
require.NoError(t, err)
defer ds.Close()
ds.encryptor = failEncryptor{}

cs := mustCompileSchema(t, Schema{Name: "ef_err", Model: &encWBModel{}})
m := encWBModel{ID: "1", Token: "plaintext", Name: "n"}
val := reflect.ValueOf(&m).Elem()
err = ds.encryptFields(cs, val)
assert.Error(t, err)
assert.Contains(t, err.Error(), "strata: encrypt field")
}

// ── decryptFields with empty cipher (skip) ────────────────────────────────────

func TestDecryptFields_EmptyCipherSkipped(t *testing.T) {
key := make([]byte, 32)
ds, err := NewDataStore(Config{EncryptionKey: key})
require.NoError(t, err)
defer ds.Close()

cs := mustCompileSchema(t, Schema{Name: "df_empty", Model: &encWBModel{}})
// Token is empty string → decryptFields should skip silently (cipher == "")
m := encWBModel{ID: "1", Token: "", Name: "n"}
val := reflect.ValueOf(&m).Elem()
err = ds.decryptFields(cs, val)
assert.NoError(t, err)
assert.Equal(t, "", m.Token) // unchanged
}

// ── buildScanDest with invalid (missing) struct field ────────────────────────

func TestBuildScanDest_InvalidField(t *testing.T) {
// cs is compiled from encWBModel (columns: ID, Token, Name).
// val is from partialModel (only ID) → FieldByName("Token") returns invalid
// → a dummy *any placeholder is used instead.
cs := mustCompileSchema(t, Schema{Name: "bsd_partial", Model: &encWBModel{}})
m := partialModel{ID: "1"}
val := reflect.ValueOf(&m).Elem()
dests := buildScanDest(val, cs)
// encWBModel has 3 non-omit-cache columns (ID, Token, Name).
// partialModel only has ID → Token and Name use dummy placeholders.
assert.Len(t, dests, 3)
}
