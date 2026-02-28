package strata_test

import (
"testing"

"github.com/AndrewDonelson/strata"
"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"
)

type User struct {
ID    string `strata:"primary_key"`
Name  string
Email string `strata:"unique"`
Age   int
}

func TestRegister_Success(t *testing.T) {
ds, err := strata.NewDataStore(strata.Config{})
require.NoError(t, err)
defer ds.Close()

err = ds.Register(strata.Schema{Name: "user", Model: &User{}})
assert.NoError(t, err)
}

func TestRegister_Duplicate(t *testing.T) {
ds, err := strata.NewDataStore(strata.Config{})
require.NoError(t, err)
defer ds.Close()

require.NoError(t, ds.Register(strata.Schema{Name: "user", Model: &User{}}))
err = ds.Register(strata.Schema{Name: "user", Model: &User{}})
assert.ErrorIs(t, err, strata.ErrSchemaDuplicate)
}

func TestRegister_NilModel(t *testing.T) {
ds, err := strata.NewDataStore(strata.Config{})
require.NoError(t, err)
defer ds.Close()

err = ds.Register(strata.Schema{Name: "bad", Model: nil})
assert.ErrorIs(t, err, strata.ErrInvalidModel)
}

type NoPK struct {
Name string
}

func TestRegister_NoPrimaryKey(t *testing.T) {
ds, err := strata.NewDataStore(strata.Config{})
require.NoError(t, err)
defer ds.Close()

err = ds.Register(strata.Schema{Name: "nopk", Model: &NoPK{}})
assert.ErrorIs(t, err, strata.ErrNoPrimaryKey)
}
