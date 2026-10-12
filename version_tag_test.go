package strata_test

import (
	"testing"

	"github.com/AndrewDonelson/strata"
	"github.com/stretchr/testify/require"
)

func TestRegister_VersionTagValidation(t *testing.T) {
	type stringVersion struct {
		ID      string `strata:"primary_key"`
		Version string `strata:"version"`
	}
	type twoVersions struct {
		ID string `strata:"primary_key"`
		A  int64  `strata:"version"`
		B  int64  `strata:"version"`
	}
	type ok struct {
		ID      string `strata:"primary_key"`
		Version int64  `strata:"version"`
	}
	cases := []struct {
		name   string
		schema strata.Schema
		valid  bool
	}{
		{"string field", strata.Schema{Name: "v_str", Model: &stringVersion{}}, false},
		{"two version fields", strata.Schema{Name: "v_two", Model: &twoVersions{}}, false},
		{"write-behind", strata.Schema{Name: "v_wb", Model: &ok{}, WriteMode: strata.WriteBehind}, false},
		{"valid", strata.Schema{Name: "v_ok", Model: &ok{}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds, err := strata.NewDataStore(strata.Config{})
			require.NoError(t, err)
			defer ds.Close()
			err = ds.Register(tc.schema)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, strata.ErrInvalidVersionTag)
			}
		})
	}
}
