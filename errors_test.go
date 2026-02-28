package strata_test

import (
"errors"
"testing"

"github.com/AndrewDonelson/strata"
)

func TestErrors_Sentinel(t *testing.T) {
errs := []error{
strata.ErrNotFound,
strata.ErrSchemaNotFound,
strata.ErrSchemaDuplicate,
strata.ErrNoPrimaryKey,
strata.ErrInvalidModel,
strata.ErrMissingPrimaryKey,
strata.ErrL1Unavailable,
strata.ErrL2Unavailable,
strata.ErrL3Unavailable,
strata.ErrUnavailable,
strata.ErrTxFailed,
strata.ErrInvalidConfig,
}
for _, e := range errs {
if e == nil {
t.Fatalf("nil sentinel error")
}
}
}

func TestErrors_Is(t *testing.T) {
wrapped := strata.ErrNotFound
if !errors.Is(wrapped, strata.ErrNotFound) {
t.Fatal("expected ErrNotFound")
}
}
