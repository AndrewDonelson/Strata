package strata_test

import (
"strings"
"testing"

"github.com/AndrewDonelson/strata"
"github.com/stretchr/testify/assert"
)

func TestQuery_BasicSelect(t *testing.T) {
q := strata.Q().Build()
sql, args := q.ToSQL("users", nil, 100)
assert.Contains(t, sql, "SELECT")
assert.Contains(t, sql, "FROM users")
assert.Contains(t, sql, "LIMIT 100")
assert.Empty(t, args)
}

func TestQuery_Where(t *testing.T) {
q := strata.Q().Where("name = $1", "alice").Build()
sql, args := q.ToSQL("users", nil, 100)
assert.Contains(t, sql, "WHERE name = $1")
assert.Equal(t, []any{"alice"}, args)
}

func TestQuery_OrderByDesc(t *testing.T) {
q := strata.Q().OrderBy("created_at").Desc().Build()
sql, _ := q.ToSQL("users", nil, 50)
assert.True(t, strings.Contains(sql, "ORDER BY created_at DESC"))
}

func TestQuery_LimitOffset(t *testing.T) {
q := strata.Q().Limit(10).Offset(20).Build()
sql, _ := q.ToSQL("users", nil, 100)
assert.Contains(t, sql, "LIMIT 10")
assert.Contains(t, sql, "OFFSET 20")
}

func TestQuery_Fields(t *testing.T) {
q := strata.Q().Fields("id", "name").Build()
sql, _ := q.ToSQL("users", nil, 100)
assert.Contains(t, sql, "SELECT id, name FROM")
}
