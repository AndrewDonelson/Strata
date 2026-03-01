// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// query.go — fluent Query builder for constructing parameterised SELECT
// statements with WHERE, ORDER BY, LIMIT, and OFFSET clauses, consumed by
// Search, SearchTyped, SearchCached, and WarmCache.

package strata

import (
	"fmt"
	"strings"
)

// Query specifies search parameters for the Search and SearchCached operations.
type Query struct {
	Where   string
	Args    []any
	OrderBy string
	Desc    bool
	Limit   int
	Offset  int
	Fields  []string
	ForceL3 bool
	ForceL2 bool
}

// queryBuilder is the fluent builder for Query.
type queryBuilder struct{ q Query }

// Q returns a new fluent query builder.
func Q() *queryBuilder { return &queryBuilder{} }

func (b *queryBuilder) Where(clause string, args ...any) *queryBuilder {
	b.q.Where = clause
	b.q.Args = args
	return b
}
func (b *queryBuilder) OrderBy(col string) *queryBuilder      { b.q.OrderBy = col; return b }
func (b *queryBuilder) Desc() *queryBuilder                   { b.q.Desc = true; return b }
func (b *queryBuilder) Limit(n int) *queryBuilder             { b.q.Limit = n; return b }
func (b *queryBuilder) Offset(n int) *queryBuilder            { b.q.Offset = n; return b }
func (b *queryBuilder) Fields(fields ...string) *queryBuilder { b.q.Fields = fields; return b }
func (b *queryBuilder) ForceL3() *queryBuilder                { b.q.ForceL3 = true; return b }
func (b *queryBuilder) ForceL2() *queryBuilder                { b.q.ForceL2 = true; return b }
func (b *queryBuilder) Build() Query                          { return b.q }

// toSQL converts a Query into a SQL SELECT statement.
func (q Query) ToSQL(table string, columns []string, defaultLimit int) (string, []any) {
	cols := "*"
	if len(q.Fields) > 0 {
		cols = strings.Join(q.Fields, ", ")
	} else if len(columns) > 0 {
		cols = strings.Join(columns, ", ")
	}
	sql := fmt.Sprintf("SELECT %s FROM %s", cols, table)
	args := q.Args
	if args == nil {
		args = []any{}
	}
	if q.Where != "" {
		sql += " WHERE " + q.Where
	}
	if q.OrderBy != "" {
		sql += " ORDER BY " + q.OrderBy
		if q.Desc {
			sql += " DESC"
		}
	}
	limit := q.Limit
	if limit == 0 {
		limit = defaultLimit
	}
	if limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", limit)
	}
	if q.Offset > 0 {
		sql += fmt.Sprintf(" OFFSET %d", q.Offset)
	}
	return sql, args
}
