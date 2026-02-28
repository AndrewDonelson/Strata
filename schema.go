package strata

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/AndrewDonelson/strata/internal/l3"
)

// WriteMode controls how writes flow through the three tiers.
type WriteMode int

const (
	WriteThrough        WriteMode = iota // L3 -> L2 -> L1, maximum safety
	WriteBehind                          // L1 immediately, L2/L3 async, max performance
	WriteThroughL1Async                  // L3+L2 sync, L1 lazily on next read
)

// EvictionPolicy determines which L1 entry is evicted when MaxEntries is reached.
type EvictionPolicy int

const (
	EvictLRU EvictionPolicy = iota
	EvictLFU
	EvictFIFO
)

// MemPolicy configures L1 in-memory cache behavior for a schema.
type MemPolicy struct {
	TTL        time.Duration
	MaxEntries int
	Eviction   EvictionPolicy
}

// RedisPolicy configures L2 Redis cache behavior for a schema.
type RedisPolicy struct {
	TTL       time.Duration
	KeyPrefix string
}

// PostgresPolicy configures L3 Postgres persistence for a schema.
type PostgresPolicy struct {
	TableName   string
	ReadReplica string
	PartitionBy string
}

// Index defines a database index on one or more columns.
type Index struct {
	Fields []string
	Unique bool
	Name   string
}

// SchemaHooks provides optional lifecycle callbacks.
type SchemaHooks struct {
	BeforeSet    func(ctx context.Context, value any) error
	AfterSet     func(ctx context.Context, value any)
	BeforeGet    func(ctx context.Context, id string)
	AfterGet     func(ctx context.Context, value any)
	OnEvict      func(ctx context.Context, key string, value any)
	OnWriteError func(ctx context.Context, key string, err error)
}

// Schema defines one data collection and its caching policy.
type Schema struct {
	Name      string
	Model     any
	L1        MemPolicy
	L2        RedisPolicy
	L3        PostgresPolicy
	WriteMode WriteMode
	Indexes   []Index
	Hooks     SchemaHooks
}

// compiledSchema is the internal representation of a registered Schema.
type compiledSchema struct {
	Schema
	columns   []l3.ColumnDef
	pkColumn  l3.ColumnDef
	pkIndex   int
	tableName string
	modelType reflect.Type
	l1Prefix  string // pre-computed cs.Name+":", avoids fmt.Sprintf on every Get
}

// schemaRegistry holds all registered schemas.
type schemaRegistry struct {
	mu      sync.RWMutex
	schemas map[string]*compiledSchema
}

func newSchemaRegistry() *schemaRegistry {
	return &schemaRegistry{schemas: make(map[string]*compiledSchema)}
}

func (r *schemaRegistry) register(s Schema) (*compiledSchema, error) {
	if s.Model == nil {
		return nil, ErrInvalidModel
	}
	t := reflect.TypeOf(s.Model)
	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return nil, ErrInvalidModel
	}
	structType := t.Elem()

	if s.Name == "" {
		s.Name = toSnakeCase(structType.Name())
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.schemas[s.Name]; exists {
		return nil, ErrSchemaDuplicate
	}

	cols, err := l3.ReflectSchema(s.Model)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidModel, err)
	}

	pkIdx := -1
	for i, col := range cols {
		if col.IsPK {
			pkIdx = i
			break
		}
	}
	if pkIdx == -1 {
		return nil, ErrNoPrimaryKey
	}

	tableName := s.L3.TableName
	if tableName == "" {
		tableName = s.Name
	}

	cs := &compiledSchema{
		Schema:    s,
		columns:   cols,
		pkColumn:  cols[pkIdx],
		pkIndex:   pkIdx,
		tableName: tableName,
		modelType: structType,
		l1Prefix:  s.Name + ":",
	}
	r.schemas[s.Name] = cs
	return cs, nil
}

func (r *schemaRegistry) get(name string) (*compiledSchema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cs, ok := r.schemas[name]
	if !ok {
		return nil, ErrSchemaNotFound
	}
	return cs, nil
}

func (r *schemaRegistry) all() []*compiledSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*compiledSchema, 0, len(r.schemas))
	for _, cs := range r.schemas {
		out = append(out, cs)
	}
	return out
}

func (cs *compiledSchema) newModel() any {
	return reflect.New(cs.modelType).Interface()
}

func (cs *compiledSchema) getPK(value any) (any, error) {
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	pk := v.Field(cs.pkIndex).Interface()
	if isZeroValue(pk) {
		return nil, ErrMissingPrimaryKey
	}
	return pk, nil
}

// toSnakeCase converts CamelCase to snake_case.
func toSnakeCase(s string) string {
	return l3.ToSnakeCase(s)
}

// isZeroValue returns true if v is the zero value for its type.
func isZeroValue(v any) bool {
	if v == nil {
		return true
	}
	return reflect.DeepEqual(v, reflect.Zero(reflect.TypeOf(v)).Interface())
}
