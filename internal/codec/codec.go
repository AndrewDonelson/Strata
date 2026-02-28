// Package codec provides encode/decode interfaces for cache serialization.
package codec

// Codec encodes and decodes values for cache storage.
type Codec interface {
	// Marshal serializes v into bytes.
	Marshal(v any) ([]byte, error)
	// Unmarshal deserializes data into v (must be a pointer).
	Unmarshal(data []byte, v any) error
	// Name returns the codec identifier used for diagnostics.
	Name() string
}
