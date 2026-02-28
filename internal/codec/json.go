package codec

import "encoding/json"

// JSON is the default codec using standard library encoding/json.
type JSON struct{}

// Marshal serializes v to JSON bytes.
func (JSON) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal deserializes JSON bytes into v.
func (JSON) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// Name returns "json".
func (JSON) Name() string { return "json" }

// Default is the default codec instance.
var Default Codec = JSON{}
