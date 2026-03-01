// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// json.go — JSON codec implementation wrapping encoding/json; primarily used
// in tests and when human-readable Redis values are preferred.

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
