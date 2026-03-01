// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// msgpack.go — MessagePack codec implementation using vmihailenco/msgpack;
// the default codec for L2 Redis storage due to its compact binary format.

package codec

import "github.com/vmihailenco/msgpack/v5"

// MsgPack is a high-performance codec using MessagePack encoding.
type MsgPack struct{}

// Marshal serializes v to MessagePack bytes.
func (MsgPack) Marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

// Unmarshal deserializes MessagePack bytes into v.
func (MsgPack) Unmarshal(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}

// Name returns "msgpack".
func (MsgPack) Name() string { return "msgpack" }
