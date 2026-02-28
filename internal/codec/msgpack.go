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
