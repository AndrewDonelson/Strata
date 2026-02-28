package codec_test

import (
"testing"

"github.com/AndrewDonelson/strata/internal/codec"
"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"
)

type item struct {
ID   int    `json:"id" msgpack:"id"`
Name string `json:"name" msgpack:"name"`
}

func TestJSONCodec(t *testing.T) {
c := codec.JSON{}
orig := item{ID: 1, Name: "test"}
b, err := c.Marshal(orig)
require.NoError(t, err)

var got item
require.NoError(t, c.Unmarshal(b, &got))
assert.Equal(t, orig, got)
assert.Equal(t, "json", c.Name())
}

func TestMsgPackCodec(t *testing.T) {
c := codec.MsgPack{}
orig := item{ID: 42, Name: "pack"}
b, err := c.Marshal(orig)
require.NoError(t, err)

var got item
require.NoError(t, c.Unmarshal(b, &got))
assert.Equal(t, orig, got)
assert.Equal(t, "msgpack", c.Name())
}
