// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// message.go -- L4Message wire envelope for P2P gossip.

package l4

// MsgType identifies the kind of gossip message.
type MsgType string

const (
	MsgPublish     MsgType = "publish"
	MsgRevoke      MsgType = "revoke"
	MsgConfirm     MsgType = "confirm"
	MsgPeerList    MsgType = "peer_list"
	MsgPeerRequest MsgType = "peer_request"
	MsgSync        MsgType = "sync"
	MsgSyncRequest MsgType = "sync_request"
	MsgPing        MsgType = "ping"
	MsgPong        MsgType = "pong"
)

// L4Message is the wire-level envelope sent between L4 nodes.
type L4Message struct {
	Type       MsgType             `json:"type"`
	From       string              `json:"from"`
	Payload    interface{}         `json:"payload,omitempty"`
	Record     *L4Record           `json:"record,omitempty"`
	Revocation *L4RevocationRecord `json:"revocation,omitempty"`
	Peers      []L4Peer            `json:"peers,omitempty"`
	Records    []L4Record          `json:"records,omitempty"`
	FromHash   string              `json:"from_hash,omitempty"`
	AppID      string              `json:"app_id,omitempty"`
}
