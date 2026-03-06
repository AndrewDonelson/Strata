// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// layer.go -- L4Layer interface, disabledLayer, and activeLayer implementations.

package l4

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

// RecordHandler is called in a goroutine when a record has been stored locally.
type RecordHandler func(record L4Record)

// L4Layer is the public API for the L4 distributed sync layer.
type L4Layer interface {
	Publish(appID, nodeID string, payload map[string]interface{}) (L4Record, error)
	Query(appID, recordID string) (L4Record, error)
	Revoke(appID, recordID string) error
	Subscribe(appID string, handler RecordHandler) error
	Unsubscribe(appID string) error
	Status() L4Status
	PeerCount() int
	Shutdown() error
}

// ---------------------------------------------------------------------------
// disabledLayer
// ---------------------------------------------------------------------------

type disabledLayer struct{}

func (d *disabledLayer) Publish(_, _ string, _ map[string]interface{}) (L4Record, error) {
	return L4Record{}, ErrL4Disabled
}
func (d *disabledLayer) Query(_, _ string) (L4Record, error)             { return L4Record{}, ErrL4Disabled }
func (d *disabledLayer) Revoke(_, _ string) error                        { return ErrL4Disabled }
func (d *disabledLayer) Subscribe(_ string, _ RecordHandler) error        { return ErrL4Disabled }
func (d *disabledLayer) Unsubscribe(_ string) error                       { return ErrL4Disabled }
func (d *disabledLayer) Status() L4Status                                 { return L4Status{Enabled: false} }
func (d *disabledLayer) PeerCount() int                                   { return 0 }
func (d *disabledLayer) Shutdown() error                                  { return nil }

// ---------------------------------------------------------------------------
// New / NewWithComponents
// ---------------------------------------------------------------------------

// New creates an L4Layer from the provided config.
func New(cfg Config) (L4Layer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if !cfg.Enabled {
		return &disabledLayer{}, nil
	}
	signer, err := NewSigner()
	if err != nil {
		return nil, err
	}
	store := NewMemStore()
	hub := NewMemTransportHub()
	nodeID := signer.PublicKeyHex()
	transport := NewMemTransport(nodeID, cfg.MaxPeers, hub, nil)
	return NewWithComponents(cfg, signer, store, transport)
}

// NewWithComponents creates a fully wired activeLayer with injected components.
func NewWithComponents(cfg Config, signer L4Signer, store L4Store, transport L4Transport) (L4Layer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if !cfg.Enabled {
		return &disabledLayer{}, nil
	}
	nodeID := ""
	if signer != nil {
		nodeID = signer.PublicKeyHex()
	}
	al := &activeLayer{
		cfg:       cfg,
		nodeID:    nodeID,
		signer:    signer,
		store:     store,
		transport: transport,
		subs:      make(map[string]RecordHandler),
		pending:   make(map[string]int),
		quit:      make(chan struct{}),
	}
	if hs, ok := transport.(HandlerSetter); ok {
		hs.SetHandler(al.handleMessage)
	}
	go al.syncLoop()
	return al, nil
}

// ---------------------------------------------------------------------------
// activeLayer
// ---------------------------------------------------------------------------

type activeLayer struct {
	cfg       Config
	nodeID    string
	signer    L4Signer
	store     L4Store
	transport L4Transport
	mu        sync.RWMutex
	subs      map[string]RecordHandler
	pending   map[string]int // record hash -> confirmation count
	quit      chan struct{}
}

func (al *activeLayer) Publish(appID, nodeID string, payload map[string]interface{}) (L4Record, error) {
	recordUUID := uuid.New().String()

	// Determine previous hash for this appID chain.
	prevHash := GenesisHash
	latest, err := al.store.Latest(appID, 1)
	if err == nil && len(latest) > 0 {
		prevHash = latest[0].Hash
	}

	now := time.Now().UnixNano()
	rec := L4Record{
		UUID:       recordUUID,
		AppID:      appID,
		NodeID:     nodeID,
		Payload:    payload,
		Timestamp:  now,
		VerifiedAt: VerifiedAtFromTimestamp(now),
		PrevHash:   prevHash,
		Status:     StatusPending,
	}
	if _, err := rec.ComputeHash(); err != nil {
		return L4Record{}, err
	}

	if al.signer != nil {
		sig, err := al.signer.Sign(&rec)
		if err != nil {
			return L4Record{}, err
		}
		rec.NodeSig = sig
	}

	if err := al.store.Put(rec); err != nil {
		if err == ErrAlreadyExists {
			return L4Record{}, ErrAlreadyPublished
		}
		return L4Record{}, err
	}

	msg := L4Message{Type: MsgPublish, From: al.nodeID, Record: &rec}
	_ = al.transport.Broadcast(msg)
	return rec, nil
}

func (al *activeLayer) Query(appID, recordID string) (L4Record, error) {
	rec, err := al.store.Get(appID, recordID)
	if err != nil {
		return L4Record{}, err
	}
	return *rec, nil
}

func (al *activeLayer) Revoke(appID, recordID string) error {
	rec, err := al.store.Get(appID, recordID)
	if err != nil {
		return err
	}
	if rec.Status == StatusRevoked {
		return ErrAlreadyRevoked
	}
	rec.Status = StatusRevoked
	rec.Revoked = true
	al.storeUpdate(*rec)

	revRec := L4RevocationRecord{AppID: appID, UUID: recordID, Hash: rec.Hash, RevokedAt: time.Now().UnixNano()}
	msg := L4Message{Type: MsgRevoke, From: al.nodeID, Revocation: &revRec}
	_ = al.transport.Broadcast(msg)
	return nil
}

func (al *activeLayer) Subscribe(appID string, handler RecordHandler) error {
	al.mu.Lock()
	defer al.mu.Unlock()
	al.subs[appID] = handler
	return nil
}

func (al *activeLayer) Unsubscribe(appID string) error {
	al.mu.Lock()
	defer al.mu.Unlock()
	delete(al.subs, appID)
	return nil
}

func (al *activeLayer) Status() L4Status {
	h, _ := al.store.Height()
	return L4Status{
		Enabled:     true,
		BlockHeight: h,
		PeerCount:   len(al.transport.Peers()),
		NodeID:      al.nodeID,
	}
}

func (al *activeLayer) PeerCount() int {
	return len(al.transport.Peers())
}

func (al *activeLayer) Shutdown() error {
	close(al.quit)
	_ = al.transport.Close()
	_ = al.store.Close()
	return nil
}

// handleMessage is the transport message handler.
func (al *activeLayer) handleMessage(from L4Peer, msg L4Message) {
	switch msg.Type {
	case MsgPublish:
		if msg.Record != nil {
			al.handleInboundRecord(from, *msg.Record)
		}
	case MsgConfirm:
		if msg.Record != nil {
			al.handleConfirmation(*msg.Record)
		}
	case MsgRevoke:
		if msg.Revocation != nil {
			al.handleRevocation(*msg.Revocation)
		}
	case MsgPeerList:
		if len(msg.Peers) > 0 {
			al.handlePeerList(msg.Peers)
		}
	case MsgPeerRequest:
		al.gossipPeers(from)
	case MsgPing:
		pong := L4Message{Type: MsgPong, From: al.nodeID}
		_ = al.transport.Send(from.NodeID, pong)
	}
}

func (al *activeLayer) handleInboundRecord(from L4Peer, rec L4Record) {
	// Verify signature if present.
	if al.signer != nil && len(rec.NodeSig) > 0 {
		// Use the signer's public key for verification if we have our own key,
		// otherwise use the NodeID as public key hex.
		pubKeyHex := al.nodeID
		if !al.signer.Verify(&rec, pubKeyHex, rec.NodeSig) {
			// Try sender's NodeID - may be a different node's signature.
			if from.NodeID != "" && from.NodeID != al.nodeID {
				if !al.signer.Verify(&rec, from.NodeID, rec.NodeSig) {
					return // silently drop tampered records
				}
			}
		}
	}
	// Store; ignore duplicate.
	if err := al.store.Put(rec); err != nil && err != ErrAlreadyExists {
		return
	}
	// Send confirmation back.
	confirm := L4Message{Type: MsgConfirm, From: al.nodeID, Record: &rec}
	_ = al.transport.Send(from.NodeID, confirm)
	// Fire subscriber.
	al.mu.RLock()
	handler := al.subs[rec.AppID]
	al.mu.RUnlock()
	if handler != nil {
		go handler(rec)
	}
}

func (al *activeLayer) handleConfirmation(rec L4Record) {
	if rec.Hash == "" {
		return
	}
	al.mu.Lock()
	al.pending[rec.Hash]++
	count := al.pending[rec.Hash]
	al.mu.Unlock()
	if count >= al.cfg.Quorum {
		existing, err := al.store.GetByHash(rec.Hash)
		if err != nil {
			return
		}
		existing.Status = StatusConfirmed
		existing.Confirmed = true
		al.storeUpdate(*existing)
	}
}

func (al *activeLayer) handleRevocation(revRec L4RevocationRecord) {
	existing, err := al.store.Get(revRec.AppID, revRec.UUID)
	if err != nil {
		return
	}
	if existing.Status == StatusRevoked {
		return
	}
	existing.Status = StatusRevoked
	existing.Revoked = true
	al.storeUpdate(*existing)
}

func (al *activeLayer) handlePeerList(peers []L4Peer) {
	for _, p := range peers {
		if p.NodeID == al.nodeID {
			continue
		}
		_ = al.transport.Connect(p)
	}
}

func (al *activeLayer) gossipPeers(to L4Peer) {
	peers := al.transport.Peers()
	msg := L4Message{Type: MsgPeerList, From: al.nodeID, Peers: peers}
	_ = al.transport.Send(to.NodeID, msg)
}

func (al *activeLayer) syncLoop() {
	if al.cfg.SyncInterval <= 0 {
		return
	}
	ticker := time.NewTicker(al.cfg.SyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-al.quit:
			return
		case <-ticker.C:
			for _, p := range al.transport.Peers() {
				al.gossipPeers(p)
			}
		}
	}
}

// storeUpdate writes an updated record back to the store.
func (al *activeLayer) storeUpdate(rec L4Record) {
	switch s := al.store.(type) {
	case *memStore:
		s.update(rec)
	case *boltStore:
		_ = s.updateRecord(rec)
	}
}
