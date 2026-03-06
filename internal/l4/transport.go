// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// transport.go -- TCP transport and in-memory transport for L4 layer.

package l4

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
)

// msgHandler is called when a message is received from a peer.
type msgHandler func(from L4Peer, msg L4Message)

// HandlerSetter allows deferred wiring of the message handler.
type HandlerSetter interface {
	SetHandler(handler func(L4Peer, L4Message))
}

// L4Transport abstracts the peer-to-peer communication channel.
type L4Transport interface {
	Listen(addr string) error
	Connect(peer L4Peer) error
	Disconnect(nodeID string) error
	Broadcast(msg L4Message) error
	Send(nodeID string, msg L4Message) error
	Peers() []L4Peer
	Close() error
}

// ---------------------------------------------------------------------------
// tcpTransport
// ---------------------------------------------------------------------------

type tcpConn struct {
	peer L4Peer
	conn net.Conn
	enc  *json.Encoder
	mu   sync.Mutex
}

type tcpTransport struct {
	nodeID   string
	maxPeers int
	handler  msgHandler
	mu       sync.RWMutex
	peers    map[string]*tcpConn
	listener net.Listener
}

// NewTCPTransport creates a new TCP-based transport.
func NewTCPTransport(nodeID string, maxPeers int, handler msgHandler) L4Transport {
	return &tcpTransport{
		nodeID:   nodeID,
		maxPeers: maxPeers,
		handler:  handler,
		peers:    make(map[string]*tcpConn),
	}
}

func (t *tcpTransport) SetHandler(handler func(L4Peer, L4Message)) {
	t.handler = msgHandler(handler)
}

func (t *tcpTransport) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("l4 tcp: listen %s: %w", addr, err)
	}
	t.listener = ln
	go t.acceptLoop()
	return nil
}

func (t *tcpTransport) acceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			return
		}
		go t.readLoop(conn)
	}
}

func (t *tcpTransport) readLoop(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	var remotePeer L4Peer
	for scanner.Scan() {
		var msg L4Message
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}
		if msg.Type == MsgPing {
			remotePeer = L4Peer{NodeID: msg.From}
			t.mu.Lock()
			if _, ok := t.peers[msg.From]; !ok && len(t.peers) < t.maxPeers {
				t.peers[msg.From] = &tcpConn{
					peer: remotePeer,
					conn: conn,
					enc:  json.NewEncoder(conn),
				}
			}
			t.mu.Unlock()
			pong := L4Message{Type: MsgPong, From: t.nodeID}
			_ = json.NewEncoder(conn).Encode(pong)
			continue
		}
		if t.handler != nil {
			t.handler(remotePeer, msg)
		}
	}
	if remotePeer.NodeID != "" {
		t.mu.Lock()
		delete(t.peers, remotePeer.NodeID)
		t.mu.Unlock()
	}
}

func (t *tcpTransport) Connect(peer L4Peer) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.peers[peer.NodeID]; ok {
		return nil
	}
	if len(t.peers) >= t.maxPeers {
		return ErrNoPeers
	}
	conn, err := net.Dial("tcp", peer.Address)
	if err != nil {
		return fmt.Errorf("l4 tcp: connect %s: %w", peer.Address, err)
	}
	c := &tcpConn{peer: peer, conn: conn, enc: json.NewEncoder(conn)}
	t.peers[peer.NodeID] = c
	go t.readLoop(conn)
	// Send hello ping
	hello := L4Message{Type: MsgPing, From: t.nodeID}
	_ = c.enc.Encode(hello)
	return nil
}

func (t *tcpTransport) Disconnect(nodeID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	c, ok := t.peers[nodeID]
	if !ok {
		return nil
	}
	_ = c.conn.Close()
	delete(t.peers, nodeID)
	return nil
}

func (t *tcpTransport) Broadcast(msg L4Message) error {
	t.mu.RLock()
	list := make([]*tcpConn, 0, len(t.peers))
	for _, c := range t.peers {
		list = append(list, c)
	}
	t.mu.RUnlock()
	for _, c := range list {
		c.mu.Lock()
		_ = c.enc.Encode(msg)
		c.mu.Unlock()
	}
	return nil
}

func (t *tcpTransport) Send(nodeID string, msg L4Message) error {
	t.mu.RLock()
	c, ok := t.peers[nodeID]
	t.mu.RUnlock()
	if !ok {
		return fmt.Errorf("l4 tcp: peer %s not connected", nodeID)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enc.Encode(msg)
}

func (t *tcpTransport) Peers() []L4Peer {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([]L4Peer, 0, len(t.peers))
	for _, c := range t.peers {
		result = append(result, c.peer)
	}
	return result
}

func (t *tcpTransport) Close() error {
	if t.listener != nil {
		_ = t.listener.Close()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, c := range t.peers {
		_ = c.conn.Close()
		delete(t.peers, id)
	}
	return nil
}

// ---------------------------------------------------------------------------
// MemTransportHub -- shared in-process registry for memTransport
// ---------------------------------------------------------------------------

// MemTransportHub wires in-process memTransports together for testing.
type MemTransportHub struct {
	mu         sync.RWMutex
	transports map[string]*memTransport
}

// NewMemTransportHub creates a new hub.
func NewMemTransportHub() *MemTransportHub {
	return &MemTransportHub{transports: make(map[string]*memTransport)}
}

// register is called by each memTransport on creation.
func (h *MemTransportHub) register(t *memTransport) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.transports[t.nodeID] = t
}

// Connect establishes a bi-directional link between nodeIDA and nodeIDB.
func (h *MemTransportHub) Connect(nodeIDA, nodeIDB string) {
	h.mu.RLock()
	a, okA := h.transports[nodeIDA]
	b, okB := h.transports[nodeIDB]
	h.mu.RUnlock()
	if !okA || !okB {
		return
	}
	peerA := L4Peer{NodeID: nodeIDA}
	peerB := L4Peer{NodeID: nodeIDB}
	a.mu.Lock()
	if _, exists := a.peers[nodeIDB]; !exists {
		a.peers[nodeIDB] = peerB
	}
	a.mu.Unlock()
	b.mu.Lock()
	if _, exists := b.peers[nodeIDA]; !exists {
		b.peers[nodeIDA] = peerA
	}
	b.mu.Unlock()
}

// ConnectSafe connects two transports only if both are below maxPeers. Returns true on success.
func (h *MemTransportHub) ConnectSafe(nodeIDA, nodeIDB string, maxPeers int) bool {
	h.mu.RLock()
	a, okA := h.transports[nodeIDA]
	b, okB := h.transports[nodeIDB]
	h.mu.RUnlock()
	if !okA || !okB {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(a.peers) >= maxPeers || len(b.peers) >= maxPeers {
		return false
	}
	peerA := L4Peer{NodeID: nodeIDA}
	peerB := L4Peer{NodeID: nodeIDB}
	if _, exists := a.peers[nodeIDB]; !exists {
		a.peers[nodeIDB] = peerB
	}
	if _, exists := b.peers[nodeIDA]; !exists {
		b.peers[nodeIDA] = peerA
	}
	return true
}

// PeerCount returns the number of peers the given node ID has.
func (h *MemTransportHub) PeerCount(nodeID string) int {
	h.mu.RLock()
	t, ok := h.transports[nodeID]
	h.mu.RUnlock()
	if !ok {
		return 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.peers)
}

// ---------------------------------------------------------------------------
// memTransport
// ---------------------------------------------------------------------------

type memTransport struct {
	nodeID   string
	maxPeers int
	hub      *MemTransportHub
	handler  msgHandler
	mu       sync.RWMutex
	peers    map[string]L4Peer
}

// NewMemTransport creates an in-process transport backed by hub.
func NewMemTransport(nodeID string, maxPeers int, hub *MemTransportHub, handler msgHandler) L4Transport {
	t := &memTransport{
		nodeID:   nodeID,
		maxPeers: maxPeers,
		hub:      hub,
		handler:  handler,
		peers:    make(map[string]L4Peer),
	}
	hub.register(t)
	return t
}

func (m *memTransport) SetHandler(handler func(L4Peer, L4Message)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handler = msgHandler(handler)
}

func (m *memTransport) Listen(_ string) error { return nil }

func (m *memTransport) Connect(peer L4Peer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.peers) >= m.maxPeers {
		return ErrNoPeers
	}
	m.peers[peer.NodeID] = peer
	return nil
}

func (m *memTransport) Disconnect(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.peers, nodeID)
	return nil
}

func (m *memTransport) deliver(from L4Peer, msg L4Message) {
	m.mu.RLock()
	h := m.handler
	m.mu.RUnlock()
	if h != nil {
		h(from, msg)
	}
}

func (m *memTransport) Broadcast(msg L4Message) error {
	m.mu.RLock()
	peers := make([]L4Peer, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, p)
	}
	m.mu.RUnlock()
	self := L4Peer{NodeID: m.nodeID}
	for _, p := range peers {
		m.hub.mu.RLock()
		tgt, ok := m.hub.transports[p.NodeID]
		m.hub.mu.RUnlock()
		if ok {
			go tgt.deliver(self, msg)
		}
	}
	return nil
}

func (m *memTransport) Send(nodeID string, msg L4Message) error {
	m.hub.mu.RLock()
	tgt, ok := m.hub.transports[nodeID]
	m.hub.mu.RUnlock()
	if !ok {
		return fmt.Errorf("l4 mem: peer %s not found in hub", nodeID)
	}
	go tgt.deliver(L4Peer{NodeID: m.nodeID}, msg)
	return nil
}

func (m *memTransport) Peers() []L4Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]L4Peer, 0, len(m.peers))
	for _, p := range m.peers {
		result = append(result, p)
	}
	return result
}

func (m *memTransport) Close() error {
	m.hub.mu.Lock()
	delete(m.hub.transports, m.nodeID)
	m.hub.mu.Unlock()
	return nil
}
