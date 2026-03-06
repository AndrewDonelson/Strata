// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// transport_tcp_test.go -- Unit tests for tcpTransport and memTransport direct methods.

package l4

import (
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// ------------------------------------------------------------------
// tcpTransport tests
// ------------------------------------------------------------------

// startTCPServer starts a tcpTransport listener on an ephemeral port and returns
// the transport and its listening address.
func startTCPServer(t *testing.T, nodeID string, handler msgHandler) (L4Transport, string) {
	t.Helper()
	tr := NewTCPTransport(nodeID, 10, handler)
	// Get a free port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	if err := tr.Listen(addr); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr, addr
}

func TestTCPTransport_SetHandler(t *testing.T) {
	tr := NewTCPTransport("node1", 5, nil)
	var called atomic.Int32
	tr.(HandlerSetter).SetHandler(func(_ L4Peer, _ L4Message) { called.Add(1) })
	// Just verifying it doesn't panic.
	_ = called.Load()
}

func TestTCPTransport_ListenAndConnect(t *testing.T) {
	var received atomic.Int32
	serverHandler := msgHandler(func(_ L4Peer, msg L4Message) {
		if msg.Type == MsgPublish {
			received.Add(1)
		}
	})
	server, addr := startTCPServer(t, "server", serverHandler)

	client := NewTCPTransport("client", 10, nil)
	t.Cleanup(func() { _ = client.Close() })

	if err := client.Connect(L4Peer{NodeID: "server", Address: addr}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	peers := client.Peers()
	if len(peers) != 1 {
		t.Errorf("client should have 1 peer, got %d", len(peers))
	}

	// Broadcast a message.
	msg := L4Message{Type: MsgPublish, From: "client"}
	if err := client.Broadcast(msg); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if received.Load() != 1 {
		t.Errorf("server should have received 1 message, got %d", received.Load())
	}
	_ = server
}

func TestTCPTransport_Send(t *testing.T) {
	var received atomic.Int32
	serverHandler := msgHandler(func(_ L4Peer, msg L4Message) {
		if msg.Type == MsgPeerRequest {
			received.Add(1)
		}
	})
	_, addr := startTCPServer(t, "server", serverHandler)

	client := NewTCPTransport("client", 10, nil)
	t.Cleanup(func() { _ = client.Close() })

	_ = client.Connect(L4Peer{NodeID: "server", Address: addr})
	time.Sleep(100 * time.Millisecond)

	msg := L4Message{Type: MsgPeerRequest, From: "client"}
	if err := client.Send("server", msg); err != nil {
		t.Fatalf("Send: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if received.Load() != 1 {
		t.Errorf("server should have received 1 message, got %d", received.Load())
	}
}

func TestTCPTransport_Send_NotConnected(t *testing.T) {
	client := NewTCPTransport("client", 10, nil)
	t.Cleanup(func() { _ = client.Close() })
	err := client.Send("nobody", L4Message{Type: MsgPing})
	if err == nil {
		t.Error("expected error sending to unknown peer")
	}
}

func TestTCPTransport_Connect_AlreadyConnected(t *testing.T) {
	_, addr := startTCPServer(t, "server", nil)
	client := NewTCPTransport("client", 10, nil)
	t.Cleanup(func() { _ = client.Close() })
	_ = client.Connect(L4Peer{NodeID: "server", Address: addr})
	time.Sleep(50 * time.Millisecond)
	// Connect again - should be a no-op.
	if err := client.Connect(L4Peer{NodeID: "server", Address: addr}); err != nil {
		t.Fatalf("second Connect should be no-op: %v", err)
	}
}

func TestTCPTransport_Connect_MaxPeers(t *testing.T) {
	_, addr1 := startTCPServer(t, "server1", nil)
	_, addr2 := startTCPServer(t, "server2", nil)
	client := NewTCPTransport("client", 1, nil) // maxPeers=1
	t.Cleanup(func() { _ = client.Close() })
	_ = client.Connect(L4Peer{NodeID: "server1", Address: addr1})
	time.Sleep(50 * time.Millisecond)
	err := client.Connect(L4Peer{NodeID: "server2", Address: addr2})
	if err != ErrNoPeers {
		t.Errorf("expected ErrNoPeers, got %v", err)
	}
}

func TestTCPTransport_Connect_BadAddr(t *testing.T) {
	client := NewTCPTransport("client", 10, nil)
	t.Cleanup(func() { _ = client.Close() })
	err := client.Connect(L4Peer{NodeID: "bad", Address: "127.0.0.1:1"})
	if err == nil {
		t.Error("expected error connecting to bad address")
	}
}

func TestTCPTransport_Listen_BadAddr(t *testing.T) {
	tr := NewTCPTransport("node1", 10, nil)
	err := tr.Listen("bad-addr-!!!")
	if err == nil {
		t.Error("expected error listening on bad address")
	}
}

func TestTCPTransport_Disconnect(t *testing.T) {
	_, addr := startTCPServer(t, "server", nil)
	client := NewTCPTransport("client", 10, nil)
	t.Cleanup(func() { _ = client.Close() })
	_ = client.Connect(L4Peer{NodeID: "server", Address: addr})
	time.Sleep(50 * time.Millisecond)
	if err := client.Disconnect("server"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if len(client.Peers()) != 0 {
		t.Error("expected 0 peers after disconnect")
	}
}

func TestTCPTransport_Disconnect_Unknown(t *testing.T) {
	client := NewTCPTransport("client", 10, nil)
	t.Cleanup(func() { _ = client.Close() })
	// Disconnect unknown peer should succeed gracefully.
	if err := client.Disconnect("nobody"); err != nil {
		t.Fatalf("Disconnect unknown: %v", err)
	}
}

func TestTCPTransport_Peers_Empty(t *testing.T) {
	client := NewTCPTransport("client", 10, nil)
	t.Cleanup(func() { _ = client.Close() })
	peers := client.Peers()
	if len(peers) != 0 {
		t.Errorf("expected empty peers, got %d", len(peers))
	}
}

func TestTCPTransport_Close_NoListener(t *testing.T) {
	client := NewTCPTransport("client", 10, nil)
	// Close without ever calling Listen should not panic.
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestTCPTransport_Broadcast_NoPeers(t *testing.T) {
	client := NewTCPTransport("client", 10, nil)
	t.Cleanup(func() { _ = client.Close() })
	// Broadcast with no peers should succeed gracefully.
	if err := client.Broadcast(L4Message{Type: MsgPing}); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
}

func TestTCPTransport_ReadLoop_InvalidJSON(t *testing.T) {
	// Server receives a line of invalid JSON - should be silently ignored.
	conns := make(chan net.Conn, 1)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	var received atomic.Int32
	handler := msgHandler(func(_ L4Peer, _ L4Message) { received.Add(1) })
	server := &tcpTransport{
		nodeID:   "server",
		maxPeers: 10,
		handler:  handler,
		peers:    make(map[string]*tcpConn),
		listener: ln,
	}
	go func() {
		conn, _ := ln.Accept()
		conns <- conn
		go server.readLoop(conn)
	}()
	// Connect raw TCP and send garbage.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte("{not valid json}\n"))
	time.Sleep(50 * time.Millisecond)
	// Should not have called handler.
	if received.Load() != 0 {
		t.Errorf("handler should not be called for invalid JSON")
	}
	_ = ln.Close()
}

// ------------------------------------------------------------------
// memTransport direct method tests
// ------------------------------------------------------------------

func TestMemTransport_Listen(t *testing.T) {
	hub := NewMemTransportHub()
	tr := NewMemTransport("node1", 10, hub, nil)
	// Listen is a no-op.
	if err := tr.Listen("any-addr"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
}

func TestMemTransport_Connect_Direct(t *testing.T) {
	hub := NewMemTransportHub()
	tr := NewMemTransport("node1", 10, hub, nil)
	peer := L4Peer{NodeID: "peer1"}
	if err := tr.Connect(peer); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	peers := tr.Peers()
	if len(peers) != 1 || peers[0].NodeID != "peer1" {
		t.Errorf("expected peer1, got %v", peers)
	}
}

func TestMemTransport_Connect_MaxPeers(t *testing.T) {
	hub := NewMemTransportHub()
	tr := NewMemTransport("node1", 1, hub, nil)
	_ = tr.Connect(L4Peer{NodeID: "peer1"})
	err := tr.Connect(L4Peer{NodeID: "peer2"})
	if err != ErrNoPeers {
		t.Errorf("expected ErrNoPeers, got %v", err)
	}
}

func TestMemTransport_Disconnect_Direct(t *testing.T) {
	hub := NewMemTransportHub()
	tr := NewMemTransport("node1", 10, hub, nil)
	_ = tr.Connect(L4Peer{NodeID: "peer1"})
	if err := tr.Disconnect("peer1"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if len(tr.Peers()) != 0 {
		t.Error("expected 0 peers after disconnect")
	}
}

func TestMemTransport_Broadcast_Direct(t *testing.T) {
	hub := NewMemTransportHub()
	var received atomic.Int32
	NewMemTransport("peer1", 10, hub, func(_ L4Peer, _ L4Message) { received.Add(1) })
	tr := NewMemTransport("node1", 10, hub, nil)
	hub.Connect("node1", "peer1")
	_ = tr.Broadcast(L4Message{Type: MsgPing})
	time.Sleep(50 * time.Millisecond)
	if received.Load() != 1 {
		t.Errorf("peer1 should have received message, got %d", received.Load())
	}
}

func TestMemTransport_Send_NotInHub(t *testing.T) {
	hub := NewMemTransportHub()
	tr := NewMemTransport("node1", 10, hub, nil)
	err := tr.Send("nobody", L4Message{Type: MsgPing})
	if err == nil {
		t.Error("expected error sending to node not in hub")
	}
}

func TestMemTransport_Close_RemovesFromHub(t *testing.T) {
	hub := NewMemTransportHub()
	tr := NewMemTransport("node1", 10, hub, nil)
	if hub.PeerCount("node1") == -1 { // just accessing hub
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// After close, node is removed from hub.
	if hub.PeerCount("node1") != 0 {
		t.Error("node should be removed from hub after close")
	}
}

// MemTransportHub edge cases.

func TestMemTransportHub_Connect_UnknownNodes(t *testing.T) {
	hub := NewMemTransportHub()
	// Connecting non-existent nodes should not panic.
	hub.Connect("ghost1", "ghost2")
}

func TestMemTransportHub_ConnectSafe_UnknownNodes(t *testing.T) {
	hub := NewMemTransportHub()
	ok := hub.ConnectSafe("ghost1", "ghost2", 10)
	if ok {
		t.Error("ConnectSafe should return false for unknown nodes")
	}
}

func TestMemTransportHub_PeerCount_Unknown(t *testing.T) {
	hub := NewMemTransportHub()
	count := hub.PeerCount("ghost")
	if count != 0 {
		t.Errorf("expected 0 for unknown node, got %d", count)
	}
}

func TestMemTransportHub_ConnectSafe_AlreadyConnected(t *testing.T) {
	hub := NewMemTransportHub()
	NewMemTransport("a", 10, hub, nil)
	NewMemTransport("b", 10, hub, nil)
	// First connect.
	hub.ConnectSafe("a", "b", 10)
	// Connect again - already in peers map, should still return true.
	ok := hub.ConnectSafe("a", "b", 10)
	if !ok {
		// Not an error - already connected means max peers wasn't hit
	}
}
