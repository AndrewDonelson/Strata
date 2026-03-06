// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// api.go -- HTTP API server for L4 ledger mode.

package l4

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// rateLimiter -- sliding window per-IP (100 req/min)
// ---------------------------------------------------------------------------

const rateLimitPerMin = 100

type rateLimiter struct {
	mu      sync.Mutex
	windows map[string][]time.Time
}

func newRateLimiter() *rateLimiter {
	return &rateLimiter{windows: make(map[string][]time.Time)}
}

// Allow returns true if the request from ip is within the limit.
func (r *rateLimiter) Allow(ip string) bool {
	now := time.Now()
	cutoff := now.Add(-time.Minute)
	r.mu.Lock()
	defer r.mu.Unlock()
	times := r.windows[ip]
	var filtered []time.Time
	for _, t := range times {
		if t.After(cutoff) {
			filtered = append(filtered, t)
		}
	}
	filtered = append(filtered, now)
	r.windows[ip] = filtered
	return len(filtered) <= rateLimitPerMin
}

// ---------------------------------------------------------------------------
// APIServer
// ---------------------------------------------------------------------------

// APIServer exposes the L4 layer over HTTP for ledger mode.
type APIServer struct {
	layer     L4Layer
	store     L4Store
	transport L4Transport
	rl        *rateLimiter
	srv       *http.Server
}

// NewAPIServer constructs an APIServer.
func NewAPIServer(layer L4Layer, store L4Store, transport L4Transport) *APIServer {
	return &APIServer{
		layer:     layer,
		store:     store,
		transport: transport,
		rl:        newRateLimiter(),
	}
}

func (a *APIServer) buildMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/query/", a.handleQuery)
	mux.HandleFunc("/peers", a.handlePeers)
	mux.HandleFunc("/sync", a.handleSync)
	return a.corsMiddleware(a.rateLimitMiddleware(mux))
}

// Listen starts the HTTP server on addr.
func (a *APIServer) Listen(addr string) error {
	a.srv = &http.Server{Addr: addr, Handler: a.buildMux()}
	return a.srv.ListenAndServe()
}

// ListenOnListener starts the HTTP server on the provided net.Listener.
// Useful for testing with ephemeral ports.
func (a *APIServer) ListenOnListener(ln net.Listener) error {
	a.srv = &http.Server{Handler: a.buildMux()}
	return a.srv.Serve(ln)
}

// Shutdown gracefully stops the API server.
func (a *APIServer) Shutdown(ctx context.Context) error {
	if a.srv == nil {
		return nil
	}
	return a.srv.Shutdown(ctx)
}

func (a *APIServer) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		next.ServeHTTP(w, r)
	})
}

func (a *APIServer) rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		if !a.rl.Allow(ip) {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func clientIP(r *http.Request) string {
	ip := r.RemoteAddr
	if colon := strings.LastIndex(ip, ":"); colon >= 0 {
		ip = ip[:colon]
	}
	return ip
}

// GET /query/{uuid}?app_id=<appID>
func (a *APIServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	recordID := strings.TrimPrefix(r.URL.Path, "/query/")
	if recordID == "" {
		http.Error(w, "missing uuid", http.StatusBadRequest)
		return
	}
	appID := r.URL.Query().Get("app_id")
	if appID == "" {
		http.Error(w, "missing app_id", http.StatusBadRequest)
		return
	}
	rec, err := a.layer.Query(appID, recordID)
	if err != nil {
		if err == ErrNotFound {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(rec)
}

// GET /peers
func (a *APIServer) handlePeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	peers := a.transport.Peers()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(peers)
}

// POST /sync
func (a *APIServer) handleSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	height, _ := a.store.Height()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]int64{"height": height})
}
