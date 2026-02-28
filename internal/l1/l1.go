// Package l1 provides a sharded, concurrent in-memory cache with TTL and eviction.
package l1

import (
	"container/list"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AndrewDonelson/strata/internal/clock"
)

const numShards = 256

// EvictionPolicy determines which entry is removed when MaxEntries is reached.
type EvictionPolicy int

const (
	LRU  EvictionPolicy = iota // Least Recently Used
	LFU                        // Least Frequently Used
	FIFO                       // First In, First Out
)

// Options configures an L1 Store.
type Options struct {
	TTL           time.Duration
	MaxEntries    int
	Eviction      EvictionPolicy
	SweepInterval time.Duration
	Clock         clock.Clock
	OnEvict       func(key string, value any)
}

// entry holds a cached value and metadata.
type entry struct {
	key       string
	value     any
	expiresAt time.Time
	freq      int
	elem      *list.Element
}

// shard is one partition of the cache.
type shard struct {
	mu         sync.RWMutex
	items      map[string]*entry
	evictList  *list.List
	maxEntries int
	policy     EvictionPolicy
	onEvict    func(key string, value any)
}

// Store is the sharded in-memory cache.
type Store struct {
	shards [numShards]*shard
	opts   Options
	clock  clock.Clock
	hits   atomic.Int64
	misses atomic.Int64
	stopCh chan struct{}
}

// New creates a new L1 Store.
func New(opts Options) *Store {
	if opts.Clock == nil {
		opts.Clock = clock.Real{}
	}
	if opts.SweepInterval == 0 {
		opts.SweepInterval = 30 * time.Second
	}
	s := &Store{opts: opts, clock: opts.Clock, stopCh: make(chan struct{})}
	for i := 0; i < numShards; i++ {
		s.shards[i] = &shard{
			items:      make(map[string]*entry),
			evictList:  list.New(),
			maxEntries: opts.MaxEntries,
			policy:     opts.Eviction,
			onEvict:    opts.OnEvict,
		}
	}
	go s.sweepLoop()
	return s
}

func (s *Store) getShard(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return s.shards[h.Sum32()%numShards]
}

// Set stores value under key with an optional TTL.
func (s *Store) Set(key string, value any, ttl time.Duration) {
	if ttl == 0 {
		ttl = s.opts.TTL
	}
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = s.clock.Now().Add(ttl)
	}

	if e, ok := sh.items[key]; ok {
		e.value = value
		e.expiresAt = expiresAt
		e.freq++
		if sh.policy != LFU {
			sh.evictList.MoveToFront(e.elem)
		}
		return
	}

	if sh.maxEntries > 0 && len(sh.items) >= sh.maxEntries {
		sh.evict()
	}

	e := &entry{key: key, value: value, expiresAt: expiresAt, freq: 1}
	switch sh.policy {
	case LRU, FIFO:
		e.elem = sh.evictList.PushFront(e)
	case LFU:
		e.elem = sh.evictList.PushBack(e)
	}
	sh.items[key] = e
}

// Get retrieves a value by key.
func (s *Store) Get(key string) (any, bool) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.items[key]
	if !ok {
		s.misses.Add(1)
		return nil, false
	}
	if !e.expiresAt.IsZero() && s.clock.Now().After(e.expiresAt) {
		sh.removeEntry(e)
		s.misses.Add(1)
		return nil, false
	}
	e.freq++
	if sh.policy == LRU {
		sh.evictList.MoveToFront(e.elem)
	}
	s.hits.Add(1)
	return e.value, true
}

// Delete removes a key from the cache.
func (s *Store) Delete(key string) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if e, ok := sh.items[key]; ok {
		sh.removeEntry(e)
	}
}

// Flush removes all entries from all shards.
func (s *Store) Flush() {
	for i := 0; i < numShards; i++ {
		sh := s.shards[i]
		sh.mu.Lock()
		sh.items = make(map[string]*entry)
		sh.evictList.Init()
		sh.mu.Unlock()
	}
}

// FlushSchema removes all entries whose key starts with prefix.
func (s *Store) FlushSchema(prefix string) {
	for i := 0; i < numShards; i++ {
		sh := s.shards[i]
		sh.mu.Lock()
		for k, e := range sh.items {
			if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
				sh.removeEntry(e)
			}
		}
		sh.mu.Unlock()
	}
}

// EntryMeta holds metadata returned by GetWithMeta.
type EntryMeta struct {
	Value        any
	TTLRemaining time.Duration
	HitCount     int
}

// GetWithMeta retrieves a value and its metadata.
func (s *Store) GetWithMeta(key string) (EntryMeta, bool) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.items[key]
	if !ok {
		s.misses.Add(1)
		return EntryMeta{}, false
	}
	now := s.clock.Now()
	if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
		sh.removeEntry(e)
		s.misses.Add(1)
		return EntryMeta{}, false
	}
	e.freq++
	if sh.policy == LRU {
		sh.evictList.MoveToFront(e.elem)
	}
	var remaining time.Duration
	if !e.expiresAt.IsZero() {
		remaining = e.expiresAt.Sub(now)
	}
	s.hits.Add(1)
	return EntryMeta{Value: e.value, TTLRemaining: remaining, HitCount: e.freq}, true
}

// Stats holds hit/miss/entry counts.
type Stats struct {
	Hits    int64
	Misses  int64
	Entries int64
}

// Stats returns current statistics.
func (s *Store) Stats() Stats {
	var total int64
	for i := 0; i < numShards; i++ {
		sh := s.shards[i]
		sh.mu.RLock()
		total += int64(len(sh.items))
		sh.mu.RUnlock()
	}
	return Stats{Hits: s.hits.Load(), Misses: s.misses.Load(), Entries: total}
}

// Close stops background goroutines.
func (s *Store) Close() {
	close(s.stopCh)
}

func (s *Store) sweepLoop() {
	ticker := time.NewTicker(s.opts.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.sweep()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Store) sweep() {
	now := s.clock.Now()
	for i := 0; i < numShards; i++ {
		sh := s.shards[i]
		sh.mu.Lock()
		for _, e := range sh.items {
			if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
				sh.removeEntry(e)
			}
		}
		sh.mu.Unlock()
	}
}

func (sh *shard) evict() {
	switch sh.policy {
	case LRU, FIFO:
		if back := sh.evictList.Back(); back != nil {
			sh.removeEntry(back.Value.(*entry))
		}
	case LFU:
		var minEntry *entry
		for _, e := range sh.items {
			if minEntry == nil || e.freq < minEntry.freq {
				minEntry = e
			}
		}
		if minEntry != nil {
			sh.removeEntry(minEntry)
		}
	}
}

func (sh *shard) removeEntry(e *entry) {
	delete(sh.items, e.key)
	if e.elem != nil {
		sh.evictList.Remove(e.elem)
	}
	if sh.onEvict != nil {
		sh.onEvict(e.key, e.value)
	}
}
