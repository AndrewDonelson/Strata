package strata

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const defaultInvalidationChannel = "strata:invalidate"

// invalidationMsg is the Redis pub/sub payload for L1 invalidation.
type invalidationMsg struct {
	Schema string `json:"schema"`
	ID     string `json:"id"`
	Op     string `json:"op"` // "set" | "delete" | "invalidate_all"
}

// dirtyEntry holds a value pending write-behind flush to L3.
type dirtyEntry struct {
	schemaName string
	id         string
	value      any
	retries    int
	lastErr    error
}

// syncEngine manages L1 invalidation (Redis pub/sub) and write-behind flushing.
type syncEngine struct {
	ds         *DataStore
	dirtyMu    sync.Mutex
	dirty      map[string]*dirtyEntry
	dirtyCount atomic.Int64
	stopCh     chan struct{}
	flushCh    chan struct{}
	wg         sync.WaitGroup
}

func newSyncEngine(ds *DataStore) *syncEngine {
	return &syncEngine{
		ds:      ds,
		dirty:   make(map[string]*dirtyEntry),
		stopCh:  make(chan struct{}),
		flushCh: make(chan struct{}, 1),
	}
}

func (se *syncEngine) start() {
	if se.ds.l2 != nil {
		se.wg.Add(1)
		go se.subscribeLoop()
	}
	if se.ds.cfg.DefaultWriteMode == WriteBehind {
		se.wg.Add(1)
		go se.writeBehindLoop()
	}
}

func (se *syncEngine) stop() {
	close(se.stopCh)
	se.wg.Wait()
}

func (se *syncEngine) publishInvalidation(ctx context.Context, schema, id, op string) {
	if se.ds.l2 == nil {
		return
	}
	msg := invalidationMsg{Schema: schema, ID: id, Op: op}
	b, _ := json.Marshal(msg)
	ch := se.ds.cfg.InvalidationChannel
	if ch == "" {
		ch = defaultInvalidationChannel
	}
	_ = se.ds.l2.Publish(ctx, ch, b)
}

func (se *syncEngine) subscribeLoop() {
	defer se.wg.Done()
	ch := se.ds.cfg.InvalidationChannel
	if ch == "" {
		ch = defaultInvalidationChannel
	}
	for {
		select {
		case <-se.stopCh:
			return
		default:
		}
		ctx, cancel := context.WithCancel(context.Background())
		sub := se.ds.l2.Subscribe(ctx, ch)
		func() {
			defer cancel()
			msgCh := sub.Channel()
			for {
				select {
				case <-se.stopCh:
					_ = sub.Close()
					return
				case msg, ok := <-msgCh:
					if !ok {
						return
					}
					se.handleInvalidation(msg.Payload)
				}
			}
		}()
		select {
		case <-se.stopCh:
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (se *syncEngine) handleInvalidation(payload string) {
	var msg invalidationMsg
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		if se.ds.logger != nil {
			se.ds.logger.Warn("strata: malformed invalidation message", "payload", payload, "err", err)
		}
		return
	}
	if se.ds.l1 == nil {
		return
	}
	switch msg.Op {
	case "set", "delete":
		se.ds.l1.Delete(fmt.Sprintf("%s:%s", msg.Schema, msg.ID))
	case "invalidate_all":
		se.ds.l1.FlushSchema(msg.Schema + ":")
	}
}

func (se *syncEngine) queueDirty(schemaName, id string, value any) {
	key := schemaName + ":" + id
	se.dirtyMu.Lock()
	se.dirty[key] = &dirtyEntry{schemaName: schemaName, id: id, value: value}
	count := int64(len(se.dirty))
	se.dirtyMu.Unlock()
	se.dirtyCount.Store(count)

	threshold := se.ds.cfg.WriteBehindFlushThreshold
	if threshold == 0 {
		threshold = 100
	}
	if int(count) >= threshold {
		select {
		case se.flushCh <- struct{}{}:
		default:
		}
	}
}

func (se *syncEngine) writeBehindLoop() {
	defer se.wg.Done()
	interval := se.ds.cfg.WriteBehindFlushInterval
	if interval == 0 {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-se.stopCh:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = se.flushDirty(ctx)
			cancel()
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = se.flushDirty(ctx)
			cancel()
		case <-se.flushCh:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = se.flushDirty(ctx)
			cancel()
		}
	}
}

func (se *syncEngine) flushDirty(ctx context.Context) error {
	se.dirtyMu.Lock()
	if len(se.dirty) == 0 {
		se.dirtyMu.Unlock()
		return nil
	}
	snapshot := se.dirty
	se.dirty = make(map[string]*dirtyEntry, len(snapshot))
	se.dirtyCount.Store(0)
	se.dirtyMu.Unlock()

	maxRetries := se.ds.cfg.WriteBehindMaxRetry
	if maxRetries == 0 {
		maxRetries = 5
	}
	var failed []*dirtyEntry
	for _, entry := range snapshot {
		if entry.retries >= maxRetries {
			cs, err := se.ds.registry.get(entry.schemaName)
			if err == nil && cs.Hooks.OnWriteError != nil {
				cs.Hooks.OnWriteError(ctx, entry.id, entry.lastErr)
			}
			if se.ds.logger != nil {
				se.ds.logger.Error("strata: write-behind max retries exceeded",
					"schema", entry.schemaName, "id", entry.id)
			}
			continue
		}
		cs, err := se.ds.registry.get(entry.schemaName)
		if err != nil {
			continue
		}
		if err := se.ds.writeToL3(ctx, cs, entry.value); err != nil {
			entry.retries++
			entry.lastErr = err
			failed = append(failed, entry)
		}
	}
	if len(failed) > 0 {
		se.dirtyMu.Lock()
		for _, e := range failed {
			se.dirty[e.schemaName+":"+e.id] = e
		}
		se.dirtyCount.Store(int64(len(se.dirty)))
		se.dirtyMu.Unlock()
	}
	return nil
}

// FlushDirty blocks until all write-behind entries are flushed to L3.
func (ds *DataStore) FlushDirty(ctx context.Context) error {
	if ds.sync == nil {
		return nil
	}
	return ds.sync.flushDirty(ctx)
}
