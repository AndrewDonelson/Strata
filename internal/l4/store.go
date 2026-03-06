// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// store.go -- BoltDB-backed persistence for L4 ledger mode + in-memory store
// for peer mode and tests.

package l4

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// L4Store persists the block chain (ledger mode) or buffers records (peer mode).
type L4Store interface {
	Put(record L4Record) error
	Get(appID, uuid string) (*L4Record, error)
	GetByHash(hash string) (*L4Record, error)
	Latest(appID string, limit int) ([]L4Record, error)
	Height() (int64, error)
	Close() error
}

const bucketRecords = "records"
const bucketByHash  = "by_hash"

// ---------------------------------------------------------------------------
// boltStore
// ---------------------------------------------------------------------------

type boltStore struct {
	db  *bolt.DB
	mu  sync.RWMutex
	cnt int64
}

// NewBoltStore opens or creates a BoltDB file at dataDir/l4.db.
func NewBoltStore(dataDir string) (L4Store, error) {
	path := filepath.Join(dataDir, "l4.db")
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("l4: open bolt store %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketRecords))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(bucketByHash))
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("l4: init bolt buckets: %w", err)
	}
	var cnt int64
	_ = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketRecords))
		if b != nil {
			cnt = int64(b.Stats().KeyN)
		}
		return nil
	})
	return &boltStore{db: db, cnt: cnt}, nil
}

func storeKey(appID, uuid string) []byte { return []byte(appID + ":" + uuid) }

func (s *boltStore) Put(record L4Record) error {
	key := storeKey(record.AppID, record.UUID)
	return s.db.Update(func(tx *bolt.Tx) error {
		recs := tx.Bucket([]byte(bucketRecords))
		if recs.Get(key) != nil {
			return ErrAlreadyExists
		}
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("l4: marshal: %w", err)
		}
		if err := recs.Put(key, data); err != nil {
			return err
		}
		if record.Hash != "" {
			_ = tx.Bucket([]byte(bucketByHash)).Put([]byte(record.Hash), key)
		}
		s.mu.Lock()
		s.cnt++
		s.mu.Unlock()
		return nil
	})
}

func (s *boltStore) Get(appID, uuid string) (*L4Record, error) {
	var rec L4Record
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket([]byte(bucketRecords)).Get(storeKey(appID, uuid))
		if data == nil {
			return ErrNotFound
		}
		return json.Unmarshal(data, &rec)
	})
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *boltStore) GetByHash(hash string) (*L4Record, error) {
	var rec L4Record
	err := s.db.View(func(tx *bolt.Tx) error {
		key := tx.Bucket([]byte(bucketByHash)).Get([]byte(hash))
		if key == nil {
			return ErrNotFound
		}
		data := tx.Bucket([]byte(bucketRecords)).Get(key)
		if data == nil {
			return ErrNotFound
		}
		return json.Unmarshal(data, &rec)
	})
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *boltStore) Latest(appID string, limit int) ([]L4Record, error) {
	prefix := []byte(appID + ":")
	var results []L4Record
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketRecords))
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil; k, v = c.Next() {
			if len(k) < len(prefix) {
				break
			}
			match := true
			for i, ch := range prefix {
				if k[i] != ch {
					match = false
					break
				}
			}
			if !match {
				break
			}
			var rec L4Record
			if json.Unmarshal(v, &rec) == nil {
				results = append(results, rec)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Timestamp > results[j].Timestamp })
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

func (s *boltStore) Height() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cnt, nil
}

func (s *boltStore) Close() error { return s.db.Close() }

func (s *boltStore) updateRecord(record L4Record) error {
	key := storeKey(record.AppID, record.UUID)
	return s.db.Update(func(tx *bolt.Tx) error {
		data, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return tx.Bucket([]byte(bucketRecords)).Put(key, data)
	})
}

// ---------------------------------------------------------------------------
// memStore -- in-process store for tests and peer mode
// ---------------------------------------------------------------------------

type memStore struct {
	mu      sync.RWMutex
	records map[string]L4Record
	byHash  map[string]string
}

// NewMemStore returns an in-memory L4Store.
func NewMemStore() L4Store {
	return &memStore{records: make(map[string]L4Record), byHash: make(map[string]string)}
}

func (m *memStore) Put(record L4Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := record.AppID + ":" + record.UUID
	if _, ok := m.records[key]; ok {
		return ErrAlreadyExists
	}
	m.records[key] = record
	if record.Hash != "" {
		m.byHash[record.Hash] = key
	}
	return nil
}

func (m *memStore) Get(appID, uuid string) (*L4Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.records[appID+":"+uuid]
	if !ok {
		return nil, ErrNotFound
	}
	cp := rec
	return &cp, nil
}

func (m *memStore) GetByHash(hash string) (*L4Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key, ok := m.byHash[hash]
	if !ok {
		return nil, ErrNotFound
	}
	rec, ok := m.records[key]
	if !ok {
		return nil, ErrNotFound
	}
	cp := rec
	return &cp, nil
}

func (m *memStore) Latest(appID string, limit int) ([]L4Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	prefix := appID + ":"
	var results []L4Record
	for k, v := range m.records {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			cp := v
			results = append(results, cp)
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Timestamp > results[j].Timestamp })
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

func (m *memStore) Height() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.records)), nil
}

func (m *memStore) Close() error { return nil }

func (m *memStore) update(record L4Record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[record.AppID+":"+record.UUID] = record
}
