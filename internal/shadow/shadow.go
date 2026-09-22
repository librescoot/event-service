// Package shadow keeps a local copy of the datastore hashes the adapter
// watches.
//
// It exists because the Librescoot hash pattern publishes only the field name
// that changed, never the value and never the previous value. Every derived
// event that reports a transition needs a "from", and the only way to have one
// is to remember what was there before.
//
// It also suppresses redundant work: HashPublisher.Set publishes on every
// write whether or not the value actually changed, so a meaningful fraction of
// incoming notifications carry no news.
package shadow

import (
	"sort"
	"sync"
)

// Store is safe for concurrent use. The adapter reads it from rule-guard
// evaluation while the watcher goroutine writes it.
type Store struct {
	mu     sync.RWMutex
	hashes map[string]map[string]string
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{hashes: make(map[string]map[string]string)}
}

// Observe records value for hash/field and reports what was there before.
//
// changed is false only when the field was already set to exactly this value.
// A first observation reports changed=true with an empty prev, so callers can
// distinguish "new field" from "unchanged" and decide for themselves whether
// an initial value is worth an event.
func (s *Store) Observe(hash, field, value string) (prev string, changed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fields, ok := s.hashes[hash]
	if !ok {
		fields = make(map[string]string)
		s.hashes[hash] = fields
	}
	prev, existed := fields[field]
	if existed && prev == value {
		return prev, false
	}
	fields[field] = value
	return prev, true
}

// Change is one observed field change in a batch.
type Change struct {
	Field, From, To string
}

// ObserveBatch installs an atomic selected-field snapshot before returning any
// changes to publish. Invalid oversized values are cleared without fake events.
func (s *Store) ObserveBatch(hash string, values map[string]string, invalid []string) []Change {
	keys := make([]string, 0, len(values))
	for field := range values {
		keys = append(keys, field)
	}
	sort.Strings(keys)
	s.mu.Lock()
	defer s.mu.Unlock()
	fields := s.hashes[hash]
	if fields == nil {
		fields = make(map[string]string)
		s.hashes[hash] = fields
	}
	for _, field := range invalid {
		fields[field] = ""
	}
	var changes []Change
	for _, field := range keys {
		value := values[field]
		previous, existed := fields[field]
		fields[field] = value
		if !existed || previous != value {
			changes = append(changes, Change{Field: field, From: previous, To: value})
		}
	}
	return changes
}

// Get returns the last observed value, or empty if the field is unknown.
func (s *Store) Get(hash, field string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hashes[hash][field]
}

// Snapshot returns an immutable lookup for a dry run, so multiple conditions
// cannot observe different revisions while the adapter receives updates.
func (s *Store) Snapshot() func(string, string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := make(map[string]map[string]string, len(s.hashes))
	for hash, fields := range s.hashes {
		copyFields := make(map[string]string, len(fields))
		for field, value := range fields {
			copyFields[field] = value
		}
		values[hash] = copyFields
	}
	return func(hash, field string) string { return values[hash][field] }
}

// Seed installs current values without reporting them as changes. The adapter
// calls this at startup after HGETALL so that the first real notification
// produces a correct "from" instead of an empty one.
func (s *Store) Seed(hash string, fields map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dst, ok := s.hashes[hash]
	if !ok {
		dst = make(map[string]string, len(fields))
		s.hashes[hash] = dst
	}
	for k, v := range fields {
		dst[k] = v
	}
}
