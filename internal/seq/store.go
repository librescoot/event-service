package seq

import (
	"encoding/json"
	"fmt"

	"github.com/librescoot/eventbus"
)

// PendingHash is the datastore hash durable steps are recorded in. One field
// per waiting step, keyed by the run's id.
const PendingHash = "extensions:pending"

// Hasher is the datastore surface the pending store needs. Depending on three
// operations rather than a whole client keeps the store testable, and keeps
// the package free of the datastore library.
type Hasher interface {
	HSet(key, field string, value any) error
	HGetAll(key string) (map[string]string, error)
	HDel(key string, fields ...string) error
}

// Pending is one step waiting out its after delay, written down so a service
// restart during the wait does not lose it. Rule and Step are what replay
// needs to find the step again; Iter is the repeat pass the run was on, so a
// replayed run finishes the passes it had left. Event is the trigger, because
// a step's when is evaluated against whatever fired its own run.
//
// FireAt is when the step is due, not when it was recorded: replay works out
// the remaining delay from it, and drops a record too far past due to be
// worth running.
type Pending struct {
	ID     string         `json:"id"`
	Rule   string         `json:"rule"`
	Source string         `json:"source"`
	Step   int            `json:"step"`
	Iter   int            `json:"iter"`
	FireAt int64          `json:"fire-at"` // unix milliseconds
	Event  eventbus.Event `json:"event"`
}

// PendingStore reads and writes the pending records. There is no sweep and no
// expiry: a record is written when a durable step is scheduled and removed
// when it resolves, so a scooter with nothing running writes nothing at all.
type PendingStore struct {
	c    Hasher
	log  Logger
	hash string
}

// NewPendingStore returns a store over the standard hash.
func NewPendingStore(c Hasher, log Logger) *PendingStore {
	return newPendingStoreIn(c, log, PendingHash)
}

// newPendingStoreIn is the same over a caller-chosen hash, which is how tests
// keep out of each other's way.
func newPendingStoreIn(c Hasher, log Logger, hash string) *PendingStore {
	return &PendingStore{c: c, log: log, hash: hash}
}

// Put writes a record. It overwrites whatever the run's id held before, which
// is what a run reaching its second durable step wants: one run has at most
// one step waiting at a time.
func (s *PendingStore) Put(p Pending) error {
	if p.ID == "" {
		return fmt.Errorf("pending record needs an id")
	}
	b, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("encode pending %s: %w", p.ID, err)
	}
	if err := s.c.HSet(s.hash, p.ID, string(b)); err != nil {
		return fmt.Errorf("record pending %s: %w", p.ID, err)
	}
	return nil
}

// Drop removes a record. Removing one that is not there is not an error: the
// step that fires and the cancel that arrives at the same moment both aim at
// the same field, and only one of them can be first.
func (s *PendingStore) Drop(id string) error {
	if id == "" {
		return nil
	}
	if err := s.c.HDel(s.hash, id); err != nil {
		return fmt.Errorf("remove pending %s: %w", id, err)
	}
	return nil
}

// Load reads every record. A value that will not decode is logged and removed
// rather than returned or fatal: whatever wrote it is not going to write it
// better on the next boot, and leaving it in place turns one bad record into
// one log line per start forever.
func (s *PendingStore) Load() ([]Pending, error) {
	raw, err := s.c.HGetAll(s.hash)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", s.hash, err)
	}

	out := make([]Pending, 0, len(raw))
	for id, v := range raw {
		var p Pending
		if err := json.Unmarshal([]byte(v), &p); err != nil {
			s.log.Printf("pending %s: unreadable record dropped: %v", id, err)
			if err := s.Drop(id); err != nil {
				s.log.Printf("pending: %v", err)
			}
			continue
		}
		// The field name is the id that Drop has to name later, so it wins
		// over whatever the value claims: a record whose two ids disagree is
		// still removable.
		p.ID = id
		out = append(out, p)
	}
	return out, nil
}
