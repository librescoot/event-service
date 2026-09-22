package inputs

import (
	"encoding/json"
	"fmt"
	"sort"
)

// MaxStateBytes bounds the configured field-value footprint for selected
// watches. Built-in hash watchers retain their existing behavior.
const MaxStateBytes = 1024 * 1024

// Clone detaches declarations and their field selections from mutable config.
func Clone(configs []Config) []Config {
	if configs == nil {
		return nil
	}
	out := append([]Config(nil), configs...)
	for i := range out {
		out[i].Fields = append([]string(nil), configs[i].Fields...)
	}
	return out
}

// ValidateSet checks aggregate subscription and selected-state budgets as well
// as conflicting interpretations of a notification channel.
func ValidateSet(configs []Config) error {
	kinds := make(map[string]string)
	seen := make(map[string]bool)
	fields := make(map[string]map[string]int)
	for _, c := range configs {
		if err := c.Validate(); err != nil {
			return err
		}
		name, kind := c.Hash, "hash"
		if c.Channel != "" {
			name, kind = c.Channel, "channel"
		}
		if previous, ok := kinds[name]; ok && previous != kind {
			return fmt.Errorf("input %q is both a hash and a raw channel", name)
		}
		kinds[name] = kind
		normalized := c
		normalized.Fields = append([]string(nil), c.Fields...)
		sort.Strings(normalized.Fields)
		normalized.MaxBytes = c.Limit()
		if c.Channel != "" && c.Format == "" {
			normalized.Format = "string"
		}
		key, _ := json.Marshal(normalized)
		seen[string(key)] = true
		if c.Hash == "" {
			continue
		}
		if fields[c.Hash] == nil {
			fields[c.Hash] = make(map[string]int)
		}
		for _, field := range c.Fields {
			if c.Limit() > fields[c.Hash][field] {
				fields[c.Hash][field] = c.Limit()
			}
		}
	}
	if len(seen) > MaxTotal {
		return fmt.Errorf("at most %d distinct configured inputs", MaxTotal)
	}
	bytes := 0
	for hash, selected := range fields {
		if len(selected) > MaxFields {
			return fmt.Errorf("hash %q exceeds %d selected fields", hash, MaxFields)
		}
		for _, limit := range selected {
			bytes += limit
		}
	}
	if bytes > MaxStateBytes {
		return fmt.Errorf("selected input state exceeds %d-byte budget", MaxStateBytes)
	}
	return nil
}
