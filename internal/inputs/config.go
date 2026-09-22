// Package inputs describes opt-in adapters for existing datastore traffic.
package inputs

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	MaxPerRule = 16
	MaxTotal   = 64
	MaxFields  = 64
	MaxBytes   = 65536
)

// Config selects one exact notification source. A hash without a topic is a
// state-only dependency; a channel always needs a topic. Fields are explicit
// so subscribing to a hash does not implicitly publish its hot telemetry.
type Config struct {
	Hash     string   `toml:"hash"`
	Fields   []string `toml:"fields"`
	Channel  string   `toml:"channel"`
	Topic    string   `toml:"topic"`
	Format   string   `toml:"format"`
	MaxBytes int      `toml:"max-bytes"`
}

func (c Config) Limit() int {
	if c.MaxBytes == 0 {
		return 4096
	}
	return c.MaxBytes
}

func exactName(s string) bool {
	return s != "" && len(s) <= 128 && utf8.ValidString(s) && !strings.ContainsAny(s, "*?[]\\") &&
		!strings.ContainsFunc(s, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) })
}

func (c Config) Validate() error {
	if (c.Hash == "") == (c.Channel == "") {
		return fmt.Errorf("input requires exactly one of hash or channel")
	}
	name := c.Hash
	if c.Channel != "" {
		name = c.Channel
	}
	if !exactName(name) || strings.HasPrefix(name, "ev:") || strings.HasPrefix(name, "__key") {
		return fmt.Errorf("input source must be an exact name outside ev: and Redis key-notification namespaces")
	}
	if c.MaxBytes < 0 || c.MaxBytes > MaxBytes {
		return fmt.Errorf("input max-bytes must be 0..%d (0 uses the default)", MaxBytes)
	}
	if c.Topic != "" {
		if !strings.HasPrefix(c.Topic, "input.") || !exactName(c.Topic) || strings.Contains(c.Topic, ":") {
			return fmt.Errorf("input topic must be an exact topic with the input. prefix")
		}
		for _, part := range strings.Split(c.Topic, ".") {
			if part == "" {
				return fmt.Errorf("input topic has an empty segment")
			}
		}
	}
	if c.Hash != "" {
		if len(c.Fields) == 0 || len(c.Fields) > MaxFields || c.Format != "" {
			return fmt.Errorf("hash input requires 1..%d fields and no format", MaxFields)
		}
		seen := make(map[string]bool)
		for _, field := range c.Fields {
			if !exactName(field) || seen[field] {
				return fmt.Errorf("input field must be an exact, unique name")
			}
			seen[field] = true
		}
	} else if len(c.Fields) != 0 || c.Topic == "" || (c.Format != "" && c.Format != "string" && c.Format != "json") {
		return fmt.Errorf("channel input needs a topic, no fields, and string or json format")
	}
	return nil
}
