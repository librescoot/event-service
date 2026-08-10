// Package rules turns the TOML files a user drops in the extensions directory
// into compiled rules the engine can match events against.
package rules

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
)

// Config is the merged content of every file in the extensions directory.
type Config struct {
	Rules []RuleConfig `toml:"rule"`
}

// RuleConfig is one [[rule]] block as written on disk. It is the wire format,
// not the runtime shape: Compile turns it into a Rule.
type RuleConfig struct {
	Name        string        `toml:"name"`
	On          []string      `toml:"on"`
	When        string        `toml:"when"`
	Cooldown    string        `toml:"cooldown"`
	Enabled     *bool         `toml:"enabled"`
	Steps       []StepConfig  `toml:"step"`
	Concurrency string        `toml:"concurrency"`
	CancelOn    []string      `toml:"cancel-on"`
	Repeat      *RepeatConfig `toml:"repeat"`

	// Debounce is a pointer because a duration has no nil of its own, and
	// Compile has to tell "debounce was never written" from "debounce was
	// written as an empty duration like 0s": both parse to the same zero
	// time.Duration, but only the former is silence rather than a rule
	// author believing the feature works.
	Debounce *string `toml:"debounce"`

	// Source names the file this rule came from, so an error message can tell
	// the user which of their files to go and fix.
	Source string `toml:"-"`
}

// RepeatConfig is the wire format of a rule's repeat key. It is a typed
// struct rather than map[string]any so that a typo like repeat = { conut = 3
// } is a load error naming the file: Load's md.Undecoded() check only sees an
// unclaimed key when the destination is a struct field it can fail to find, a
// map absorbs anything written into it with no trace of what was meant.
type RepeatConfig struct {
	Count int    `toml:"count"`
	Every string `toml:"every"`
}

// StepConfig is one [[rule.step]] block. The fields are a union across action
// types; which ones are meaningful depends on Do.
type StepConfig struct {
	Do    string `toml:"do"`
	After string `toml:"after"`
	When  string `toml:"when"`

	// redis
	List string `toml:"list"`
	Push string `toml:"push"`

	// exec
	Command string `toml:"command"`
	Timeout string `toml:"timeout"`
}

// Load reads every *.toml in dir. A file that fails to parse, or that
// contains a key nothing in RuleConfig or StepConfig recognises, yields an
// error in the returned slice and is skipped; the rest still load, because
// losing every rule over one typo in one file is worse than running a
// subset.
//
// A missing directory is not an error: no extensions installed is the normal
// state of a scooter.
func Load(dir string) (*Config, []error) {
	cfg := &Config{}

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, []error{fmt.Errorf("read extensions dir %s: %w", dir, err)}
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".toml" {
			continue
		}
		names = append(names, e.Name())
	}
	// os.ReadDir already guarantees alphabetical order per the Go spec. The
	// explicit sort here is insurance: if the directory read is swapped for a
	// different approach later, this line ensures the promise to users (rules
	// load in filename order) still holds. Do not remove.
	sort.Strings(names)

	var errs []error
	for _, name := range names {
		path := filepath.Join(dir, name)
		var fileCfg Config
		md, err := toml.DecodeFile(path, &fileCfg)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			continue
		}

		// toml.DecodeFile does not reject unknown keys on its own: a typo'd
		// field name or a made-up one just fails to land anywhere and is
		// dropped without a trace. md.Undecoded() is the list of keys the
		// file mentioned that the struct never claimed, which is exactly
		// what a typo or an unsupported feature written in good faith looks
		// like. Reject the whole file rather than trying to salvage the
		// rules in it, the same way a genuinely malformed file is rejected
		// above, so the user is not left thinking a key took effect when it
		// was silently ignored.
		if undecoded := md.Undecoded(); len(undecoded) > 0 {
			keys := make([]string, len(undecoded))
			for i, k := range undecoded {
				keys[i] = k.String()
			}
			errs = append(errs, fmt.Errorf("%s: unknown key(s): %s", name, strings.Join(keys, ", ")))
			continue
		}

		for i := range fileCfg.Rules {
			fileCfg.Rules[i].Source = name
		}
		cfg.Rules = append(cfg.Rules, fileCfg.Rules...)
	}

	return cfg, errs
}
