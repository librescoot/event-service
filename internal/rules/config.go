// Package rules turns the TOML files a user drops in the extensions directory
// into compiled rules the engine can match events against.
package rules

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/BurntSushi/toml"
)

// Config is the merged content of every file in the extensions directory.
type Config struct {
	Rules []RuleConfig `toml:"rule"`
}

// RuleConfig is one [[rule]] block as written on disk. It is the wire format,
// not the runtime shape: Compile turns it into a Rule.
type RuleConfig struct {
	Name        string       `toml:"name"`
	On          []string     `toml:"on"`
	When        string       `toml:"when"`
	Cooldown    string       `toml:"cooldown"`
	Debounce    string       `toml:"debounce"`
	Enabled     *bool        `toml:"enabled"`
	Steps       []StepConfig `toml:"step"`
	Concurrency string       `toml:"concurrency"`
	CancelOn    []string     `toml:"cancel-on"`
	Repeat      map[string]any `toml:"repeat"`

	// Source names the file this rule came from, so an error message can tell
	// the user which of their files to go and fix.
	Source string `toml:"-"`
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

// Load reads every *.toml in dir. A file that fails to parse yields an error
// in the returned slice and is skipped; the rest still load, because losing
// every rule over one typo in one file is worse than running a subset.
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
	// Deterministic order so rule numbering in logs is stable across boots.
	sort.Strings(names)

	var errs []error
	for _, name := range names {
		path := filepath.Join(dir, name)
		var fileCfg Config
		if _, err := toml.DecodeFile(path, &fileCfg); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			continue
		}
		for i := range fileCfg.Rules {
			fileCfg.Rules[i].Source = name
		}
		cfg.Rules = append(cfg.Rules, fileCfg.Rules...)
	}

	return cfg, errs
}
