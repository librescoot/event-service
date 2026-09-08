// Package control manages desired extension configuration, never live execution.
package control

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"unicode"
	"unicode/utf8"

	"github.com/BurntSushi/toml"
	"github.com/librescoot/event-service/api"
	"github.com/librescoot/event-service/internal/rules"
)

const (
	maxEntries    = 128
	maxFileBytes  = 64 * 1024
	maxTotalBytes = 1024 * 1024
	maxRules      = 512
	maxSteps      = 128
	overrideFile  = ".enabled.json"
)

type Manager struct {
	mu       sync.Mutex
	dir      string
	snapshot func() rules.StateFunc
	info     func(string) api.RuleSummary
	status   func() api.StatusResponse
	applied  string
}

type catalogue struct {
	configs         []rules.RuleConfig
	diagnostics     []string
	perRule         [][]string
	overrides       map[string]bool
	revision        string
	unsafe          bool
	overrideInvalid bool
	overrideBytes   int
	entries         int
	bytes           int
}

type enabledFile struct {
	Version int             `json:"version"`
	Rules   map[string]bool `json:"rules"`
}

func New(dir string, snapshot func() rules.StateFunc) *Manager {
	return &Manager{dir: filepath.Clean(dir), snapshot: snapshot}
}

// SetRuntime must be called before serving requests. Callbacks are read-only.
func (m *Manager) SetRuntime(info func(string) api.RuleSummary, status func() api.StatusResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.info, m.status = info, status
}

func (m *Manager) lookup() rules.StateFunc {
	if m.snapshot != nil {
		return m.snapshot()
	}
	return func(string, string) string { return "" }
}

// RuntimeConfig captures the revision the caller is about to apply. Disabled
// definitions remain present so the runtime can compile them for durable replay.
func (m *Manager) RuntimeConfig() ([]rules.RuleConfig, []error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.load()
	m.applied = c.revision
	errs := make([]error, 0, len(c.diagnostics))
	for _, d := range c.diagnostics {
		errs = append(errs, errors.New(d))
	}
	if c.overrideInvalid {
		return nil, errs
	}
	return c.configs, errs
}

func openDirectory(path string) (*os.Root, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("extensions directory must be a real directory, not a symlink")
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	actual, err := root.Stat(".")
	if err != nil || !os.SameFile(st, actual) {
		_ = root.Close()
		return nil, errors.New("extensions directory changed while opening")
	}
	return root, nil
}

func readRegular(root *os.Root, name string) ([]byte, error) {
	st, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, errors.New("must be a regular file (no symlinks)")
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	actual, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !actual.Mode().IsRegular() {
		return nil, errors.New("must be a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxFileBytes+1))
	if err != nil {
		return data, err
	}
	if len(data) > maxFileBytes {
		return data, errors.New("file exceeds 64 KiB")
	}
	return data, nil
}

func parseDefinition(data []byte) ([]rules.RuleConfig, error) {
	var cfg rules.Config
	md, err := toml.Decode(string(data), &cfg)
	if err != nil {
		return nil, err
	}
	if keys := md.Undecoded(); len(keys) != 0 {
		return nil, fmt.Errorf("unknown TOML keys: %v", keys)
	}
	return cfg.Rules, nil
}

func (m *Manager) load() *catalogue { return m.loadIgnoring("") }

func (m *Manager) loadIgnoring(ownTemp string) *catalogue {
	c := &catalogue{overrides: make(map[string]bool)}
	h := sha256.New()
	// Length prefixes make filenames, raw bytes, and error markers unambiguous.
	hash := func(kind, name string, data []byte) {
		_, _ = fmt.Fprintf(h, "%d:%s%d:%s%d:", len(kind), kind, len(name), name, len(data))
		h.Write(data)
	}
	defer func() {
		c.revision = hex.EncodeToString(h.Sum(nil))
		if c.overrideInvalid {
			c.diagnostics = append(c.diagnostics, "enabled override authority invalid or unreadable; no runtime rules will be activated until repaired")
		}
	}()
	bad := func(name string, err error) {
		d := fmt.Sprintf("%s: %v", name, err)
		c.diagnostics = append(c.diagnostics, d)
		c.unsafe = true
		if name == overrideFile || name == "directory" {
			c.overrideInvalid = true
		}
		hash("error", name, []byte(err.Error()))
	}
	root, err := openDirectory(m.dir)
	if err != nil {
		if !os.IsNotExist(err) {
			bad("directory", err)
		}
		return c
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		bad("directory", err)
		return c
	}
	bound := maxEntries + 1
	if ownTemp != "" {
		bound++
	}
	entries, err := dir.ReadDir(bound)
	_ = dir.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		bad("directory", err)
		return c
	}
	if ownTemp != "" {
		for i, e := range entries {
			if e.Name() == ownTemp {
				entries = append(entries[:i], entries[i+1:]...)
				break
			}
		}
	}
	c.entries = len(entries)
	if len(entries) > maxEntries {
		bad("directory", errors.New("exceeds 128 entries"))
		return c
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	// Presence without a successful read is not equivalent to no overrides.
	// This also fails closed if an earlier aggregate limit stops the scan.
	for _, e := range entries {
		if e.Name() == overrideFile {
			c.overrideInvalid = true
		}
	}
	for _, e := range entries {
		name := e.Name()
		if name != overrideFile && filepath.Ext(name) != ".toml" {
			continue
		}
		data, err := readRegular(root, name)
		hash("file", name, data)
		c.bytes += len(data)
		if name == overrideFile {
			c.overrideBytes = len(data)
		}
		if c.bytes > maxTotalBytes {
			bad(name, errors.New("aggregate exceeds 1 MiB"))
			break
		}
		if err != nil {
			bad(name, err)
			continue
		}
		if name == overrideFile {
			var overrides enabledFile
			if err := decodeJSON(data, &overrides, "version", "rules"); err != nil {
				bad(name, err)
				continue
			}
			if overrides.Version != 1 || overrides.Rules == nil {
				bad(name, errors.New("expected version 1 and rules object"))
				continue
			}
			// A null boolean must not silently turn into false.
			var raw struct {
				Rules map[string]json.RawMessage `json:"rules"`
			}
			_ = json.Unmarshal(data, &raw)
			valid := true
			keys := make([]string, 0, len(raw.Rules))
			for key := range raw.Rules {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				value := raw.Rules[key]
				if err := validName(key); err != nil {
					bad(name, err)
					valid = false
				}
				if string(value) == "null" {
					bad(name, errors.New("enabled values must be booleans"))
					valid = false
				}
			}
			if valid {
				c.overrides = overrides.Rules
				c.overrideInvalid = false
			}
			continue
		}
		configs, err := parseDefinition(data)
		if err != nil {
			bad(name, err)
			continue
		}
		if len(c.configs)+len(configs) > maxRules {
			bad(name, errors.New("exceeds 512 rules"))
			continue
		}
		for _, cfg := range configs {
			cfg.Source = name
			if len(cfg.Steps) > maxSteps {
				bad(name, fmt.Errorf("rule %q exceeds 128 steps", cfg.Name))
				continue
			}
			c.configs = append(c.configs, cfg)
		}
	}
	lookup := m.lookup()
	counts := make(map[string]int)
	for _, cfg := range c.configs {
		counts[cfg.Name]++
	}
	for i := range c.configs {
		cfg := &c.configs[i]
		enabled := cfg.Enabled == nil || *cfg.Enabled
		if value, ok := c.overrides[cfg.Name]; ok {
			enabled = value
		}
		cfg.Enabled = &enabled
		var ds []string
		if err := validName(cfg.Name); err != nil {
			ds = append(ds, err.Error())
		}
		if counts[cfg.Name] > 1 {
			ds = append(ds, "ambiguous duplicate name")
		}
		if len(cfg.Steps) <= maxSteps {
			if _, err := rules.ValidateDefinition(*cfg, lookup); err != nil {
				ds = append(ds, err.Error())
			}
		} else {
			ds = append(ds, "exceeds 128 steps")
		}
		c.perRule = append(c.perRule, ds)
		for _, d := range ds {
			c.diagnostics = append(c.diagnostics, fmt.Sprintf("%s: rule %q: %s", cfg.Source, cfg.Name, d))
		}
	}
	return c
}

func validName(name string) error {
	if !utf8.ValidString(name) || len(name) < 1 || len(name) > 128 || strings.TrimSpace(name) != name || name == "." || name == ".." || strings.ContainsAny(name, "/\\") {
		return errors.New("name must be 1..128 bytes, trimmed, without slash/backslash, and not a dot path")
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return errors.New("name must not contain control characters")
		}
	}
	return nil
}

func decodeJSON(data []byte, dst any, required ...string) error {
	if len(data) == 0 {
		data = []byte("{}")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return errors.New("request must be a JSON object")
	}
	for _, name := range required {
		value, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("%s is required", name)
		}
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	return nil
}

func (c *catalogue) unique(name string) (int, error) {
	if err := validName(name); err != nil {
		return 0, err
	}
	found := -1
	for i, cfg := range c.configs {
		if cfg.Name != name {
			continue
		}
		if found >= 0 {
			return 0, fmt.Errorf("name %q is ambiguous", name)
		}
		found = i
	}
	if found < 0 {
		return 0, fmt.Errorf("unknown rule %q", name)
	}
	return found, nil
}

func (m *Manager) summary(c *catalogue, i int) api.RuleSummary {
	cfg := c.configs[i]
	var s api.RuleSummary
	if m.info != nil {
		s = m.info(cfg.Name)
	}
	s.Name, s.Source, s.Enabled = cfg.Name, cfg.Source, *cfg.Enabled
	s.Diagnostics = append(append([]string(nil), s.Diagnostics...), c.perRule[i]...)
	return s
}

func encodeRule(cfg rules.RuleConfig) ([]byte, error) {
	var buf bytes.Buffer
	err := toml.NewEncoder(&buf).Encode(rules.Config{Rules: []rules.RuleConfig{cfg}})
	return buf.Bytes(), err
}

func (m *Manager) Handle(ctx context.Context, method string, payload json.RawMessage) (any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(payload) > api.MaxRequestBytes {
		return nil, errors.New("request exceeds size limit")
	}
	c := m.load()
	switch method {
	case api.MethodList:
		var req api.ListRequest
		if err := decodeJSON(payload, &req); err != nil {
			return nil, err
		}
		if req.Offset < 0 || req.Limit < 0 || req.Limit > 100 {
			return nil, errors.New("offset must be nonnegative and limit 0..100")
		}
		if req.Limit == 0 {
			req.Limit = 50
		}
		start := min(req.Offset, len(c.configs))
		end := start + min(req.Limit, len(c.configs)-start)
		out := api.ListResponse{Rules: []api.RuleSummary{}, Total: len(c.configs), Revision: c.revision, AppliedRevision: m.applied, PendingRestart: c.revision != m.applied, Diagnostics: c.diagnostics}
		for i := start; i < end; i++ {
			out.Rules = append(out.Rules, m.summary(c, i))
		}
		return out, nil
	case api.MethodShow:
		var req api.ShowRequest
		if err := decodeJSON(payload, &req, "name"); err != nil {
			return nil, err
		}
		i, err := c.unique(req.Name)
		if err != nil {
			return nil, err
		}
		data, err := encodeRule(c.configs[i])
		if err != nil {
			return nil, err
		}
		return api.ShowResponse{Rule: m.summary(c, i), Definition: string(data), Revision: c.revision, PendingRestart: c.revision != m.applied}, nil
	case api.MethodStatus:
		if err := decodeJSON(payload, &api.Empty{}); err != nil {
			return nil, err
		}
		var out api.StatusResponse
		if m.status != nil {
			out = m.status()
		}
		out.APIVersion, out.Revision, out.AppliedRevision, out.PendingRestart = api.ProtocolVersion, c.revision, m.applied, c.revision != m.applied
		return out, nil
	case api.MethodTest:
		var req api.TestRequest
		if err := decodeJSON(payload, &req, "name", "event"); err != nil {
			return nil, err
		}
		i, err := c.unique(req.Name)
		if err != nil {
			return nil, err
		}
		if len(c.configs[i].Steps) > maxSteps {
			return nil, errors.New("rule exceeds 128 steps")
		}
		return rules.Preview(c.configs[i], req.Event, m.lookup())
	case api.MethodAdd:
		var req api.AddRequest
		if err := decodeJSON(payload, &req, "definition", "expected_revision"); err != nil {
			return nil, err
		}
		if err := mutationAllowed(c, req.ExpectedRevision); err != nil {
			return nil, err
		}
		configs, err := parseDefinition([]byte(req.Definition))
		if err != nil {
			return nil, err
		}
		if len(configs) != 1 {
			return nil, errors.New("definition must contain exactly one rule")
		}
		cfg := configs[0]
		if err := validName(cfg.Name); err != nil {
			return nil, err
		}
		for _, existing := range c.configs {
			if existing.Name == cfg.Name {
				return nil, fmt.Errorf("rule %q already exists", cfg.Name)
			}
		}
		if len(cfg.Steps) > maxSteps || len(c.configs) >= maxRules {
			return nil, errors.New("rule or step limit exceeded")
		}
		if _, err := rules.ValidateDefinition(cfg, m.lookup()); err != nil {
			return nil, err
		}
		// Preserve an explicit effective default in the managed definition.
		if cfg.Enabled == nil {
			enabled := true
			cfg.Enabled = &enabled
		}
		data, err := encodeRule(cfg)
		if err != nil {
			return nil, err
		}
		name := fmt.Sprintf("managed-%x.toml", sha256.Sum256([]byte(cfg.Name)))
		if err := m.commit(ctx, c, name, data, true); err != nil {
			return nil, err
		}
		return m.mutationResult(cfg.Name), nil
	case api.MethodSetEnabled:
		var req api.SetEnabledRequest
		if err := decodeJSON(payload, &req, "name", "enabled", "expected_revision"); err != nil {
			return nil, err
		}
		if err := mutationAllowed(c, req.ExpectedRevision); err != nil {
			return nil, err
		}
		i, err := c.unique(req.Name)
		if err != nil {
			return nil, err
		}
		if *c.configs[i].Enabled == req.Enabled {
			return api.MutationResponse{Name: req.Name, Revision: c.revision, PendingRestart: c.revision != m.applied, Message: "desired state unchanged"}, nil
		}
		c.overrides[req.Name] = req.Enabled
		data, err := json.Marshal(enabledFile{Version: 1, Rules: c.overrides})
		if err != nil {
			return nil, err
		}
		if err := m.commit(ctx, c, overrideFile, data, false); err != nil {
			return nil, err
		}
		return m.mutationResult(req.Name), nil
	default:
		return nil, fmt.Errorf("unknown method %q", method)
	}
}

func mutationAllowed(c *catalogue, expected string) error {
	if expected == "" || expected != c.revision {
		return errors.New("revision conflict: re-read desired configuration")
	}
	if c.unsafe {
		return errors.New("configuration has read or syntax diagnostics; mutation refused")
	}
	return nil
}

func (m *Manager) mutationResult(name string) api.MutationResponse {
	c := m.load()
	return api.MutationResponse{Name: name, Revision: c.revision, PendingRestart: c.revision != m.applied, Message: "desired configuration persisted; restart required to apply"}
}

func (m *Manager) commit(ctx context.Context, before *catalogue, dest string, data []byte, add bool) error {
	if len(data) > maxFileBytes {
		return errors.New("file exceeds 64 KiB")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := openDirectory(m.dir)
	if os.IsNotExist(err) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err = os.MkdirAll(m.dir, 0700); err != nil {
			return err
		}
		root, err = openDirectory(m.dir)
	}
	if err != nil {
		return err
	}
	defer root.Close()
	oldBytes := 0
	exists := false
	if st, err := root.Lstat(dest); err == nil {
		exists = true
		if !st.Mode().IsRegular() {
			return errors.New("destination is not a regular file")
		}
		if add {
			return errors.New("managed destination already exists")
		}
		oldBytes = before.overrideBytes
	} else if !os.IsNotExist(err) {
		return err
	}
	if !exists && before.entries >= maxEntries {
		return errors.New("directory entry limit exceeded")
	}
	if before.bytes-oldBytes+len(data) > maxTotalBytes {
		return errors.New("aggregate exceeds 1 MiB")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	temp := fmt.Sprintf(".control-%x.tmp", random)
	f, err := root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	defer root.Remove(temp)
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	// The temporary file is ancillary. Exclude it from the directory-entry
	// bound while rechecking, without ever ignoring someone else's files.
	if err := m.recheck(before, root, temp, dest); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if add {
		err = root.Link(temp, dest)
	} else {
		err = root.Rename(temp, dest)
	}
	if err != nil {
		return err
	}
	if add {
		if err := root.Remove(temp); err != nil {
			return fmt.Errorf("state may have persisted; re-read configuration: removing temporary file: %w", err)
		}
	}
	dir, err := root.Open(".")
	if err == nil {
		err = dir.Sync()
		closeErr := dir.Close()
		if err == nil {
			err = closeErr
		}
	}
	if err != nil {
		return fmt.Errorf("state may have persisted; re-read configuration: directory sync: %w", err)
	}
	return nil
}

func (m *Manager) recheck(before *catalogue, root *os.Root, temp, dest string) error {
	current := m.loadIgnoring(temp)
	if err := mutationAllowed(current, before.revision); err != nil {
		return err
	}
	if _, err := root.Lstat(dest); os.IsNotExist(err) {
		if current.entries >= maxEntries {
			return errors.New("directory entry limit exceeded")
		}
	} else if err != nil {
		return err
	}
	opened, err := root.Stat(".")
	if err != nil {
		return err
	}
	now, err := os.Lstat(m.dir)
	if err != nil {
		return err
	}
	if !os.SameFile(opened, now) || now.Mode()&os.ModeSymlink != 0 {
		return errors.New("extensions directory changed; re-read configuration")
	}
	return nil
}
