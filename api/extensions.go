// Package api defines the extension-management protocol. Mutations change
// desired configuration only; event-service applies it on its next restart.
package api

import "github.com/librescoot/eventbus"

const (
	Channel           = "extensions:rpc"
	ProtocolVersion   = 1
	MethodList        = "v1.list"
	MethodShow        = "v1.show"
	MethodAdd         = "v1.add"
	MethodSetEnabled  = "v1.set-enabled"
	MethodTest        = "v1.test"
	MethodStatus      = "v1.status"
	MaxRequestBytes   = 64 * 1024
	MaxReplyBytes     = 512 * 1024
	MaxQueuedRequests = 32
)

type Empty struct{}

type RuleSummary struct {
	Name        string   `json:"name"`
	Source      string   `json:"source"`
	Enabled     bool     `json:"enabled"`
	Loaded      bool     `json:"loaded"`
	LastFire    int64    `json:"last_fire,omitempty"`
	Errors      uint64   `json:"errors"`
	ActiveRuns  int      `json:"active_runs"`
	Diagnostics []string `json:"diagnostics,omitempty"`
}

type ListRequest struct {
	Offset int `json:"offset,omitempty"`
	Limit  int `json:"limit,omitempty"`
}

type ListResponse struct {
	Rules           []RuleSummary `json:"rules"`
	Total           int           `json:"total"`
	Revision        string        `json:"revision"`
	AppliedRevision string        `json:"applied_revision"`
	PendingRestart  bool          `json:"pending_restart"`
	Diagnostics     []string      `json:"diagnostics,omitempty"`
}

type ShowRequest struct {
	Name string `json:"name"`
}

type ShowResponse struct {
	Rule           RuleSummary `json:"rule"`
	Definition     string      `json:"definition"`
	Revision       string      `json:"revision"`
	PendingRestart bool        `json:"pending_restart"`
}

type AddRequest struct {
	Definition       string `json:"definition"`
	ExpectedRevision string `json:"expected_revision"`
}

type SetEnabledRequest struct {
	Name             string `json:"name"`
	Enabled          bool   `json:"enabled"`
	ExpectedRevision string `json:"expected_revision"`
}

type MutationResponse struct {
	Name           string `json:"name"`
	Revision       string `json:"revision"`
	PendingRestart bool   `json:"pending_restart"`
	Message        string `json:"message"`
}

type TestRequest struct {
	Name  string         `json:"name"`
	Event eventbus.Event `json:"event"`
}

type StepPreview struct {
	Index       int    `json:"index"`
	Kind        string `json:"kind"`
	Condition   bool   `json:"condition"`
	After       string `json:"after,omitempty"`
	Durable     bool   `json:"durable"`
	Description string `json:"description"`
	Error       string `json:"error,omitempty"`
}

type TestResponse struct {
	Name        string        `json:"name"`
	Enabled     bool          `json:"enabled"`
	Matched     bool          `json:"matched"`
	Steps       []StepPreview `json:"steps"`
	Cooldown    string        `json:"cooldown,omitempty"`
	Debounce    string        `json:"debounce,omitempty"`
	RepeatCount int           `json:"repeat_count"`
	RepeatEvery string        `json:"repeat_every,omitempty"`
	Error       string        `json:"error,omitempty"`
	Note        string        `json:"note"`
}

type StatusResponse struct {
	Version         string            `json:"version"`
	APIVersion      int               `json:"api_version"`
	Revision        string            `json:"revision"`
	AppliedRevision string            `json:"applied_revision"`
	PendingRestart  bool              `json:"pending_restart"`
	QueueDepth      int               `json:"queue_depth"`
	QueueCapacity   int               `json:"queue_capacity"`
	Workers         int               `json:"workers"`
	BusyWorkers     int               `json:"busy_workers"`
	Counters        map[string]string `json:"counters"`
}
