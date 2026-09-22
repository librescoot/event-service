package adapter

import (
	"encoding/json"
	"fmt"
	"log"
	"slices"

	"github.com/librescoot/event-service/internal/inputs"
	"github.com/librescoot/eventbus"
)

type configuredField struct{ hash, field string }
type configuredRoute struct {
	topic, format string
	limit         int
}

// configuredSource is immutable after construction; callbacks own their events.
type configuredSource struct {
	hashes, channels []string
	fields           map[configuredField][]configuredRoute
	limits           map[string]map[string]int
	messages         map[string][]configuredRoute
}

// NewConfiguredSource builds opt-in routes, without taking ownership of configs.
// Startup seeding and unchanged-value suppression remain the adapter's job.
func NewConfiguredSource(configs []inputs.Config) (Source, error) {
	s := &configuredSource{
		fields:   make(map[configuredField][]configuredRoute),
		limits:   make(map[string]map[string]int),
		messages: make(map[string][]configuredRoute),
	}
	for i, c := range configs {
		if err := c.Validate(); err != nil {
			return nil, fmt.Errorf("input %d: %w", i+1, err)
		}
		r := configuredRoute{topic: c.Topic, format: c.Format, limit: c.Limit()}
		if c.Hash != "" {
			if !slices.Contains(s.hashes, c.Hash) {
				s.hashes = append(s.hashes, c.Hash)
			}
			if s.limits[c.Hash] == nil {
				s.limits[c.Hash] = make(map[string]int)
			}
			for _, field := range c.Fields {
				s.limits[c.Hash][field] = max(s.limits[c.Hash][field], c.Limit())
			}
			if c.Topic == "" {
				continue
			}
			for _, field := range c.Fields {
				key := configuredField{c.Hash, field}
				s.fields[key] = appendConfiguredRoute(s.fields[key], r)
			}
		} else {
			if !slices.Contains(s.channels, c.Channel) {
				s.channels = append(s.channels, c.Channel)
			}
			if r.format == "" {
				r.format = "string"
			}
			s.messages[c.Channel] = appendConfiguredRoute(s.messages[c.Channel], r)
		}
	}
	return s, nil
}

func appendConfiguredRoute(routes []configuredRoute, next configuredRoute) []configuredRoute {
	for i, r := range routes {
		if r.topic == next.topic && r.format == next.format {
			// Equivalent outputs need only one route. The union accepts a value
			// if at least one declaration's byte limit permits it.
			routes[i].limit = max(r.limit, next.limit)
			return routes
		}
	}
	return append(routes, next)
}

func (s *configuredSource) Hashes() []string   { return slices.Clone(s.hashes) }
func (s *configuredSource) Channels() []string { return slices.Clone(s.channels) }

// FieldLimits selects bounded reads, including state-only dependencies.
func (s *configuredSource) FieldLimits(hash string) map[string]int {
	limits := make(map[string]int, len(s.limits[hash]))
	for field, limit := range s.limits[hash] {
		limits[field] = limit
	}
	return limits
}

func (s *configuredSource) OnField(hash, field, value, prev string) []eventbus.Event {
	var events []eventbus.Event
	for _, r := range s.fields[configuredField{hash, field}] {
		if len(value) > r.limit || len(prev) > r.limit {
			log.Print("configured input: skipping oversized field transition")
			continue
		}
		e := eventbus.New(r.topic, "adapter")
		e.From, e.To = prev, value
		e.Data = map[string]any{"hash": hash, "field": field}
		events = append(events, e)
	}
	return events
}

func (s *configuredSource) OnMessage(channel, payload string) []eventbus.Event {
	var events []eventbus.Event
	for _, r := range s.messages[channel] {
		if len(payload) > r.limit {
			log.Print("configured input: skipping oversized channel payload")
			continue
		}
		var decoded any = payload
		if r.format == "json" {
			if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
				// Decoder errors can include payload contents; do not log them.
				log.Print("configured input: skipping invalid JSON payload")
				continue
			}
		}
		e := eventbus.New(r.topic, "adapter")
		e.Data = map[string]any{"channel": channel, "payload": decoded}
		events = append(events, e)
	}
	return events
}
