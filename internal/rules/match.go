package rules

import "strings"

// MatchTopic reports whether topic matches pattern.
//
// A pattern is either an exact topic or a dotted prefix ending in ".*", which
// matches that segment and everything below it. A bare "*" matches everything.
//
// Mid-pattern stars are deliberately not supported. A glob language grows
// teeth quickly, and the failure mode of a half-understood pattern is a rule
// that fires on events the author never considered. Matching literally means a
// typo fires nothing instead.
func MatchTopic(pattern, topic string) bool {
	if pattern == "" {
		return false
	}
	if pattern == "*" {
		return true
	}
	if prefix, ok := strings.CutSuffix(pattern, ".*"); ok {
		return strings.HasPrefix(topic, prefix+".")
	}
	return pattern == topic
}
