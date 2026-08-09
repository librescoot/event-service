package action

import (
	"context"
	"fmt"

	"github.com/librescoot/eventbus"
)

// Pusher is the one datastore operation the redis action needs. Depending on
// this rather than the whole client keeps the action testable without a
// running datastore.
type Pusher interface {
	LPush(key string, values ...any) (int64, error)
}

// redisAction pushes a fixed string onto an existing command queue. This is
// the recommended action for anything that stays on the scooter: it costs one
// datastore round trip and spawns no process.
type redisAction struct {
	client Pusher
	list   string
	push   string
}

// NewRedisAction validates the configuration up front, so a typo is a load
// error the user sees immediately rather than a rule that fails at 3am.
func NewRedisAction(c Pusher, list, push string) (Action, error) {
	if list == "" {
		return nil, fmt.Errorf("redis action needs a list")
	}
	if push == "" {
		return nil, fmt.Errorf("redis action needs a push value")
	}
	return &redisAction{client: c, list: list, push: push}, nil
}

func (a *redisAction) Kind() string { return "redis" }

func (a *redisAction) Do(ctx context.Context, e eventbus.Event) error {
	if _, err := a.client.LPush(a.list, a.push); err != nil {
		return fmt.Errorf("lpush %s: %w", a.list, err)
	}
	return nil
}
