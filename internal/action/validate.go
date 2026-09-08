package action

import (
	"fmt"

	"github.com/librescoot/event-service/internal/canbus"
)

// Validate checks an action without constructing it or accessing a dependency.
// Dry runs must use this path, never Build or an Action.Do method.
func Validate(s Spec) error {
	switch s.Do {
	case "redis":
		if s.List == "" {
			return fmt.Errorf("redis action needs a list")
		}
		if s.Push == "" {
			return fmt.Errorf("redis action needs a push value")
		}
	case "exec":
		if s.Command == "" {
			return fmt.Errorf("exec action needs a command")
		}
		if _, err := parseTimeout(s.Timeout); err != nil {
			return err
		}
	case "can":
		_, err := canbus.Parse(canbus.Spec{Iface: s.Iface, ID: s.ID, Data: s.Data, RTR: s.RTR, DLC: s.DLC})
		return err
	case "lua", "http":
		return fmt.Errorf("action %q is not supported yet", s.Do)
	case "":
		return fmt.Errorf("step is missing do")
	default:
		return fmt.Errorf("unknown action %q", s.Do)
	}
	return nil
}
