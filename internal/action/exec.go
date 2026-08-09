package action

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/librescoot/eventbus"
)

const defaultExecTimeout = 10 * time.Second

// execAction runs a user script. The script is untrusted in the sense that it
// is whatever the owner wrote, so it gets its own process group, a hard
// timeout, and a lowered priority: this service shares one core with the
// vehicle state machine.
type execAction struct {
	command string
	timeout time.Duration
}

// NewExecAction validates the command up front. A zero timeout means the
// default rather than no timeout, because an unbounded script would hold a
// worker forever.
func NewExecAction(command string, timeout time.Duration) (Action, error) {
	if command == "" {
		return nil, fmt.Errorf("exec action needs a command")
	}
	if timeout <= 0 {
		timeout = defaultExecTimeout
	}
	return &execAction{command: command, timeout: timeout}, nil
}

func (a *execAction) Kind() string { return "exec" }

func (a *execAction) Do(ctx context.Context, e eventbus.Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, a.command)
	cmd.Stdin = bytes.NewReader(body)
	cmd.Env = append(os.Environ(), envFor(e)...)

	// Own process group so the timeout kills the whole tree, not just the
	// direct child. Kill the group on cancel for the same reason: a script
	// that backgrounds a child would otherwise leak it past the worker that
	// launched it, and TasksMax=64 leaves no room for that.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("%s timed out after %s", a.command, a.timeout)
		}
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return fmt.Errorf("%s: %w: %s", a.command, err, msg)
		}
		return fmt.Errorf("%s: %w", a.command, err)
	}
	return nil
}

// envFor flattens the envelope so a shell script can read it without parsing
// JSON. Only scalar data values are exported; anything structured stays on
// stdin, where it can be read properly.
func envFor(e eventbus.Event) []string {
	env := []string{
		"LS_TOPIC=" + e.Topic,
		"LS_SRC=" + e.Src,
		"LS_FROM=" + e.From,
		"LS_TO=" + e.To,
		"LS_ID=" + e.ID,
	}
	for k, v := range e.Data {
		switch v.(type) {
		case string, bool, int, int64, float64:
			env = append(env, fmt.Sprintf("LS_DATA_%s=%v", envKey(k), v))
		}
	}
	return env
}

// envKey makes a data key safe for an environment variable name.
func envKey(k string) string {
	up := strings.ToUpper(k)
	return strings.Map(func(r rune) rune {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, up)
}
