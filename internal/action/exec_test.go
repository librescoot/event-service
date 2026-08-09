package action

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/librescoot/eventbus"
)

func writeScript(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "s.sh")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body), 0755); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestExecActionPassesEnvelopeOnStdin(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out")
	script := writeScript(t, "cat > "+out+"\n")

	a, err := NewExecAction(script, 5*time.Second)
	if err != nil {
		t.Fatalf("NewExecAction: %v", err)
	}
	e := eventbus.Event{Topic: "alarm.triggered", To: "level-2-triggered"}
	if err := a.Do(context.Background(), e); err != nil {
		t.Fatalf("Do: %v", err)
	}

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read stdin capture: %v", err)
	}
	if !strings.Contains(string(b), `"topic":"alarm.triggered"`) {
		t.Errorf("stdin did not carry the envelope: %s", b)
	}
}

func TestExecActionSetsEnvVars(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out")
	script := writeScript(t, "echo \"$LS_TOPIC|$LS_TO|$LS_DATA_SLOT\" > "+out+"\n")

	a, _ := NewExecAction(script, 5*time.Second)
	e := eventbus.Event{Topic: "battery.inserted", To: "present", Data: map[string]any{"slot": 1}}
	if err := a.Do(context.Background(), e); err != nil {
		t.Fatalf("Do: %v", err)
	}

	b, _ := os.ReadFile(out)
	got := strings.TrimSpace(string(b))
	if got != "battery.inserted|present|1" {
		t.Errorf("env = %q, want battery.inserted|present|1", got)
	}
}

// TestExecActionKillsOnTimeout must prove the script itself died, not merely
// that Do returned quickly: a bug that abandons the process (leaks it to
// init, or only breaks the pipe without sending a signal) would still make
// Do return an error near the deadline while sleep 30 lives on. This test
// has the script write its own pid to a file before sleeping, so it can poll
// /proc for that pid after Do returns and fail if the process is still
// alive.
func TestExecActionKillsOnTimeout(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "pid")
	script := writeScript(t, "echo $$ > "+pidFile+"\nsleep 30\n")
	a, _ := NewExecAction(script, 300*time.Millisecond)

	start := time.Now()
	err := a.Do(context.Background(), eventbus.Event{Topic: "x.y"})
	elapsed := time.Since(start)

	if err == nil {
		t.Error("a script that outlives its timeout must return an error")
	}
	if elapsed > 5*time.Second {
		t.Errorf("took %v; the timeout did not kill the script", elapsed)
	}

	pidBytes, readErr := os.ReadFile(pidFile)
	if readErr != nil {
		t.Fatalf("read pid file: %v", readErr)
	}
	pid, convErr := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if convErr != nil {
		t.Fatalf("parse pid: %v", convErr)
	}

	deadline := time.Now().Add(2 * time.Second)
	for processAlive(pid) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if processAlive(pid) {
		t.Errorf("pid %d is still alive after timeout; the script was not killed", pid)
	}
}

// TestExecActionKillsBackgroundedChildren proves the whole process group
// dies, not just the direct child. The script backgrounds a long sleep with
// its own pid recorded, then exits (or is killed) before that sleep does. A
// plain cmd.Process.Kill() would only ever reach the shell, leaving the
// backgrounded sleep as an orphan: exactly the leak the process-group design
// exists to prevent on a box with TasksMax=64.
func TestExecActionKillsBackgroundedChildren(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "child-pid")
	script := writeScript(t, "sleep 30 & echo $! > "+pidFile+"\nwait\n")
	a, _ := NewExecAction(script, 300*time.Millisecond)

	err := a.Do(context.Background(), eventbus.Event{Topic: "x.y"})
	if err == nil {
		t.Error("a script that outlives its timeout must return an error")
	}

	var pidBytes []byte
	deadline := time.Now().Add(2 * time.Second)
	for {
		b, readErr := os.ReadFile(pidFile)
		if readErr == nil && len(strings.TrimSpace(string(b))) > 0 {
			pidBytes = b
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child pid file never appeared: %v", readErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	pid, convErr := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if convErr != nil {
		t.Fatalf("parse child pid: %v", convErr)
	}

	deadline = time.Now().Add(2 * time.Second)
	for processAlive(pid) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if processAlive(pid) {
		t.Errorf("backgrounded child pid %d is still alive after timeout; the process group was not killed", pid)
	}
}

func processAlive(pid int) bool {
	_, err := os.Stat("/proc/" + strconv.Itoa(pid))
	return err == nil
}

func TestExecActionRejectsMissingCommand(t *testing.T) {
	if _, err := NewExecAction("", time.Second); err == nil {
		t.Error("empty command must be rejected at construction")
	}
}

func TestExecActionReportsNonZeroExit(t *testing.T) {
	script := writeScript(t, "exit 3\n")
	a, _ := NewExecAction(script, 5*time.Second)
	if err := a.Do(context.Background(), eventbus.Event{Topic: "x.y"}); err == nil {
		t.Error("a non-zero exit must surface as an error")
	}
}

// TestExecActionKind guards the pool's log line: Pool.run logs
// "%s action failed" using Kind(), so a wrong or empty Kind would make
// failures unattributable in the log.
func TestExecActionKind(t *testing.T) {
	a, _ := NewExecAction("/bin/true", time.Second)
	if a.Kind() != "exec" {
		t.Errorf("Kind() = %q, want exec", a.Kind())
	}
}

// TestExecActionRejectsMissingScript catches a command that does not
// resolve to an executable at all (not merely one that exits non-zero): the
// os/exec "file not found" error must still surface as a Do error rather
// than a panic on a nil cmd.Process inside cmd.Cancel.
func TestExecActionRejectsMissingScript(t *testing.T) {
	a, _ := NewExecAction(filepath.Join(t.TempDir(), "does-not-exist.sh"), time.Second)
	if err := a.Do(context.Background(), eventbus.Event{Topic: "x.y"}); err == nil {
		t.Error("a missing script must surface as an error from Do")
	}
}

// TestExecActionZeroTimeoutUsesDefault documents that NewExecAction treats a
// zero timeout as "use the default", not "no timeout": an unbounded script
// would hold a pool worker forever, which on a two-worker pool is enough to
// stall every other rule.
func TestExecActionZeroTimeoutUsesDefault(t *testing.T) {
	a, err := NewExecAction("/bin/true", 0)
	if err != nil {
		t.Fatalf("NewExecAction: %v", err)
	}
	if err := a.Do(context.Background(), eventbus.Event{Topic: "x.y"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
}
