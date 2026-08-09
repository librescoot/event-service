package action

import (
	"strings"
	"testing"
)

func TestBuildRedis(t *testing.T) {
	a, err := Build(Spec{Do: "redis", List: "scooter:horn", Push: "on"}, &fakePusher{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if a.Kind() != "redis" {
		t.Errorf("Kind() = %q", a.Kind())
	}
}

func TestBuildExec(t *testing.T) {
	a, err := Build(Spec{Do: "exec", Command: "/bin/true", Timeout: "5s"}, &fakePusher{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if a.Kind() != "exec" {
		t.Errorf("Kind() = %q", a.Kind())
	}
}

func TestBuildRejectsUnknownKind(t *testing.T) {
	_, err := Build(Spec{Do: "telepathy"}, &fakePusher{})
	if err == nil {
		t.Fatal("unknown action kind must be rejected")
	}
}

// can, lua and http are named in the design but not built yet. The error must
// say so, rather than reading as a typo.
func TestBuildNamesDeferredKinds(t *testing.T) {
	for _, kind := range []string{"can", "lua", "http"} {
		_, err := Build(Spec{Do: kind}, &fakePusher{})
		if err == nil {
			t.Errorf("%s should be rejected for now", kind)
			continue
		}
		if !strings.Contains(err.Error(), "not supported yet") {
			t.Errorf("%s: error should say not supported yet, got %v", kind, err)
		}
	}
}

func TestBuildRejectsBadTimeout(t *testing.T) {
	if _, err := Build(Spec{Do: "exec", Command: "/bin/true", Timeout: "soon"}, &fakePusher{}); err == nil {
		t.Error("an unparseable timeout must be rejected")
	}
}
