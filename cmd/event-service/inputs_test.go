package main

import (
	"fmt"
	"testing"

	"github.com/librescoot/event-service/internal/adapter"
	"github.com/librescoot/event-service/internal/inputs"
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/event-service/internal/shadow"
)

func inputRule(name string, configs ...inputs.Config) *rules.Rule {
	return &rules.Rule{Name: name, Inputs: configs, Steps: []rules.Step{{Config: rules.StepConfig{Do: "exec", Command: "/bin/true"}}}}
}

func TestConfiguredInputsRejectOnlyConflictingRule(t *testing.T) {
	ad := adapter.New(nil, nil, shadow.NewStore())
	ad.Register(adapter.NewVehicleSource())
	good := inputRule("good", inputs.Config{Hash: "extra", Fields: []string{"mode"}, Topic: "input.extra"})
	bad := inputRule("bad", inputs.Config{Channel: "vehicle", Topic: "input.wrong"})
	plain := inputRule("plain")
	accepted, source, errs := configureInputs(ad, []*rules.Rule{good, bad, plain})
	if len(errs) != 1 || len(accepted) != 2 || accepted[0] != good || accepted[1] != plain {
		t.Fatalf("accepted=%v errors=%v", accepted, errs)
	}
	if len(source.Hashes()) != 1 || source.Hashes()[0] != "extra" || len(source.Channels()) != 0 {
		t.Fatalf("subscriptions: %v %v", source.Hashes(), source.Channels())
	}
}

func TestCleanupStateDependenciesTakePriority(t *testing.T) {
	ad := adapter.New(nil, nil, shadow.NewStore())
	fields := make([]string, 16)
	for i := range fields {
		fields[i] = fmt.Sprintf("field%d", i)
	}
	cleanup := inputRule("cleanup", inputs.Config{Hash: "old", Fields: fields, Topic: "input.old", MaxBytes: 65536}, inputs.Config{Channel: "old:messages", Topic: "input.old-message"})
	cleanup.ReplayOnly = true
	fresh := inputRule("fresh", inputs.Config{Hash: "new", Fields: []string{"state"}, Topic: "input.new"})
	plain := inputRule("plain")
	accepted, source, errs := configureInputs(ad, []*rules.Rule{fresh, cleanup, plain})
	if len(errs) != 1 || len(accepted) != 2 || accepted[0] != cleanup || accepted[1] != plain {
		t.Fatalf("accepted=%v errors=%v", accepted, errs)
	}
	if len(source.Channels()) != 0 || len(source.Hashes()) != 1 || source.Hashes()[0] != "old" {
		t.Fatalf("subscriptions: %v %v", source.Hashes(), source.Channels())
	}
	if got := source.OnField("old", fields[0], "new", "old"); len(got) != 0 {
		t.Fatal("disabled cleanup dependency produced a new event")
	}
	if cleanup.Inputs[0].Topic != "input.old" {
		t.Fatal("modified original compiled input configuration")
	}
}

func TestInvalidActionDoesNotSubscribeInputs(t *testing.T) {
	ad := adapter.New(nil, nil, shadow.NewStore())
	bad := inputRule("bad", inputs.Config{Channel: "external", Topic: "input.bad"})
	bad.Steps[0].Config.Do = "unsupported"
	accepted, source, errs := configureInputs(ad, []*rules.Rule{bad})
	if len(errs) != 1 || len(accepted) != 0 || len(source.Channels()) != 0 {
		t.Fatalf("accepted=%v errors=%v channels=%v", accepted, errs, source.Channels())
	}
}
