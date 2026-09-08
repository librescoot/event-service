package rules

import "testing"

func TestLegacyFingerprintsRemainStable(t *testing.T) {
	for _, tc := range []struct {
		config StepConfig
		want   string
	}{
		{StepConfig{Do: "redis", After: "30s", List: "test:cleanup", Push: "off"}, "2f94278a54ab108"},
		{StepConfig{Do: "exec", After: "10s", Command: "/data/cleanup.sh", Timeout: "5s"}, "9471cdc3f2ab2860"},
	} {
		if got := stepFingerprint(tc.config, true); got != tc.want {
			t.Errorf("%s fingerprint = %s, want %s", tc.config.Do, got, tc.want)
		}
	}
}

func TestCANFingerprintIncludesAllFrameArguments(t *testing.T) {
	base := StepConfig{Do: "can", After: "1s", Iface: "can0", ID: "123", Data: "01"}
	want := stepFingerprint(base, true)
	changes := []func(*StepConfig){
		func(c *StepConfig) { c.Iface = "can1" },
		func(c *StepConfig) { c.ID = "124" },
		func(c *StepConfig) { c.Data = "02" },
		func(c *StepConfig) { c.RTR = true },
		func(c *StepConfig) { n := 0; c.DLC = &n },
	}
	for i, change := range changes {
		c := base
		change(&c)
		if stepFingerprint(c, true) == want {
			t.Errorf("frame argument mutation %d did not change fingerprint", i)
		}
	}
}
