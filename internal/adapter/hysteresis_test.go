package adapter

import "testing"

func TestCrossFiresOnDownwardCrossing(t *testing.T) {
	th := NewThresholds([]int{25, 50, 75}, 3)
	th.Cross("battery:0", 80)

	level, crossed := th.Cross("battery:0", 49)
	if !crossed {
		t.Fatal("crossed = false, want true")
	}
	if level != 50 {
		t.Errorf("level = %d, want 50", level)
	}
}

func TestCrossDoesNotRefireWithinMargin(t *testing.T) {
	th := NewThresholds([]int{25, 50, 75}, 3)
	th.Cross("battery:0", 80)
	th.Cross("battery:0", 49)

	for _, v := range []int{50, 51, 52, 49, 48} {
		if _, crossed := th.Cross("battery:0", v); crossed {
			t.Errorf("value %d re-fired inside the margin", v)
		}
	}
}

func TestCrossFiresAgainOnceClearOfMargin(t *testing.T) {
	th := NewThresholds([]int{25, 50, 75}, 3)
	th.Cross("battery:0", 80)
	th.Cross("battery:0", 49)

	if _, crossed := th.Cross("battery:0", 54); !crossed {
		t.Error("recrossing upward clear of the margin should fire")
	}
}

func TestCrossIsPerKey(t *testing.T) {
	th := NewThresholds([]int{50}, 3)
	th.Cross("battery:0", 80)
	th.Cross("battery:0", 40)

	if _, crossed := th.Cross("battery:1", 40); crossed {
		t.Error("battery:1 has no prior reading, first observation must not fire")
	}
}

func TestFirstObservationNeverFires(t *testing.T) {
	th := NewThresholds([]int{50}, 3)
	if _, crossed := th.Cross("aux", 10); crossed {
		t.Error("first observation fired; there is no previous value to cross from")
	}
}
