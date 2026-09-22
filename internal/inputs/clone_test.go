package inputs

import "testing"

func TestCloneDetachesSelections(t *testing.T) {
	original := []Config{{Hash: "custom", Fields: []string{"mode"}, Topic: "input.custom"}}
	copy := Clone(original)
	original[0].Hash = "changed"
	original[0].Fields[0] = "different"
	if copy[0].Hash != "custom" || copy[0].Fields[0] != "mode" {
		t.Fatal("cloned input retained mutable configuration")
	}
	if Clone(nil) != nil {
		t.Fatal("nil input did not remain nil")
	}
}
