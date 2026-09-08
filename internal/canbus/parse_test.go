package canbus

import (
	"strings"
	"testing"

	"github.com/brutella/can"
)

func intp(n int) *int { return &n }

func TestParseValid(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec Spec
		want can.Frame
	}{
		{"zero", Spec{Iface: "can0", ID: "0"}, can.Frame{}},
		{"standard", Spec{Iface: "can0", ID: "0x7ff", Data: "aA00"}, can.Frame{ID: 0x7ff, Length: 2, Data: [8]byte{0xaa, 0}}},
		{"extended", Spec{Iface: "can0", ID: "800"}, can.Frame{ID: effFlag | 0x800}},
		{"max", Spec{Iface: "can0", ID: "0X1FFFFFFF", Data: "00 01\t02\n03 04 05 06 FF"}, can.Frame{ID: effFlag | maxID, Length: 8, Data: [8]byte{0, 1, 2, 3, 4, 5, 6, 255}}},
		{"empty whitespace", Spec{Iface: "can0", ID: "abc", Data: " \t"}, can.Frame{ID: effFlag | 0xabc}},
		{"max interface", Spec{Iface: strings.Repeat("a", 15), ID: "1"}, can.Frame{ID: 1}},
		{"rtr default", Spec{Iface: "can0", ID: "10", RTR: true}, can.Frame{ID: rtrFlag | 0x10}},
		{"rtr zero", Spec{Iface: "can0", ID: "10", RTR: true, DLC: intp(0)}, can.Frame{ID: rtrFlag | 0x10}},
		{"rtr max", Spec{Iface: "can0", ID: "800", RTR: true, DLC: intp(8)}, can.Frame{ID: effFlag | rtrFlag | 0x800, Length: 8}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse(tc.spec)
			if err != nil || got != tc.want {
				t.Fatalf("Parse = %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}
}

func TestParseRejects(t *testing.T) {
	base := Spec{Iface: "can0", ID: "123"}
	for _, id := range []string{"", "0x", "-1", "+1", "0x-1", "0x+1", "20000000", "ffffffff", "100000000", "0x0X1", "1g", "1_2", " 1", "1 ", "1\n"} {
		t.Run("id/"+id, func(t *testing.T) {
			s := base
			s.ID = id
			if _, err := Parse(s); err == nil {
				t.Fatal("accepted invalid id")
			}
		})
	}
	for _, iface := range []string{"", strings.Repeat("a", 16), "can 0", "can\t0", "can\n0", "can\u00a00", "can/0", "can\x000", strings.Repeat("é", 8)} {
		t.Run("iface/"+iface, func(t *testing.T) {
			s := base
			s.Iface = iface
			if _, err := Parse(s); err == nil {
				t.Fatal("accepted invalid interface")
			}
		})
	}
	for _, data := range []string{"0", "001", "gg", "0x00", "0 1", "0011 22", "00 1122", "00:11", strings.Repeat("00", 9), strings.Repeat("00 ", 9)} {
		t.Run("data/"+data, func(t *testing.T) {
			s := base
			s.Data = data
			if _, err := Parse(s); err == nil {
				t.Fatal("accepted invalid data")
			}
		})
	}
	for _, s := range []Spec{
		{Iface: "can0", ID: "1", DLC: intp(0)},
		{Iface: "can0", ID: "1", Data: "00", DLC: intp(1)},
		{Iface: "can0", ID: "1", RTR: true, Data: "00"},
		{Iface: "can0", ID: "1", RTR: true, DLC: intp(-1)},
		{Iface: "can0", ID: "1", RTR: true, DLC: intp(9)},
	} {
		if _, err := Parse(s); err == nil {
			t.Errorf("accepted %+v", s)
		}
	}
}
