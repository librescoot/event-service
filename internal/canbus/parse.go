// Package canbus validates classical CAN frames and sends them without retries.
package canbus

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/brutella/can"
)

const (
	effFlag uint32 = 1 << 31
	rtrFlag uint32 = 1 << 30
	errFlag uint32 = 1 << 29
	maxID   uint32 = (1 << 29) - 1
)

type Spec struct {
	Iface, ID, Data string
	RTR             bool
	DLC             *int
}

func validateIface(iface string) error {
	// Linux IFNAMSIZ includes the terminating NUL; bound bytes, not runes.
	if len(iface) == 0 || len(iface) > 15 || strings.ContainsAny(iface, "/\x00") || strings.ContainsFunc(iface, unicode.IsSpace) {
		return fmt.Errorf("invalid CAN interface %q: require 1..15 bytes without whitespace, slash or NUL", iface)
	}
	return nil
}

// Parse is pure: it neither resolves interfaces nor opens sockets.
func Parse(spec Spec) (can.Frame, error) {
	var frame can.Frame
	if err := validateIface(spec.Iface); err != nil {
		return frame, err
	}
	id := spec.ID
	if strings.HasPrefix(id, "0x") || strings.HasPrefix(id, "0X") {
		id = id[2:]
	}
	if id == "" || strings.ContainsAny(id, "+-") {
		return frame, fmt.Errorf("invalid CAN identifier %q", spec.ID)
	}
	n, err := strconv.ParseUint(id, 16, 29)
	if err != nil {
		return frame, fmt.Errorf("invalid CAN identifier %q: %w", spec.ID, err)
	}
	frame.ID = uint32(n)
	if frame.ID > 0x7ff {
		frame.ID |= effFlag
	}

	data := strings.TrimSpace(spec.Data)
	if spec.RTR {
		if data != "" {
			return can.Frame{}, fmt.Errorf("RTR frame must not contain data")
		}
		frame.ID |= rtrFlag
		if spec.DLC != nil {
			if *spec.DLC < 0 || *spec.DLC > 8 {
				return can.Frame{}, fmt.Errorf("RTR DLC must be 0..8")
			}
			frame.Length = uint8(*spec.DLC)
		}
		return frame, nil
	}
	if spec.DLC != nil {
		return can.Frame{}, fmt.Errorf("DLC is only allowed for RTR frames; data frames derive it from data")
	}
	if strings.ContainsFunc(data, unicode.IsSpace) {
		fields := strings.Fields(data)
		if len(fields) > 8 {
			return can.Frame{}, fmt.Errorf("CAN data exceeds 8 bytes")
		}
		for _, field := range fields {
			if len(field) != 2 {
				return can.Frame{}, fmt.Errorf("separated CAN data must use exactly two hex digits per byte")
			}
		}
		data = strings.Join(fields, "")
	}
	if len(data) > 16 {
		return can.Frame{}, fmt.Errorf("CAN data exceeds 8 bytes")
	}
	decoded, err := hex.DecodeString(data)
	if err != nil {
		return can.Frame{}, fmt.Errorf("invalid CAN data: %w", err)
	}
	frame.Length = uint8(len(decoded))
	copy(frame.Data[:], decoded)
	return frame, nil
}

func validateFrame(frame can.Frame) error {
	if frame.Length > 8 {
		return fmt.Errorf("CAN frame length must be 0..8")
	}
	if frame.Flags != 0 || frame.Res0 != 0 || frame.Res1 != 0 || frame.ID&errFlag != 0 {
		return fmt.Errorf("unsupported CAN frame flags or reserved fields")
	}
	if frame.ID&effFlag == 0 && frame.ID&maxID > 0x7ff {
		return fmt.Errorf("CAN identifier above 0x7ff requires EFF")
	}
	if frame.ID&rtrFlag != 0 && frame.Data != [8]uint8{} {
		return fmt.Errorf("RTR frame must not contain data")
	}
	return nil
}
