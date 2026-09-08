//go:build !linux

package canbus

import "errors"

func dialSocket(string) (connection, error) {
	return nil, errors.New("CAN sending is unsupported on this platform")
}
