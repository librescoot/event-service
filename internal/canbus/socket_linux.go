//go:build linux

package canbus

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// Per-dial operations let tests exercise every ownership transfer without sockets.
type socketOps struct {
	socket func(int, int, int) (int, error)
	index  func(int, string) (int, error)
	filter func(int, int, int, []unix.CanFilter) error
	bind   func(int, unix.Sockaddr) error
	close  func(int) error
	write  func(int, []byte) (int, error)
}

func dialSocket(iface string) (connection, error) {
	return openSocket(iface, socketOps{
		socket: unix.Socket, index: interfaceIndex,
		filter: unix.SetsockoptCanRawFilter, bind: unix.Bind,
		close: unix.Close, write: unix.Write,
	})
}

func interfaceIndex(fd int, name string) (int, error) {
	req, err := unix.NewIfreq(name)
	if err != nil {
		return 0, err
	}
	if err := unix.IoctlIfreq(fd, unix.SIOCGIFINDEX, req); err != nil {
		return 0, err
	}
	return int(req.Uint32()), nil
}

func openSocket(iface string, ops socketOps) (_ connection, err error) {
	if err := validateIface(iface); err != nil {
		return nil, err
	}
	fd, err := ops.socket(unix.AF_CAN, unix.SOCK_RAW|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.CAN_RAW)
	if err != nil {
		return nil, fmt.Errorf("socket: %w", err)
	}
	defer func() {
		if err != nil {
			if closeErr := ops.close(fd); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close unbound CAN socket: %w", closeErr))
			}
		}
	}()
	index, err := ops.index(fd, iface)
	if err != nil {
		return nil, fmt.Errorf("interface index: %w", err)
	}
	// Index zero binds all interfaces, which must never happen for explicit names.
	if index <= 0 {
		return nil, fmt.Errorf("invalid CAN interface index %d", index)
	}
	// A nil filter slice installs zero filters, disabling reception entirely.
	if err = ops.filter(fd, unix.SOL_CAN_RAW, unix.CAN_RAW_FILTER, nil); err != nil {
		return nil, fmt.Errorf("disable CAN receive filters: %w", err)
	}
	if err = ops.bind(fd, &unix.SockaddrCAN{Ifindex: index}); err != nil {
		return nil, fmt.Errorf("bind: %w", err)
	}
	return &socketConnection{fd: fd, write: ops.write, close: ops.close}, nil
}

type socketConnection struct {
	fd    int
	write func(int, []byte) (int, error)
	close func(int) error
}

func (c *socketConnection) Write(data []byte) (int, error) { return c.write(c.fd, data) }
func (c *socketConnection) Close() error                   { return c.close(c.fd) }
