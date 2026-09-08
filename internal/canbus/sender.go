package canbus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/brutella/can"
)

// Stats counts Send calls. Sent means kernel acceptance, not a bus ACK.
type Stats struct{ Sent, Errors uint64 }

type connection interface {
	Write([]byte) (int, error)
	Close() error
}

type Sender struct {
	mu          sync.Mutex
	connections map[string]connection
	dial        func(string) (connection, error)
	debugf      func(string, ...any)
	stats       Stats
	closed      bool
	closeErr    error
}

// Bound retained descriptors even when callers generate interface names dynamically.
const maxOpenInterfaces = 64

func New(debugf func(string, ...any)) *Sender {
	return &Sender{connections: make(map[string]connection), dial: dialSocket, debugf: debugf}
}

// Send makes one immediate nonblocking write. It never retries a frame, including
// on EAGAIN or EINTR; a failed connection is discarded for the next caller.
func (s *Sender) Send(ctx context.Context, iface string, frame can.Frame) (err error) {
	initialErr := ctx.Err()
	s.mu.Lock()
	defer func() {
		if err != nil {
			s.stats.Errors++
		} else {
			s.stats.Sent++
		}
		s.mu.Unlock()
		// Callbacks may inspect Stats or Close this sender.
		if s.debugf != nil {
			if err != nil {
				s.debugf("CAN send on %s failed: %v", iface, err)
			} else {
				s.debugf("CAN sent on %s: id=%08x dlc=%d", iface, frame.ID, frame.Length)
			}
		}
	}()
	if initialErr != nil {
		return initialErr
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return errors.New("CAN sender is closed")
	}
	if err = validateIface(iface); err != nil {
		return err
	}
	if err = validateFrame(frame); err != nil {
		return err
	}
	payload, err := can.Marshal(frame)
	if err != nil {
		return fmt.Errorf("marshal CAN frame: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	conn := s.connections[iface]
	if conn == nil {
		if len(s.connections) >= maxOpenInterfaces {
			return fmt.Errorf("CAN socket limit (%d interfaces) reached", maxOpenInterfaces)
		}
		conn, err = s.dial(iface)
		if err != nil {
			return fmt.Errorf("open CAN interface %s: %w", iface, err)
		}
		s.connections[iface] = conn
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	n, err := conn.Write(payload)
	if err == nil && n != len(payload) {
		err = io.ErrShortWrite
	}
	if err != nil {
		delete(s.connections, iface)
		if closeErr := conn.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close failed CAN socket: %w", closeErr))
		}
		return fmt.Errorf("write CAN interface %s: %w", iface, err)
	}
	return nil
}

func (s *Sender) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// Close is idempotent, including its returned cleanup error. Descriptors are
// never retried on close failure: on Linux they may already have been reused.
func (s *Sender) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	for iface, conn := range s.connections {
		if err := conn.Close(); err != nil {
			s.closeErr = errors.Join(s.closeErr, fmt.Errorf("close CAN interface %s: %w", iface, err))
		}
		delete(s.connections, iface)
	}
	return s.closeErr
}
