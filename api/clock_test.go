package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Shift only Redis TIME responses, not the process clock or stored data.
// Every connection has its own parser, just as it has its own Redis replies.
type shiftedClockConn struct {
	net.Conn
	reader    *bufio.Reader
	offset    time.Duration
	timeReply bool
	calls     *atomic.Int32
	pending   []byte
}

func (c *shiftedClockConn) Write(p []byte) (int, error) {
	if bytes.Equal(bytes.ToLower(p), []byte("*1\r\n$4\r\ntime\r\n")) {
		c.timeReply = true
		c.calls.Add(1)
	}
	return c.Conn.Write(p)
}
func (c *shiftedClockConn) Read(p []byte) (int, error) {
	if c.timeReply && len(c.pending) == 0 {
		lines := make([]string, 5)
		for i := range lines {
			s, err := c.reader.ReadString('\n')
			if err != nil {
				return 0, err
			}
			lines[i] = s
		}
		if lines[0] != "*2\r\n" {
			return 0, fmt.Errorf("unexpected TIME reply %q", lines)
		}
		seconds, err := strconv.ParseInt(strings.TrimSpace(lines[2]), 10, 64)
		if err != nil {
			return 0, err
		}
		value := strconv.FormatInt(seconds+int64(c.offset/time.Second), 10)
		c.pending = []byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n%s%s", len(value), value, lines[3], lines[4]))
		c.timeReply = false
	}
	if len(c.pending) > 0 {
		n := copy(p, c.pending)
		c.pending = c.pending[n:]
		return n, nil
	}
	return c.reader.Read(p)
}

func TestRPCWithUnsynchronizedHostAndDatastoreClocks(t *testing.T) {
	for _, offset := range []time.Duration{-7 * 24 * time.Hour, 7 * 24 * time.Hour} {
		t.Run(offset.String(), func(t *testing.T) {
			base := testRedis(t)
			opts := *base.Options()
			var calls atomic.Int32
			opts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return &shiftedClockConn{Conn: conn, reader: bufio.NewReader(conn), offset: offset, calls: &calls}, nil
			}
			client := redis.NewClient(&opts)
			defer client.Close()
			cancel, done := startServer(t, client, func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > callTimeout || time.Until(deadline) <= 0 {
					return nil, fmt.Errorf("invalid local handler budget")
				}
				return StatusResponse{Version: "shifted-clock"}, nil
			})
			defer stopServer(t, cancel, done)
			result, err := Call[Empty, StatusResponse](context.Background(), client, MethodStatus, Empty{})
			if err != nil || result.Version != "shifted-clock" {
				t.Fatalf("clock offset %s: %+v %v", offset, result, err)
			}
			if calls.Load() < 2 {
				t.Fatal("both endpoints must use the datastore clock")
			}
		})
	}
}
