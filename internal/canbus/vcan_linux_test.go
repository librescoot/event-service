//go:build linux

package canbus

import (
	"context"
	"os"
	"testing"

	"github.com/brutella/can"
	"golang.org/x/sys/unix"
)

// Run only inside an externally prepared isolated network namespace containing
// vcan-evtest. This test neither creates nor configures any interfaces.
func TestVcanIntegration(t *testing.T) {
	if os.Getenv("EVENT_SERVICE_TEST_VCAN") != "1" {
		t.Skip("requires EVENT_SERVICE_TEST_VCAN=1 and externally created vcan-evtest in an isolated netns")
	}
	const iface = "vcan-evtest"
	fd, err := unix.Socket(unix.AF_CAN, unix.SOCK_RAW|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.CAN_RAW)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := unix.Close(fd); err != nil {
			t.Errorf("close receiver: %v", err)
		}
	})
	index, err := interfaceIndex(fd, iface)
	if err != nil {
		t.Fatalf("lookup %s: %v", iface, err)
	}
	if index <= 0 {
		t.Fatalf("invalid interface index %d", index)
	}
	if err := unix.Bind(fd, &unix.SockaddrCAN{Ifindex: index}); err != nil {
		t.Fatal(err)
	}
	sender := New(t.Logf)
	t.Cleanup(func() {
		if err := sender.Close(); err != nil {
			t.Errorf("close sender: %v", err)
		}
	})
	for _, spec := range []Spec{
		{Iface: iface, ID: "123", Data: "01 23 45 67 89 ab cd ef"},
		{Iface: iface, ID: "1fffffff", Data: "ab cd"},
		{Iface: iface, ID: "321", RTR: true, DLC: intp(8)},
		{Iface: iface, ID: "12345", RTR: true, DLC: intp(3)},
	} {
		want, err := Parse(spec)
		if err != nil {
			t.Fatal(err)
		}
		if err := sender.Send(context.Background(), iface, want); err != nil {
			t.Fatal(err)
		}
		poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, err := unix.Poll(poll, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 || poll[0].Revents&unix.POLLIN == 0 {
			t.Fatalf("CAN receive timed out or failed: n=%d revents=%x", n, poll[0].Revents)
		}
		var payload [16]byte
		n, err = unix.Read(fd, payload[:])
		if err != nil || n != len(payload) {
			t.Fatalf("read CAN frame: n=%d err=%v", n, err)
		}
		var got can.Frame
		if err := can.Unmarshal(payload[:], &got); err != nil {
			t.Fatal(err)
		}
		if got.ID != want.ID || got.Length != want.Length {
			t.Fatalf("received %+v, want %+v", got, want)
		}
		if !spec.RTR && got.Data != want.Data {
			t.Fatalf("received data %x, want %x", got.Data, want.Data)
		}
	}
	if got := sender.Stats(); got != (Stats{Sent: 4}) {
		t.Fatal(got)
	}
}
