//go:build linux

package canbus

import (
	"errors"
	"reflect"
	"testing"

	"golang.org/x/sys/unix"
)

func TestSocketOwnershipAndSetup(t *testing.T) {
	failure := errors.New("setup failure")
	cleanup := errors.New("cleanup failure")
	for _, stage := range []string{"socket", "index", "zero index", "filter", "bind", "success"} {
		for _, closeFails := range []bool{false, true} {
			t.Run(stage+map[bool]string{true: "/close-error", false: "/close-ok"}[closeFails], func(t *testing.T) {
				var calls []string
				closes := 0
				ops := socketOps{
					socket: func(domain, typ, protocol int) (int, error) {
						calls = append(calls, "socket")
						if domain != unix.AF_CAN || typ != unix.SOCK_RAW|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC || protocol != unix.CAN_RAW {
							t.Fatal("incorrect socket flags")
						}
						if stage == "socket" {
							return -1, failure
						}
						return 42, nil
					},
					index: func(fd int, name string) (int, error) {
						calls = append(calls, "index")
						if fd != 42 || name != "can0" {
							t.Fatal("bad index lookup")
						}
						if stage == "index" {
							return 0, failure
						}
						if stage == "zero index" {
							return 0, nil
						}
						return 7, nil
					},
					filter: func(fd, level, opt int, filters []unix.CanFilter) error {
						calls = append(calls, "filter")
						if fd != 42 || level != unix.SOL_CAN_RAW || opt != unix.CAN_RAW_FILTER || len(filters) != 0 {
							t.Fatal("receive filters not disabled")
						}
						if stage == "filter" {
							return failure
						}
						return nil
					},
					bind: func(fd int, addr unix.Sockaddr) error {
						calls = append(calls, "bind")
						if fd != 42 || addr.(*unix.SockaddrCAN).Ifindex != 7 {
							t.Fatal("bad bind")
						}
						if stage == "bind" {
							return failure
						}
						return nil
					},
					close: func(fd int) error {
						if fd != 42 {
							t.Fatal("closed wrong fd")
						}
						closes++
						if closeFails {
							return cleanup
						}
						return nil
					},
					write: func(fd int, p []byte) (int, error) {
						if fd != 42 {
							t.Fatal("wrote wrong fd")
						}
						return len(p), nil
					},
				}
				conn, err := openSocket("can0", ops)
				if stage == "success" {
					if err != nil || conn == nil || closes != 0 {
						t.Fatal("ownership transfer failed", err)
					}
					if !reflect.DeepEqual(calls, []string{"socket", "index", "filter", "bind"}) {
						t.Fatal(calls)
					}
					if n, err := conn.Write([]byte{1}); err != nil || n != 1 {
						t.Fatal(n, err)
					}
					err = conn.Close()
					if closeFails != errors.Is(err, cleanup) {
						t.Fatal(err)
					}
				} else {
					if err == nil || conn != nil {
						t.Fatal("setup failure ignored")
					}
					if stage != "zero index" && !errors.Is(err, failure) {
						t.Fatal("lost setup error", err)
					}
					if stage == "socket" {
						if closes != 0 {
							t.Fatal("closed unowned fd")
						}
						return
					}
					if closeFails && !errors.Is(err, cleanup) {
						t.Fatal("lost cleanup failure", err)
					}
				}
				if closes != 1 {
					t.Fatalf("fd leak or duplicate close: %d", closes)
				}
			})
		}
	}
}

func TestInvalidInterfaceDoesNotOpenSocket(t *testing.T) {
	if conn, err := openSocket("", socketOps{}); conn != nil || err == nil {
		t.Fatal("invalid interface accepted")
	}
}
