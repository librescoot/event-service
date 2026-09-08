package action

import (
	"context"
	"fmt"

	"github.com/brutella/can"
	"github.com/librescoot/event-service/internal/canbus"
	"github.com/librescoot/eventbus"
)

// CANSender is owned by the service, not individual rules. Constructing an
// action only validates a frame; the first execution opens its socket.
type CANSender interface {
	Send(context.Context, string, can.Frame) error
}

type canAction struct {
	sender CANSender
	iface  string
	frame  can.Frame
}

func newCANAction(spec canbus.Spec, sender CANSender) (Action, error) {
	frame, err := canbus.Parse(spec)
	if err != nil {
		return nil, err
	}
	if sender == nil {
		return nil, fmt.Errorf("CAN sender is unavailable")
	}
	return &canAction{sender: sender, iface: spec.Iface, frame: frame}, nil
}

func (a *canAction) Kind() string { return "can" }

func (a *canAction) Do(ctx context.Context, _ eventbus.Event) error {
	return a.sender.Send(ctx, a.iface, a.frame)
}
