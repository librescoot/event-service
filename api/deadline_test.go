package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestServerCapsProcessingDespiteClockSkewWindow(t *testing.T) {
	client := testRedis(t)
	id := strings.Repeat("c", 32)
	req := rpcRequest{id, MethodStatus, Channel + ":reply:" + id, time.Now().Add(15 * time.Second).UnixMilli(), json.RawMessage(`{}`)}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	serveRequest(context.Background(), client, func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
		called = true
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > callTimeout {
			t.Fatal("handler budget exceeds five seconds")
		}
		return Empty{}, nil
	}, body)
	if !called {
		t.Fatal("clock-skew window did not accept request")
	}
}
