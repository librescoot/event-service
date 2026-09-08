package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const callTimeout = 5 * time.Second

const enqueueScript = `
if redis.call('LLEN', KEYS[1]) >= tonumber(ARGV[2]) then return 0 end
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], 30)
return 1
`

type rpcRequest struct {
	ID           string          `json:"id"`
	Method       string          `json:"method"`
	ReplyChannel string          `json:"reply_channel"`
	Deadline     int64           `json:"deadline"`
	Payload      json.RawMessage `json:"payload"`
}

type rpcReply struct {
	OK      bool            `json:"ok"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// Use an owned connection pool: retries can duplicate a mutation after a lost
// enqueue response, and closing the pool on cancellation interrupts socket I/O.
func rpcClient(client *redis.Client) *redis.Client {
	opts := *client.Options()
	opts.MaxRetries = -1
	opts.ContextTimeoutEnabled = true
	opts.DialTimeout = time.Second
	opts.ReadTimeout = time.Second
	opts.WriteTimeout = time.Second
	opts.PoolTimeout = time.Second
	return redis.NewClient(&opts)
}

func rpcContextError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Socket deadlines can fire just before the context timer is scheduled.
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return err
}

func uncertain(err error) error {
	return fmt.Errorf("extension RPC outcome unknown; reconcile via show/list before retry: %w", err)
}

// Call sends a single request without retrying it. A missing response does not
// imply that a mutation failed; inspect show/list before trying again.
func Call[Req, Resp any](ctx context.Context, client *redis.Client, method string, req Req) (Resp, error) {
	var zero Resp
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if !strings.HasPrefix(method, "v1.") || len(method) == 3 {
		return zero, errors.New("invalid RPC method")
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return zero, fmt.Errorf("encode request: %w", err)
	}
	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return zero, err
	}
	id := hex.EncodeToString(random[:])
	deadline, _ := ctx.Deadline()
	env := rpcRequest{id, method, Channel + ":reply:" + id, deadline.UnixMilli(), payload}
	body, err := json.Marshal(env)
	if err != nil {
		return zero, err
	}
	if len(body) > MaxRequestBytes {
		return zero, errors.New("RPC request exceeds size limit")
	}
	c := rpcClient(client)
	defer c.Close()
	stopClient := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stopClient()
	sub := c.Subscribe(ctx, env.ReplyChannel)
	defer sub.Close()
	stopSub := context.AfterFunc(ctx, func() { _ = sub.Close() })
	defer stopSub()
	// Receive the subscription acknowledgement before making the request visible.
	ack, err := sub.ReceiveTimeout(ctx, time.Until(deadline))
	if err != nil {
		return zero, fmt.Errorf("subscribe to RPC reply: %w", rpcContextError(ctx, err))
	}
	subscription, ok := ack.(*redis.Subscription)
	if !ok || subscription.Kind != "subscribe" || subscription.Channel != env.ReplyChannel {
		return zero, errors.New("invalid RPC subscription acknowledgement")
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	queued, err := c.Eval(ctx, enqueueScript, []string{Channel}, body, MaxQueuedRequests).Int()
	if err != nil {
		return zero, uncertain(rpcContextError(ctx, err))
	}
	if queued != 1 {
		return zero, errors.New("extension RPC queue is full")
	}
	for {
		msg, err := sub.ReceiveTimeout(ctx, time.Until(deadline))
		if err != nil {
			return zero, uncertain(rpcContextError(ctx, err))
		}
		message, ok := msg.(*redis.Message)
		if !ok {
			continue
		}
		if len(message.Payload) > MaxReplyBytes {
			return zero, uncertain(errors.New("RPC reply exceeds size limit"))
		}
		var reply rpcReply
		if err := json.Unmarshal([]byte(message.Payload), &reply); err != nil {
			return zero, uncertain(fmt.Errorf("decode reply: %w", err))
		}
		if !reply.OK {
			if reply.Error == "" {
				reply.Error = "RPC handler failed"
			}
			return zero, errors.New(reply.Error)
		}
		var result Resp
		if err := json.Unmarshal(reply.Payload, &result); err != nil {
			return zero, uncertain(fmt.Errorf("decode reply payload: %w", err))
		}
		return result, nil
	}
}

// Serve handles requests sequentially until ctx is canceled. Handlers must
// respect their context; no detached request goroutines are created.
func Serve(ctx context.Context, client *redis.Client, handler func(context.Context, string, json.RawMessage) (any, error)) error {
	if handler == nil {
		return errors.New("nil RPC handler")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c := rpcClient(client)
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	for ctx.Err() == nil {
		// A command deadline also bounds go-redis's extra BRPOP read-time grace.
		popCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		values, err := c.BRPop(popCtx, time.Second, Channel).Result()
		cancel()
		if ctx.Err() != nil {
			break
		}
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			timer := time.NewTimer(200 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
			continue
		}
		if len(values) != 2 {
			continue
		}
		serveRequest(ctx, c, handler, []byte(values[1]))
	}
	return ctx.Err()
}

func serveRequest(ctx context.Context, c *redis.Client, handler func(context.Context, string, json.RawMessage) (any, error), body []byte) {
	if ctx.Err() != nil || len(body) > MaxRequestBytes {
		return
	}
	var req rpcRequest
	if json.Unmarshal(body, &req) != nil {
		return
	}
	decoded, err := hex.DecodeString(req.ID)
	if err != nil || len(decoded) != 16 || hex.EncodeToString(decoded) != req.ID || req.ReplyChannel != Channel+":reply:"+req.ID {
		return
	}
	now := time.Now()
	deadline := time.UnixMilli(req.Deadline)
	if !deadline.After(now) || deadline.After(now.Add(30*time.Second)) || !strings.HasPrefix(req.Method, "v1.") || len(req.Method) == 3 || len(req.Payload) == 0 {
		return
	}
	// Allow a small clock-skew window in the envelope, but never grant a
	// handler more than the protocol's five-second processing budget.
	if ceiling := now.Add(callTimeout); deadline.After(ceiling) {
		deadline = ceiling
	}
	requestCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if requestCtx.Err() != nil {
		return
	}
	result, handlerErr := handler(requestCtx, req.Method, req.Payload)
	reply := rpcReply{OK: handlerErr == nil}
	if handlerErr != nil {
		reply.Error = handlerErr.Error()
	} else {
		reply.Payload, err = json.Marshal(result)
		if err != nil {
			reply = rpcReply{Error: "cannot encode RPC response"}
		}
	}
	encoded, err := json.Marshal(reply)
	if err != nil || len(encoded) > MaxReplyBytes {
		encoded, _ = json.Marshal(rpcReply{Error: "RPC reply exceeds size limit; reconcile via show/list before retry"})
	}
	if ctx.Err() != nil {
		return
	}
	// Publishing once avoids replaying anything after a transport failure.
	publishCtx, publishCancel := context.WithTimeout(ctx, time.Second)
	defer publishCancel()
	_ = c.Publish(publishCtx, req.ReplyChannel, encoded).Err()
}
