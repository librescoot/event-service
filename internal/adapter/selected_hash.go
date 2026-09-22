package adapter

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const selectedHashReadTimeout = 2 * time.Second

// Bound both the script's work and the largest possible reply, independently
// of the number and size of unselected fields in the hash.
const (
	selectedHashMaxFields = 1024
	selectedHashMaxBytes  = 1 << 20
)

const selectedHashReadScript = `
local result = {}
for i = 1, #ARGV, 2 do
  local field = ARGV[i]
  local length = redis.call('HSTRLEN', KEYS[1], field)
  if length > tonumber(ARGV[i+1]) then
    result[#result+1] = {'oversized', ''}
  else
    local value = redis.call('HGET', KEYS[1], field)
    if value == false then
      result[#result+1] = {'missing', ''}
    else
      result[#result+1] = {'normal', value}
    end
  end
end
return result
`

type selectedHashWatcher struct {
	client     *redis.Client
	hash       string
	fields     []string
	args       []interface{}
	onField    func(string, string)
	invalidate func(string)
	// onSnapshot, when supplied before Start, replaces individual callbacks.
	// It lets the adapter install a whole snapshot before publishing events.
	onSnapshot func(map[string]string, []string)
	configErr  error

	mu      sync.Mutex
	started bool
	ready   bool
	active  bool
	stopped bool
	ctx     context.Context
	cancel  context.CancelFunc
	pubsub  *redis.PubSub
	reader  *redis.Client
	cleaned chan struct{}
	wg      sync.WaitGroup
}

func newSelectedHashWatcher(client *redis.Client, hash string, fields map[string]int, onField func(field, value string), invalidate func(field string)) *selectedHashWatcher {
	w := &selectedHashWatcher{client: client, hash: hash, onField: onField, invalidate: invalidate}
	if client == nil || hash == "" || onField == nil || invalidate == nil || len(fields) == 0 || len(fields) > selectedHashMaxFields {
		w.configErr = fmt.Errorf("invalid selected hash watcher configuration")
	}
	for field := range fields {
		w.fields = append(w.fields, field)
	}
	sort.Strings(w.fields)
	total := 0
	for _, field := range w.fields {
		limit := fields[field]
		if limit < 0 || limit > selectedHashMaxBytes || total > selectedHashMaxBytes-limit {
			w.configErr = fmt.Errorf("selected hash byte budget exceeds limits")
			break
		}
		total += limit
		w.args = append(w.args, field, limit)
	}
	return w
}

// Start confirms the exact subscription before synchronously seeding selected
// fields. Activate must be called only after the caller clears seed suppression.
// A watcher is single-use; Stop is safe before Start and after failed Start.
func (w *selectedHashWatcher) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started || w.stopped {
		return fmt.Errorf("selected hash watcher already started or stopped")
	}
	w.started = true
	if w.configErr != nil {
		return w.configErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	w.ctx, w.cancel = context.WithCancel(ctx)
	// A dedicated client makes deadlines effective even when the supplied
	// client's ContextTimeoutEnabled is false, and prevents retry amplification.
	opts := *w.client.Options()
	opts.ContextTimeoutEnabled = true
	opts.MaxRetries = -1
	opts.DialTimeout = selectedHashReadTimeout
	opts.ReadTimeout = selectedHashReadTimeout
	opts.WriteTimeout = selectedHashReadTimeout
	opts.PoolTimeout = selectedHashReadTimeout
	w.reader = redis.NewClient(&opts)
	seedCtx, cancel := context.WithTimeout(w.ctx, selectedHashReadTimeout)
	defer cancel()
	w.pubsub = w.reader.Subscribe(seedCtx, w.hash)
	w.cleaned = make(chan struct{})
	context.AfterFunc(w.ctx, func() {
		_ = w.pubsub.Close()
		_ = w.reader.Close()
		close(w.cleaned)
	})
	fail := func(err error) error {
		w.cancel()
		<-w.cleaned
		return err
	}
	ack, err := w.pubsub.Receive(seedCtx)
	if err != nil {
		return fail(err)
	}
	sub, ok := ack.(*redis.Subscription)
	if !ok || sub.Kind != "subscribe" || sub.Channel != w.hash {
		return fail(fmt.Errorf("selected hash subscription was not confirmed"))
	}
	if err := w.refresh(seedCtx); err != nil {
		return fail(err)
	}
	w.ready = true
	return nil
}

func (w *selectedHashWatcher) Activate() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.ready || w.active || w.stopped || w.ctx.Err() != nil {
		return
	}
	w.active = true
	messages := w.pubsub.ChannelWithSubscriptions(redis.WithChannelHealthCheckInterval(0))
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-w.ctx.Done():
				return
			case message, ok := <-messages:
				if !ok || w.ctx.Err() != nil {
					return
				}
				switch message.(type) {
				case *redis.Subscription:
					// Pub/sub cannot recover missed notifications. Do not replay
					// current values as transitions after reconnect; the shadow
					// remains stale until the next real notification refreshes it.
					log.Printf("selected hash %q resubscribed; missed notifications are not replayed", w.hash)
				case *redis.Message:
					if err := w.refresh(w.ctx); err != nil && w.ctx.Err() == nil {
						log.Printf("refresh selected hash %q: %v", w.hash, err)
					}
				}
			}
		}
	}()
}

func (w *selectedHashWatcher) refresh(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, selectedHashReadTimeout)
	defer cancel()
	raw, err := w.reader.Eval(ctx, selectedHashReadScript, []string{w.hash}, w.args...).Slice()
	if err != nil {
		return err
	}
	if len(raw) != len(w.fields) {
		return fmt.Errorf("invalid selected hash reply length")
	}
	// Validate the entire snapshot before allowing any callbacks.
	type value struct{ tag, text string }
	values := make([]value, len(raw))
	for i, item := range raw {
		tuple, ok := item.([]interface{})
		if !ok || len(tuple) != 2 {
			return fmt.Errorf("invalid selected hash tuple")
		}
		tag, tagOK := tuple[0].(string)
		text, textOK := tuple[1].(string)
		if !tagOK || !textOK || (tag != "normal" && tag != "missing" && tag != "oversized") {
			return fmt.Errorf("invalid selected hash value")
		}
		values[i] = value{tag, text}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.onSnapshot != nil {
		snapshot := make(map[string]string, len(values))
		var invalid []string
		for i, value := range values {
			if value.tag == "oversized" {
				log.Printf("selected hash %q field %q exceeds byte limit", w.hash, w.fields[i])
				invalid = append(invalid, w.fields[i])
			} else {
				snapshot[w.fields[i]] = value.text
			}
		}
		w.onSnapshot(snapshot, invalid)
		return nil
	}
	for i, value := range values {
		if value.tag == "oversized" {
			log.Printf("selected hash %q field %q exceeds byte limit", w.hash, w.fields[i])
			w.invalidate(w.fields[i])
		} else {
			w.onField(w.fields[i], value.text)
		}
	}
	return nil
}

func (w *selectedHashWatcher) Stop() error {
	w.mu.Lock()
	w.stopped = true
	if w.cancel != nil {
		w.cancel()
	}
	cleaned := w.cleaned
	w.mu.Unlock()
	if cleaned != nil {
		<-cleaned
	}
	w.wg.Wait()
	return nil
}
