package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/topic"
)

// Compacting a run on request.
//
// A host watching a long turn (a console with a "compact" button, an operator
// who sees the context window filling) can ask the run to summarize the older
// part of its history now, rather than wait for the deployment's budget. The
// turn compacts before its next model call, exactly as the budget would have
// made it, and publishes context.compacted with Trigger "requested" and who
// asked. The deployment has to take requests: WithCompactRequests on the
// runtime and agentrt.Compaction.OnRequest on its agents, both of which
// fiber.WithCompaction sets.
//
// # How the request reaches the turn
//
// The way a stop does, minus the ledger. It is published to a control stream
// of its own (CompactTopic), keyed by the asker's tenant and the run, and the
// attempt executing the run listens there for as long as its turn lasts. Only
// a request made under the run's own tenant is heard.
//
// # A request is live, and only live
//
// A stop is recorded on the run because it decides the run's outcome, and a
// request that waited for the next attempt would still be the right thing to
// obey. A compaction decides nothing about the outcome, and a request that
// waited would be wrong: a turn continued after a park, or retried after a
// crash, would summarize a history the person never saw grow. So a request is
// honoured only by the attempt that was listening when it was made, judged by
// its timestamp against the moment the attempt began to listen. One made while
// the run is queued, parked, between attempts or over does nothing, and one
// made after the turn's last model call does nothing either. Timestamps come
// from the asking process's clock, so a request made within a clock skew of an
// attempt starting may be dropped; ask again.
//
// It is not durable and not acknowledged. Compact returns once the request is
// published; the run's stream says whether the turn compacted.

// compactVerb names the compaction stream within a run's control topics.
const compactVerb = "compact"

// ErrCompactUnavailable reports a runtime that does not carry requests to
// compact: it was built without WithCompactRequests, or its bus can be
// published to and not subscribed to, so no turn it executes could hear one.
var ErrCompactUnavailable = errors.New("runtime: compaction on request is not available")

// WithCompactRequests makes every turn listen for requests to compact, and
// Compact publish them. Pass it with agentrt.Compaction.OnRequest on the
// executor's agents: one without the other either listens for requests no
// turn obeys, or obeys requests no turn hears.
//
// Listening costs one bus subscription per running turn, beside the one for
// stop requests; on Redis that is a pooled connection held for the turn. A
// runtime without the option holds none and Compact refuses with
// ErrCompactUnavailable.
func WithCompactRequests() Option {
	return func(r *Runtime) { r.compactRequests = true }
}

// CompactRequest asks one run to compact its history.
type CompactRequest struct {
	// Tenant is the scope the request is made under, from the caller's
	// credential. Only a run of the same tenant hears it.
	Tenant string
	// RunID is the run, as Acceptance.RunID reported it.
	RunID string
	// By is who asked. It is published on context.compacted. Optional.
	By string
}

// CompactTopic is where a tenant's requests to compact a run are published:
// agent:_control:compact-<key>, the key a hash of the tenant and the run id,
// for the reasons StopTopic gives.
func CompactTopic(tenant, runID string) (topic.Topic, error) {
	if _, err := topic.Run("probe", runID); err != nil {
		return topic.Topic{}, err
	}
	h := sha256.New()
	var n [binary.MaxVarintLen64]byte
	for _, field := range []string{tenant, runID} {
		h.Write(n[:binary.PutUvarint(n[:], uint64(len(field)))])
		h.Write([]byte(field))
	}
	return topic.Control(compactVerb, hex.EncodeToString(h.Sum(nil)[:16]))
}

// Compact asks a run to compact its history before its next model call.
//
// It publishes the request and returns. Like Stop, it is the same call with
// the same nil result for a run that does not exist, belongs to another
// tenant, is not executing a turn, or has ended, so its answer never says
// whether a run exists. What happened is on the run's stream: a
// context.compacted event if the turn compacted.
//
// It returns ErrInvalid for a request that names no run and
// ErrCompactUnavailable when this runtime does not take requests
// (WithCompactRequests) or its bus cannot be subscribed to.
func (r *Runtime) Compact(ctx context.Context, req CompactRequest) error {
	if !r.compactRequests {
		return fmt.Errorf("%w: this runtime was built without WithCompactRequests", ErrCompactUnavailable)
	}
	if r.control == nil {
		return fmt.Errorf("%w: the bus this runtime was built with cannot be subscribed to", ErrCompactUnavailable)
	}
	req.RunID = strings.TrimSpace(req.RunID)
	tp, err := CompactTopic(req.Tenant, req.RunID)
	if err != nil {
		return fmt.Errorf("%w: compact: %w", ErrInvalid, err)
	}
	at := r.now().UTC()
	body, err := payload.Encode(payload.CompactRequest{RunID: req.RunID, By: clip(req.By), At: at})
	if err != nil {
		return fmt.Errorf("runtime: compact %s: %w", req.RunID, err)
	}
	e := envelope.Event{
		Topic:   tp,
		RunID:   tp.RunID(),
		Tenant:  req.Tenant,
		Agent:   topic.ControlName,
		Kind:    envelope.KindCompact,
		Payload: body,
		At:      at,
	}
	if err := r.bus.Publish(ctx, e); err != nil {
		return fmt.Errorf("runtime: compact %s: %w", req.RunID, err)
	}
	return nil
}

type compactSignalKey struct{}

// CompactSignal returns the channel on which the turn executing under ctx is
// asked to compact, or nil when it cannot be. An Executor hands it to the
// agent loop as agentrt.Turn.Compact; one that does not simply never
// compacts on request.
func CompactSignal(ctx context.Context) <-chan agentrt.CompactRequest {
	ch, _ := ctx.Value(compactSignalKey{}).(chan agentrt.CompactRequest)
	return ch
}

// listenCompact puts a CompactSignal on ctx and relays to it the requests to
// compact this run made from now until release is called.
func (r *Runtime) listenCompact(ctx context.Context, in Input, runID string) (context.Context, func()) {
	if !r.compactRequests || r.control == nil {
		return ctx, func() {}
	}
	tp, err := CompactTopic(in.Tag.Tenant, runID)
	if err != nil {
		r.logger.Error("fiber/runtime: compact topic is invalid", "run_id", runID, "error", err)
		return ctx, func() {}
	}
	// One slot: a request that finds it full folds into the one waiting,
	// which is what the turn would do with it anyway.
	signal := make(chan agentrt.CompactRequest, 1)
	since := r.now().UTC()
	listenCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sub := envelope.Subscription{Topic: tp, Filter: envelope.Filter{Kinds: []string{string(envelope.KindCompact)}}}
		for listenCtx.Err() == nil {
			// A resubscription resumes after the last request read, so a
			// request already relayed is not relayed again: each one would
			// cost a summary.
			err := r.relayCompact(listenCtx, &sub, in.Tag.Tenant, runID, since, signal)
			if listenCtx.Err() != nil {
				return
			}
			r.logger.Warn("fiber/runtime: lost the compact subscription; listening again",
				"run_id", runID, "error", err)
			select {
			case <-listenCtx.Done():
				return
			case <-time.After(stopRetry):
			}
		}
	}()
	return context.WithValue(ctx, compactSignalKey{}, signal), func() {
		cancel()
		<-done
	}
}

// relayCompact reads one subscription and relays every request this attempt
// should honour, until the subscription ends. It advances sub.From past every
// event it reads, so the caller resubscribes where it left off.
func (r *Runtime) relayCompact(ctx context.Context, sub *envelope.Subscription, tenant, runID string, since time.Time, signal chan<- agentrt.CompactRequest) error {
	stream, err := r.control.Subscribe(ctx, *sub)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()
	for {
		e, err := stream.Recv(ctx)
		if err != nil {
			return err
		}
		if e.Seq > sub.From {
			sub.From = e.Seq
		}
		if e.Kind != envelope.KindCompact || e.Tenant != tenant {
			continue
		}
		body, err := payload.Decode[payload.CompactRequest](e)
		if err != nil || body.RunID != runID || body.At.Before(since) {
			// Not a request this run's topic should carry, or one made
			// before this attempt was listening (see "A request is live").
			continue
		}
		select {
		case signal <- agentrt.CompactRequest{By: body.By}:
		default:
		}
	}
}
