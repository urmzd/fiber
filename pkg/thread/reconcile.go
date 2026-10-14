package thread

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// Message is one message as its surface holds it now.
type Message struct {
	// ID is the surface's own stable id for the message: a Slack timestamp,
	// a GitHub comment id. It is required, and it must not change when the
	// message is edited, because it is how the journal knows it has already
	// seen the message.
	ID string
	// Text is the message as it reads now. A message with no text (an
	// upload, a reaction) is not journaled.
	Text string
	// Actor is who posted it.
	Actor envelope.Actor
	// At is when it was posted.
	At time.Time
	// Own marks a message this deployment posted itself: an agent's rendered
	// answer or its progress. Reconcile skips these. What an agent said
	// reaches the journal as the run's final answer, appended by the run;
	// the surface's copy is a rendering of that, possibly truncated or
	// reformatted, and journaling it too would record every answer twice.
	Own bool
}

// Lister is what a connector implements to let a thread be reconciled with
// its surface.
//
// Messages returns every message in the thread surface names, oldest first,
// as the surface holds them now. surface is a tag's Source address, unreduced,
// so the connector sees the same address it built. tenant scopes the call the
// way it scopes everything else: a connector serving several tenants must not
// answer from another one's credentials.
//
// A connector that implements Lister must also tell the runtime which surface
// message a tag is (see runtime.WithMessageID). Otherwise the tagging message
// is journaled once here, as a message someone posted, and once by its run.
type Lister interface {
	Messages(ctx context.Context, tenant string, surface address.Address) ([]Message, error)
}

// Reconcile appends every surface message the journal has not seen, in
// surface order, as user entries.
//
// It only appends. A message edited on the surface keeps the text the journal
// first saw, and a message deleted from the surface stays in the journal: a
// run already pinned to that history must be able to read it again unchanged,
// and a rewritten entry would move the first differing byte of every later
// request into the middle of the conversation. The surface stays the record
// of what the thread says today; the journal is the record of what the agent
// was shown.
//
// skip names surface messages to leave out. pkg/runtime passes the id of the
// message that raised the tag being accepted: that message is the run's new
// request, and it joins the journal when the run completes rather than
// appearing in the history the same run is pinned to.
//
// Reconcile is idempotent and safe to run concurrently with other appends: it
// is one Append, and Append recognizes the messages it already holds by their
// surface ids.
func Reconcile(ctx context.Context, store Store, lister Lister, key Key, surface address.Address, skip ...string) error {
	if store == nil || lister == nil {
		return fmt.Errorf("%w: reconcile needs a store and a lister", ErrInvalid)
	}
	if err := key.Validate(); err != nil {
		return err
	}
	if surface.IsZero() || key.OneShot() {
		// Nothing to list: the thread has no surface a second message could
		// have been posted on.
		return nil
	}
	msgs, err := lister.Messages(ctx, key.Tenant, surface)
	if err != nil {
		return fmt.Errorf("thread: reconcile %s: list surface: %w", key, err)
	}
	skipped := make(map[string]bool, len(skip))
	for _, id := range skip {
		skipped[id] = true
	}
	entries := make([]Entry, 0, len(msgs))
	for i, m := range msgs {
		if m.ID == "" {
			return fmt.Errorf("%w: reconcile %s: surface message %d of %d has no id", ErrInvalid, key, i+1, len(msgs))
		}
		if m.Own || skipped[m.ID] || strings.TrimSpace(m.Text) == "" {
			continue
		}
		entries = append(entries, Entry{Role: RoleUser, Text: m.Text, Actor: m.Actor, SurfaceID: m.ID, At: m.At})
	}
	if len(entries) == 0 {
		return nil
	}
	if _, err := store.Append(ctx, key, entries...); err != nil {
		return fmt.Errorf("thread: reconcile %s: %w", key, err)
	}
	return nil
}
