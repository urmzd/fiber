// Package files is the part of file intake every trigger shares: collecting a
// message's files onto its tag under the per-tag caps, logging what was left
// out, and handing the tag off without discarding work the provider stopped
// waiting for.
//
// What a file is on each surface, where it may be fetched from and with which
// credential stays in each connector. This package never sees a URL.
package files

import (
	"context"
	"log/slog"
	"time"

	"github.com/urmzd/mandatum/pkg/attachment/intake"
	"github.com/urmzd/mandatum/pkg/connectors"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// HandoffGrace bounds how long a tag that carries files waits to be taken
// once its files are in. See Attach.
const HandoffGrace = 10 * time.Second

// Attach brings in candidates onto tag as attachments, in order, under
// tenant, with the tag's source as each file's origin. It returns the context
// to hand the tag off with, and its cancel.
//
// Fetching can outlast a provider's acknowledgement window (Slack waits three
// seconds), and a provider that gives up closes the request. So the files are
// fetched under a context that survives the request, bounded by the caps'
// timeout, and the handoff that follows gets a short grace of its own rather
// than the request's context: discarding a tag whose files are already in
// would only make the provider's retry fetch them again. A retry that does
// arrive carries the same tag id, and the runtime accepts it as the run it
// already has.
//
// With no candidates, or file intake disabled, it changes nothing and returns
// ctx itself.
func Attach(ctx context.Context, cfg connectors.Files, connector, tenant string, tag *envelope.Tag, candidates []intake.Candidate) (context.Context, context.CancelFunc) {
	if !cfg.Enabled() || len(candidates) == 0 || tenant == "" {
		return ctx, func() {}
	}
	detached := context.WithoutCancel(ctx)
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	tag.Attachments = intake.Collect(detached, tenant, tag.Source, cfg.Caps, candidates, func(s intake.Skip) {
		log.Warn("connector: a file on the message was not brought in; the agent is told it could not be read",
			"connector", connector, "tag", tag.ID, "file", s.Name, "reason", s.Reason, "error", s.Err)
	})
	return context.WithTimeout(detached, HandoffGrace)
}
