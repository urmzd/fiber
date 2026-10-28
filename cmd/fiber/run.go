package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/urmzd/fiber"
	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/topic"
)

// The scope and the person a local run belongs to. Both are fixed: nothing
// outlives the process, so there is no one else's data to keep apart from,
// and a run acts for this one person.
const (
	tenant = "local"
	origin = "cli"
	author = "fiber run"
)

// actor is who a local run acts for and who asked.
var actor = envelope.Actor{ID: "local", Display: "fiber run"}

// defaultTimeout bounds a run when -timeout is not given. It is generous: an
// agent's tool loop on a real model takes a while, and a retried attempt
// waits out its backoff first.
const defaultTimeout = 5 * time.Minute

// drainQuiet is how long the stream is read for once the ledger reports a run
// failed, for the events it published before it did. They are already on
// the bus by then, so this is only the time it takes to read them.
const drainQuiet = 150 * time.Millisecond

// drainLimit bounds reading a completed run's stream up to its last event.
const drainLimit = 2 * time.Second

// runFlags are the run command's flags.
type runFlags struct {
	provider, model string
	offline, tools  bool
	json            bool
	timeout         time.Duration
}

// runCommand is "fiber run".
func runCommand(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var f runFlags
	fs := flag.NewFlagSet("fiber run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { usage(stderr) }
	fs.StringVar(&f.provider, "provider", "", "run on this model provider instead of the spec's")
	fs.StringVar(&f.model, "model", "", "run on this model instead of the spec's")
	fs.BoolVar(&f.offline, "offline", false, "run on the offline provider, which calls no model")
	fs.BoolVar(&f.tools, "tools", false, "list the tools the agent would be granted, and run nothing")
	fs.BoolVar(&f.json, "json", false, "print one JSON object per event")
	fs.DurationVar(&f.timeout, "timeout", defaultTimeout, "give up after this long (0 waits forever)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitCompleted
		}
		return exitBadInput
	}
	bad := func(format string, a ...any) int {
		fmt.Fprintf(stderr, "fiber run: "+format+"\n", a...)
		return exitBadInput
	}
	rest := fs.Args()
	for _, a := range rest[min(1, len(rest)):] {
		// Only a flag's own name: a MESSAGE may start with a dash.
		name, _, _ := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if strings.HasPrefix(a, "-") && fs.Lookup(name) != nil {
			return bad("flag %s comes after SPEC; flags go before it", a)
		}
	}
	switch {
	case len(rest) == 0:
		return bad("no SPEC; usage: fiber run [flags] SPEC MESSAGE")
	case len(rest) == 1 && !f.tools:
		return bad("no MESSAGE; usage: fiber run [flags] SPEC MESSAGE")
	case len(rest) > 2:
		return bad("too many arguments; quote MESSAGE if it has spaces")
	case f.offline && f.provider != "":
		return bad("-offline and -provider both choose the provider; give one")
	case f.timeout < 0:
		return bad("-timeout %s is negative", f.timeout)
	}
	path, message := rest[0], ""
	if len(rest) == 2 {
		message = rest[1]
	}
	if strings.TrimSpace(message) == "" && !f.tools {
		return bad("MESSAGE is empty")
	}

	spec, err := loadSpec(path, stdin)
	if err != nil {
		return bad("%v", err)
	}
	f.override(&spec)

	// The operator log: warnings only, such as a granted tool this
	// composition cannot resolve. It goes to stderr, so stdout is the run's
	// events alone. A listing says everything that log would.
	log := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelWarn, ReplaceAttr: dropTime}))
	if f.tools {
		log = slog.New(slog.DiscardHandler)
	}
	app, err := fiber.New(ctx, fiber.WithLogger(log))
	if err != nil {
		fmt.Fprintf(stderr, "fiber run: compose: %v\n", err)
		return exitFailed
	}
	defer func() { _ = app.Close() }()

	rev, err := app.Registry().Create(ctx, tenant, spec, author)
	if err != nil {
		return bad("%s: %v", path, err)
	}
	tag := envelope.Tag{
		ID:     "cli-" + strconv.FormatInt(time.Now().UnixNano(), 36),
		Tenant: tenant,
		Agent:  rev.Spec.Name,
		Origin: origin,
		Text:   message,
		Actor:  actor,
		At:     time.Now().UTC(),
	}
	if f.tools {
		return listTools(ctx, app, tag, rev, f.json, stdout, stderr)
	}

	// Delivery and execution, in this process, for as long as the command
	// runs. The router has nothing to deliver: the tag names no route and no
	// surface, and the events are read off the bus instead.
	runCtx, stopApp := context.WithCancel(ctx)
	appDone := make(chan error, 1)
	go func() { appDone <- app.Run(runCtx) }()
	defer func() {
		stopApp()
		if err := <-appDone; err != nil {
			fmt.Fprintf(stderr, "fiber run: shut down: %v\n", err)
		}
	}()

	var out renderer = &textRenderer{w: stdout}
	if f.json {
		out = &jsonRenderer{enc: json.NewEncoder(stdout)}
	}
	return follow(ctx, app, tag, f.timeout, out, stderr)
}

// override applies the provider and model flags to spec.
func (f runFlags) override(spec *agentspec.AgentSpec) {
	if f.offline {
		// Every model call goes to the offline provider, so a fallback
		// that would need a key of its own is dropped with the rest.
		spec.Provider = agentspec.ProviderOffline
		spec.Fallbacks, spec.FallbackOn = nil, nil
	}
	if f.provider != "" {
		spec.Provider = f.provider
	}
	if f.model != "" {
		spec.Model = f.model
	}
}

// loadSpec reads one agent spec from path, or from stdin when path is "-".
// It is strict: a field the spec format does not have is an error rather than
// ignored, because a misspelt field is the mistake a local check is for.
func loadSpec(path string, stdin io.Reader) (agentspec.AgentSpec, error) {
	r := stdin
	if path != "-" {
		f, err := os.Open(path)
		if err != nil {
			return agentspec.AgentSpec{}, err
		}
		defer func() { _ = f.Close() }()
		r = f
	}
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	var spec agentspec.AgentSpec
	if err := dec.Decode(&spec); err != nil {
		if errors.Is(err, io.EOF) {
			return agentspec.AgentSpec{}, fmt.Errorf("%s: empty, want an agent spec as JSON", path)
		}
		return agentspec.AgentSpec{}, fmt.Errorf("%s: not an agent spec: %w", path, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return agentspec.AgentSpec{}, fmt.Errorf("%s: more than one JSON value; a spec is one object", path)
	}
	return spec, nil
}

// follow raises tag, prints its run's events as they stream, and returns the
// exit status for how the run ended. It waits for the run for timeout, or
// for as long as ctx lasts when timeout is zero; ctx ends on an interrupt.
//
// The events are read off the run's topic, subscribed before the tag is
// raised, and the ledger decides how the run ended: a failed event on the
// bus says what an attempt did, and only the ledger knows whether the run
// will be attempted again.
func follow(ctx context.Context, app *fiber.App, tag envelope.Tag, timeout time.Duration, out renderer, stderr io.Writer) int {
	wait := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		wait, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	runID := runtime.RunID(tag.Tenant, tag.ID)
	if _, err := topic.Run(tag.Agent, runID); err != nil {
		fmt.Fprintf(stderr, "fiber run: %v\n", err)
		return exitBadInput
	}
	gaveUp := func() int {
		out.close()
		if ctx.Err() != nil {
			fmt.Fprintf(stderr, "fiber run: interrupted; run %s did not finish\n", runID)
		} else {
			fmt.Fprintf(stderr, "fiber run: timed out after %s; run %s did not finish\n", timeout, runID)
		}
		return exitFailed
	}
	events := stream(wait, app.Bus(), bus.Subscription{
		Tenant:   runtime.TenantOf(tag),
		Channels: []string{runtime.ChannelOf(tag)},
		Types:    []string{"run", "run.usage", "run.context"},
		Run:      runID,
	}, stderr)
	if _, err := app.Invoke(wait, tag); err != nil {
		if wait.Err() != nil {
			return gaveUp()
		}
		fmt.Fprintf(stderr, "fiber run: raise the tag: %v\n", err)
		return exitFailed
	}
	type ending struct {
		out runtime.Output
		err error
	}
	ended := make(chan ending, 1)
	go func() {
		o, err := app.Runtime().Result(wait, runID)
		ended <- ending{o, err}
	}()
	var seen uint64 // the highest sequence printed
	show := func(e event.Envelope) {
		out.event(e)
		seen = max(seen, e.Seq)
	}
	for {
		select {
		case e, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			show(e)
			if e.Type == event.TypeParked {
				// Nothing here can answer it, and the run waits for an
				// answer however long this command does.
				out.close()
				fmt.Fprintf(stderr, "fiber run: run %s parked on a question; fiber run cannot answer it\n", runID)
				return exitFailed
			}
		case end := <-ended:
			if end.err != nil && wait.Err() != nil {
				return gaveUp()
			}
			if end.err != nil {
				drain(events, show, drainQuiet, func() bool { return false })
				out.close()
				fmt.Fprintf(stderr, "fiber run: run %s failed: %v\n", runID, end.err)
				return exitFailed
			}
			drain(events, show, drainLimit, func() bool { return seen >= end.out.LastSeq })
			out.close()
			if s := end.out.Stopped; s != nil {
				fmt.Fprintf(stderr, "fiber run: run %s was stopped: %s\n", runID, s.Reason)
				return exitFailed
			}
			return exitCompleted
		case <-wait.Done():
			return gaveUp()
		}
	}
}

// stream delivers the events of one run, in order, until ctx ends. A
// subscription that falls behind the bus's backlog resumes after the last
// event it delivered, which is what an event's Seq is for; any other error
// ends the stream, and the ledger still says how the run ended.
func stream(ctx context.Context, b bus.Bus, want bus.Subscription, stderr io.Writer) <-chan event.Envelope {
	sub, err := b.Subscribe(ctx, want)
	events := make(chan event.Envelope)
	if err != nil {
		if ctx.Err() == nil {
			fmt.Fprintf(stderr, "fiber run: subscribe to run %s: %v\n", want.Run, err)
		}
		close(events)
		return events
	}
	go func() {
		defer close(events)
		var last uint64
		for {
			e, err := sub.Recv(ctx)
			if errors.Is(err, bus.ErrSlowConsumer) {
				_ = sub.Close()
				want.From = last
				if sub, err = b.Subscribe(ctx, want); err == nil {
					continue
				}
			}
			if err != nil {
				_ = sub.Close()
				if ctx.Err() == nil {
					fmt.Fprintf(stderr, "fiber run: read run %s: %v\n", want.Run, err)
				}
				return
			}
			last = e.Seq
			select {
			case events <- e:
			case <-ctx.Done():
				_ = sub.Close()
				return
			}
		}
	}()
	return events
}

// drain prints what is left on events once the run has ended: until done
// holds, until nothing arrives for quiet, or until the stream ends.
func drain(events <-chan event.Envelope, show func(event.Envelope), quiet time.Duration, done func() bool) {
	if events == nil || done() {
		return
	}
	t := time.NewTimer(quiet)
	defer t.Stop()
	for {
		select {
		case e, ok := <-events:
			if !ok {
				return
			}
			show(e)
			if done() {
				return
			}
			t.Reset(quiet)
		case <-t.C:
			return
		}
	}
}

// dropTime leaves the time out of the operator log: the command's output is
// read as it happens.
func dropTime(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 && a.Key == slog.TimeKey {
		return slog.Attr{}
	}
	return a
}
