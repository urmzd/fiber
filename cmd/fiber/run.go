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
	"go.yaml.in/yaml/v3"

	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/runtime"
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
var actor = event.Actor{ID: "local", Display: "fiber run"}

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
	policy          string
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
	fs.StringVar(&f.provider, "provider", "", "run on this model provider instead of the definition's")
	fs.StringVar(&f.model, "model", "", "run on this model instead of the definition's")
	fs.StringVar(&f.policy, "policy", "", "read the agent's policy from this JSON file")
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
			return bad("flag %s comes after DEFINITION; flags go before it", a)
		}
	}
	switch {
	case len(rest) == 0:
		return bad("no DEFINITION; usage: fiber run [flags] DEFINITION MESSAGE")
	case len(rest) == 1 && !f.tools:
		return bad("no MESSAGE; usage: fiber run [flags] DEFINITION MESSAGE")
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

	content, err := loadDefinition(path, stdin)
	if err != nil {
		return bad("%v", err)
	}
	policy, err := loadPolicy(f.policy)
	if err != nil {
		return bad("%v", err)
	}
	if content, err = f.override(content); err != nil {
		return bad("%s: %v", path, err)
	}
	if f.offline {
		policy.FallbackOn = nil
	}

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

	rec, err := app.Definitions().Put(ctx, definition.Record{Tenant: tenant, Content: content, Policy: policy, CreatedBy: author})
	if err != nil {
		return bad("%s: %v", path, err)
	}
	// One file is the whole tree here, so a sub-agent it names cannot
	// resolve, and that is the file's mistake.
	checks := definition.Checks{Providers: app.Providers()}
	if _, err := definition.NewResolver(app.Definitions(), checks).Resolve(ctx, tenant, rec.Name); err != nil {
		return bad("%s: %v", path, err)
	}
	trigger := cliMessage(message, rec.Name)
	if f.tools {
		return listTools(ctx, app, trigger, rec, f.json, stdout, stderr)
	}

	// Delivery and execution, in this process, for as long as the command
	// runs. Nothing is delivered: the message has no surface, and the events
	// are read off the bus instead.
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
	return follow(ctx, app, trigger, rec.Name, f.timeout, out, stderr)
}

// cliMessage is the message.posted.v1 a local run answers. Its source is not an
// address, so the run has no surface, and it publishes on cli://local/cli.
func cliMessage(text, agent string) event.Envelope {
	return event.Envelope{
		ID:      "cli-" + strconv.FormatInt(time.Now().UnixNano(), 36),
		Source:  origin + ":" + tenant,
		Type:    event.TypeMessagePosted,
		Channel: origin + "://" + tenant + "/" + origin,
		Time:    time.Now().UTC(),
		Tenant:  tenant,
		Data:    event.MessagePosted{Request: event.Request{Text: text, Actor: actor, Mentions: []string{agent}}},
	}
}

// override applies the provider and model flags to a definition file. It
// edits model.use in the frontmatter and leaves the rest of the file as it
// was written; -offline also drops the fallbacks, which would each need a key
// of their own.
func (f runFlags) override(content string) (string, error) {
	provider := f.provider
	if f.offline {
		provider = definition.ProviderOffline
	}
	if provider == "" && f.model == "" {
		return content, nil
	}
	front, body, err := splitFrontmatter(content)
	if err != nil {
		return "", err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(front), &doc); err != nil || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return "", errors.New("the frontmatter is not a YAML mapping")
	}
	model := child(doc.Content[0], "model")
	if model == nil || model.Kind != yaml.MappingNode {
		return "", errors.New("no model to override; the definition must name one")
	}
	use := child(model, "use")
	if use == nil || use.Kind != yaml.ScalarNode {
		return "", errors.New("model.use is not set")
	}
	prov, name, _ := strings.Cut(use.Value, "/")
	if provider != "" {
		prov = provider
	}
	if f.model != "" {
		name = f.model
	}
	use.Value, use.Style = prov+"/"+name, yaml.DoubleQuotedStyle
	if f.offline {
		for i := 0; i+1 < len(model.Content); i += 2 {
			if model.Content[i].Value == "fallback" {
				model.Content = append(model.Content[:i], model.Content[i+2:]...)
				break
			}
		}
	}
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return "", err
	}
	return "---\n" + string(out) + "---\n" + body, nil
}

// child is the value under key in a YAML mapping, or nil.
func child(m *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

// splitFrontmatter splits a definition file into its frontmatter and body.
func splitFrontmatter(content string) (front, body string, err error) {
	rest, ok := strings.CutPrefix(strings.TrimLeft(content, "\ufeff"), "---\n")
	if !ok {
		rest, ok = strings.CutPrefix(content, "---\r\n")
	}
	if !ok {
		return "", "", errors.New("the file does not start with a --- line")
	}
	i := strings.Index(rest, "\n---")
	if i < 0 {
		return "", "", errors.New("the frontmatter has no closing --- line")
	}
	front = rest[:i+1]
	body = strings.TrimPrefix(strings.TrimPrefix(rest[i+4:], "\r"), "\n")
	return front, body, nil
}

// loadDefinition reads one .agent.md file from path, or from stdin when path
// is "-".
func loadDefinition(path string, stdin io.Reader) (string, error) {
	r := stdin
	if path != "-" {
		f, err := os.Open(path)
		if err != nil {
			return "", err
		}
		defer func() { _ = f.Close() }()
		r = f
	}
	raw, err := io.ReadAll(io.LimitReader(r, maxDefinition+1))
	switch {
	case err != nil:
		return "", fmt.Errorf("%s: %w", path, err)
	case len(raw) > maxDefinition:
		return "", fmt.Errorf("%s: larger than %d bytes", path, maxDefinition)
	case strings.TrimSpace(string(raw)) == "":
		return "", fmt.Errorf("%s: empty, want an .agent.md definition", path)
	}
	return string(raw), nil
}

// maxDefinition bounds a definition file.
const maxDefinition = 1 << 20

// loadPolicy reads a policy from a JSON file. No path is the zero policy,
// which grants nothing. It is strict: a field a policy does not have is an
// error rather than ignored, because a misspelt field is the mistake a local
// check is for.
func loadPolicy(path string) (definition.Policy, error) {
	if path == "" {
		return definition.Policy{}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return definition.Policy{}, err
	}
	defer func() { _ = f.Close() }()
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	var p definition.Policy
	if err := dec.Decode(&p); err != nil {
		if errors.Is(err, io.EOF) {
			return definition.Policy{}, fmt.Errorf("%s: empty, want a policy as JSON", path)
		}
		return definition.Policy{}, fmt.Errorf("%s: not a policy: %w", path, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return definition.Policy{}, fmt.Errorf("%s: more than one JSON value; a policy is one object", path)
	}
	return p, nil
}

// follow accepts the run of agent answering trigger, prints its events as
// they stream, and returns the exit status for how the run ended. It waits
// for the run for timeout, or for as long as ctx lasts when timeout is zero;
// ctx ends on an interrupt.
//
// The events are read off the run's channel, subscribed before the run is
// accepted, and the ledger decides how the run ended: a failed event on the
// bus says what an attempt did, and only the ledger knows whether the run
// will be attempted again.
func follow(ctx context.Context, app *fiber.App, trigger event.Envelope, agent string, timeout time.Duration, out renderer, stderr io.Writer) int {
	wait := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		wait, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	runID := runtime.RunKey(trigger.Tenant, trigger.ID, agent)
	if err := runtime.CheckRun(agent, runID); err != nil {
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
		Tenant:   trigger.Tenant,
		Channels: []string{trigger.Channel},
		Types:    []string{"run", "run.usage", "run.context"},
		Run:      runID,
	}, stderr)
	if _, err := app.Runtime().Accept(wait, trigger, agent); err != nil {
		if wait.Err() != nil {
			return gaveUp()
		}
		fmt.Fprintf(stderr, "fiber run: accept the run: %v\n", err)
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
