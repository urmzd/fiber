package slack

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// Fake is a complete in-memory Slack, not a stub.
//
// It ships in the package rather than in a test file for the same reason the
// in-memory bus does: an integrator wiring the mesh together, a demo, or an
// example should be able to run the whole path — mention in, run, edited message
// out — with no credentials and no network. Everything the sink relies on is
// modelled: a message has a timestamp, an edit replaces its text, an edit of a
// timestamp that does not exist fails the way Slack fails it, and reactions
// accumulate per message. Sessions and commands follow Slack's own rule that a
// code channel is its own session while every other channel has one per thread.
type Fake struct {
	mu       sync.Mutex
	messages []FakeMessage
	byTS     map[string]int
	seq      int64
	code     map[string]bool
	sessions []Session
	commands map[string][]Command
	// Err, when set, fails every call. It is how a test drives the retry path.
	Err error
}

// FakeMessage is one message in the fake workspace.
type FakeMessage struct {
	Workspace string
	Channel   string
	ThreadTS  string
	TS        string
	Text      string
	// Edits counts how many times the text was replaced, which is what a test
	// asserting "one message, edited" looks at.
	Edits     int
	Reactions []string
}

// NewFake returns an empty fake workspace.
func NewFake() *Fake {
	return &Fake{
		byTS:     make(map[string]int),
		code:     make(map[string]bool),
		commands: make(map[string][]Command),
	}
}

// CodeChannel marks channel as a code channel, the only kind of channel that
// accepts commands and the only kind whose session has no thread.
func (f *Fake) CodeChannel(channel string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.code[channel] = true
}

// SetStatus implements API.
func (f *Fake) SetStatus(_ context.Context, s Session) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return f.Err
	}
	const method = "/agents.sessions.setStatus"
	switch {
	case s.Channel == "":
		return &Error{Method: method, Code: "channel_not_found"}
	case f.code[s.Channel] && s.ThreadTS != "":
		return &Error{Method: method, Code: "thread_ts_not_allowed"}
	case !f.code[s.Channel] && s.ThreadTS == "":
		return &Error{Method: method, Code: "thread_ts_required"}
	}
	switch s.Status {
	case StatusProcessing, StatusActive, StatusSuspended, StatusClosed:
	default:
		return &Error{Method: method, Code: "invalid_status"}
	}
	f.sessions = append(f.sessions, s)
	return nil
}

// SetCommands implements API.
func (f *Fake) SetCommands(_ context.Context, _, channel string, commands []Command) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return f.Err
	}
	const method = "/agents.conversations.setCommands"
	if !f.code[channel] {
		return &Error{Method: method, Code: "not_agent_channel"}
	}
	if len(commands) > MaxCommands {
		return &Error{Method: method, Code: "too_many_commands"}
	}
	f.commands[channel] = append([]Command(nil), commands...)
	return nil
}

// Statuses returns every status report accepted, oldest first.
func (f *Fake) Statuses() []Session {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Session(nil), f.sessions...)
}

// Commands returns the commands registered in channel.
func (f *Fake) Commands(channel string) []Command {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Command(nil), f.commands[channel]...)
}

// PostMessage implements API.
func (f *Fake) PostMessage(_ context.Context, m Message) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return "", f.Err
	}
	if m.Channel == "" {
		return "", &Error{Method: "/chat.postMessage", Code: "channel_not_found"}
	}
	f.seq++
	// Slack timestamps are "<seconds>.<microseconds>" and are unique per
	// channel; the fake keeps the shape so that anything parsing one still works.
	ts := strconv.FormatInt(time.Unix(1700000000, 0).Unix(), 10) + "." + fmt.Sprintf("%06d", f.seq)
	f.messages = append(f.messages, FakeMessage{
		Workspace: m.Workspace,
		Channel:   m.Channel,
		ThreadTS:  m.ThreadTS,
		TS:        ts,
		Text:      m.Text,
	})
	f.byTS[ts] = len(f.messages) - 1
	return ts, nil
}

// UpdateMessage implements API.
func (f *Fake) UpdateMessage(_ context.Context, m Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return f.Err
	}
	i, ok := f.byTS[m.TS]
	if !ok {
		return &Error{Method: "/chat.update", Code: "message_not_found"}
	}
	f.messages[i].Text = m.Text
	f.messages[i].Edits++
	return nil
}

// AddReaction implements API.
func (f *Fake) AddReaction(_ context.Context, r Reaction) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return f.Err
	}
	i, ok := f.byTS[r.TS]
	if !ok {
		return &Error{Method: "/reactions.add", Code: "message_not_found"}
	}
	for _, have := range f.messages[i].Reactions {
		if have == r.Name {
			return &Error{Method: "/reactions.add", Code: "already_reacted"}
		}
	}
	f.messages[i].Reactions = append(f.messages[i].Reactions, r.Name)
	return nil
}

// Messages returns every message posted, oldest first.
func (f *Fake) Messages() []FakeMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]FakeMessage(nil), f.messages...)
}

// Message returns the message at ts.
func (f *Fake) Message(ts string) (FakeMessage, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i, ok := f.byTS[ts]
	if !ok {
		return FakeMessage{}, false
	}
	return f.messages[i], true
}
