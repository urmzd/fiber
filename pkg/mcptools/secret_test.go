package mcptools_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/urmzd/fiber/pkg/mcptools"
)

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func slogTo(w io.Writer, mu *sync.Mutex) *slog.Logger {
	return slog.New(slog.NewJSONHandler(lockedWriter{mu: mu, w: w}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// holder keeps a Secret in an unexported field, which fmt prints without
// calling the Secret's methods. The value must still not appear.
type holder struct {
	name   string
	secret mcptools.Secret
}

type exported struct {
	Name   string
	Secret mcptools.Secret
}

func TestASecretPrintsAsRedactedEverywhere(t *testing.T) {
	s := mcptools.NewSecret(token)
	if s.Reveal() != token {
		t.Fatal("Reveal lost the value")
	}
	var forms []string
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%10s", "%T %v"} {
		forms = append(forms,
			fmt.Sprintf(verb, s),
			fmt.Sprintf(verb, &s),
			fmt.Sprintf(verb, holder{"gh", s}),
			fmt.Sprintf(verb, &holder{"gh", s}),
			fmt.Sprintf(verb, exported{"gh", s}),
			fmt.Sprintf(verb, []mcptools.Secret{s}),
			fmt.Sprintf(verb, map[string]mcptools.Secret{"k": s}),
		)
	}
	forms = append(forms, s.String(), s.GoString(), fmt.Sprint(s), fmt.Sprintln(s))
	for _, v := range []any{s, exported{"gh", s}, map[string]any{"k": s}} {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		forms = append(forms, string(b))
	}
	for _, f := range forms {
		if strings.Contains(f, token) {
			t.Errorf("a secret printed as %q", f)
		}
	}
	if got := fmt.Sprintf("%v", s); got != mcptools.Redacted {
		t.Errorf("%%v = %q, want %q", got, mcptools.Redacted)
	}
}

func TestASecretIsRedactedInEverySlogHandler(t *testing.T) {
	s := mcptools.NewSecret(token)
	var buf bytes.Buffer
	for _, h := range []slog.Handler{
		slog.NewJSONHandler(&buf, nil),
		slog.NewTextHandler(&buf, nil),
	} {
		l := slog.New(h)
		l.Info("connecting", "credential", s, "nested", exported{"gh", s})
		l.With("credential", s).WithGroup("g").Info("again", "c", s)
	}
	if strings.Contains(buf.String(), token) {
		t.Fatalf("the secret reached a log:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), mcptools.Redacted) {
		t.Fatalf("expected %s in the log:\n%s", mcptools.Redacted, buf.String())
	}
}
