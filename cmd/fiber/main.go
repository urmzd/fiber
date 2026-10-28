// Command fiber validates an agent definition locally: it runs the
// definition once, in this process, and prints what the run does.
//
//	fiber run [flags] DEFINITION MESSAGE
//
// fiber is a library first. This command is a program built on it like any
// other: it composes fiber.New with nothing but the in-memory backends and a
// temporary workspace, stores the agent the DEFINITION file (.agent.md) defines, raises one
// tag at it with MESSAGE as the request, and prints the run's events as they
// stream. The exit status says how the run ended: 0 when it completed, 1 when
// it failed, was stopped, parked on a question or ran out of time, and 2 when
// the command line, the definition or the policy is wrong.
//
// It listens on nothing, reads no configuration file and keeps nothing once
// it exits. It reads no environment variable of its own; the built-in model
// providers read their vendors' (ANTHROPIC_API_KEY, OPENAI_API_KEY and
// OPENAI_BASE_URL, OLLAMA_HOST) when the agent is built, as they do in any
// program composing fiber. -offline runs the definition on the offline provider,
// which calls no model, so a definition can be checked with no key at all.
//
// It is for local test, not for serving: a program that serves agents
// composes fiber.New itself (Humagen is one), with its own backends,
// connectors and credentials.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

// Exit statuses.
const (
	// exitCompleted is a run that completed, or a listing that was printed.
	exitCompleted = 0
	// exitFailed is a run that did not complete: it failed, was stopped,
	// parked on a question this command cannot answer, or ran out of time.
	exitFailed = 1
	// exitBadInput is a command line, definition or policy that is wrong.
	exitBadInput = 2
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// cli runs the command line args and returns the exit status. It is main
// without the process: the tests call it as main does.
func cli(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return exitBadInput
	}
	switch args[0] {
	case "run":
		return runCommand(ctx, args[1:], stdin, stdout, stderr)
	case "help", "-h", "-help", "--help":
		usage(stdout)
		return exitCompleted
	default:
		fmt.Fprintf(stderr, "fiber: unknown command %q\n\n", args[0])
		usage(stderr)
		return exitBadInput
	}
}

func usage(w io.Writer) {
	fmt.Fprint(w, `Usage: fiber run [flags] DEFINITION MESSAGE

Run the agent DEFINITION defines once, in process, with MESSAGE as its
request, and print its events as they stream. DEFINITION is an .agent.md
file, or - for standard input. Everything is in memory and nothing is kept.

Flags go before DEFINITION:
  -policy FILE      read the agent's policy from this JSON file (default: the
                    zero policy, which grants nothing)
  -provider NAME    run on this model provider instead of the definition's
  -model NAME       run on this model instead of the definition's
  -offline          run on the offline provider: no model is called and no key
                    is needed (the definition's fallbacks are dropped)
  -tools            list the tools the agent would be granted, and run nothing
                    (MESSAGE is then optional)
  -json             print one JSON object per event instead of text
  -timeout D        give up after D, such as 30s or 5m (0 waits forever;
                    default 5m)

Exit status: 0 the run completed, 1 it did not (it failed, was stopped,
parked on a question or timed out), 2 the command line, the definition or the policy is wrong.
`)
}
