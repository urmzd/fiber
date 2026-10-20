// Command fiber is the agent platform and its pub/sub bus.
//
// It runs the core (serve), an execution node (work), and the client commands
// that drive them (agent, tag, listen). It is fiberapp with no extensions; a
// binary that adds model providers, tools or connectors calls fiberapp.Main
// with options instead. See internal/cli/core.go for the composition root.
package main

import (
	"os"

	"github.com/urmzd/fiber/fiberapp"
)

// Build metadata, injected with -ldflags at release time. The defaults are what
// a `go build` with no flags produces, and they say so rather than claiming a
// version that was never released.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	os.Exit(fiberapp.Main(fiberapp.Version{Version: version, Commit: commit, Date: date}))
}
