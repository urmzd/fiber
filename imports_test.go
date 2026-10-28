package fiber

import (
	"os/exec"
	"strings"
	"testing"
)

// removedPackages are the bridges the event union replaced. Stored JSON from
// before it is read only through pkg/event/legacy.
var removedPackages = []string{
	"github.com/urmzd/fiber/pkg/envelope",
	"github.com/urmzd/fiber/pkg/topic",
	"github.com/urmzd/fiber/pkg/agentrt",
	"github.com/urmzd/fiber/pkg/agentrt/payload",
	"github.com/urmzd/fiber/pkg/router",
}

// Nothing in the module, its tests or its examples depends on a removed
// package, directly or through a dependency.
func TestNoBridgePackages(t *testing.T) {
	t.Parallel()
	out, err := exec.Command("go", "list", "-deps", "-test", "github.com/urmzd/fiber/...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	deps := strings.Fields(string(out))
	if len(deps) == 0 {
		t.Fatal("go list returned no packages")
	}
	for _, dep := range deps {
		for _, gone := range removedPackages {
			if dep == gone || strings.HasPrefix(dep, gone+"/") || strings.HasPrefix(dep, gone+" ") {
				t.Errorf("depends on %s, which was removed", dep)
			}
		}
	}
}
