package turn_test

import (
	"os/exec"
	"strings"
	"testing"
)

// The turn API, its conformance suite and the sandbox decorator compile
// without saige: only pkg/turn/native implements a Runtime on it.
func TestOnlyNativeImportsSaige(t *testing.T) {
	t.Parallel()
	for _, pkg := range []string{
		"github.com/urmzd/fiber/pkg/turn",
		"github.com/urmzd/fiber/pkg/turn/runtimetest",
		"github.com/urmzd/fiber/pkg/turn/sandbox",
		"github.com/urmzd/fiber/pkg/event",
	} {
		out, err := exec.Command("go", "list", "-deps", pkg).CombinedOutput()
		if err != nil {
			t.Fatalf("go list %s: %v\n%s", pkg, err, out)
		}
		for _, dep := range strings.Fields(string(out)) {
			if strings.HasPrefix(dep, "github.com/urmzd/saige") {
				t.Errorf("%s depends on %s", pkg, dep)
			}
			if dep == "github.com/urmzd/fiber/pkg/turn/native" {
				t.Errorf("%s depends on the native runtime", pkg)
			}
		}
	}
}
