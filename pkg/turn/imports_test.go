package turn_test

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
)

const (
	module = "github.com/urmzd/fiber"
	saige  = "github.com/urmzd/saige"
)

// mayImportSaige reports whether a package may import saige: the native
// runtime and its alias packages, and the agentrt bridge.
func mayImportSaige(pkg string) bool {
	for _, root := range []string{module + "/pkg/turn/native", module + "/pkg/agentrt"} {
		if pkg == root || strings.HasPrefix(pkg, root+"/") {
			return true
		}
	}
	return false
}

// testsMayImportSaige lists packages whose tests still import saige directly.
// The runtime tests are kept unchanged while the agentrt bridge exists.
var testsMayImportSaige = map[string]bool{module + "/pkg/runtime": true}

// Every package in the module, its tests and the examples import saige only
// under pkg/turn/native and the agentrt bridge.
func TestOnlyNativeImportsSaige(t *testing.T) {
	t.Parallel()
	out, err := exec.Command("go", "list", "-json=ImportPath,Imports,TestImports,XTestImports", module+"/...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	dec := json.NewDecoder(strings.NewReader(string(out)))
	seen := 0
	for dec.More() {
		var p struct {
			ImportPath                         string
			Imports, TestImports, XTestImports []string
		}
		if err := dec.Decode(&p); err != nil {
			t.Fatalf("decode go list: %v", err)
		}
		seen++
		if mayImportSaige(p.ImportPath) {
			continue
		}
		imports := p.Imports
		if !testsMayImportSaige[p.ImportPath] {
			imports = append(append(imports, p.TestImports...), p.XTestImports...)
		}
		for _, imp := range imports {
			if imp == saige || strings.HasPrefix(imp, saige+"/") {
				t.Errorf("%s imports %s", p.ImportPath, imp)
			}
		}
	}
	if seen == 0 {
		t.Fatal("go list returned no packages")
	}
}

// The turn API, its conformance suite and the sandbox decorator compile
// without saige or the native runtime, directly or through a dependency.
func TestTurnDoesNotDependOnNative(t *testing.T) {
	t.Parallel()
	for _, pkg := range []string{
		module + "/pkg/turn",
		module + "/pkg/turn/runtimetest",
		module + "/pkg/turn/sandbox",
		module + "/pkg/event",
	} {
		out, err := exec.Command("go", "list", "-deps", pkg).CombinedOutput()
		if err != nil {
			t.Fatalf("go list %s: %v\n%s", pkg, err, out)
		}
		for _, dep := range strings.Fields(string(out)) {
			if strings.HasPrefix(dep, saige) {
				t.Errorf("%s depends on %s", pkg, dep)
			}
			if mayImportSaige(dep) {
				t.Errorf("%s depends on %s", pkg, dep)
			}
		}
	}
}
