package mcptools

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	saigetypes "github.com/urmzd/fiber/pkg/turn/native/saigetypes"
)

// parameterSchema decodes an accepted JSON schema into saige's form: an
// object, or nothing. saige cannot resolve references, so each local
// "#/$defs/<name>" reference is first replaced by the schema it names (see
// inlineDefs) and $defs is dropped. A nullable property, written as
// anyOf/oneOf of one type and {"type":"null"} or as a type array of one type
// and "null", becomes that type with Nullable set. Any other anyOf, oneOf,
// allOf, unresolvable $ref or multi-type array is refused rather than
// dropped, so a tool is never offered to a model with a schema that means
// something else. Every refusal wraps ErrUnsupportedSchema and names the
// location in the schema and the reason.
func parameterSchema(raw json.RawMessage) (saigetypes.ParameterSchema, schemaStats, error) {
	var stats schemaStats
	if len(raw) == 0 {
		return saigetypes.ParameterSchema{Type: saigetypes.SchemaObject}, stats, nil
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return saigetypes.ParameterSchema{}, stats, fmt.Errorf("%w: schema is not a JSON object: %w", ErrUnsupportedSchema, err)
	}
	if err := inlineDefs(doc, &stats); err != nil {
		return saigetypes.ParameterSchema{}, stats, err
	}
	if err := normalizeNode(doc, "$", 0); err != nil {
		return saigetypes.ParameterSchema{}, stats, err
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return saigetypes.ParameterSchema{}, stats, fmt.Errorf("%w: schema is not JSON Schema: %w", ErrUnsupportedSchema, err)
	}
	var s saigetypes.ParameterSchema
	if err := json.Unmarshal(b, &s); err != nil {
		return saigetypes.ParameterSchema{}, stats, fmt.Errorf("%w: schema is not JSON Schema: %w", ErrUnsupportedSchema, err)
	}
	if s.Type == "" {
		s.Type = saigetypes.SchemaObject
	}
	if s.Type != saigetypes.SchemaObject {
		return saigetypes.ParameterSchema{}, stats, fmt.Errorf("%w: schema type %q, want object", ErrUnsupportedSchema, s.Type)
	}
	return s, stats, nil
}

// schemaStats describes what conversion did, for the debug log.
type schemaStats struct {
	Defs    int // definitions the schema carried
	Inlined int // references replaced by their definition
}

const (
	// defsPrefix is the only reference form inlineDefs resolves.
	defsPrefix = "#/$defs/"
	// maxInlined bounds how many references one schema may expand, so a
	// small schema whose definitions reference each other many times over
	// cannot grow without limit.
	maxInlined = 1000
)

// inlineDefs replaces, in place, every {"$ref": "#/$defs/<name>"} in doc with
// a copy of the definition it names, then deletes $defs. Keywords written
// beside the $ref (a description, a default) are kept and win over the
// definition's. A reference is refused, with the reason, when it is not a
// local #/$defs reference, names no definition, or reaches itself through
// the definitions it expands (a recursive type has no finite inline form).
func inlineDefs(doc map[string]any, stats *schemaStats) error {
	var defs map[string]any
	if v, ok := doc["$defs"]; ok {
		m, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("%w: schema at $.$defs is not an object", ErrUnsupportedSchema)
		}
		defs = m
		delete(doc, "$defs")
	}
	stats.Defs = len(defs)
	in := &inliner{defs: defs, stats: stats}
	_, err := in.walk(doc, "$", nil)
	return err
}

type inliner struct {
	defs  map[string]any
	stats *schemaStats
}

// walk returns v with its references inlined. chain lists the definitions
// being expanded around v, outermost first, to catch recursion.
func (in *inliner) walk(v any, path string, chain []string) (any, error) {
	switch n := v.(type) {
	case []any:
		for i, e := range n {
			out, err := in.walk(e, path+"["+strconv.Itoa(i)+"]", chain)
			if err != nil {
				return nil, err
			}
			n[i] = out
		}
		return n, nil
	case map[string]any:
		if ref, ok := n["$ref"]; ok {
			return in.expand(n, ref, path, chain)
		}
		for k, e := range n {
			out, err := in.walk(e, childPath(path, k), chain)
			if err != nil {
				return nil, err
			}
			n[k] = out
		}
		return n, nil
	default:
		return v, nil
	}
}

func (in *inliner) expand(node map[string]any, ref any, path string, chain []string) (any, error) {
	s, ok := ref.(string)
	if !ok {
		return nil, fmt.Errorf("%w: schema at %s has a $ref that is not a string, which is not supported", ErrUnsupportedSchema, path)
	}
	if !strings.HasPrefix(s, defsPrefix) {
		return nil, fmt.Errorf("%w: schema at %s uses $ref %q, which is not supported: only local %s<name> references are resolved",
			ErrUnsupportedSchema, path, s, defsPrefix)
	}
	rest := strings.TrimPrefix(s, defsPrefix)
	name := unescapePointer(rest)
	def, ok := in.defs[name]
	if !ok || strings.Contains(rest, "/") {
		return nil, fmt.Errorf("%w: schema at %s uses $ref %q, which is not supported: it names no definition in $defs (have %s)",
			ErrUnsupportedSchema, path, s, defNames(in.defs))
	}
	for i, c := range chain {
		if c == name {
			cycle := append(append([]string{}, chain[i:]...), name)
			return nil, fmt.Errorf("%w: schema at %s uses $ref %q, which is not supported: the definition is recursive (%s)",
				ErrUnsupportedSchema, path, s, strings.Join(cycle, " -> "))
		}
	}
	body, ok := def.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: schema at $.$defs.%s is not an object", ErrUnsupportedSchema, name)
	}
	in.stats.Inlined++
	if in.stats.Inlined > maxInlined {
		return nil, fmt.Errorf("%w: schema at %s: inlining $defs expands more than %d references", ErrUnsupportedSchema, path, maxInlined)
	}
	expanded, err := in.walk(deepCopy(body), path, append(chain, name))
	if err != nil {
		return nil, err
	}
	out := expanded.(map[string]any)
	for k, v := range node {
		if k == "$ref" {
			continue
		}
		v, err := in.walk(v, childPath(path, k), chain)
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, nil
}

// childPath names a schema location the way normalizeNode does: a property
// by its name, array items as [], any other keyword by itself.
func childPath(path, key string) string {
	switch key {
	case "properties":
		return path
	case "items":
		return path + "[]"
	}
	return path + "." + key
}

func unescapePointer(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "~1", "/"), "~0", "~")
}

func defNames(defs map[string]any) string {
	if len(defs) == 0 {
		return "none"
	}
	names := make([]string, 0, len(defs))
	for k := range defs {
		names = append(names, k)
	}
	slices.Sort(names)
	return strings.Join(names, ", ")
}

func deepCopy(v any) any {
	switch n := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(n))
		for k, e := range n {
			out[k] = deepCopy(e)
		}
		return out
	case []any:
		out := make([]any, len(n))
		for i, e := range n {
			out[i] = deepCopy(e)
		}
		return out
	default:
		return v
	}
}

// maxSchemaDepth bounds recursion through nested properties and items.
const maxSchemaDepth = 32

// normalizeProperties rewrites node's properties and items in place, see
// parameterSchema. The root goes through normalizeNode too, so a root
// allOf, $ref or union is refused rather than read as an empty object.
func normalizeProperties(node map[string]any, path string, depth int) error {
	if depth > maxSchemaDepth {
		return fmt.Errorf("%w: schema at %s nests deeper than %d levels", ErrUnsupportedSchema, path, maxSchemaDepth)
	}
	if props, ok := node["properties"].(map[string]any); ok {
		for name, v := range props {
			p, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("%w: schema at %s is not an object", ErrUnsupportedSchema, path+"."+name)
			}
			if err := normalizeNode(p, path+"."+name, depth+1); err != nil {
				return err
			}
		}
	}
	if items, ok := node["items"].(map[string]any); ok {
		if err := normalizeNode(items, path+"[]", depth+1); err != nil {
			return err
		}
	}
	return nil
}

func normalizeNode(p map[string]any, path string, depth int) error {
	if _, ok := p["$ref"]; ok {
		// inlineDefs resolves or refuses every $ref first; this guards a
		// caller that skips it.
		return fmt.Errorf("%w: schema at %s uses $ref, which is not supported", ErrUnsupportedSchema, path)
	}
	if _, ok := p["allOf"]; ok {
		return fmt.Errorf("%w: schema at %s uses allOf, which is not supported", ErrUnsupportedSchema, path)
	}
	for _, key := range []string{"anyOf", "oneOf"} {
		alts, ok := p[key]
		if !ok {
			continue
		}
		list, _ := alts.([]any)
		var only map[string]any
		nulls := 0
		for _, a := range list {
			m, ok := a.(map[string]any)
			if !ok {
				return fmt.Errorf("%w: schema at %s: %s entry is not an object", ErrUnsupportedSchema, path, key)
			}
			if t, _ := m["type"].(string); t == "null" && len(m) == 1 {
				nulls++
				continue
			}
			if only != nil {
				return fmt.Errorf("%w: schema at %s uses %s of several types, which is not supported", ErrUnsupportedSchema, path, key)
			}
			only = m
		}
		if only == nil || nulls != 1 {
			return fmt.Errorf("%w: schema at %s uses %s other than one type or null, which is not supported", ErrUnsupportedSchema, path, key)
		}
		delete(p, key)
		for k, v := range only {
			if _, set := p[k]; !set {
				p[k] = v
			}
		}
		p["nullable"] = true
	}
	if types, ok := p["type"].([]any); ok {
		var only string
		nulls := 0
		for _, t := range types {
			s, _ := t.(string)
			switch {
			case s == "null":
				nulls++
			case s != "" && only == "":
				only = s
			default:
				return fmt.Errorf("%w: schema at %s has several types, which is not supported", ErrUnsupportedSchema, path)
			}
		}
		if only == "" || nulls > 1 {
			return fmt.Errorf("%w: schema at %s has no single type", ErrUnsupportedSchema, path)
		}
		p["type"] = only
		if nulls == 1 {
			p["nullable"] = true
		}
	}
	return normalizeProperties(p, path, depth)
}
