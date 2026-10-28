package fiber

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/urmzd/fiber/pkg/knowledge"
	"github.com/urmzd/fiber/pkg/turn/native"
)

// retrieverFor binds the store to one run's tenant and to the sources its
// pinned revision declares. An agent with no sources gets no retriever at all,
// so it is never grounded in a corpus it did not ask for.
func retrieverFor(store knowledge.Store) func(tenant string, sources []string) native.Retriever {
	return func(tenant string, sources []string) native.Retriever {
		if len(sources) == 0 {
			return nil
		}
		return knowledge.NewRetriever(store, tenant, sources)
	}
}

// indexable is the set of extensions Index reads when walking a directory. It
// is a closed list because a directory holds binaries, images and lockfiles
// too, and embedding those buys nothing but noise in every search.
var indexable = map[string]bool{
	".md": true, ".mdx": true, ".txt": true, ".rst": true, ".adoc": true,
}

// IndexResult is what Index did.
type IndexResult struct {
	// Files is how many files were read; Skipped how many were not text.
	Files, Skipped int
	// Changed is how many documents were new or different, and Chunks how
	// many chunks they were written as. Re-indexing unchanged files
	// changes nothing.
	Changed, Chunks int
}

// Index writes text files into the knowledge store under source, for tenant.
// An agent whose spec lists source under "sources" is then grounded in them,
// and its answers carry citations. A directory is walked for Markdown and
// plain text (hidden directories skipped); a file named explicitly is
// indexed whatever its extension. Each document's URI is its path as given.
func (a *App) Index(ctx context.Context, tenant, source string, paths ...string) (IndexResult, error) {
	var res IndexResult
	if strings.TrimSpace(source) == "" {
		return res, configf("index: a source name is required")
	}
	files, err := collectIndexable(paths)
	if err != nil {
		return res, err
	}
	for _, path := range files {
		body, err := os.ReadFile(path)
		if err != nil {
			return res, fmt.Errorf("fiber: read %s: %w", path, err)
		}
		res.Files++
		if !utf8.Valid(body) {
			res.Skipped++
			continue
		}
		rec, err := a.docs.Upsert(ctx, tenant, knowledge.Document{
			Source:  source,
			URI:     filepath.ToSlash(path),
			Title:   filepath.Base(path),
			Content: string(body),
		})
		if err != nil {
			return res, fmt.Errorf("fiber: index %s: %w", path, err)
		}
		if rec.Changed {
			res.Changed++
			res.Chunks += rec.Chunks
		}
	}
	return res, nil
}

// collectIndexable expands the arguments into files, in a stable order.
func collectIndexable(paths []string) ([]string, error) {
	var out []string
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, configf("index: %v", err)
		}
		if !info.IsDir() {
			out = append(out, p)
			continue
		}
		err = filepath.WalkDir(p, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if name := d.Name(); path != p && strings.HasPrefix(name, ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if indexable[strings.ToLower(filepath.Ext(path))] {
				out = append(out, path)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("fiber: walk %s: %w", p, err)
		}
	}
	return out, nil
}
