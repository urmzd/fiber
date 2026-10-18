package cli

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/knowledge"
)

// Environment variables that select the embedding model the knowledge layer
// indexes and searches with. The server and `fiber index` must agree on
// them: a corpus embedded with one model is not searchable with another, and
// the store excludes chunks from the dense arm rather than compare vectors that
// do not share a space.
const (
	// EnvEmbeddingsURL is the base URL of an OpenAI-compatible embeddings
	// API. Unset keeps the offline hashing embedder, which needs no key and
	// matches on word overlap rather than meaning.
	EnvEmbeddingsURL = "FIBER_EMBEDDINGS_URL"
	// EnvEmbeddingsModel names the model to request.
	EnvEmbeddingsModel = "FIBER_EMBEDDINGS_MODEL"
	// EnvEmbeddingsKey is the bearer credential for that API.
	EnvEmbeddingsKey = "FIBER_EMBEDDINGS_KEY"
	// EnvEmbeddingsDimensions is the width of the vectors it returns.
	EnvEmbeddingsDimensions = "FIBER_EMBEDDINGS_DIMENSIONS"
)

// embeddingsConfig is the embedder selection, read from the environment by the
// commands so that newCore itself never does.
type embeddingsConfig struct {
	URL, Model, Key string
	Dimensions      int
}

func embeddingsFromEnv() (embeddingsConfig, error) {
	cfg := embeddingsConfig{
		URL:   strings.TrimSpace(os.Getenv(EnvEmbeddingsURL)),
		Model: strings.TrimSpace(os.Getenv(EnvEmbeddingsModel)),
		Key:   os.Getenv(EnvEmbeddingsKey),
	}
	if raw := strings.TrimSpace(os.Getenv(EnvEmbeddingsDimensions)); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return cfg, usagef("$%s must be a positive integer, got %q", EnvEmbeddingsDimensions, raw)
		}
		cfg.Dimensions = n
	}
	return cfg, nil
}

func newEmbedder(cfg embeddingsConfig) (knowledge.Embedder, error) {
	if cfg.URL == "" {
		return knowledge.NewHashing(0), nil
	}
	e, err := knowledge.NewHTTP(knowledge.HTTPConfig{
		BaseURL:    cfg.URL,
		Model:      cfg.Model,
		APIKey:     cfg.Key,
		Dimensions: cfg.Dimensions,
	})
	if err != nil {
		return nil, usagef("embeddings: %v", err)
	}
	return e, nil
}

// newKnowledge selects the knowledge store: Postgres when there is a pool, so
// an indexed corpus outlives the process, and memory otherwise.
func newKnowledge(ctx context.Context, pool *pgxpool.Pool, cfg embeddingsConfig) (knowledge.Store, error) {
	emb, err := newEmbedder(cfg)
	if err != nil {
		return nil, err
	}
	if pool == nil {
		return knowledge.NewMemory(emb), nil
	}
	store := knowledge.NewPostgres(pool, emb)
	if err := store.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("%w: knowledge: %w", errUnavailable, err)
	}
	return store, nil
}

// retrieverFor binds the store to one run's tenant and to the sources its
// pinned revision declares. An agent with no sources gets no retriever at all,
// so it is never grounded in a corpus it did not ask for.
func retrieverFor(store knowledge.Store) func(tenant string, sources []string) agentrt.Retriever {
	return func(tenant string, sources []string) agentrt.Retriever {
		if len(sources) == 0 {
			return nil
		}
		return knowledge.NewRetriever(store, tenant, sources)
	}
}

// indexable is the set of extensions `fiber index` reads when walking a
// directory. It is a closed list because a directory holds binaries, images and
// lockfiles too, and embedding those buys nothing but noise in every search.
var indexable = map[string]bool{
	".md": true, ".mdx": true, ".txt": true, ".rst": true, ".adoc": true,
}

func newIndexCmd() *cobra.Command {
	var (
		source string
		tenant string
	)
	cmd := &cobra.Command{
		Use:   "index --source <name> <path>...",
		Short: "Index files into a knowledge source that agents can cite",
		Long: "index reads text files and writes them to the knowledge store under a source\n" +
			"name. An agent whose spec lists that name under \"sources\" is then grounded in\n" +
			"them, and its answers carry citations.\n\n" +
			"It writes to Postgres directly rather than through the server, so it needs\n" +
			"$" + EnvPostgres + " and the same $" + EnvEmbeddingsURL + " settings the server runs with.\n" +
			"Re-indexing an unchanged file is a no-op. A directory is walked for\n" +
			"Markdown and plain text; a file named explicitly is indexed whatever its\n" +
			"extension. Each document's URI is its path as given.",
		Example:      "  fiber index --source handbook ./docs",
		Args:         cobra.MinimumNArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, paths []string) error {
			ui, err := resolveUI(cmd)
			if err != nil {
				return err
			}
			if strings.TrimSpace(source) == "" {
				return usagef("--source is required")
			}
			return runIndex(cmd.Context(), ui, tenant, source, paths)
		},
	}
	cmd.Flags().StringVar(&source, "source", "", "Knowledge source name, as agent specs refer to it")
	cmd.Flags().StringVar(&tenant, "tenant", envOr(EnvTenant, DefaultTenant),
		"Authorization scope to index under ($"+EnvTenant+")")
	return cmd
}

func runIndex(ctx context.Context, ui *ui, tenant, source string, paths []string) error {
	url := strings.TrimSpace(os.Getenv(EnvPostgres))
	if url == "" {
		return usagef("index needs $%s: the in-memory knowledge store is private to the server process, so there would be nothing to write to", EnvPostgres)
	}
	emb, err := embeddingsFromEnv()
	if err != nil {
		return err
	}
	pool, err := newPostgres(url)
	if err != nil {
		return err
	}
	defer pool.Close()
	store, err := newKnowledge(ctx, pool, emb)
	if err != nil {
		return err
	}

	files, err := collectIndexable(paths)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return usagef("no indexable files under %s", strings.Join(paths, ", "))
	}

	var changed, chunks int
	for _, path := range files {
		body, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("cli: read %s: %w", path, err)
		}
		if !utf8.Valid(body) {
			ui.logf("  %s %s is not text\n", ui.yellow("skip"), path)
			continue
		}
		uri := filepath.ToSlash(path)
		rec, err := store.Upsert(ctx, tenant, knowledge.Document{
			Source:  source,
			URI:     uri,
			Title:   filepath.Base(path),
			Content: string(body),
		})
		if err != nil {
			return fmt.Errorf("cli: index %s: %w", path, err)
		}
		if rec.Changed {
			changed++
			chunks += rec.Chunks
		}
	}
	ui.logf("%s %d files into %s: %d changed, %d chunks written\n",
		ui.bold("indexed"), len(files), source, changed, chunks)
	return nil
}

// collectIndexable expands the arguments into files, in a stable order.
func collectIndexable(paths []string) ([]string, error) {
	var out []string
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, usagef("%v", err)
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
			return nil, fmt.Errorf("cli: walk %s: %w", p, err)
		}
	}
	return out, nil
}
