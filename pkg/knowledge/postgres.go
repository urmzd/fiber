package knowledge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/fiber/pkg/internal/pgrename"
)

// schema is everything the Postgres store owns: one row per document, one row
// per chunk. %d is the embedding width.
//
// The document row is the thing a writer locks and the thing that holds the
// content hash, so "is this already indexed" is one primary key lookup. Chunks
// hang off it with ON DELETE CASCADE, which makes deleting a document one
// statement and makes an orphaned chunk impossible.
//
// source and uri are COLLATE "C" because ties in a ranking are broken by
// comparing them, and a locale-aware collation would break those ties
// differently from the Go comparison Memory uses.
//
// The embedding column has a fixed width. pgvector allows a column without
// one, but then a vector of the wrong width is accepted at write time and
// fails at query time, in a search that has nothing to do with the document
// that caused it.
//
// The extension is created in "public" by name. Left to itself, CREATE
// EXTENSION installs into the first schema on the search path, which for a
// connection with a private schema in front (as every test here has) would
// put the type where no other connection can see it, and dropping that schema
// would take the extension with it.
//
// There is deliberately no approximate (HNSW or IVFFlat) index on embedding.
// Every search is filtered by tenant, an approximate index is consulted before
// that filter, and a tenant with a small share of the table would then get
// fewer results than exist, or none. The dense arm is an exact scan of the
// tenant's chunks, found through the primary key. That is the right trade
// until one tenant's corpus is large enough for the scan to hurt, and the
// place to revisit it is here.
const schema = `
CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;

CREATE TABLE IF NOT EXISTS fiber_knowledge_documents (
	tenant   text    NOT NULL,
	source   text    COLLATE "C" NOT NULL,
	uri      text    COLLATE "C" NOT NULL,
	title    text    NOT NULL,
	hash     text    NOT NULL,
	model    text    NOT NULL,
	metadata jsonb   NOT NULL,
	chunks   integer NOT NULL,
	PRIMARY KEY (tenant, source, uri)
);

CREATE TABLE IF NOT EXISTS fiber_knowledge_chunks (
	tenant     text       NOT NULL,
	source     text       COLLATE "C" NOT NULL,
	uri        text       COLLATE "C" NOT NULL,
	idx        integer    NOT NULL CHECK (idx >= 0),
	start_byte integer    NOT NULL,
	end_byte   integer    NOT NULL,
	start_line integer    NOT NULL,
	end_line   integer    NOT NULL,
	heading    text       NOT NULL,
	body       text       NOT NULL,
	embedding  vector(%d) NOT NULL,
	terms      tsvector   NOT NULL,
	PRIMARY KEY (tenant, source, uri, idx),
	FOREIGN KEY (tenant, source, uri)
		REFERENCES fiber_knowledge_documents (tenant, source, uri) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS fiber_knowledge_chunks_terms
	ON fiber_knowledge_chunks USING gin (terms);
`

// migrateLock is the advisory lock Migrate holds, for the reason
// pkg/definition gives: CREATE ... IF NOT EXISTS is not safe against itself.
// CREATE EXTENSION is database-wide, so the lock has to be too, which an
// advisory lock is. The value differs from the definition source's so the two
// migrations do not wait on each other.
const migrateLock = 0x6b6e6f77 // "know"

// Postgres is a Store that outlives the process.
//
// It keeps the contract Memory documents and is held to it by the same tests,
// which run against Postgres whenever $FIBER_POSTGRES_TEST_URL is set.
//
// The dense arm is pgvector cosine distance. The lexical arm is Postgres full
// text search ranked by ts_rank_cd. Fusion and reranking are the shared Go
// code, so the only place the backends can rank differently is inside the
// lexical arm, and they do: ts_rank_cd scores by how close together the
// matched terms sit and knows nothing about how rare a term is, while Memory's
// BM25 weights rare terms and term frequency. The same chunks match in both;
// their order within the lexical arm can differ.
//
// It requires the pgvector extension. Migrate creates it in the "public"
// schema, which needs a role allowed to; if the extension is already
// installed, any role will do. The "vector" type and its operators must be
// reachable through the connection's search_path, so a connection that sets a
// private search_path has to keep "public" on it.
//
// It does not own the pool. The caller opened it and the caller closes it.
type Postgres struct {
	engine
	pool *pgxpool.Pool
}

// NewPostgres returns a store on pool that embeds with embedder. It does not
// touch the database; call Migrate once before the first use.
func NewPostgres(pool *pgxpool.Pool, embedder Embedder, opts ...Option) *Postgres {
	return &Postgres{engine: newEngine(embedder, opts), pool: pool}
}

var _ Store = (*Postgres)(nil)

// Migrate creates the extension, tables and index this store needs if they
// are not there. It is idempotent and safe to run from every replica at
// startup.
//
// It fails with ErrDimensions when the chunk table already exists with a
// different embedding width, which is what happens when the embedding model is
// swapped for one of another size. The store cannot convert the old vectors;
// the operator drops fiber_knowledge_chunks and fiber_knowledge_documents
// and indexes again.
func (p *Postgres) Migrate(ctx context.Context) error {
	dims := p.embedder.Dimensions()
	if dims <= 0 {
		return fmt.Errorf("%w: embedder %q reports %d dimensions", ErrInvalid, p.embedder.Model(), dims)
	}
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(migrateLock)); err != nil {
			return err
		}
		// A database created before the rename is moved forward first.
		if _, err := tx.Exec(ctx, pgrename.SQL("knowledge_documents", "knowledge_chunks")); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(schema, dims)); err != nil {
			return err
		}
		// For a vector column the type modifier is its width.
		var have int
		if err := tx.QueryRow(ctx,
			`SELECT atttypmod FROM pg_attribute
			  WHERE attrelid = 'fiber_knowledge_chunks'::regclass AND attname = 'embedding'`,
		).Scan(&have); err != nil {
			return err
		}
		if have != dims {
			return fmt.Errorf("%w: the table holds %d-dimensional vectors and %q produces %d", ErrDimensions, have, p.embedder.Model(), dims)
		}
		// Fails here, with the configuration's name in the message, rather
		// than on the first upsert.
		var ok bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_ts_config WHERE cfgname = $1)`, p.textConfig).Scan(&ok); err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: no text search configuration named %q", ErrInvalid, p.textConfig)
		}
		return nil
	})
	if err != nil {
		return wrap("migrate", err)
	}
	return nil
}

// Upsert indexes doc.
//
// The hash is checked twice. The first check is a plain read, before the
// embedder is called, and is what makes resubmitting an unchanged corpus cost
// one indexed lookup per document. The second runs inside the transaction with
// the document row locked, because two upserts of one document can both pass
// the first.
//
// The embedder is called between the two, outside any transaction: an
// embedding request can take seconds, and a transaction held open across it
// would pin a connection and a row lock for that long.
func (p *Postgres) Upsert(ctx context.Context, tenant string, doc Document) (Receipt, error) {
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	d, err := p.prepare(tenant, doc)
	if err != nil {
		return Receipt{}, err
	}
	receipt := Receipt{Source: d.doc.Source, URI: d.doc.URI, Hash: d.hash, Chunks: len(d.chunks)}

	var current string
	err = p.pool.QueryRow(ctx,
		`SELECT hash FROM fiber_knowledge_documents WHERE tenant = $1 AND source = $2 AND uri = $3`,
		tenant, d.doc.Source, d.doc.URI,
	).Scan(&current)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return Receipt{}, wrap("upsert", err)
	case current == d.hash:
		return receipt, nil
	}

	vectors, err := p.embedChunks(ctx, d)
	if err != nil {
		return Receipt{}, err
	}
	metadata, err := json.Marshal(d.doc.Metadata)
	if err != nil {
		return Receipt{}, fmt.Errorf("knowledge: encode metadata of %q: %w", d.doc.URI, err)
	}
	if d.doc.Metadata == nil {
		metadata = []byte(`{}`)
	}

	// One array per column, so the chunks go in as a single statement
	// however many there are.
	n := len(d.chunks)
	var (
		idx       = make([]int32, n)
		starts    = make([]int32, n)
		ends      = make([]int32, n)
		startLine = make([]int32, n)
		endLine   = make([]int32, n)
		headings  = make([]string, n)
		bodies    = make([]string, n)
		literals  = make([]string, n)
		terms     = make([]string, n)
	)
	for i, c := range d.chunks {
		idx[i] = int32(c.Locator.Chunk)
		starts[i], ends[i] = int32(c.Locator.Start), int32(c.Locator.End)
		startLine[i], endLine[i] = int32(c.Locator.StartLine), int32(c.Locator.EndLine)
		headings[i] = c.Locator.Heading
		bodies[i] = c.Text
		literals[i] = vectorLiteral(vectors[i])
		// Postgres is given the words, not the text. See tokenize.
		terms[i] = strings.Join(tokenize(indexText(d.doc.Title, c)), " ")
	}

	err = pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		// Claim the row before locking it, so two first-time upserts of
		// one document meet on the primary key instead of both finding
		// nothing to lock. The placeholder hash is empty, which no real
		// hash is, and it never commits: this transaction either replaces
		// it or rolls back.
		if _, err := tx.Exec(ctx,
			`INSERT INTO fiber_knowledge_documents (tenant, source, uri, title, hash, model, metadata, chunks)
			 VALUES ($1, $2, $3, '', '', '', '{}', 0)
			 ON CONFLICT (tenant, source, uri) DO NOTHING`,
			tenant, d.doc.Source, d.doc.URI); err != nil {
			return err
		}
		var locked string
		if err := tx.QueryRow(ctx,
			`SELECT hash FROM fiber_knowledge_documents
			  WHERE tenant = $1 AND source = $2 AND uri = $3 FOR UPDATE`,
			tenant, d.doc.Source, d.doc.URI,
		).Scan(&locked); err != nil {
			return err
		}
		if locked == d.hash {
			return nil
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM fiber_knowledge_chunks WHERE tenant = $1 AND source = $2 AND uri = $3`,
			tenant, d.doc.Source, d.doc.URI); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO fiber_knowledge_chunks
			   (tenant, source, uri, idx, start_byte, end_byte, start_line, end_line, heading, body, embedding, terms)
			 SELECT $1, $2, $3, u.idx, u.start_byte, u.end_byte, u.start_line, u.end_line, u.heading, u.body,
			        u.embedding::vector, to_tsvector($4::regconfig, u.terms)
			   FROM unnest($5::integer[], $6::integer[], $7::integer[], $8::integer[], $9::integer[],
			               $10::text[], $11::text[], $12::text[], $13::text[])
			     AS u(idx, start_byte, end_byte, start_line, end_line, heading, body, embedding, terms)`,
			tenant, d.doc.Source, d.doc.URI, p.textConfig,
			idx, starts, ends, startLine, endLine, headings, bodies, literals, terms); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE fiber_knowledge_documents
			    SET title = $4, hash = $5, model = $6, metadata = $7, chunks = $8
			  WHERE tenant = $1 AND source = $2 AND uri = $3`,
			tenant, d.doc.Source, d.doc.URI, d.doc.Title, d.hash, p.embedder.Model(), metadata, n); err != nil {
			return err
		}
		receipt.Changed = true
		return nil
	})
	if err != nil {
		return Receipt{}, wrap("upsert", err)
	}
	return receipt, nil
}

// Delete removes a document; its chunks go with it by cascade.
func (p *Postgres) Delete(ctx context.Context, tenant, source, uri string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	source, uri, err := checkKey(tenant, source, uri)
	if err != nil {
		return err
	}
	tag, err := p.pool.Exec(ctx,
		`DELETE FROM fiber_knowledge_documents WHERE tenant = $1 AND source = $2 AND uri = $3`,
		tenant, source, uri)
	if err != nil {
		return wrap("delete", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: document %q in source %q", ErrNotFound, uri, source)
	}
	return nil
}

// Search runs both arms in one statement.
func (p *Postgres) Search(ctx context.Context, tenant string, q Query) ([]Hit, error) {
	hits, err := p.search(ctx, tenant, q, p.arms)
	if err != nil {
		return nil, err
	}
	if hits == nil {
		hits = []Hit{}
	}
	return hits, nil
}

// searchSQL returns both arms' candidates, the dense arm first, each in rank
// order.
//
// It is one statement so that it is one snapshot. Two queries could straddle a
// re-index and return a document's old chunks from one arm and its new chunks
// from the other, which is exactly the mixture Upsert promises nobody sees.
//
// The dense arm joins the document to compare models: a chunk embedded by a
// different model sits in a different space, and its distance to this query is
// a number without a meaning. Such chunks stay findable by the lexical arm
// until they are indexed again.
//
// A cosine distance is NaN when either vector is all zeros, and NaN compares
// greater than every number in Postgres, so the distance bound excludes those
// rows without a special case.
const searchSQL = `
WITH dense AS (
	SELECT c.source, c.uri, c.idx, 1 - (c.embedding <=> $3::vector) AS score
	  FROM fiber_knowledge_chunks c
	  JOIN fiber_knowledge_documents d
	    ON d.tenant = c.tenant AND d.source = c.source AND d.uri = c.uri
	 WHERE c.tenant = $1
	   AND ($2::text[] IS NULL OR c.source = ANY ($2::text[]))
	   AND d.model = $4
	   AND (c.embedding <=> $3::vector) < $5::float8
	 ORDER BY c.embedding <=> $3::vector, c.source, c.uri, c.idx
	 LIMIT $6
), lexical AS (
	SELECT c.source, c.uri, c.idx, ts_rank_cd(c.terms, q.query, 1)::float8 AS score
	  FROM fiber_knowledge_chunks c,
	       (SELECT to_tsquery($7::regconfig, $8::text) AS query WHERE $8::text <> '') q
	 WHERE c.tenant = $1
	   AND ($2::text[] IS NULL OR c.source = ANY ($2::text[]))
	   AND c.terms @@ q.query
	 ORDER BY score DESC, c.source, c.uri, c.idx
	 LIMIT $6
), picked AS (
	SELECT 0 AS arm, row_number() OVER (ORDER BY score DESC, source, uri, idx) AS pos, * FROM dense
	UNION ALL
	SELECT 1 AS arm, row_number() OVER (ORDER BY score DESC, source, uri, idx) AS pos, * FROM lexical
)
SELECT p.arm, p.score, c.source, c.uri, c.idx, c.start_byte, c.end_byte, c.start_line, c.end_line,
       c.heading, c.body, d.title, d.metadata
  FROM picked p
  JOIN fiber_knowledge_chunks c
    ON c.tenant = $1 AND c.source = p.source AND c.uri = p.uri AND c.idx = p.idx
  JOIN fiber_knowledge_documents d
    ON d.tenant = $1 AND d.source = p.source AND d.uri = p.uri
 ORDER BY p.arm, p.pos
`

func (p *Postgres) arms(ctx context.Context, s scope) (dense, lexical []candidate, err error) {
	// Terms are letters and digits only (see tokenize), so joining them
	// with the OR operator is a well-formed tsquery and cannot be made to
	// say anything else. OR, because a question shares a few words with its
	// answer and never all of them.
	rows, err := p.pool.Query(ctx, searchSQL,
		s.tenant, s.sources, vectorLiteral(s.vector), p.embedder.Model(),
		1-minSimilarity, s.k, p.textConfig, strings.Join(s.terms, " | "))
	if err != nil {
		return nil, nil, wrap("search", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			arm      int
			c        candidate
			metadata []byte
		)
		h := &c.hit
		if err := rows.Scan(&arm, &c.score, &h.Source, &h.URI, &h.Locator.Chunk,
			&h.Locator.Start, &h.Locator.End, &h.Locator.StartLine, &h.Locator.EndLine,
			&h.Locator.Heading, &h.Text, &h.Title, &metadata); err != nil {
			return nil, nil, wrap("search", err)
		}
		if err := json.Unmarshal(metadata, &h.Metadata); err != nil {
			return nil, nil, fmt.Errorf("knowledge: decode metadata of %q: %w", h.URI, err)
		}
		if len(h.Metadata) == 0 {
			h.Metadata = nil
		}
		if arm == 0 {
			dense = append(dense, c)
		} else {
			lexical = append(lexical, c)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, wrap("search", err)
	}
	return dense, lexical, nil
}

// vectorLiteral renders v the way pgvector reads it, "[1,2,3]". Passing the
// text and casting it in SQL is what lets this package use pgvector without a
// Go library for its wire format. The values are finite: embed checked.
func vectorLiteral(v []float32) string {
	var b strings.Builder
	b.Grow(len(v)*10 + 2)
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			x = 0
		}
		b.WriteString(strconv.FormatFloat(float64(x), 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}

// wrap adds this package's context to a database failure and leaves the
// sentinels alone, as pkg/definition does.
func wrap(op string, err error) error {
	for _, sentinel := range []error{ErrNotFound, ErrInvalid, ErrDimensions, ErrEmbedding, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, sentinel) {
			return err
		}
	}
	return fmt.Errorf("knowledge: postgres %s: %w", op, err)
}
