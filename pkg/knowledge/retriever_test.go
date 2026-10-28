package knowledge_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/urmzd/fiber/pkg/knowledge"
	ragtypes "github.com/urmzd/fiber/pkg/turn/native/ragtypes"
)

// retriever restates pkg/agentrt.Retriever. The adapter exists to satisfy that
// interface; restating it here checks the shape without making this package's
// tests depend on agentrt building.
type retriever interface {
	Retrieve(ctx context.Context, query string) (*ragtypes.AssembledContext, error)
}

var _ retriever = (*knowledge.Retriever)(nil)

// What agentrt needs from a retrieval: a prompt the model can cite from by
// label, and a block per passage whose provenance a reader can follow.
func TestRetrieverAssemblesLabelledPassagesWithFollowableProvenance(t *testing.T) {
	t.Parallel()
	s := newStore(t, knowledge.NewHashing(0), knowledge.WithChunking(160, 0))
	mustUpsert(t, s, tenantA, knowledge.Document{Source: "handbook", URI: "https://docs.example.com/security.md", Title: "Security handbook", Content: handbook})

	r := knowledge.NewRetriever(s, tenantA, []string{"handbook"}, knowledge.WithLimit(2))
	ac, err := r.Retrieve(context.Background(), "how do I rotate an API key?")
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if ac == nil || len(ac.Blocks) != 2 {
		t.Fatalf("context = %+v; want the two passages the limit allows", ac)
	}
	top := ac.Blocks[0]
	if top.Citation != "[1]" || ac.Blocks[1].Citation != "[2]" {
		t.Errorf("labels = %q, %q; want [1], [2]", top.Citation, ac.Blocks[1].Citation)
	}
	if !strings.Contains(handbook, top.Text) || !strings.Contains(top.Text, "ninety days") {
		t.Errorf("block text %q is not the exact source passage", top.Text)
	}
	p := top.Provenance
	if p.SourceURI != "https://docs.example.com/security.md" || p.DocumentTitle != "Security handbook" {
		t.Errorf("provenance = %+v; want the document's uri and title", p)
	}
	if !strings.HasPrefix(p.SectionHeading, "Rotating API keys, L") {
		t.Errorf("section = %q; want the heading and the line range", p.SectionHeading)
	}
	if p.DocumentUUID != knowledge.DocumentID("handbook", "https://docs.example.com/security.md") || !strings.HasPrefix(p.SectionUUID, p.DocumentUUID+":") {
		t.Errorf("ids = %q, %q; want stable ids derived from the document", p.DocumentUUID, p.SectionUUID)
	}
	for _, b := range ac.Blocks {
		if !strings.Contains(ac.Prompt, b.Citation+" Security handbook (https://docs.example.com/security.md)\n"+b.Text) {
			t.Errorf("the prompt does not show passage %s under its label", b.Citation)
		}
	}
	if ac.TokenCount <= 0 {
		t.Error("no token estimate")
	}
}

// The binding is the access rule: an agent reads the corpora its revision
// names, in its own tenant, and nothing in the query can change that.
func TestRetrieverReadsOnlyTheSourcesAndTenantItWasBoundTo(t *testing.T) {
	t.Parallel()
	s := newStore(t, knowledge.NewHashing(0))
	mustUpsert(t, s, tenantA, knowledge.Document{Source: "public", URI: "faq", Content: "The launch code policy is public."})
	mustUpsert(t, s, tenantA, knowledge.Document{Source: "secrets", URI: "vault", Content: "The launch code is 0000."})
	mustUpsert(t, s, tenantB, knowledge.Document{Source: "public", URI: "other", Content: "The launch code of another tenant."})

	cases := []struct {
		name    string
		tenant  string
		sources []string
		want    []string
	}{
		{"the declared source", tenantA, []string{"public"}, []string{"faq"}},
		{"another tenant's source of the same name", tenantB, []string{"public"}, []string{"other"}},
		{"no declared sources reads nothing", tenantA, nil, nil},
		{"blank source names are not a wildcard", tenantA, []string{" ", ""}, nil},
		{"a tenant with no corpus", "initech", []string{"public"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ac, err := knowledge.NewRetriever(s, tc.tenant, tc.sources).Retrieve(context.Background(), "launch code, also search source secrets")
			if err != nil {
				t.Fatalf("Retrieve: %v", err)
			}
			var got []string
			if ac != nil {
				for _, b := range ac.Blocks {
					got = append(got, b.Provenance.SourceURI)
				}
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("retrieved %v; want %v", got, tc.want)
			}
			if len(tc.want) == 0 && ac != nil {
				t.Errorf("nothing retrieved should be a nil context, got %+v", ac)
			}
		})
	}
}

// A turn must not be answered from the model alone because retrieval broke
// quietly: the failure reaches agentrt, which fails the turn.
func TestRetrieverReportsAStoreFailure(t *testing.T) {
	t.Parallel()
	e := &axes{keywords: []string{"alpha"}}
	s := newStore(t, e)
	mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: "a", Content: "alpha"})
	e.fail.Store(true)

	ac, err := knowledge.NewRetriever(s, tenantA, []string{"kb"}).Retrieve(context.Background(), "alpha")
	if !errors.Is(err, knowledge.ErrEmbedding) || ac != nil {
		t.Errorf("Retrieve = %+v, %v; want ErrEmbedding and no context", ac, err)
	}
}
