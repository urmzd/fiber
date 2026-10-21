package durable_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/urmzd/anchor/pkg/run"

	"github.com/urmzd/fiber/pkg/durable"
)

// A prefix read returns the records whose key starts with a prefix and
// nothing else, the same records the ledger itself loads, with LIKE's
// wildcards taken literally.
func TestRecordsWithPrefixReadsOnlyTheRecordsAsked(t *testing.T) {
	ctx := context.Background()
	pool := newPool(t)
	lgr, _, err := durable.Open(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := lgr.Accept(ctx, run.Run{ID: "r1", Name: "w", Input: []byte(`{}`), Status: run.StatusPending, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"event:fiber.answer.q1#0", "fiber.question.q1#0", "fiber.turn.agent#0", "event:fiber_answer.x#0", "event:fiber.stop#0"} {
		if _, err := lgr.Record(ctx, run.Record{RunID: "r1", Key: key, Kind: run.KindStep, Name: key, Status: run.RecordOK, Output: []byte(`{"k":"` + key + `"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := durable.NewRecords(pool).RecordsWithPrefix(ctx, "r1", "event:fiber.answer.", "fiber.question.")
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, rec := range got {
		keys = append(keys, rec.Key)
		if string(rec.Output) != `{"k":"`+rec.Key+`"}` || rec.RunID != "r1" || rec.Status != run.RecordOK {
			t.Errorf("record %s read back as %+v", rec.Key, rec)
		}
	}
	sort.Strings(keys)
	if len(keys) != 2 || keys[0] != "event:fiber.answer.q1#0" || keys[1] != "fiber.question.q1#0" {
		t.Fatalf("keys = %v", keys)
	}
	if none, err := durable.NewRecords(pool).RecordsWithPrefix(ctx, "r1", "event:fiber%"); err != nil || len(none) != 0 {
		t.Fatalf("a wildcard in a prefix matched %v (%v)", none, err)
	}
}
