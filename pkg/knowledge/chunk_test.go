package knowledge_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/urmzd/fiber/pkg/knowledge"
)

// The property every citation rests on: whatever the input, a chunk's text is
// exactly the bytes its locator points at, and nothing with words in it falls
// between two chunks.
func TestEveryChunkIsAnExactSliceAndNoTextIsLost(t *testing.T) {
	t.Parallel()
	inputs := map[string]string{
		"paragraphs":         "First paragraph here.\n\nSecond paragraph here.\n\nThird one.",
		"windows newlines":   "First paragraph.\r\n\r\nSecond paragraph.\r\n",
		"leading whitespace": "\n\n   indented start\n\nend\n\n\n",
		"one long paragraph": strings.Repeat("word ", 400),
		"no whitespace":      strings.Repeat("x", 500),
		"multibyte":          strings.Repeat("日本語のテキスト。", 60),
		"emoji run":          strings.Repeat("🙂", 90),
		"headings and code":  "# Title\nintro\n\n## Setup\n```sh\n# not a heading\nmake init\n```\n\n## Use\nrun it\n",
	}
	for name, content := range inputs {
		for _, size := range []int{1, 7, 40, 1200} {
			chunks := knowledge.Split(content, size, 0)
			covered := 0
			for i, c := range chunks {
				l := c.Locator
				if l.Chunk != i {
					t.Errorf("%s/%d: chunk %d is numbered %d", name, size, i, l.Chunk)
				}
				if l.Start < covered || l.End <= l.Start || l.End > len(content) {
					t.Fatalf("%s/%d: chunk %d spans [%d,%d) after %d", name, size, i, l.Start, l.End, covered)
				}
				if content[l.Start:l.End] != c.Text {
					t.Errorf("%s/%d: chunk %d text is not content[%d:%d]", name, size, i, l.Start, l.End)
				}
				if !utf8.ValidString(c.Text) {
					t.Errorf("%s/%d: chunk %d splits a character", name, size, i)
				}
				if strings.TrimSpace(c.Text) != c.Text {
					t.Errorf("%s/%d: chunk %d has surrounding whitespace: %q", name, size, i, c.Text)
				}
				if gap := content[covered:l.Start]; strings.TrimSpace(gap) != "" {
					t.Errorf("%s/%d: %q was dropped before chunk %d", name, size, gap, i)
				}
				covered = l.End
			}
			if rest := content[covered:]; strings.TrimSpace(rest) != "" {
				t.Errorf("%s/%d: %q was dropped at the end", name, size, rest)
			}
		}
	}
}

func TestChunksRespectTheSizeAndKeepParagraphsWhole(t *testing.T) {
	t.Parallel()
	content := "Alpha one.\n\nBeta two.\n\nGamma three.\n\n" + strings.Repeat("long ", 30) + "\n\nOmega."
	chunks := knowledge.Split(content, 30, 0)
	for _, c := range chunks {
		if len(c.Text) > 30 {
			t.Errorf("chunk %d is %d bytes; the size is 30", c.Locator.Chunk, len(c.Text))
		}
	}
	// Two short paragraphs fit a chunk, a third does not, and none is cut.
	if chunks[0].Text != "Alpha one.\n\nBeta two." || chunks[1].Text != "Gamma three." {
		t.Errorf("first chunks = %q, %q; want paragraphs packed whole", chunks[0].Text, chunks[1].Text)
	}
	// The long paragraph is cut at spaces, so no chunk starts or ends
	// inside a word.
	for _, c := range chunks[2 : len(chunks)-1] {
		for _, w := range strings.Fields(c.Text) {
			if w != "long" {
				t.Errorf("chunk %d cut a word: %q", c.Locator.Chunk, c.Text)
			}
		}
	}
	if chunks[len(chunks)-1].Text != "Omega." {
		t.Errorf("last chunk = %q", chunks[len(chunks)-1].Text)
	}
}

// A fact on a boundary should be whole in at least one chunk, which is what
// overlap buys. It must not buy an endless loop or a chunk over the size.
func TestOverlapRepeatsTheTailOfTheChunkBefore(t *testing.T) {
	t.Parallel()
	content := "Aaaa aaaa.\n\nBbbb bbbb.\n\nCccc cccc.\n\nDddd dddd.\n\nEeee eeee."
	plain := knowledge.Split(content, 24, 0)
	lapped := knowledge.Split(content, 24, 12)

	if plain[0].Text != "Aaaa aaaa.\n\nBbbb bbbb." || plain[1].Text != "Cccc cccc.\n\nDddd dddd." {
		t.Fatalf("without overlap: %q, %q", plain[0].Text, plain[1].Text)
	}
	if lapped[0].Text != "Aaaa aaaa.\n\nBbbb bbbb." || lapped[1].Text != "Bbbb bbbb.\n\nCccc cccc." {
		t.Fatalf("with overlap: %q, %q; want the second chunk to start at the first one's last paragraph", lapped[0].Text, lapped[1].Text)
	}
	for i, c := range lapped {
		if len(c.Text) > 24 {
			t.Errorf("chunk %d is %d bytes with overlap; the size is 24", i, len(c.Text))
		}
		if i > 0 && c.Locator.Start <= lapped[i-1].Locator.Start {
			t.Errorf("chunk %d does not start after chunk %d", i, i-1)
		}
	}
	if last := lapped[len(lapped)-1]; !strings.HasSuffix(last.Text, "Eeee eeee.") {
		t.Errorf("the end of the document is missing: %q", last.Text)
	}
}

func TestLocatorsCarryLinesAndTheNearestHeading(t *testing.T) {
	t.Parallel()
	content := "preface\n\n# Guide\nintro line\n\n## Install\n```sh\n# a comment, not a heading\nmake\n```\n\nmore install\n\n## Use ##\nrun\n#notaheading\n"
	type want struct {
		first, heading string
		start, end     int
	}
	wants := []want{
		{"preface", "", 1, 1},
		{"# Guide", "Guide", 3, 4},
		{"## Install", "Install", 6, 10},
		{"more install", "Install", 12, 12},
		{"## Use ##", "Use", 14, 16},
	}
	chunks := knowledge.Split(content, 60, 0)
	if len(chunks) != len(wants) {
		t.Fatalf("got %d chunks, want %d: %+v", len(chunks), len(wants), chunks)
	}
	for i, w := range wants {
		c := chunks[i]
		if first, _, _ := strings.Cut(c.Text, "\n"); first != w.first {
			t.Errorf("chunk %d starts %q; want %q", i, first, w.first)
		}
		if c.Locator.Heading != w.heading {
			t.Errorf("chunk %d heading = %q; want %q", i, c.Locator.Heading, w.heading)
		}
		if c.Locator.StartLine != w.start || c.Locator.EndLine != w.end {
			t.Errorf("chunk %d lines = %d to %d; want %d to %d", i, c.Locator.StartLine, c.Locator.EndLine, w.start, w.end)
		}
	}
	if got := chunks[1].Locator.String(); got != "L3-L4" {
		t.Errorf("locator renders as %q; want L3-L4", got)
	}
	if got := chunks[0].Locator.String(); got != "L1" {
		t.Errorf("single-line locator renders as %q; want L1", got)
	}
}

func TestContentWithNoWordsYieldsNoChunks(t *testing.T) {
	t.Parallel()
	for _, content := range []string{"", "   ", "\n\n\t\r\n"} {
		if chunks := knowledge.Split(content, 0, 0); len(chunks) != 0 {
			t.Errorf("Split(%q) = %+v; want nothing", content, chunks)
		}
	}
}
