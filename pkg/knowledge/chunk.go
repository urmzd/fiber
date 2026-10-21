package knowledge

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Chunk is one passage of a document and where it came from.
type Chunk struct {
	Locator Locator
	// Text is Content[Locator.Start:Locator.End], unmodified.
	Text string
}

type chunking struct{ size, overlap int }

// span is a half-open byte range of the content. head marks a unit that
// begins with a heading.
type span struct {
	start, end int
	head       bool
}

// Split cuts content into chunks of at most size bytes, the way a store built
// with WithChunking(size, overlap) would. It is exported so a caller can see
// how a document will be indexed without indexing it.
func Split(content string, size, overlap int) []Chunk {
	c := chunking{size: DefaultChunkSize, overlap: DefaultChunkOverlap}
	if size >= 1 && overlap >= 0 && overlap < size {
		c = chunking{size: size, overlap: overlap}
	}
	return split(content, c)
}

// split cuts content into chunks.
//
// It works in two passes. The first finds units: paragraphs, with a paragraph
// longer than the target cut at whitespace. The second packs consecutive units
// into chunks. A chunk is always one contiguous range of the content, so its
// text is a substring and its locator is exact; that rules out "cleaning" the
// text, and is the reason the chunker never rewrites whitespace.
//
// A chunk never runs across a heading. A heading is the author saying the
// subject has changed, and a chunk that straddles one is about two things: its
// embedding is an average of both, and the section a citation names is wrong
// for half of it.
//
// Paragraphs are the unit because they are the smallest thing an author marked
// as belonging together. Cutting at a fixed width regardless would put the
// sentence that answers a question half in one chunk and half in the next.
func split(content string, cfg chunking) []Chunk {
	units, headings := scan(content, cfg.size)
	if len(units) == 0 {
		return nil
	}

	var spans []span
	first := 0 // index of the unit the current chunk starts at
	for first < len(units) {
		last := first
		for last+1 < len(units) && !units[last+1].head && units[last+1].end-units[first].start <= cfg.size {
			last++
		}
		spans = append(spans, span{start: units[first].start, end: units[last].end})
		next := last + 1
		if next >= len(units) {
			break
		}
		// Overlap: start the next chunk at an earlier unit when the units
		// it repeats fit the overlap budget and the new unit still fits
		// the chunk. It never starts at or before this chunk's first
		// unit, which is what guarantees progress, and it never reaches
		// back across a heading.
		start := next
		for start-1 > first && !units[start].head &&
			units[next-1].end-units[start-1].start <= cfg.overlap &&
			units[next].end-units[start-1].start <= cfg.size {
			start--
		}
		first = start
	}

	out := make([]Chunk, len(spans))
	line, pos := 1, 0
	for i, s := range spans {
		// Spans start in increasing order, so lines are counted once.
		line += strings.Count(content[pos:s.start], "\n")
		pos = s.start
		text := content[s.start:s.end]
		out[i] = Chunk{
			Text: text,
			Locator: Locator{
				Chunk:     i,
				Start:     s.start,
				End:       s.end,
				StartLine: line,
				EndLine:   line + strings.Count(text, "\n"),
				Heading:   headingAt(headings, s.start),
			},
		}
	}
	return out
}

// heading is a Markdown heading and the offset it starts at.
type heading struct {
	at   int
	text string
}

// headingAt returns the last heading that starts at or before offset.
func headingAt(hs []heading, offset int) string {
	i, found := slices.BinarySearchFunc(hs, offset, func(h heading, at int) int { return h.at - at })
	if !found {
		i--
	}
	if i < 0 {
		return ""
	}
	return hs[i].text
}

// scan finds the units of content and its headings.
//
// A unit is a run of non-blank lines. A heading line starts a new unit even
// with no blank line before it, so a section's first chunk begins at its
// heading. Lines inside a fenced code block are never headings: "# install"
// in a shell snippet is a comment.
func scan(content string, size int) ([]span, []heading) {
	var (
		units    []span
		headings []heading
		open     = -1 // start of the unit being built, or -1
		end      int  // end of its last non-blank line
		head     bool // whether it begins with a heading
		fenced   bool
	)
	flush := func() {
		if open >= 0 {
			units = appendUnit(units, content, span{open, end, head}, size)
			open, head = -1, false
		}
	}
	for pos := 0; pos < len(content); {
		stop := strings.IndexByte(content[pos:], '\n')
		next := len(content)
		if stop >= 0 {
			next = pos + stop + 1
		}
		raw := content[pos:next]
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			flush()
		} else {
			lead := len(raw) - len(strings.TrimLeftFunc(raw, unicode.IsSpace))
			at := pos + lead
			if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
				fenced = !fenced
			} else if text, ok := headingText(trimmed); ok && !fenced {
				flush()
				headings = append(headings, heading{at: at, text: text})
				head = true
			}
			if open < 0 {
				open = at
			}
			end = at + len(trimmed)
		}
		pos = next
	}
	flush()
	return units, headings
}

// headingText reports whether line is an ATX heading ("## Title") and returns
// its text.
func headingText(line string) (string, bool) {
	level := 0
	for level < len(line) && line[level] == '#' {
		level++
	}
	if level == 0 || level > 6 || level == len(line) || (line[level] != ' ' && line[level] != '\t') {
		return "", false
	}
	text := strings.TrimSpace(strings.TrimRight(strings.TrimSpace(line[level:]), "#"))
	return text, text != ""
}

// appendUnit adds u to units, cutting it first if it is longer than size.
//
// A cut prefers the last whitespace in the second half of the window, so a
// word is not split when there is somewhere else to cut and a piece is never
// left absurdly short. Text with no whitespace at all (a minified blob, a long
// URL) is cut at the size, backed up to a rune boundary so a multi-byte
// character is never divided.
func appendUnit(units []span, content string, u span, size int) []span {
	for u.end-u.start > size {
		window := content[u.start : u.start+size]
		cut := strings.LastIndexFunc(window, unicode.IsSpace)
		if cut < size/2 {
			cut = size
			for cut > 1 && !utf8.RuneStart(content[u.start+cut]) {
				cut--
			}
			if !utf8.RuneStart(content[u.start+cut]) {
				// size is smaller than one character: take the
				// whole character rather than loop on nothing.
				_, w := utf8.DecodeRuneInString(content[u.start:])
				cut = w
			}
		}
		piece := strings.TrimRightFunc(content[u.start:u.start+cut], unicode.IsSpace)
		if piece != "" {
			units = append(units, span{u.start, u.start + len(piece), u.head})
		}
		rest := content[u.start+cut : u.end]
		u.start, u.head = u.end-len(strings.TrimLeftFunc(rest, unicode.IsSpace)), false
	}
	if u.end > u.start {
		units = append(units, u)
	}
	return units
}

// hash is the identity of an indexed document: its content and everything that
// decides how that content is indexed.
//
// The embedding model and the chunking parameters are in it because an
// "unchanged" document whose vectors came from a different model is not
// indexed the way this store would index it, and treating it as a no-op would
// leave vectors from two spaces in one index. Source and URI are not in it:
// they are the key.
//
// Fields are length-prefixed so no two different documents can produce the
// same byte stream by moving a boundary.
func (e *engine) hash(doc Document) string {
	h := sha256.New()
	write := func(s string) {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(s)))
		h.Write(n[:])
		h.Write([]byte(s))
	}
	write("fiber.knowledge.v1")
	write(e.embedder.Model())
	write(strconv.Itoa(e.embedder.Dimensions()))
	write(strconv.Itoa(e.chunk.size))
	write(strconv.Itoa(e.chunk.overlap))
	write(e.textConfig)
	write(doc.Title)
	write(doc.Content)
	keys := make([]string, 0, len(doc.Metadata))
	for k := range doc.Metadata {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		write(k)
		write(doc.Metadata[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}
