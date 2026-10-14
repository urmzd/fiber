// Package attachment is the typed reference to a file an agent was handed.
//
// A file reaches an agent from wherever its tag was raised: a Slack upload, an
// image pasted into a GitHub issue, a recording on a Jira ticket, a path given
// to the CLI. By the time the core sees it, none of that matters. The bytes are
// in pkg/blob under their SHA-256, and what travels with the tag is an
// Attachment: the digest, what the bytes are, what they were called, how big
// they are and where they were attached.
//
// # Why a reference and not the bytes
//
// A tag is a run's durable input. It is hashed to match a replayed call
// against its record, journaled, forwarded to a sandbox that may be another
// process, and copied onto every delegated child run. A ten megabyte recording
// cannot ride along on all of that; a hundred-odd bytes naming it can. And
// because the name is a content digest, the reference means the same bytes on
// every attempt, which is what keeps a replay from diverging.
//
// # Why the media type is sniffed
//
// The uploader's claim about a file (its extension, the Content-Type header,
// the mimetype a surface reports) is input from outside and is not believed.
// Routing decides whether bytes are handed to a model as an image, decoded as
// text, or passed to a transcriber, so a claim that could steer it would be an
// attack surface: a script named photo.png, an HTML login page served where an
// image was expected. Sniff reads the leading bytes and reports what the
// content is. A name can only refine a type the content has already proven,
// for example telling a ZIP container that it is a .docx, or plain text that
// it is Markdown. It can never promote bytes into a kind they are not.
//
// # Layering
//
// This package is a leaf: stdlib and pkg/address only, so pkg/envelope can
// carry an Attachment on a Tag without the wire contract growing a dependency
// on storage or on the agent loop. Getting bytes in lives in the intake
// subpackage; turning a reference into model input lives in resolve, which is
// the only part that knows about saige.
package attachment

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/urmzd/mandatum/pkg/address"
)

// ErrInvalid reports a malformed attachment. Callers match with errors.Is.
var ErrInvalid = errors.New("attachment: invalid")

// Bounds on the descriptive fields. An attachment rides on a tag, so it must
// stay small whatever an uploader named a file.
const (
	// MaxName bounds Name in bytes.
	MaxName = 255
	// MaxAlt bounds Alt in bytes.
	MaxAlt = 1024
	// MaxPerTag bounds how many attachments one tag may carry. It is a wire
	// bound, deliberately above any per-run budget a resolver applies, so
	// that the resolver's budget is the limit a user actually meets and gets
	// an explanation for.
	MaxPerTag = 64
)

// Attachment is a reference to one stored file. It is a small value: safe to
// copy, to serialize onto a tag, and to hand to a delegated child run.
type Attachment struct {
	// Digest is the lowercase hex SHA-256 of the content, and the blob's key
	// within the tenant.
	Digest string `json:"digest"`
	// MediaType is what the content is, as sniffed from its bytes. It has no
	// parameters.
	MediaType string `json:"media_type"`
	// Name is the file's display name, cleaned of path components and control
	// characters. It is a label for people and models, never a path.
	Name string `json:"name,omitempty"`
	// Size is the content length in bytes.
	Size int64 `json:"size"`
	// Origin is where the file was attached: the issue, thread or ticket.
	// Zero when the surface has no address, as with the CLI.
	Origin address.Address `json:"origin,omitzero"`
	// Alt is a description supplied with the file, such as Markdown image alt
	// text or a Slack file title. It is the uploader's words and is presented
	// to a model as such.
	Alt string `json:"alt,omitempty"`
}

// Kind returns the routing kind of the attachment's media type.
func (a Attachment) Kind() Kind { return KindOf(a.MediaType) }

// Label is the name to show for the attachment: its Name, or a placeholder
// that still tells a reader a file was there.
func (a Attachment) Label() string {
	if a.Name != "" {
		return a.Name
	}
	return "unnamed file"
}

// Validate checks that the attachment is well formed. It is what a server
// applies to an attachment that arrived on the wire, where every field is a
// claim.
func (a Attachment) Validate() error {
	if err := validDigest(a.Digest); err != nil {
		return err
	}
	if a.Size < 0 {
		return fmt.Errorf("%w: negative size %d", ErrInvalid, a.Size)
	}
	if a.MediaType == "" || len(a.MediaType) > 127 || !strings.Contains(a.MediaType, "/") ||
		strings.ContainsAny(a.MediaType, " ;\t") || a.MediaType != strings.ToLower(a.MediaType) || !printable(a.MediaType) {
		return fmt.Errorf("%w: media type %q is not a bare lowercase type/subtype", ErrInvalid, a.MediaType)
	}
	if a.Name != CleanName(a.Name) {
		return fmt.Errorf("%w: name is not clean: it must be a bare file name of at most %d bytes with no control characters", ErrInvalid, MaxName)
	}
	if a.Alt != CleanAlt(a.Alt) {
		return fmt.Errorf("%w: alt text is not clean: at most %d bytes on one line", ErrInvalid, MaxAlt)
	}
	return nil
}

// ValidateAll checks a tag's attachment list: each entry, and the count.
func ValidateAll(as []Attachment) error {
	if len(as) > MaxPerTag {
		return fmt.Errorf("%w: %d attachments, a tag carries at most %d", ErrInvalid, len(as), MaxPerTag)
	}
	for i, a := range as {
		if err := a.Validate(); err != nil {
			return fmt.Errorf("%w (attachment %d)", err, i)
		}
	}
	return nil
}

// Inherit returns the attachments a delegated child run should carry: the
// ones the parent holds, then the ones passed with the delegation, with a
// file that appears in both listed once at its first position.
//
// Order is preserved and the result is a function of its inputs alone. The
// list becomes part of the child's durable input, and a model sees files in
// list order, so an order that varied between attempts would be a replay that
// diverges.
func Inherit(parent, own []Attachment) []Attachment {
	if len(parent)+len(own) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(parent)+len(own))
	out := make([]Attachment, 0, len(parent)+len(own))
	for _, list := range [][]Attachment{parent, own} {
		for _, a := range list {
			if seen[a.Digest] {
				continue
			}
			seen[a.Digest] = true
			out = append(out, a)
		}
	}
	return out
}

// CleanName reduces an uploader's file name to a display label: the final
// path element, with control characters removed, at most MaxName bytes. A
// name is shown to people and written into a model's prompt, so it must not
// be able to carry a path or break out of the line it is printed on.
func CleanName(name string) string {
	name = strings.ReplaceAll(name, `\`, "/")
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	name = clean(name, MaxName)
	if name == "." || name == ".." {
		return ""
	}
	return name
}

// CleanAlt reduces alt text to one line of at most MaxAlt bytes.
func CleanAlt(alt string) string { return clean(alt, MaxAlt) }

// clean drops control and invalid characters, folds whitespace runs to one
// space, trims, and cuts on a rune boundary.
func clean(s string, limit int) string {
	var b strings.Builder
	space := false
	for _, r := range s {
		switch {
		case r == utf8.RuneError, unicode.IsControl(r) && !unicode.IsSpace(r), unicode.Is(unicode.Cf, r):
			continue
		case unicode.IsSpace(r):
			space = b.Len() > 0
			continue
		}
		if space {
			b.WriteByte(' ')
			space = false
		}
		if b.Len()+utf8.RuneLen(r) > limit {
			break
		}
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}

func printable(s string) bool {
	for _, r := range s {
		if r > unicode.MaxASCII || unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validDigest(d string) error {
	if len(d) != 64 {
		return fmt.Errorf("%w: digest must be 64 hex digits, got %d characters", ErrInvalid, len(d))
	}
	for i := 0; i < len(d); i++ {
		if c := d[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return fmt.Errorf("%w: digest is not lowercase hex", ErrInvalid)
		}
	}
	return nil
}
