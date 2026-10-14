package github

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/connector"
	"github.com/urmzd/mandatum/pkg/connectors/mention"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// MutationAPI supports explicit, collaborator-authorized changes to a PR head.
// CommitChanges must compare the expected head atomically with the branch write.
type MutationAPI interface {
	CanWrite(context.Context, Ref, string) (bool, error)
	CommitChanges(context.Context, Ref, string, string, ChangeRequest) (CommitResult, error)
}

// FileEdit replaces a complete UTF-8 text file, or deletes it. Paths are always
// repository-relative; branch and repository are bound by the triggering run.
type FileEdit struct {
	Path    string `json:"path"`
	Content string `json:"content,omitempty"`
	Delete  bool   `json:"delete,omitempty"`
}

// UnmarshalJSON distinguishes an intentional empty file from an omitted
// replacement, so a missing model argument cannot silently erase a file.
func (f *FileEdit) UnmarshalJSON(raw []byte) error {
	var wire struct {
		Path    string  `json:"path"`
		Content *string `json:"content"`
		Delete  bool    `json:"delete"`
	}
	if err := decodeArguments(raw, &wire); err != nil {
		return err
	}
	if !wire.Delete && wire.Content == nil {
		return fmt.Errorf("github: replacement edit needs content")
	}
	f.Path, f.Delete, f.Content = wire.Path, wire.Delete, ""
	if wire.Content != nil {
		f.Content = *wire.Content
	}
	return nil
}

// ChangeRequest is one atomic commit. All files succeed together or none do.
type ChangeRequest struct {
	Message string     `json:"message"`
	Files   []FileEdit `json:"files"`
}

// CommitResult records the actual commit produced by GitHub, not a model claim.
type CommitResult struct {
	SHA       string `json:"sha"`
	URL       string `json:"url"`
	Recovered bool   `json:"recovered,omitempty"`
}

func (c *Connector) mutationAction() connector.Action {
	return connector.Action{Name: "github_apply_changes", Description: "Apply an explicitly requested /fix as one atomic commit to this PR's pinned head. Requires a signed request from a repository writer. Read files before editing. If the branch moved, ask for a new request. This does not run tests.", Schema: json.RawMessage(`{"type":"object","properties":{"message":{"type":"string","description":"Short commit message."},"files":{"type":"array","minItems":1,"maxItems":20,"items":{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string","description":"Complete replacement text, not a diff."},"delete":{"type":"boolean"}},"required":["path"],"additionalProperties":false}}},"required":["message","files"],"additionalProperties":false}`), Invoke: c.applyChanges}
}

func (c *Connector) mutationMAC(target address.Address) []byte {
	// A separate protocol prefix prevents using a webhook signature as a grant,
	// even when a deployment uses its webhook secret as the signing key.
	fields := []string{"mandatum.github.mutation.v1", target.Workspace, strings.Join(target.Path, "/"), target.Params["head_sha"], target.Params["write_actor"], target.Params["write_run"], target.Params["write_operation"], target.Params["write_workflow"], target.Params["write_attempt"]}
	encoded, _ := json.Marshal(fields)
	mac := hmac.New(sha256.New, c.cfg.MutationSecret)
	_, _ = mac.Write(encoded)
	return mac.Sum(nil)
}

func (c *Connector) grantMutation(ctx context.Context, tag *envelope.Tag) error {
	if !c.cfg.Mutations || tag.Source.Path[1] != "pull" || tag.Actor.Bot {
		return nil
	}
	event := tag.Meta["github_event"]
	if event != "issue_comment" && event != "pull_request_review_comment" {
		return nil
	}
	command := strings.Fields(tag.Text)
	if len(command) == 0 || (command[0] != "/fix" && command[0] != "/check") {
		return nil
	}
	// GitHub delivery UUIDs are already valid run IDs. Refuse synthetic or
	// malformed IDs so the grant and runtime identity cannot diverge.
	if !mention.Valid(tag.ID) || !validSHA(tag.Source.Params["head_sha"]) {
		return fmt.Errorf("github: fix request needs delivery ID and pinned head")
	}
	ref, err := c.ref(tag.Source)
	if err != nil {
		return err
	}
	allowed, err := c.cfg.API.(MutationAPI).CanWrite(ctx, ref, tag.Actor.ID)
	if err != nil {
		return err
	}
	if !allowed {
		return nil
	} // The agent can still explain the denied capability.
	operation := strings.TrimPrefix(command[0], "/")
	source := tag.Source.WithParam("write_actor", tag.Actor.ID).WithParam("write_run", tag.ID).WithParam("write_operation", operation)
	if operation == "check" {
		if len(command) < 2 {
			return nil
		}
		id, err := strconv.ParseInt(command[1], 10, 64)
		if err != nil || id <= 0 {
			return nil
		}
		api, ok := c.cfg.API.(WorkflowAPI)
		if !ok {
			return fmt.Errorf("github: workflow execution unavailable")
		}
		run, err := api.WorkflowRun(ctx, ref, id)
		if err != nil {
			return err
		}
		if run.HeadSHA != source.Params["head_sha"] {
			return nil
		}
		source = source.WithParam("write_workflow", strconv.FormatInt(id, 10)).WithParam("write_attempt", strconv.Itoa(run.Attempt))
	}
	tag.Source = source.WithParam("write_grant", hex.EncodeToString(c.mutationMAC(source)))
	return nil
}

func (c *Connector) authorizeMutation(ctx context.Context, target address.Address, operation string) (Ref, error) {
	ref, err := c.ref(target)
	if err != nil {
		return Ref{}, err
	}
	if !c.cfg.Mutations || target.Path[1] != "pull" || !validSHA(target.Params["head_sha"]) || target.Params["write_operation"] != operation || connector.RunID(ctx) == "" || target.Params["write_run"] != connector.RunID(ctx) {
		return Ref{}, fmt.Errorf("github: mutation requires an authorized %s request in this run", operation)
	}
	signature, err := hex.DecodeString(target.Params["write_grant"])
	if err != nil || !hmac.Equal(signature, c.mutationMAC(target)) {
		return Ref{}, fmt.Errorf("github: invalid mutation grant")
	}
	allowed, err := c.cfg.API.(MutationAPI).CanWrite(ctx, ref, target.Params["write_actor"])
	if err != nil {
		return Ref{}, err
	}
	if !allowed {
		return Ref{}, fmt.Errorf("github: requester no longer has repository write permission")
	}
	return ref, nil
}

func (c *Connector) applyChanges(ctx context.Context, target address.Address, raw json.RawMessage) (connector.Result, error) {
	ref, err := c.authorizeMutation(ctx, target, "fix")
	if err != nil {
		return connector.Result{}, err
	}
	var args ChangeRequest
	if err := decodeArguments(raw, &args); err != nil {
		return connector.Result{}, err
	}
	if err := validateChanges(args); err != nil {
		return connector.Result{}, err
	}
	result, err := c.cfg.API.(MutationAPI).CommitChanges(ctx, ref, target.Params["head_sha"], target.Params["write_run"], args)
	if err != nil {
		return connector.Result{}, err
	}
	data, _ := json.Marshal(result)
	return connector.Result{Summary: fmt.Sprintf("Created commit %s: %s. Tests have not been run by this action.", result.SHA, result.URL), Address: refAddress(ref, "pull").WithParam("head_sha", result.SHA), Data: data}, nil
}

func decodeArguments(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return fmt.Errorf("github: arguments: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("github: expected one arguments object")
	}
	return nil
}

func validateChanges(args ChangeRequest) error {
	if strings.TrimSpace(args.Message) == "" || len(args.Message) > 200 || strings.ContainsAny(args.Message, "\r\n") {
		return fmt.Errorf("github: commit message must be one line of at most 200 bytes")
	}
	if len(args.Files) == 0 || len(args.Files) > 20 {
		return fmt.Errorf("github: commit needs 1 to 20 file edits")
	}
	total := 0
	seen := map[string]bool{}
	for _, file := range args.Files {
		if !utf8.ValidString(file.Content) || strings.ContainsRune(file.Content, 0) {
			return fmt.Errorf("github: edits must be UTF-8 text")
		}
		if file.Path == "" || path.IsAbs(file.Path) || path.Clean(file.Path) != file.Path || file.Path == "." || file.Path == ".." || strings.HasPrefix(file.Path, "../") || strings.ContainsAny(file.Path, "\\\x00\r\n") {
			return fmt.Errorf("github: invalid repository file path %q", file.Path)
		}
		for _, segment := range strings.Split(file.Path, "/") {
			if strings.EqualFold(segment, ".git") {
				return fmt.Errorf("github: reserved file path")
			}
		}
		if seen[file.Path] {
			return fmt.Errorf("github: duplicate edit for %q", file.Path)
		}
		seen[file.Path] = true
		if file.Delete && file.Content != "" {
			return fmt.Errorf("github: deletion cannot include replacement content")
		}
		total += len(file.Content)
	}
	if total > 500000 {
		return fmt.Errorf("github: edit payload exceeds 500000 bytes")
	}
	return nil
}

func mutationMarker(run, expected string, args ChangeRequest) string {
	encoded, _ := json.Marshal(struct {
		Run, Head string
		Changes   ChangeRequest
	}{run, expected, args})
	sum := sha256.Sum256(encoded)
	return "Mandatum-Change: " + hex.EncodeToString(sum[:])
}
