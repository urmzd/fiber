package github

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/connectors/internal/sink"
	"github.com/urmzd/mandatum/pkg/connectors/mention"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// GuideAPI maintains a PR-scoped guide separately from run-scoped answers.
// The implementation must refuse an update if the PR no longer has HeadSHA.
type GuideAPI interface {
	UpdateGuide(context.Context, Ref, Guide) error
}

// Guide carries a stable identity and the current commit's navigational guide.
// BotLogin is the authenticated bot's login, used when recovering its comment.
type Guide struct{ Agent, BotLogin, HeadSHA, Body string }

func guideMarker(agent string) string { return "<!-- mandatum:pr-guide:" + agent + " -->" }

// UpdateGuide finds the bot-authored guide even after a restart. A human copying
// its hidden marker cannot make the connector overwrite the human's comment.
func (a *HTTPAPI) UpdateGuide(ctx context.Context, ref Ref, guide Guide) error {
	if err := validGuide(ref, guide); err != nil {
		return err
	}
	marker := guideMarker(guide.Agent)
	var found *ContextComment
	for page := 1; ; page++ {
		var comments []ContextComment
		path := fmt.Sprintf("/repos/%s/%s/issues/%d/comments?per_page=100&page=%d", ref.Owner, ref.Repo, ref.Number, page)
		if err := a.client.Do(ctx, http.MethodGet, path, nil, &comments); err != nil {
			return fmt.Errorf("github: find PR guide: %w", err)
		}
		for i := range comments {
			candidate := &comments[i]
			if strings.EqualFold(strings.TrimSuffix(candidate.User.Login, "[bot]"), strings.TrimSuffix(guide.BotLogin, "[bot]")) && strings.HasPrefix(candidate.Body, marker+"\n") {
				found = candidate
				break
			}
		}
		if found != nil || len(comments) < 100 {
			break
		}
	}
	// Check after searching, immediately before the write. The connector also
	// serializes guide updates so older deliveries cannot win within a process.
	var current ContextSubject
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d", ref.Owner, ref.Repo, ref.Number)
	if err := a.client.Do(ctx, http.MethodGet, path, nil, &current); err != nil {
		return fmt.Errorf("github: read guide head: %w", err)
	}
	if current.Head.SHA != guide.HeadSHA {
		return fmt.Errorf("github: PR guide head changed from %s to %s", guide.HeadSHA, current.Head.SHA)
	}
	body := marker + "\n" + guide.Body
	if found != nil {
		if found.Body == body {
			return nil
		}
		return a.UpdateComment(ctx, ref, found.ID, body)
	}
	_, err := a.CreateComment(ctx, ref, body)
	return err
}

func validGuide(ref Ref, guide Guide) error {
	if err := validReadRef(ref, 1); err != nil {
		return err
	}
	if !validSHA(guide.HeadSHA) || !mention.Valid(guide.Agent) || address.ValidWorkspace(strings.TrimSuffix(guide.BotLogin, "[bot]")) != nil || strings.TrimSpace(guide.Body) == "" {
		return fmt.Errorf("github: guide needs an agent, bot login, head SHA and body")
	}
	if len(guide.Body)+len(guideMarker(guide.Agent))+1 > MaxBodyChars {
		return fmt.Errorf("github: PR guide exceeds comment limit")
	}
	return nil
}

func (c *Connector) guideLogin(agent string) string {
	var names []string
	for login, name := range c.cfg.Bots {
		if name == agent {
			names = append(names, strings.TrimSuffix(login, "[bot]"))
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

func guideBody(evidence Context, agent, login string, mutations bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "## PR guide\n\n**%s**\n\nCurrent commit: `%s`\n\n", reviewText(evidence.Subject.Title), evidence.Subject.Head.SHA)
	if evidence.Subject.Body != "" {
		fmt.Fprintf(&b, "### Intent\n\n%s\n\n", reviewText(sink.Clamp(evidence.Subject.Body, 1600)))
	}
	b.WriteString("### Changes to inspect\n\n")
	if len(evidence.Files) == 0 {
		b.WriteString("No file patches were returned on this page. Read the PR diff before making a review claim.\n")
	} else {
		b.WriteString("| File | Change | Evidence available |\n| :--- | :---: | :--- |\n")
		for _, file := range evidence.Files {
			available := "Patch available"
			if file.Patch == "" {
				available = "Patch unavailable; inspect the file"
			}
			fmt.Fprintf(&b, "| %s | %s | %s |\n", reviewText(sink.Clamp(file.Filename, 400)), reviewText(file.Status), available)
		}
	}
	if evidence.NextPage != 0 {
		b.WriteString("\nMore context is available. This guide lists the first page; the review must read the remaining pages.\n")
	}
	fmt.Fprintf(&b, "\n### Working with %s\n\nTag @%s in a PR comment or review thread to ask a question, request a review, or discuss a suggested fix. Each review includes severity counts, evidence and follow-up prompts.\n\nAutomatic reviews continue for new commits while the configured auto label remains. Removing it stops automatic commit reviews.\n\nReviews and checks identify the commit they cover. A completed agent run does not mean tests passed. React with 👍 or 👎 on the review, or reply and tag the agent.\n", reviewText(agent), login)
	if mutations {
		b.WriteString("\nRepository writers can follow the bot mention with `/fix <requested change>` or `/check <workflow-run-id>`. Fixes create a commit; check requests rerun an existing workflow on its exact commit. The agent reports actual results or a pending state.\n")
	}
	return b.String()
}

// UpdateGuide stores a single guide and compares the seeded current PR head.
func (f *Fake) UpdateGuide(_ context.Context, ref Ref, guide Guide) error {
	if err := validGuide(ref, guide); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return f.Err
	}
	current, ok := f.contexts[ref.String()]
	if !ok || current.Subject.Head.SHA != guide.HeadSHA {
		return fmt.Errorf("github: PR guide head changed")
	}
	body := guideMarker(guide.Agent) + "\n" + guide.Body
	for i := range f.comments {
		comment := &f.comments[i]
		if comment.Ref == ref && comment.GuideAgent == guide.Agent && comment.GuideLogin == guide.BotLogin {
			if comment.Body != body {
				comment.Body = body
				comment.Edits++
			}
			return nil
		}
	}
	f.nextID++
	f.comments = append(f.comments, FakeComment{ID: f.nextID, Ref: ref, Body: body, GuideAgent: guide.Agent, GuideLogin: guide.BotLogin})
	f.byID[f.nextID] = len(f.comments) - 1
	return nil
}

var _ GuideAPI = (*HTTPAPI)(nil)
var _ GuideAPI = (*Fake)(nil)

func (c *Connector) prepareGuide(ctx context.Context, tag *envelope.Tag) error {
	c.guideMu.Lock()
	defer c.guideMu.Unlock()
	login := c.guideLogin(tag.Agent)
	if login == "" {
		return fmt.Errorf("github: no guide bot login for %s", tag.Agent)
	}
	ref, err := c.ref(tag.Source)
	if err != nil {
		return err
	}
	evidence, err := c.cfg.API.(ContextAPI).ReadContext(ctx, ref, 1)
	if err != nil {
		return err
	}
	head := evidence.Subject.Head.SHA
	if !validSHA(head) {
		return fmt.Errorf("github: guide needs a PR head SHA")
	}
	if pinned := tag.Source.Params["head_sha"]; pinned != "" && pinned != head {
		return fmt.Errorf("github: stale PR guide trigger")
	}
	tag.Source = tag.Source.WithParam("head_sha", head)
	tag.Meta["github_head_sha"] = head
	return c.cfg.API.(GuideAPI).UpdateGuide(ctx, ref, Guide{Agent: tag.Agent, BotLogin: login, HeadSHA: head, Body: guideBody(evidence, tag.Agent, login, c.cfg.Mutations)})
}
