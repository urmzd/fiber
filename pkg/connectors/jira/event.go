package jira

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/connectors/mention"
	"github.com/urmzd/fiber/pkg/event"
)

// Headers on an inbound Jira webhook.
const (
	// HeaderIdentifier is Atlassian's own identifier for one webhook delivery,
	// and becomes the event's id.
	//
	// It is the header rather than the payload's "id" field for a specific
	// reason: the payload's id is a small ordinal that Jira reuses, so two
	// unrelated events routinely carry id 2. Using it as an idempotency key
	// would collapse unrelated runs into one — the exact opposite of the failure
	// it is meant to prevent. The header is per delivery and stable across
	// Atlassian's own retries, which is what an idempotency key has to be.
	HeaderIdentifier = "X-Atlassian-Webhook-Identifier"

	// HeaderSecret is the default header carrying the shared secret. Jira does
	// not sign its webhooks, so authentication is whatever header the person who
	// registered the webhook was able to add. See the package doc.
	HeaderSecret = "X-Fiber-Secret"
)

// Jira's webhookEvent values that this connector reads.
const (
	EventCommentCreated = "comment_created"
	EventIssueCreated   = "jira:issue_created"
	EventIssueUpdated   = "jira:issue_updated"
)

// hook is the union of the webhook payloads this connector reads. Jira sends one
// JSON object per event with a shared shape, and decoding the union and
// switching on webhookEvent is simpler than four near-identical structs.
type hook struct {
	// Timestamp is milliseconds since the epoch, which is Jira's convention
	// everywhere except inside issue fields, where it is a formatted string.
	Timestamp int64 `json:"timestamp"`

	WebhookEvent string `json:"webhookEvent"`
	// IssueEventType distinguishes what a jira:issue_updated actually was:
	// issue_commented, issue_assigned, issue_generic, and a dozen others.
	IssueEventType string `json:"issue_event_type_name"`

	User      *jiraUser  `json:"user"`
	Issue     *jiraIssue `json:"issue"`
	Comment   *comment   `json:"comment"`
	Changelog *changelog `json:"changelog"`
}

type jiraUser struct {
	AccountID   string `json:"accountId"`
	DisplayName string `json:"displayName"`
	// Name is Data Center's username; Cloud stopped sending it.
	Name string `json:"name"`
	// AccountType is "atlassian", "app" or "customer". An app is automation,
	// including this connector's own comments coming back.
	AccountType string `json:"accountType"`
}

// id returns the stable identifier for this user, preferring the account id and
// falling back to the Data Center username.
func (u *jiraUser) id() string {
	if u == nil {
		return ""
	}
	if u.AccountID != "" {
		return u.AccountID
	}
	return u.Name
}

func (u *jiraUser) display() string {
	if u == nil {
		return ""
	}
	if u.DisplayName != "" {
		return u.DisplayName
	}
	return u.Name
}

func (u *jiraUser) bot() bool { return u != nil && u.AccountType == "app" }

type jiraIssue struct {
	ID  string `json:"id"`
	Key string `json:"key"`
	// Self is the REST URL of this issue, and the only place in the payload
	// that says which Jira instance sent it.
	Self   string `json:"self"`
	Fields struct {
		Summary string `json:"summary"`
		// Description is wiki markup on v2 and an ADF document on v3.
		Description json.RawMessage `json:"description"`
		Status      *named          `json:"status"`
		IssueType   *named          `json:"issuetype"`
		Priority    *named          `json:"priority"`
		Project     *struct {
			Key  string `json:"key"`
			Name string `json:"name"`
		} `json:"project"`
		Assignee *jiraUser `json:"assignee"`
		Reporter *jiraUser `json:"reporter"`
		Labels   []string  `json:"labels"`
		// Attachment is the issue's files. Jira sends it on issue events,
		// not always on comment events.
		Attachment []jiraAttachment `json:"attachment"`
	} `json:"fields"`
}

type named struct {
	Name string `json:"name"`
}

func (n *named) name() string {
	if n == nil {
		return ""
	}
	return n.Name
}

type comment struct {
	ID     string          `json:"id"`
	Body   json.RawMessage `json:"body"`
	Author *jiraUser       `json:"author"`
	// Created and Updated are Jira's own format: "2026-07-20T12:30:00.000+0000".
	Created string `json:"created"`
	Updated string `json:"updated"`
}

type changelog struct {
	ID    string       `json:"id"`
	Items []changeItem `json:"items"`
}

type changeItem struct {
	Field string `json:"field"`
	// From and To are ids; FromString and ToString are display values.
	From       string `json:"from"`
	FromString string `json:"fromString"`
	To         string `json:"to"`
	ToString   string `json:"toString"`
}

// jiraTimeLayout is the format Jira stamps on comments. It is ISO 8601 with no
// colon in the offset, which is exactly the one thing time.RFC3339 will not
// parse.
const jiraTimeLayout = "2006-01-02T15:04:05.999-0700"

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{jiraTimeLayout, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// eventFrom translates one webhook delivery into a jira.*.v1 event, or
// reports that it publishes nothing.
//
// Three deliveries publish, and the third is the interesting one:
//
//   - A comment naming an agent (jira.comment.created.v1). This is the ordinary
//     case, and it arrives under two different webhookEvent values depending on
//     how the webhook was registered, so both are read.
//   - An issue created with a mention in its summary or description
//     (jira.issue.created.v1).
//   - An issue ASSIGNED to an agent (jira.issue.assigned.v1). Nobody wrote a
//     message; the assignment is the request, and the issue itself is what the
//     agent reads. It is the structural gesture Jira has, the same shape as
//     requesting a review from a bot on GitHub, and it is the natural way a
//     human hands work to an agent without typing anything.
//
// The event names the agents and chooses none of them: the members of the
// issue's channel decide who answers. Everything else is acknowledged and
// dropped, because an agent run per issue update would be a denial of service
// with a bill attached. An event whose actor is an app is always dropped: this
// connector's own comment arrives back as a comment_created, and acting on it
// would have the mesh answer itself forever.
func (c *Connector) eventFrom(header http.Header, h hook) (event.Envelope, bool) {
	// Loop prevention first and unconditionally.
	actor := h.User
	if h.Comment != nil && h.Comment.Author != nil {
		actor = h.Comment.Author
	}
	if actor.bot() {
		return event.Envelope{}, false
	}
	if h.Issue == nil || h.Issue.Key == "" || !ValidKey(h.Issue.Key) {
		return event.Envelope{}, false
	}
	if _, isBot := c.bots[strings.ToLower(actor.id())]; isBot && actor.id() != "" {
		return event.Envelope{}, false
	}

	site, ok := c.siteOf(h.Issue.Self)
	if !ok {
		// A delivery from an instance this connector is not configured for. It is
		// authenticated (the shared secret passed) but there is no site key to
		// address it by, so there is nothing to publish.
		return event.Envelope{}, false
	}

	req := event.Request{Actor: event.Actor{ID: actor.id(), Display: actor.display(), Bot: actor.bot()}}
	ref := c.refOf(site, h)
	var body event.Body
	switch {
	case h.Comment != nil && (h.WebhookEvent == EventCommentCreated || h.IssueEventType == "issue_commented"):
		var text string
		req.Mentions, text = mention.FindAll(c.text(h.Comment.Body), c.agents())
		if len(req.Mentions) == 0 {
			return event.Envelope{}, false
		}
		req.Text = strings.TrimSpace(text)
		body = event.JiraCommentCreated{Request: req, JiraRef: ref, CommentID: h.Comment.ID}

	case h.WebhookEvent == EventIssueCreated:
		var text string
		req.Mentions, text = mention.FindAll(c.issueText(h.Issue), c.agents())
		if len(req.Mentions) == 0 {
			return event.Envelope{}, false
		}
		req.Text = strings.TrimSpace(text)
		body = event.JiraIssueCreated{Request: req, JiraRef: ref}

	case h.WebhookEvent == EventIssueUpdated:
		// The one path with no message at all: the ticket was handed to the
		// agent, so the ticket is the request.
		assignee, changed := assigneeChange(h.Changelog)
		if !changed {
			return event.Envelope{}, false
		}
		agent, ok := c.agents().Lookup(assignee)
		if !ok {
			return event.Envelope{}, false
		}
		req.Text = strings.TrimSpace(mention.Strip(c.issueText(h.Issue), agent))
		body = event.JiraIssueAssigned{Request: req, JiraRef: ref, Agent: agent}

	default:
		return event.Envelope{}, false
	}

	commentID := ""
	if b, ok := body.(event.JiraCommentCreated); ok {
		commentID = b.CommentID
	}
	at := address.Address{Connector: Name, Workspace: site, Path: []string{h.Issue.Key}}.String()
	return event.Envelope{
		ID:      c.deliveryID(header, h, commentID),
		Source:  at,
		Type:    body.Type(),
		Subject: h.Issue.Key,
		Channel: at,
		Time:    c.observedAt(h),
		Tenant:  c.cfg.Tenant,
		Data:    body,
	}, true
}

// deliveryID is the event's id, and so part of its run key.
//
// Atlassian's own delivery identifier when it sent one, which is the case that
// makes a redelivery join the run it already started. Without it, a key composed
// from what the change itself is — the event, the issue, the comment or
// changelog id — which is stable for the same underlying change even though it
// is not stable for a retry Jira decided to send under a new identity. The
// composed form is the fallback because it is derived from the change, never
// from the clock or from a counter here: a key containing "now" would make every
// redelivery a new run, which is the failure this field exists to prevent.
func (c *Connector) deliveryID(header http.Header, h hook, commentID string) string {
	if values := header.Values(HeaderIdentifier); len(values) == 1 && values[0] != "" {
		return values[0]
	}
	parts := []string{h.WebhookEvent, h.Issue.Key}
	switch {
	case commentID != "":
		parts = append(parts, "comment", commentID)
	case h.Changelog != nil && h.Changelog.ID != "":
		parts = append(parts, "changelog", h.Changelog.ID)
	default:
		parts = append(parts, "at", strconv.FormatInt(h.Timestamp, 10))
	}
	return strings.Join(parts, ":")
}

// issueText is what an agent reads when the request was not a message: the
// summary, then the description.
func (c *Connector) issueText(issue *jiraIssue) string {
	summary := normalize(issue.Fields.Summary, c.bots)
	description := c.text(issue.Fields.Description)
	if description == "" {
		return summary
	}
	return summary + "\n\n" + description
}

// text decodes a Jira body field, resolving mentions against the configured
// bots.
func (c *Connector) text(raw json.RawMessage) string { return text(raw, c.bots) }

// observedAt is when the human acted, which is not the same as when Jira sent
// the webhook.
func (c *Connector) observedAt(h hook) time.Time {
	if h.Comment != nil {
		if at := parseTime(h.Comment.Created); !at.IsZero() {
			return at
		}
	}
	if h.Timestamp > 0 {
		return time.UnixMilli(h.Timestamp).UTC()
	}
	return time.Time{}
}

// assigneeChange reads an assignee transition out of a changelog and returns the
// account the issue was assigned TO.
func assigneeChange(log *changelog) (string, bool) {
	if log == nil {
		return "", false
	}
	for _, item := range log.Items {
		if !strings.EqualFold(item.Field, "assignee") {
			continue
		}
		if item.To == "" {
			// Unassigned. Nobody was handed anything.
			return "", false
		}
		return item.To, true
	}
	return "", false
}

// siteOf resolves which configured site a delivery came from, by matching the
// issue's own REST URL against the configured base URLs.
//
// The payload never carries the short key — it is our name for the instance, not
// Jira's — so the host is the only evidence available. A deployment serving one
// site does not need the match to succeed, since there is only one answer; a
// deployment serving several does, and a delivery from an unconfigured host
// publishes nothing rather than being attributed to whichever site happened to be
// first.
func (c *Connector) siteOf(self string) (string, bool) {
	if host := hostOf(self); host != "" {
		if key, ok := c.hosts[host]; ok {
			return key, true
		}
	}
	if len(c.cfg.Sites) == 1 {
		for key := range c.cfg.Sites {
			return key, true
		}
	}
	return "", false
}

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Host)
}

// refOf is the ticket an event is about, as its body carries it. Without it
// the agent would have to ask Jira what it is looking at before it could do
// anything, on every run.
func (c *Connector) refOf(site string, h hook) event.JiraRef {
	ref := event.JiraRef{
		Site:         site,
		Key:          h.Issue.Key,
		WebhookEvent: h.WebhookEvent,
		IssueEvent:   h.IssueEventType,
		ID:           h.Issue.ID,
		Summary:      h.Issue.Fields.Summary,
		Status:       h.Issue.Fields.Status.name(),
		IssueType:    h.Issue.Fields.IssueType.name(),
		Priority:     h.Issue.Fields.Priority.name(),
		Assignee:     h.Issue.Fields.Assignee.display(),
		Reporter:     h.Issue.Fields.Reporter.display(),
		Labels:       h.Issue.Fields.Labels,
	}
	if p := h.Issue.Fields.Project; p != nil {
		ref.Project, ref.ProjectName = p.Key, p.Name
	}
	if s, ok := c.cfg.Sites[site]; ok {
		ref.URL = s.BrowseURL(h.Issue.Key)
	}
	return ref
}

// ValidKey reports whether s is shaped like a Jira issue key: a project key of
// letters, digits and underscores starting with a letter, a hyphen, and a
// number.
//
// It is checked at the boundary because the key becomes an address path segment
// and a REST path segment. A payload that does not carry a key-shaped key is not
// a payload this connector understands, and refusing it here is cheaper than
// discovering it in a 404 from Jira later.
func ValidKey(s string) bool {
	dash := strings.LastIndexByte(s, '-')
	if dash <= 0 || dash == len(s)-1 {
		return false
	}
	project, number := s[:dash], s[dash+1:]
	for i, r := range project {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z':
		case i > 0 && (r >= '0' && r <= '9' || r == '_'):
		default:
			return false
		}
	}
	for _, r := range number {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
