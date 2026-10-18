package jira

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/urmzd/fiber/pkg/attachment"
	"github.com/urmzd/fiber/pkg/attachment/intake"
)

// MediaHost is where Jira Cloud serves attachment content from: the content
// endpoint answers 303 with a signed URL on this host. It was observed on a
// live Jira Cloud site; Atlassian's API reference says only "see the Location
// header". It gets no credential: the signature is in the URL.
const MediaHost = "api.media.atlassian.com"

// contentPath is where a site serves an attachment's bytes, under its base
// path.
const contentPath = "/rest/api/3/attachment/content/"

// jiraAttachment is one entry of issue.fields.attachment.
type jiraAttachment struct {
	ID       string `json:"id"`
	Filename string `json:"filename"`
	MimeType string `json:"mimeType"`
	Size     int64  `json:"size"`
}

// fileAllowed is the host policy for attachment downloads: each configured
// site's content endpoint and nothing else on it, then the media host the
// endpoint redirects to. Everything is HTTPS (intake refuses anything else).
func (c *Connector) fileAllowed(u *url.URL) error {
	host := strings.ToLower(u.Host)
	if host == MediaHost {
		return nil
	}
	if key, ok := c.hosts[host]; ok {
		if strings.HasPrefix(u.EscapedPath(), basePath(c.cfg.Sites[key].BaseURL)+contentPath) {
			return nil
		}
		return fmt.Errorf("%w: only the attachment content endpoint is fetched from %s", intake.ErrNotAllowed, host)
	}
	return fmt.Errorf("%w: host %q is not a configured Jira site or the Atlassian media host", intake.ErrNotAllowed, host)
}

// fileHeaders sends a site's credentials to that site's host, over HTTPS,
// and to nothing else: not to another configured site, and not to the media
// host a download redirects to.
func (c *Connector) fileHeaders(u *url.URL) http.Header {
	key, ok := c.hosts[strings.ToLower(u.Host)]
	if !ok || u.Scheme != "https" {
		return nil
	}
	site := c.cfg.Sites[key]
	if site.Email == "" || site.Token == "" {
		return nil
	}
	r := &http.Request{Header: http.Header{}}
	r.SetBasicAuth(site.Email, site.Token)
	return r.Header
}

func basePath(base string) string {
	u, err := url.Parse(base)
	if err != nil {
		return ""
	}
	return strings.TrimRight(u.EscapedPath(), "/")
}

// filesOf picks the attachments a delivery brings in. Attachments a change
// added are always taken. Otherwise a new issue, or an issue just assigned to
// an agent, brings the issue's attachments: that is the work the agent was
// handed. A comment brings none of the issue's, because every comment would
// otherwise carry every file again.
func filesOf(h hook, assigned bool) []jiraAttachment {
	if h.Issue == nil {
		return nil
	}
	all := h.Issue.Fields.Attachment
	if added := addedAttachments(h.Changelog); len(added) > 0 {
		byID := make(map[string]jiraAttachment, len(all))
		for _, a := range all {
			byID[a.ID] = a
		}
		out := make([]jiraAttachment, 0, len(added))
		for _, item := range added {
			a, ok := byID[item.To]
			if !ok {
				a = jiraAttachment{ID: item.To, Filename: item.ToString}
			}
			out = append(out, a)
		}
		return out
	}
	if h.WebhookEvent == EventIssueCreated || assigned {
		return all
	}
	return nil
}

// addedAttachments is the changelog items that added an attachment. The
// field id is not relied on: Atlassian's webhook example omits it.
func addedAttachments(cl *changelog) []changeItem {
	if cl == nil {
		return nil
	}
	var out []changeItem
	for _, item := range cl.Items {
		if strings.EqualFold(item.Field, "attachment") && item.To != "" {
			out = append(out, item)
		}
	}
	return out
}

// candidates turns a delivery's attachments into intake candidates.
func (c *Connector) candidates(site string, files []jiraAttachment) []intake.Candidate {
	if c.fetcher == nil || len(files) == 0 {
		return nil
	}
	base := strings.TrimRight(c.cfg.Sites[site].BaseURL, "/")
	out := make([]intake.Candidate, 0, len(files))
	for _, f := range files {
		if _, err := strconv.ParseUint(f.ID, 10, 64); err != nil {
			out = append(out, intake.Candidate{Name: f.Filename})
			continue
		}
		link := base + contentPath + f.ID
		out = append(out, intake.Candidate{Name: f.Filename, Size: f.Size, Fetch: func(ctx context.Context, tenant string, meta intake.Meta) (attachment.Attachment, error) {
			if c.redirectForTest != nil {
				link = c.redirectForTest(link)
			}
			return c.fetcher.Fetch(ctx, tenant, link, meta)
		}})
	}
	return out
}
