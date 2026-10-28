package jira_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urmzd/fiber/pkg/attachment/intake"
	"github.com/urmzd/fiber/pkg/blob"
	"github.com/urmzd/fiber/pkg/connectors"
	"github.com/urmzd/fiber/pkg/connectors/jira"
)

// issueCreatedWithFiles is a jira:issue_created delivery whose issue carries
// two attachments, as fields.attachment lists them.
const issueCreatedWithFiles = `{
  "timestamp": 1784603000000,
  "webhookEvent": "jira:issue_created",
  "issue_event_type_name": "issue_created",
  "user": {"accountId": "5b10a2844c20165700ede21g", "displayName": "Urmzd", "accountType": "atlassian"},
  "issue": {
    "id": "10011",
    "self": "https://acme.atlassian.net/rest/api/2/issue/10011",
    "key": "PROJ-11",
    "fields": {
      "summary": "@docs-bot please triage this",
      "description": "Screenshot and log attached.",
      "attachment": [
        {"self": "https://acme.atlassian.net/rest/api/3/attachment/10500", "id": "10500", "filename": "dashboard.png",
         "author": {"accountId": "5b10a2844c20165700ede21g"}, "created": "2026-07-20T12:29:00.000+0000", "size": 33,
         "mimeType": "image/png", "content": "https://acme.atlassian.net/rest/api/3/attachment/content/10500"},
        {"self": "https://acme.atlassian.net/rest/api/3/attachment/10501", "id": "10501", "filename": "sink.log",
         "size": 15, "mimeType": "text/plain", "content": "https://acme.atlassian.net/rest/api/3/attachment/content/10501"},
        {"id": "10502", "filename": "dump.bin", "size": 900000000, "mimeType": "application/octet-stream",
         "content": "https://acme.atlassian.net/rest/api/3/attachment/content/10502"}
      ]
    }
  }
}`

func loopback(a netip.Addr) error {
	if a.Unmap() == netip.MustParseAddr("127.0.0.1") {
		return nil
	}
	return intake.PublicOnly(a)
}

// An issue created with attachments brings them in, in order, under the
// connector's tenant, fetched from the site's content endpoint. A file over
// the cap is carried as unread, and the tag runs.
func TestAnIssueCreatedWithAttachmentsBringsThemIn(t *testing.T) {
	store := blob.NewMemory()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		switch r.URL.Path {
		case "/rest/api/3/attachment/content/10500":
			_, _ = io.WriteString(w, "\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00")
		case "/rest/api/3/attachment/content/10501":
			_, _ = io.WriteString(w, "posted twice\n")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	cfg := botConfig()
	cfg.Tenant = "acme"
	cfg.Files = connectors.Files{Store: store, Caps: intake.Caps{MaxFileBytes: 1 << 20}}
	c, tags := start(t, cfg)
	jira.ServeFilesFrom(c, intake.NewFetcher(store, intake.Hosts("127.0.0.1"), intake.WithPlainHTTP(), intake.WithNetworkPolicy(loopback)),
		func(raw string) string { u, _ := url.Parse(raw); return srv.URL + u.Path })

	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "created-files-1", issueCreatedWithFiles))
	tag := receive(t, tags)
	if tag.Tenant != "acme" || len(tag.Attachments) != 3 {
		t.Fatalf("event tenant %q, attachments %+v", tag.Tenant, tag.Attachments)
	}
	if a := tag.Attachments[0]; a.Name != "dashboard.png" || a.MediaType != "image/png" || a.Origin.String() != "jira://acme/PROJ-11" {
		t.Errorf("first = %+v", a)
	}
	if a := tag.Attachments[1]; a.Name != "sink.log" || a.MediaType != "text/plain" {
		t.Errorf("second = %+v", a)
	}
	if a := tag.Attachments[2]; a.Name != "dump.bin" || !strings.Contains(a.Unread, "over the 1.0 MB limit") {
		t.Errorf("oversized = %+v", a)
	}
	if hits.Load() != 2 {
		t.Errorf("%d downloads, want 2", hits.Load())
	}

	// A comment does not bring the issue's files again.
	comment := strings.Replace(commentCreated, `"labels": ["connectors", "bug"]`,
		`"labels": ["connectors", "bug"], "attachment": [{"id": "10500", "filename": "dashboard.png", "size": 33}]`, 1)
	c.ServeHTTP(httptest.NewRecorder(), deliver(t, "comment-files-1", comment))
	if tag := receive(t, tags); tag.Attachments != nil {
		t.Errorf("a comment carried the issue's files: %+v", tag.Attachments)
	}
}

// A site's credentials go to that site, over HTTPS, and nowhere else: not the
// media host a download redirects to, not another site. Only the content
// endpoint and the media host are fetched from.
func TestJiraCredentialsAndDownloadsStayOnTheirSite(t *testing.T) {
	c := build(t, jira.Config{Sites: map[string]jira.Site{
		"acme":  {BaseURL: "https://acme.atlassian.net", Email: "ops@acme.example", Token: "acme-token"},
		"other": {BaseURL: "https://jira.other.example/ctx", Email: "x@other.example", Token: "other-token"},
	}})
	auth := func(raw string) string {
		u, _ := url.Parse(raw)
		return jira.FileHeaders(c, u).Get("Authorization")
	}
	if got := auth("https://acme.atlassian.net/rest/api/3/attachment/content/1"); !strings.HasPrefix(got, "Basic ") {
		t.Errorf("acme content endpoint got %q", got)
	}
	for _, raw := range []string{
		"http://acme.atlassian.net/rest/api/3/attachment/content/1",
		"https://api.media.atlassian.com/file/abc/binary?token=x",
		"https://acme.atlassian.net.evil.example/rest/api/3/attachment/content/1",
	} {
		if got := auth(raw); got != "" {
			t.Errorf("%s was sent %q", raw, got)
		}
	}
	if a, b := auth("https://acme.atlassian.net/x"), auth("https://jira.other.example/ctx/x"); a == b {
		t.Error("two sites were sent the same credentials")
	}
	for raw, want := range map[string]bool{
		"https://acme.atlassian.net/rest/api/3/attachment/content/10500": true,
		"https://jira.other.example/ctx/rest/api/3/attachment/content/7": true,
		"https://api.media.atlassian.com/file/abc/binary?token=x":        true,
		"https://acme.atlassian.net/rest/api/3/myself":                   false,
		"https://jira.other.example/rest/api/3/attachment/content/7":     false,
		"https://media.atlassian.com.evil.example/file/abc":              false,
		"https://evil.example/rest/api/3/attachment/content/10500":       false,
	} {
		u, _ := url.Parse(raw)
		if got := jira.FileAllowed(c, u) == nil; got != want {
			t.Errorf("%s: allowed = %v, want %v", raw, got, want)
		}
	}

	// A content endpoint that redirects anywhere but the media host is
	// refused before anything is sent there.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://evil.example/stolen", http.StatusSeeOther)
	}))
	t.Cleanup(srv.Close)
	f := intake.NewFetcher(blob.NewMemory(), func(u *url.URL) error {
		if u.Hostname() == "127.0.0.1" {
			return nil
		}
		return jira.FileAllowed(c, u)
	}, intake.WithPlainHTTP(), intake.WithNetworkPolicy(loopback), intake.WithTimeout(5*time.Second))
	if _, err := f.Fetch(context.Background(), "acme", srv.URL+"/rest/api/3/attachment/content/1", intake.Meta{}); !errors.Is(err, intake.ErrNotAllowed) {
		t.Fatalf("redirect off the media host = %v, want ErrNotAllowed", err)
	}
}

// Attachments a change added are what an update brings, by the changelog's
// ids, in its order.
func TestAnUpdateBringsTheAttachmentsItsChangelogAdded(t *testing.T) {
	store := blob.NewMemory()
	var asked []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = append(asked, r.URL.Path)
		_, _ = io.WriteString(w, "content of "+r.URL.Path)
	}))
	t.Cleanup(srv.Close)
	cfg := botConfig()
	cfg.Tenant = "acme"
	cfg.Files = connectors.Files{Store: store}
	c, tags := start(t, cfg)
	jira.ServeFilesFrom(c, intake.NewFetcher(store, intake.Hosts("127.0.0.1"), intake.WithPlainHTTP(), intake.WithNetworkPolicy(loopback)),
		func(raw string) string { u, _ := url.Parse(raw); return srv.URL + u.Path })

	update := strings.Replace(assigned, `"items": [`, `"items": [
	  {"field": "Attachment", "fieldtype": "jira", "from": null, "fromString": null, "to": "10777", "toString": "new-trace.txt"},`, 1)
	c.ServeHTTP(httptest.NewRecorder(), deliver(t, "assign-files-1", update))
	tag := receive(t, tags)
	if len(tag.Attachments) != 1 || tag.Attachments[0].Name != "new-trace.txt" {
		t.Fatalf("attachments = %+v", tag.Attachments)
	}
	if len(asked) != 1 || asked[0] != "/rest/api/3/attachment/content/10777" {
		t.Errorf("asked for %v", asked)
	}
}
