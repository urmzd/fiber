package jira

import (
	"net/http"
	"net/url"

	"github.com/urmzd/fiber/pkg/attachment/intake"
)

// ServeFilesFrom points the connector's downloads at a local server: each
// content URL is rewritten by to, and fetched with f.
func ServeFilesFrom(c *Connector, f *intake.Fetcher, to func(string) string) {
	c.fetcher, c.redirectForTest = f, to
}

// FileAllowed and FileHeaders are the connector's host and credential
// policies for downloads.
func FileAllowed(c *Connector, u *url.URL) error       { return c.fileAllowed(u) }
func FileHeaders(c *Connector, u *url.URL) http.Header { return c.fileHeaders(u) }
