package github_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/agentrt"
	"github.com/urmzd/mandatum/pkg/connector"
	"github.com/urmzd/mandatum/pkg/connectors/github"
	"github.com/urmzd/mandatum/pkg/connectors/mention"
	"github.com/urmzd/mandatum/pkg/envelope"
)

const fixArgs = `{"message":"fix: correct value","files":[{"path":"main.go","content":"package main\n"}]}`

func fixRequest(t *testing.T, canWrite bool, command string) (*github.Connector, *github.Fake, envelope.Tag) {
	t.Helper()
	fake := github.NewFake()
	ref := github.Ref{Owner: "acme", Repo: "repo", Number: 42}
	evidence := github.Context{Subject: github.ContextSubject{Number: 42, State: "open"}}
	evidence.Subject.Head.SHA = commitSHA
	if err := fake.SeedContext(ref, evidence); err != nil {
		t.Fatal(err)
	}
	if err := fake.SeedFiles(ref, map[string]string{"main.go": "old"}); err != nil {
		t.Fatal(err)
	}
	fake.SetWritePermission(ref, "human", canWrite)
	c, tags := start(t, github.Config{API: fake, Mutations: true, MutationSecret: []byte("a stable signing secret for tests"), Agents: mention.NewNames("reviewer")})
	raw, _ := json.Marshal(map[string]any{"action": "created", "repository": map[string]any{"name": "repo", "owner": map[string]string{"login": "acme"}}, "sender": map[string]string{"login": "human", "type": "User"}, "issue": map[string]any{"number": 42, "pull_request": map[string]string{"url": "https://api.github.com/repos/acme/repo/pulls/42"}}, "comment": map[string]any{"id": 91, "body": "@reviewer " + command}})
	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "issue_comment", "fix-request-1", string(raw)))
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	select {
	case tag := <-tags:
		return c, fake, tag
	default:
		t.Fatal("missing tag")
		return nil, nil, envelope.Tag{}
	}
}

func mutation(t *testing.T, c *github.Connector) connector.Action {
	t.Helper()
	for _, a := range c.Actions() {
		if a.Name == "github_apply_changes" {
			return a
		}
	}
	t.Fatal("missing mutation action")
	return connector.Action{}
}

func TestExplicitFixCreatesOneAtomicCommitAndRecoversRetries(t *testing.T) {
	c, fake, tag := fixRequest(t, true, "/fix correct the value")
	reg := connector.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatal(err)
	}
	// The same binding is used by the durable runtime. A model cannot provide it.
	tools := agentrt.ConnectorToolsForRun(reg, tag.Source, tag.ID, nil)
	tool, err := tools.Lookup("github_apply_changes")
	if err != nil {
		t.Fatal(err)
	}
	var args map[string]any
	_ = json.Unmarshal([]byte(fixArgs), &args)
	for range 2 {
		result, err := tool.Execute(context.Background(), args)
		if err != nil || !strings.Contains(result, "Created commit") {
			t.Fatalf("result=%s err=%v", result, err)
		}
	}
	commits := fake.Commits()
	if len(commits) != 1 || commits[0].Parent != commitSHA {
		t.Fatalf("commits %+v", commits)
	}
	file, err := fake.ReadFile(context.Background(), commits[0].Ref, commits[0].Result.SHA, "main.go")
	if err != nil || file.Content != "package main\n" {
		t.Fatalf("file %+v err=%v", file, err)
	}
	original, _ := fake.ReadFile(context.Background(), commits[0].Ref, commitSHA, "main.go")
	if original.Content != "old" {
		t.Fatal("old snapshot mutated")
	}
}

func TestMutationGrantCannotMoveBetweenRunsActorsRepositoriesOrHeads(t *testing.T) {
	for _, tc := range []struct {
		name  string
		alter func(address.Address) address.Address
		run   string
	}{
		{"different run", func(a address.Address) address.Address { return a }, "another-run"},
		{"no run", func(a address.Address) address.Address { return a }, ""},
		{"forged grant", func(a address.Address) address.Address { return a.WithParam("write_grant", strings.Repeat("00", 32)) }, "fix-request-1"},
		{"actor", func(a address.Address) address.Address { return a.WithParam("write_actor", "admin") }, "fix-request-1"},
		{"head", func(a address.Address) address.Address { return a.WithParam("head_sha", strings.Repeat("b", 40)) }, "fix-request-1"},
		{"repository", func(a address.Address) address.Address {
			a.Path = append([]string(nil), a.Path...)
			a.Path[0] = "other"
			return a
		}, "fix-request-1"},
		{"operation", func(a address.Address) address.Address { return a.WithParam("write_operation", "run") }, "fix-request-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, fake, tag := fixRequest(t, true, "/fix correct the value")
			_, err := mutation(t, c).Invoke(connector.WithRunID(context.Background(), tc.run), tc.alter(tag.Source), json.RawMessage(fixArgs))
			if err == nil || len(fake.Commits()) != 0 {
				t.Fatalf("escaped grant: %v", err)
			}
		})
	}
}

func TestReadOnlyMentionsAndRevokedCollaboratorsCannotCommit(t *testing.T) {
	for _, tc := range []struct {
		name, command string
		write, revoke bool
	}{
		{"read-only user", "/fix correct the value", false, false},
		{"ordinary question", "explain the value", true, false},
		{"permission revoked", "/fix correct the value", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, fake, tag := fixRequest(t, tc.write, tc.command)
			if tc.revoke {
				fake.SetWritePermission(github.Ref{Owner: "acme", Repo: "repo", Number: 42}, "human", false)
			}
			_, err := mutation(t, c).Invoke(connector.WithRunID(context.Background(), tag.ID), tag.Source, json.RawMessage(fixArgs))
			if err == nil || len(fake.Commits()) != 0 {
				t.Fatalf("write allowed: %v", err)
			}
		})
	}
}

func TestCommitValidationAndAtomicityPreserveTheOriginalFiles(t *testing.T) {
	c, fake, tag := fixRequest(t, true, "/fix correct the value")
	for _, raw := range []string{
		`{"message":"fix","files":[{"path":"../escape","content":"bad"}]}`,
		`{"message":"fix","files":[{"path":"main.go"}]}`,
		`{"message":"fix","files":[{"path":".git/config","content":"bad"}]}`,
		`{"message":"fix","branch":"other","files":[{"path":"main.go","content":"bad"}]}`,
		`{"message":"fix","files":[{"path":"main.go","content":"bad"},{"path":"missing","delete":true}]}`,
	} {
		if _, err := mutation(t, c).Invoke(connector.WithRunID(context.Background(), tag.ID), tag.Source, json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	if len(fake.Commits()) != 0 {
		t.Fatal("partial commit")
	}
	file, _ := fake.ReadFile(context.Background(), github.Ref{Owner: "acme", Repo: "repo", Number: 42}, commitSHA, "main.go")
	if file.Content != "old" {
		t.Fatal("partial file edit")
	}
}

func TestGraphQLCommitPinsTheBranchAndRecoversAnUncertainResponse(t *testing.T) {
	current := commitSHA
	newSHA := strings.Repeat("b", 40)
	message := ""
	creates := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/pulls/42"):
			fmt.Fprintf(w, `{"state":"open","head":{"sha":%q,"ref":"feature","repo":{"full_name":"acme/repo"}}}`, current)
		case strings.Contains(r.URL.Path, "/commits/"):
			_ = json.NewEncoder(w).Encode(map[string]any{"sha": newSHA, "html_url": "https://github.com/acme/repo/commit/" + newSHA, "commit": map[string]string{"message": message}, "parents": []map[string]string{{"sha": commitSHA}}})
		case r.URL.Path == "/graphql":
			creates++
			var payload struct {
				Variables struct {
					Input struct {
						Expected string            `json:"expectedHeadOid"`
						Branch   map[string]string `json:"branch"`
						Message  map[string]string `json:"message"`
						Files    struct {
							Additions []map[string]string `json:"additions"`
						} `json:"fileChanges"`
					} `json:"input"`
				} `json:"variables"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			input := payload.Variables.Input
			if input.Expected != commitSHA || input.Branch["branchName"] != "feature" || input.Branch["repositoryNameWithOwner"] != "acme/repo" {
				t.Errorf("wrong commit input %+v", input)
			}
			decoded, _ := base64.StdEncoding.DecodeString(input.Files.Additions[0]["contents"])
			if string(decoded) != "new" {
				t.Error("content changed")
			}
			message = input.Message["headline"] + "\n\n" + input.Message["body"]
			current = newSHA
			// Simulate a successful server commit whose response is unavailable.
			http.Error(w, "response lost after commit", http.StatusBadGateway)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	api := github.NewHTTPAPIAt(server.URL, "token", server.Client())
	ref := github.Ref{Owner: "acme", Repo: "repo", Number: 42}
	args := github.ChangeRequest{Message: "fix: value", Files: []github.FileEdit{{Path: "main.go", Content: "new"}}}
	if _, err := api.CommitChanges(context.Background(), ref, commitSHA, "run", args); err == nil {
		t.Fatal("missing simulated failure")
	}
	result, err := api.CommitChanges(context.Background(), ref, commitSHA, "run", args)
	if err != nil || !result.Recovered || result.SHA != newSHA || creates != 1 {
		t.Fatalf("result %+v creates=%d err=%v", result, creates, err)
	}
	args.Files[0].Content = "different"
	if _, err := api.CommitChanges(context.Background(), ref, commitSHA, "run", args); err == nil || creates != 1 {
		t.Fatal("overwrote advanced branch")
	}
}

func TestCommitRefusesForksClosedPRsAndAtomicHeadConflicts(t *testing.T) {
	for _, tc := range []struct {
		name, state, repo string
		conflict          bool
	}{
		{"fork", "open", "someone/fork", false},
		{"closed", "closed", "acme/repo", false},
		{"concurrent head change", "open", "acme/repo", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			posts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					posts++
					fmt.Fprint(w, `{"errors":[{"message":"expectedHeadOid does not match"}]}`)
					return
				}
				fmt.Fprintf(w, `{"state":%q,"head":{"sha":%q,"ref":"feature","repo":{"full_name":%q}}}`, tc.state, commitSHA, tc.repo)
			}))
			defer server.Close()
			api := github.NewHTTPAPIAt(server.URL, "token", server.Client())
			_, err := api.CommitChanges(context.Background(), github.Ref{Owner: "acme", Repo: "repo", Number: 42}, commitSHA, "run", github.ChangeRequest{Message: "fix", Files: []github.FileEdit{{Path: "main.go", Content: "new"}}})
			if err == nil {
				t.Fatal("unexpected success")
			}
			if !tc.conflict && posts != 0 {
				t.Fatal("wrote outside open source PR")
			}
			if tc.conflict && posts != 1 {
				t.Fatal("atomic conflict not exercised")
			}
		})
	}
}
