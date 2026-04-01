// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package maintner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bradfitz/gxbm/maintner/maintpb"
	"github.com/google/go-github/v74/github"
	"golang.org/x/oauth2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestParseGithubEvents(t *testing.T) {
	tests := []struct {
		name string                    // test
		j    string                    // JSON from Github API
		e    *GitHubIssueEvent         // in-memory
		p    *maintpb.GithubIssueEvent // on disk
	}{
		{
			name: "labeled",
			j: `{
    "id": 998144526,
    "url": "https://api.github.com/repos/bradfitz/go-issue-mirror/issues/events/998144526",
    "actor": {
      "login": "bradfitz",
      "id": 2621
    },
    "event": "labeled",
    "commit_id": null,
    "commit_url": null,
    "created_at": "2017-03-13T22:39:28Z",
    "label": {
      "name": "enhancement",
      "color": "84b6eb"
    }
  }
`,
			e: &GitHubIssueEvent{
				ID:      998144526,
				Type:    "labeled",
				Created: t3339("2017-03-13T22:39:28Z"),
				Actor: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
				Label: "enhancement",
			},
			p: &maintpb.GithubIssueEvent{
				Id:        998144526,
				EventType: "labeled",
				ActorId:   2621,
				Created:   p3339("2017-03-13T22:39:28Z"),
				Label:     &maintpb.GithubLabel{Name: "enhancement"},
			},
		},

		{
			name: "unlabeled",
			j: `{
    "id": 998144526,
    "url": "https://api.github.com/repos/bradfitz/go-issue-mirror/issues/events/998144526",
    "actor": {
      "login": "bradfitz",
      "id": 2621
    },
    "event": "unlabeled",
    "commit_id": null,
    "commit_url": null,
    "created_at": "2017-03-13T22:39:28Z",
    "label": {
      "name": "enhancement",
      "color": "84b6eb"
    }
  }
`,
			e: &GitHubIssueEvent{
				ID:      998144526,
				Type:    "unlabeled",
				Created: t3339("2017-03-13T22:39:28Z"),
				Actor: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
				Label: "enhancement",
			},
			p: &maintpb.GithubIssueEvent{
				Id:        998144526,
				EventType: "unlabeled",
				ActorId:   2621,
				Created:   p3339("2017-03-13T22:39:28Z"),
				Label:     &maintpb.GithubLabel{Name: "enhancement"},
			},
		},

		{
			name: "milestoned",
			j: `{
    "id": 998144529,
    "url": "https://api.github.com/repos/bradfitz/go-issue-mirror/issues/events/998144529",
    "actor": {
      "login": "bradfitz",
      "id": 2621
    },
    "event": "milestoned",
    "commit_id": null,
    "commit_url": null,
    "created_at": "2017-03-13T22:39:28Z",
    "milestone": {
      "title": "World Domination"
    }}`,
			e: &GitHubIssueEvent{
				ID:      998144529,
				Type:    "milestoned",
				Created: t3339("2017-03-13T22:39:28Z"),
				Actor: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
				Milestone: "World Domination",
			},
			p: &maintpb.GithubIssueEvent{
				Id:        998144529,
				EventType: "milestoned",
				ActorId:   2621,
				Created:   p3339("2017-03-13T22:39:28Z"),
				Milestone: &maintpb.GithubMilestone{Title: "World Domination"},
			},
		},

		{
			name: "demilestoned",
			j: `{
    "id": 998144529,
    "url": "https://api.github.com/repos/bradfitz/go-issue-mirror/issues/events/998144529",
    "actor": {
      "login": "bradfitz",
      "id": 2621
    },
    "event": "demilestoned",
    "commit_id": null,
    "commit_url": null,
    "created_at": "2017-03-13T22:39:28Z",
    "milestone": {
      "title": "World Domination"
    }}`,
			e: &GitHubIssueEvent{
				ID:      998144529,
				Type:    "demilestoned",
				Created: t3339("2017-03-13T22:39:28Z"),
				Actor: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
				Milestone: "World Domination",
			},
			p: &maintpb.GithubIssueEvent{
				Id:        998144529,
				EventType: "demilestoned",
				ActorId:   2621,
				Created:   p3339("2017-03-13T22:39:28Z"),
				Milestone: &maintpb.GithubMilestone{Title: "World Domination"},
			},
		},

		{
			name: "assigned",
			j: `{
    "id": 998144530,
    "url": "https://api.github.com/repos/bradfitz/go-issue-mirror/issues/events/998144530",
    "actor": {
      "login": "bradfitz",
      "id": 2621
    },
    "event": "assigned",
    "commit_id": null,
    "commit_url": null,
    "created_at": "2017-03-13T22:39:28Z",
    "assignee": {
      "login": "bradfitz",
      "id": 2621
    },
    "assigner": {
      "login": "bradfitz",
      "id": 2621
    }}`,
			e: &GitHubIssueEvent{
				ID:      998144530,
				Type:    "assigned",
				Created: t3339("2017-03-13T22:39:28Z"),
				Actor: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
				Assignee: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
				Assigner: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
			},
			p: &maintpb.GithubIssueEvent{
				Id:         998144530,
				EventType:  "assigned",
				ActorId:    2621,
				Created:    p3339("2017-03-13T22:39:28Z"),
				AssigneeId: 2621,
				AssignerId: 2621,
			},
		},

		{
			name: "unassigned",
			j: `{
    "id": 1000077586,
    "url": "https://api.github.com/repos/bradfitz/go-issue-mirror/issues/events/1000077586",
    "actor": {
      "login": "dmitshur",
      "id": 1924134
    },
    "event": "unassigned",
    "commit_id": null,
    "commit_url": null,
    "created_at": "2017-03-15T00:31:42Z",
    "assignee": {
      "login": "dmitshur",
      "id": 1924134
    },
    "assigner": {
      "login": "bradfitz",
      "id": 2621
    }
  }`,
			e: &GitHubIssueEvent{
				ID:      1000077586,
				Type:    "unassigned",
				Created: t3339("2017-03-15T00:31:42Z"),
				Actor: &GitHubUser{
					ID:    1924134,
					Login: "dmitshur",
				},
				Assignee: &GitHubUser{
					ID:    1924134,
					Login: "dmitshur",
				},
				Assigner: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
			},
			p: &maintpb.GithubIssueEvent{
				Id:         1000077586,
				EventType:  "unassigned",
				ActorId:    1924134,
				Created:    p3339("2017-03-15T00:31:42Z"),
				AssigneeId: 1924134,
				AssignerId: 2621,
			},
		},

		{
			name: "locked",
			j: `{
    "id": 998144646,
    "url": "https://api.github.com/repos/bradfitz/go-issue-mirror/issues/events/998144646",
    "actor": {
      "login": "bradfitz",
      "id": 2621
    },
    "event": "locked",
    "commit_id": null,
    "commit_url": null,
    "created_at": "2017-03-13T22:39:36Z",
    "lock_reason": "off-topic"
  }`,
			e: &GitHubIssueEvent{
				ID:      998144646,
				Type:    "locked",
				Created: t3339("2017-03-13T22:39:36Z"),
				Actor: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
			},
			p: &maintpb.GithubIssueEvent{
				Id:        998144646,
				EventType: "locked",
				ActorId:   2621,
				Created:   p3339("2017-03-13T22:39:36Z"),
			},
		},

		{
			name: "unlocked",
			j: `{
    "id": 1000014895,
    "url": "https://api.github.com/repos/bradfitz/go-issue-mirror/issues/events/1000014895",
    "actor": {
      "login": "bradfitz",
      "id": 2621
    },
    "event": "unlocked",
    "commit_id": null,
    "commit_url": null,
    "created_at": "2017-03-14T23:26:21Z"
 }`,
			e: &GitHubIssueEvent{
				ID:      1000014895,
				Type:    "unlocked",
				Created: t3339("2017-03-14T23:26:21Z"),
				Actor: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
			},
			p: &maintpb.GithubIssueEvent{
				Id:        1000014895,
				EventType: "unlocked",
				ActorId:   2621,
				Created:   p3339("2017-03-14T23:26:21Z"),
			},
		},

		{
			name: "closed",
			j: `  {
    "id": 1006040931,
    "url": "https://api.github.com/repos/bradfitz/go-issue-mirror/issues/events/1006040931",
    "actor": {
      "login": "bradfitz",
      "id": 2621
    },
    "event": "closed",
    "commit_id": "e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
    "commit_url": "https://api.github.com/repos/bradfitz/go-issue-mirror/commits/e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
    "created_at": "2017-03-19T23:40:33Z"
  }`,
			e: &GitHubIssueEvent{
				ID:      1006040931,
				Type:    "closed",
				Created: t3339("2017-03-19T23:40:33Z"),
				Actor: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
				CommitID:  "e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
				CommitURL: "https://api.github.com/repos/bradfitz/go-issue-mirror/commits/e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
			},
			p: &maintpb.GithubIssueEvent{
				Id:        1006040931,
				EventType: "closed",
				ActorId:   2621,
				Created:   p3339("2017-03-19T23:40:33Z"),
				Commit: &maintpb.GithubCommit{
					Owner:    "bradfitz",
					Repo:     "go-issue-mirror",
					CommitId: "e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
				},
			},
		},

		{
			name: "reopened",
			j: `{
    "id": 1000014895,
    "url": "https://api.github.com/repos/bradfitz/go-issue-mirror/issues/events/1000014895",
    "actor": {
      "login": "bradfitz",
      "id": 2621
    },
    "event": "reopened",
    "commit_id": null,
    "commit_url": null,
    "created_at": "2017-03-14T23:26:21Z"
 }`,
			e: &GitHubIssueEvent{
				ID:      1000014895,
				Type:    "reopened",
				Created: t3339("2017-03-14T23:26:21Z"),
				Actor: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
			},
			p: &maintpb.GithubIssueEvent{
				Id:        1000014895,
				EventType: "reopened",
				ActorId:   2621,
				Created:   p3339("2017-03-14T23:26:21Z"),
			},
		},

		{
			name: "referenced",
			j: `{
    "id": 1006040930,
    "url": "https://api.github.com/repos/bradfitz/go-issue-mirror/issues/events/1006040930",
    "actor": {
      "login": "bradfitz",
      "id": 2621
    },
    "event": "referenced",
    "commit_id": "e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
    "commit_url": "https://api.github.com/repos/bradfitz/go-issue-mirror/commits/e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
    "created_at": "2017-03-19T23:40:32Z"
  }`,
			e: &GitHubIssueEvent{
				ID:      1006040930,
				Type:    "referenced",
				Created: t3339("2017-03-19T23:40:32Z"),
				Actor: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
				CommitID:  "e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
				CommitURL: "https://api.github.com/repos/bradfitz/go-issue-mirror/commits/e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
			},
			p: &maintpb.GithubIssueEvent{
				Id:        1006040930,
				EventType: "referenced",
				ActorId:   2621,
				Created:   p3339("2017-03-19T23:40:32Z"),
				Commit: &maintpb.GithubCommit{
					Owner:    "bradfitz",
					Repo:     "go-issue-mirror",
					CommitId: "e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
				},
			},
		},

		{
			name: "renamed",
			j: `{
    "id": 1006107803,
    "url": "https://api.github.com/repos/bradfitz/go-issue-mirror/issues/events/1006107803",
    "actor": {
      "login": "bradfitz",
      "id": 2621
    },
    "event": "renamed",
    "commit_id": null,
    "commit_url": null,
    "created_at": "2017-03-20T02:53:43Z",
    "rename": {
      "from": "test-2",
      "to": "test-2 new name"
    }
  }`,
			e: &GitHubIssueEvent{
				ID:      1006107803,
				Type:    "renamed",
				Created: t3339("2017-03-20T02:53:43Z"),
				Actor: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
				From: "test-2",
				To:   "test-2 new name",
			},
			p: &maintpb.GithubIssueEvent{
				Id:         1006107803,
				EventType:  "renamed",
				ActorId:    2621,
				Created:    p3339("2017-03-20T02:53:43Z"),
				RenameFrom: "test-2",
				RenameTo:   "test-2 new name",
			},
		},

		{
			name: "Extra Unknown JSON",
			j: `{
    "id": 998144526,
    "url": "https://api.github.com/repos/bradfitz/go-issue-mirror/issues/events/998144526",
    "actor": {
      "login": "bradfitz",
      "id": 2621
    },
    "event": "labeled",
    "commit_id": null,
    "commit_url": null,
    "created_at": "2017-03-13T22:39:28Z",
    "label": {
      "name": "enhancement",
      "color": "84b6eb"
    },
    "random_new_field": "some new thing that GitHub API may add"
  }
`,
			e: &GitHubIssueEvent{
				ID:      998144526,
				Type:    "labeled",
				Created: t3339("2017-03-13T22:39:28Z"),
				Actor: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
				Label:     "enhancement",
				OtherJSON: `{"random_new_field":"some new thing that GitHub API may add"}`,
			},
			p: &maintpb.GithubIssueEvent{
				Id:        998144526,
				EventType: "labeled",
				ActorId:   2621,
				Created:   p3339("2017-03-13T22:39:28Z"),
				Label:     &maintpb.GithubLabel{Name: "enhancement"},
				OtherJson: []byte(`{"random_new_field":"some new thing that GitHub API may add"}`),
			},
		},
	}

	var eventTypes []string

	for _, tt := range tests {
		evts, err := parseGithubEvents(strings.NewReader("[" + tt.j + "]"))
		if err != nil {
			t.Errorf("%s: parse JSON: %v", tt.name, err)
			continue
		}
		if len(evts) != 1 {
			t.Errorf("%s: parse JSON = %v entries; want 1", tt.name, len(evts))
			continue
		}
		gote := evts[0]
		if !reflect.DeepEqual(gote, tt.e) {
			t.Errorf("%s: JSON -> githubEvent differs: %v", tt.name, DeepDiff(gote, tt.e))
			continue
		}
		eventTypes = append(eventTypes, gote.Type)

		gotp := gote.Proto()
		if !reflect.DeepEqual(gotp, tt.p) {
			t.Errorf("%s: githubEvent -> proto differs: %v", tt.name, DeepDiff(gotp, tt.p))
			continue
		}

		var c Corpus
		c.initGithub()
		c.github.getOrCreateUserID(2621).Login = "bradfitz"
		c.github.getOrCreateUserID(1924134).Login = "dmitshur"
		gr := c.github.getOrCreateRepo("foowner", "bar")
		e2 := gr.newGithubEvent(gotp)

		if !reflect.DeepEqual(e2, tt.e) {
			t.Errorf("%s: proto -> githubEvent differs: %v", tt.name, DeepDiff(e2, tt.e))
			continue
		}
	}

	t.Logf("Tested event types: %q", eventTypes)
}

func TestParseMultipleGithubEvents(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("testdata", "TestParseMultipleGithubEvents.json"))
	if err != nil {
		t.Errorf("error while loading testdata: %s\n", err.Error())
	}
	evts, err := parseGithubEvents(strings.NewReader(string(content)))
	if err != nil {
		t.Errorf("error was not expected: %s\n", err.Error())
	}
	if len(evts) != 7 {
		t.Errorf("there should have been three events. was: %d\n", len(evts))
	}
	lastEvent := evts[len(evts)-1]
	if lastEvent.Type != "closed" {
		t.Errorf("the last event's type should have been closed. was: %s\n", lastEvent.Type)
	}
}

func TestParseMultipleGithubEventsWithForeach(t *testing.T) {
	issue := &GitHubIssue{
		PullRequest: &GitHubPullRequest{},
		events: map[int64]*GitHubIssueEvent{
			0: &GitHubIssueEvent{
				Type: "labelled",
			},
			1: &GitHubIssueEvent{
				Type: "milestone",
			},
			2: &GitHubIssueEvent{
				Type: "closed",
			},
		},
	}
	eventTypes := []string{"closed", "labelled", "milestone"}
	gatheredTypes := make([]string, 0)
	issue.ForeachEvent(func(e *GitHubIssueEvent) error {
		gatheredTypes = append(gatheredTypes, e.Type)
		return nil
	})
	sort.Strings(gatheredTypes)
	if !reflect.DeepEqual(eventTypes, gatheredTypes) {
		t.Fatalf("want event types: %v; got: %v\n", eventTypes, gatheredTypes)
	}
}

type ClientMock struct {
	err        error
	status     string
	statusCode int
	testdata   string
}

var timesDoWasCalled = 0

func (c *ClientMock) Do(req *http.Request) (*http.Response, error) {
	if len(c.testdata) < 1 {
		c.testdata = "TestParseMultipleGithubEvents.json"
	}
	timesDoWasCalled++
	content, _ := os.ReadFile(filepath.Join("testdata", c.testdata))
	headers := make(http.Header, 0)
	t := time.Now()
	var b []byte
	headers["Date"] = []string{string(t.AppendFormat(b, "Mon Jan _2 15:04:05 2006"))}
	return &http.Response{
		Body:       io.NopCloser(bytes.NewReader(content)),
		Status:     c.status,
		StatusCode: c.statusCode,
		Header:     headers,
	}, c.err
}

type MockLogger struct {
}

var eventLog = make([]string, 0)

func (m *MockLogger) Log(mut *maintpb.Mutation) error {
	for _, e := range mut.GithubIssue.Event {
		eventLog = append(eventLog, e.EventType)
	}
	return nil
}

func TestSyncEvents(t *testing.T) {
	var c Corpus
	c.initGithub()
	c.mutationLogger = &MockLogger{}
	gr := c.github.getOrCreateRepo("foowner", "bar")
	issue := &GitHubIssue{
		ID:          1001,
		PullRequest: &GitHubPullRequest{},
		events:      map[int64]*GitHubIssueEvent{},
	}
	gr.issues = map[int32]*GitHubIssue{
		1001: issue,
	}
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte("OK"))
	}))
	defer server.Close()
	p := &githubRepoPoller{
		c:             &c,
		tokenSource:   oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "foobar"}),
		gr:            gr,
		githubDirect:  github.NewClient(server.Client()),
		githubCaching: github.NewClient(server.Client()),
	}
	t.Run("successful sync", func(t2 *testing.T) {
		defer func() { eventLog = make([]string, 0) }()
		timesDoWasCalled = 0
		ctx := context.Background()
		p.client = &ClientMock{
			status:     "OK",
			statusCode: http.StatusOK,
			err:        nil,
			testdata:   "TestParseMultipleGithubEvents.json",
		}
		err := p.syncEventsOnIssue(ctx, int32(issue.ID))
		if err != nil {
			t2.Error(err)
		}
		want := []string{"labeled", "labeled", "labeled", "labeled", "labeled", "milestoned", "closed"}
		if !reflect.DeepEqual(want, eventLog) {
			t2.Errorf("want: %v; got: %v\n", want, eventLog)
		}

		wantTimesCalled := 1
		if timesDoWasCalled != wantTimesCalled {
			t.Errorf("client.Do should have been called %d times. got: %d\n", wantTimesCalled, timesDoWasCalled)
		}
	})
	t.Run("successful sync missing milestones", func(t2 *testing.T) {
		defer func() { eventLog = make([]string, 0) }()
		timesDoWasCalled = 0
		ctx := context.Background()
		p.client = &ClientMock{
			status:     "OK",
			statusCode: http.StatusOK,
			err:        nil,
			testdata:   "TestMissingMilestoneEvents.json",
		}
		err := p.syncEventsOnIssue(ctx, int32(issue.ID))
		if err != nil {
			t2.Error(err)
		}
		want := []string{"mentioned", "subscribed", "mentioned", "subscribed", "assigned", "labeled", "labeled", "milestoned", "renamed", "demilestoned", "milestoned"}
		sort.Strings(want)
		sort.Strings(eventLog)
		if !reflect.DeepEqual(want, eventLog) {
			t2.Errorf("want: %v; got: %v\n", want, eventLog)
		}

		wantTimesCalled := 1
		if timesDoWasCalled != wantTimesCalled {
			t.Errorf("client.Do should have been called %d times. got: %d\n", wantTimesCalled, timesDoWasCalled)
		}
	})
}

func TestSyncMultipleConsecutiveEvents(t *testing.T) {
	var c Corpus
	c.initGithub()
	c.mutationLogger = &MockLogger{}
	gr := c.github.getOrCreateRepo("foowner", "bar")
	issue := &GitHubIssue{
		ID:          1001,
		PullRequest: &GitHubPullRequest{},
		events:      map[int64]*GitHubIssueEvent{},
	}
	gr.issues = map[int32]*GitHubIssue{
		1001: issue,
	}
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte("OK"))
	}))
	defer server.Close()
	p := &githubRepoPoller{
		c:             &c,
		tokenSource:   oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "foobar"}),
		gr:            gr,
		githubDirect:  github.NewClient(server.Client()),
		githubCaching: github.NewClient(server.Client()),
	}
	t.Run("successful sync", func(t2 *testing.T) {
		defer func() { eventLog = make([]string, 0) }()
		timesDoWasCalled = 0
		ctx := context.Background()
		for i := 1; i < 5; i++ {
			testdata := fmt.Sprintf("TestParseMultipleGithubEvents_p%d.json", i)
			p.client = &ClientMock{
				status:     "OK",
				statusCode: http.StatusOK,
				err:        nil,
				testdata:   testdata,
			}
			err := p.syncEventsOnIssue(ctx, int32(issue.ID))
			if err != nil {
				t2.Fatal(err)
			}
		}

		want := []string{"labeled", "labeled", "labeled", "labeled", "labeled", "milestoned", "closed"}
		if !reflect.DeepEqual(want, eventLog) {
			t2.Errorf("want: %v; got: %v\n", want, eventLog)
		}

		wantTimesCalled := 4
		if timesDoWasCalled != wantTimesCalled {
			t.Errorf("client.Do should have been called %d times. got: %d\n", wantTimesCalled, timesDoWasCalled)
		}
	})
}

func TestParseGitHubReviews(t *testing.T) {
	tests := []struct {
		name string                // test
		j    string                // JSON from Github API
		e    *GitHubReview         // in-memory
		p    *maintpb.GithubReview // on disk
	}{
		{
			name: "Approved",
			j: `{
				"id": 123456,
				"node_id": "548913adsafas84asdf48a",
				"user": {
					"login": "bradfitz",
					"id": 2621
				},
				"body": "I approve this commit",
				"state": "APPROVED",
				"html_url": "https://github.com/bradfitz/go-issue-mirror/pull/21",
				"pull_request_url": "https://github.com/bradfitz/go-issue-mirror/pull/21",
				"author_association": "CONTRIBUTOR",
				"_links":{
					"html":{
						"href": "https://github.com/bradfitz/go-issue-mirror/pull/21"
					},
					"pull_request":{
						"href": "https://github.com/bradfitz/go-issue-mirror/pull/21"
					}
				},
				"submitted_at": "2018-03-22T00:26:48Z",
				"commit_id" : "e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126"
				}`,
			e: &GitHubReview{
				ID: 123456,
				Actor: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
				Body:             "I approve this commit",
				State:            "APPROVED",
				CommitID:         "e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
				ActorAssociation: "CONTRIBUTOR",
				Created:          t3339("2018-03-22T00:26:48Z"),
			},
			p: &maintpb.GithubReview{
				Id:               123456,
				ActorId:          2621,
				Body:             "I approve this commit",
				State:            "APPROVED",
				CommitId:         "e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
				ActorAssociation: "CONTRIBUTOR",
				Created:          p3339("2018-03-22T00:26:48Z"),
			},
		},
		{
			name: "Extra Unknown JSON",
			j: `{
				"id": 123456,
				"node_id": "548913adsafas84asdf48a",
				"user": {
					"login": "bradfitz",
					"id": 2621
				},
				"body": "I approve this commit",
				"state": "APPROVED",
				"html_url": "https://github.com/bradfitz/go-issue-mirror/pull/21",
				"pull_request_url": "https://github.com/bradfitz/go-issue-mirror/pull/21",
				"author_association": "CONTRIBUTOR",
				"_links":{
					"html":{
						"href": "https://github.com/bradfitz/go-issue-mirror/pull/21"
					},
					"pull_request":{
						"href": "https://github.com/bradfitz/go-issue-mirror/pull/21"
					}
				},
				"submitted_at": "2018-03-22T00:26:48Z",
				"commit_id" : "e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
				"random_new_field": "some new thing that GitHub API may add"
				}`,
			e: &GitHubReview{
				ID: 123456,
				Actor: &GitHubUser{
					ID:    2621,
					Login: "bradfitz",
				},
				Body:             "I approve this commit",
				State:            "APPROVED",
				CommitID:         "e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
				ActorAssociation: "CONTRIBUTOR",
				Created:          t3339("2018-03-22T00:26:48Z"),
				OtherJSON:        `{"random_new_field":"some new thing that GitHub API may add"}`,
			},
			p: &maintpb.GithubReview{
				Id:               123456,
				ActorId:          2621,
				Body:             "I approve this commit",
				State:            "APPROVED",
				CommitId:         "e4d70f7e8892f024e4ed3e8b99ee6c5a9f16e126",
				ActorAssociation: "CONTRIBUTOR",
				Created:          p3339("2018-03-22T00:26:48Z"),
				OtherJson:        []byte(`{"random_new_field":"some new thing that GitHub API may add"}`),
			},
		},
	}

	for _, tt := range tests {
		evts, err := parseGithubReviews(strings.NewReader("[" + tt.j + "]"))
		if err != nil {
			t.Errorf("%s: parse JSON: %v", tt.name, err)
			continue
		}
		if len(evts) != 1 {
			t.Errorf("%s: parse JSON = %v entries; want 1", tt.name, len(evts))
			continue
		}
		gote := evts[0]
		if !reflect.DeepEqual(gote, tt.e) {
			t.Errorf("%s: JSON -> githubReviewEvent differs: %v", tt.name, DeepDiff(gote, tt.e))
			continue
		}

		gotp := gote.Proto()
		if !reflect.DeepEqual(gotp, tt.p) {
			t.Errorf("%s: githubReviewEvent -> proto differs: %v", tt.name, DeepDiff(gotp, tt.p))
			continue
		}

		var c Corpus
		c.initGithub()
		c.github.getOrCreateUserID(2621).Login = "bradfitz"
		c.github.getOrCreateUserID(1924134).Login = "dmitshur"
		gr := c.github.getOrCreateRepo("foowner", "bar")
		e2 := gr.newGithubReview(gotp)

		if !reflect.DeepEqual(e2, tt.e) {
			t.Errorf("%s: proto -> githubReviewEvent differs: %v", tt.name, DeepDiff(e2, tt.e))
			continue
		}
	}
}

func TestForeachRepo(t *testing.T) {
	tests := []struct {
		name    string
		issue   *GitHubIssue
		want    []string
		wantErr error
	}{
		{
			name: "Skips non-PullRequests",
			issue: &GitHubIssue{
				PullRequest: nil,
			},
			want:    []string{},
			wantErr: nil,
		},
		{
			name: "Processes Multiple in Order",
			issue: &GitHubIssue{
				PullRequest: &GitHubPullRequest{},
				reviews: map[int64]*GitHubReview{
					0: &GitHubReview{
						Body:    "Second",
						Created: t3339("2018-04-22T00:26:48Z"),
					},
					1: &GitHubReview{
						Body:    "First",
						Created: t3339("2018-03-22T00:26:48Z"),
					},
				},
			},
			want:    []string{"First", "Second"},
			wantErr: nil,
		},
		{
			name: "Will Error",
			issue: &GitHubIssue{
				PullRequest: &GitHubPullRequest{},
				reviews: map[int64]*GitHubReview{
					0: &GitHubReview{
						Body: "Fail",
					},
				},
			},
			want:    []string{},
			wantErr: fmt.Errorf("Planned Failure"),
		},
		{
			name: "Will Error Late",
			issue: &GitHubIssue{
				PullRequest: &GitHubPullRequest{},
				reviews: map[int64]*GitHubReview{
					0: &GitHubReview{
						Body:    "First Event",
						Created: t3339("2018-03-22T00:26:48Z"),
					},
					1: &GitHubReview{
						Body:    "Fail",
						Created: t3339("2018-04-22T00:26:48Z"),
					},
					2: &GitHubReview{
						Body:    "Third Event",
						Created: t3339("2018-05-22T00:26:48Z"),
					},
				},
			},
			want:    []string{"First Event"},
			wantErr: fmt.Errorf("Planned Failure"),
		}}

	for _, tt := range tests {
		got := make([]string, 0)

		err := tt.issue.ForeachReview(func(r *GitHubReview) error {
			if r.Body == "Fail" {
				return fmt.Errorf("Planned Failure")
			}
			got = append(got, r.Body)
			return nil
		})

		if !equalError(tt.wantErr, err) {
			t.Errorf("%s: ForeachReview errs differ. got: %s, want: %s", tt.name, err, tt.wantErr)
		}

		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s: ForeachReview calls differ. got: %s want: %s", tt.name, got, tt.want)
		}
	}

	t.Log("Tested Reviews")
}

// equalError reports whether errors a and b are considered equal.
// They're equal if both are nil, or both are not nil and a.Error() == b.Error().
func equalError(a, b error) bool {
	return a == nil && b == nil || a != nil && b != nil && a.Error() == b.Error()
}

func TestCacheableURL(t *testing.T) {
	tests := []struct {
		v    string
		want bool
	}{
		{"https://api.github.com/repos/OWNER/RePO/milestones?page=1", true},
		{"https://api.github.com/repos/OWNER/RePO/milestones?page=2", false},
		{"https://api.github.com/repos/OWNER/RePO/milestones?", false},
		{"https://api.github.com/repos/OWNER/RePO/milestones", false},

		{"https://api.github.com/repos/OWNER/RePO/labels?page=1", true},
		{"https://api.github.com/repos/OWNER/RePO/labels?page=2", false},
		{"https://api.github.com/repos/OWNER/RePO/labels?", false},
		{"https://api.github.com/repos/OWNER/RePO/labels", false},

		{"https://api.github.com/repos/OWNER/RePO/foos?page=1", false},

		{"https://api.github.com/repos/OWNER/RePO/issues?page=1", false},
		{"https://api.github.com/repos/OWNER/RePO/issues?page=1&sort=updated&direction=desc", true},
	}

	for _, tt := range tests {
		got := cacheableURL(tt.v)
		if got != tt.want {
			t.Errorf("cacheableURL(%q) = %v; want %v", tt.v, got, tt.want)
		}
	}
}

func t3339(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func p3339(s string) *timestamppb.Timestamp {
	return timestamppb.New(t3339(s))
}

func TestParseGithubRefs(t *testing.T) {
	tests := []struct {
		gerritProj string // "go.googlesource.com/go", etc
		msg        string
		want       []string
	}{
		{"go.googlesource.com/go", "\nFixes #1234\n", []string{"golang/go#1234"}},
		{"go.googlesource.com/go", "Fixes #1234\n", []string{"golang/go#1234"}},
		{"go.googlesource.com/go", "Fixes #1234", []string{"golang/go#1234"}},
		{"go.googlesource.com/go", "Fixes golang/go#1234", []string{"golang/go#1234"}},
		{"go.googlesource.com/go", "Fixes golang/go#1234\n", []string{"golang/go#1234"}},
		{"go.googlesource.com/go", "Fixes golang/go#1234.", []string{"golang/go#1234"}},
		{"go.googlesource.com/go", "Mention issue #1234 a second time.\n\nFixes #1234.", []string{"golang/go#1234"}},
		{"go.googlesource.com/go", "Mention issue #1234 a second time.\n\nFixes #1234.\nUpdates #1235.", []string{"golang/go#1234", "golang/go#1235"}},
		{"go.googlesource.com/net", "Fixes golang/go#1234.", []string{"golang/go#1234"}},
		{"go.googlesource.com/net", "Fixes #1234", nil},
	}
	for _, tt := range tests {
		c := new(Corpus)
		var got []string
		for _, ref := range c.parseGithubRefs(tt.gerritProj, tt.msg) {
			got = append(got, ref.String())
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("parseGithubRefs(%q, %q) = %q; want %q", tt.gerritProj, tt.msg, got, tt.want)
		}
	}
}

func TestProcessMutation_Reactions_IssueBody(t *testing.T) {
	c := new(Corpus)

	// Create an issue.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "golang",
			Repo:   "go",
			Number: 1,
			User:   &maintpb.GithubUser{Id: 1, Login: "user1"},
		},
	})

	// Add reactions.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "golang",
			Repo:   "go",
			Number: 1,
			Reaction: []*maintpb.GithubReaction{
				{Id: 100, UserId: 1, Content: "+1"},
				{Id: 101, UserId: 2, Content: "heart"},
			},
		},
	})

	gi := c.github.repos[GitHubRepoID{"golang", "go"}].issues[1]
	if len(gi.reactions) != 2 {
		t.Fatalf("got %d reactions, want 2", len(gi.reactions))
	}
	if gi.reactions[100].Content != "+1" {
		t.Errorf("reaction 100 content = %q, want +1", gi.reactions[100].Content)
	}
	if gi.reactions[101].Content != "heart" {
		t.Errorf("reaction 101 content = %q, want heart", gi.reactions[101].Content)
	}

	// Remove one reaction.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:             "golang",
			Repo:              "go",
			Number:            1,
			RemovedReactionId: []int64{100},
		},
	})

	if len(gi.reactions) != 1 {
		t.Fatalf("got %d reactions after removal, want 1", len(gi.reactions))
	}
	if _, ok := gi.reactions[100]; ok {
		t.Error("reaction 100 should have been removed")
	}
	if gi.reactions[101] == nil {
		t.Error("reaction 101 should still exist")
	}
}

func TestProcessMutation_Reactions_Comment(t *testing.T) {
	c := new(Corpus)

	// Create issue with a comment.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "golang",
			Repo:   "go",
			Number: 2,
			User:   &maintpb.GithubUser{Id: 1, Login: "user1"},
			Comment: []*maintpb.GithubIssueCommentMutation{
				{Id: 50, User: &maintpb.GithubUser{Id: 2, Login: "user2"}, Body: "hello"},
			},
		},
	})

	// Add reaction to comment.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "golang",
			Repo:   "go",
			Number: 2,
			Comment: []*maintpb.GithubIssueCommentMutation{
				{
					Id: 50,
					Reaction: []*maintpb.GithubReaction{
						{Id: 200, UserId: 3, Content: "rocket"},
					},
				},
			},
		},
	})

	gc := c.github.repos[GitHubRepoID{"golang", "go"}].issues[2].comments[50]
	if len(gc.reactions) != 1 {
		t.Fatalf("got %d comment reactions, want 1", len(gc.reactions))
	}
	if gc.reactions[200].Content != "rocket" {
		t.Errorf("reaction content = %q, want rocket", gc.reactions[200].Content)
	}

	// Remove it.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "golang",
			Repo:   "go",
			Number: 2,
			Comment: []*maintpb.GithubIssueCommentMutation{
				{
					Id:                50,
					RemovedReactionId: []int64{200},
				},
			},
		},
	})

	if len(gc.reactions) != 0 {
		t.Fatalf("got %d comment reactions after removal, want 0", len(gc.reactions))
	}
}

func TestForeachReaction(t *testing.T) {
	c := new(Corpus)

	// Create issue with reactions.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "golang",
			Repo:   "go",
			Number: 3,
			User:   &maintpb.GithubUser{Id: 1, Login: "user1"},
			Reaction: []*maintpb.GithubReaction{
				{Id: 300, UserId: 1, Content: "+1"},
				{Id: 301, UserId: 2, Content: "heart"},
				{Id: 302, UserId: 3, Content: "-1"},
			},
		},
	})

	gi := c.github.repos[GitHubRepoID{"golang", "go"}].issues[3]
	var got []string
	gi.ForeachReaction(func(r *GitHubReaction) error {
		got = append(got, fmt.Sprintf("%d:%s", r.ID, r.Content))
		return nil
	})
	// All reactions have zero Created time, so sorted by ID.
	want := []string{"300:+1", "301:heart", "302:-1"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ForeachReaction = %v, want %v", got, want)
	}
}

func TestReactionStatus(t *testing.T) {
	c := new(Corpus)

	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "golang",
			Repo:   "go",
			Number: 4,
			User:   &maintpb.GithubUser{Id: 1, Login: "user1"},
		},
	})

	gi := c.github.repos[GitHubRepoID{"golang", "go"}].issues[4]
	if !gi.reactionsSyncedAsOf.IsZero() {
		t.Error("reactionsSyncedAsOf should be zero before any sync")
	}

	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "golang",
			Repo:   "go",
			Number: 4,
			ReactionStatus: &maintpb.GithubIssueSyncStatus{
				ServerDate: timestamppb.Now(),
			},
		},
	})

	if gi.reactionsSyncedAsOf.IsZero() {
		t.Error("reactionsSyncedAsOf should be set after reaction_status mutation")
	}
}

func TestProcessMutation_PullRequest(t *testing.T) {
	c := new(Corpus)

	// Create a PR issue.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:       "golang",
			Repo:        "go",
			Number:      10,
			PullRequest: true,
			User:        &maintpb.GithubUser{Id: 1, Login: "author"},
			Created:     timestamppb.New(t1),
		},
	})

	gi := c.github.repos[GitHubRepoID{"golang", "go"}].issues[10]
	if !gi.IsPullRequest() {
		t.Fatal("expected issue to be a pull request")
	}
	if gi.PullRequest.Issue != gi {
		t.Error("PullRequest.Issue back-pointer not set")
	}

	// Apply PR detail mutation with merge info and branches.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:           "golang",
			Repo:            "go",
			Number:          10,
			PullRequest:     true,
			Draft:           &maintpb.BoolChange{Val: false},
			Merged:          &maintpb.BoolChange{Val: true},
			MergedAt:        timestamppb.New(t2),
			MergedBy:        &maintpb.GithubUser{Id: 2, Login: "merger"},
			MergeCommitHash: "abc123def456abc123def456abc123def456abc1",
			Head: &maintpb.GithubPullRequestBranch{
				Ref:   "feature-branch",
				Hash:  "1111111111111111111111111111111111111111",
				Owner: "contributor",
				Repo:  "go",
			},
			Base: &maintpb.GithubPullRequestBranch{
				Ref:   "main",
				Hash:  "2222222222222222222222222222222222222222",
				Owner: "golang",
				Repo:  "go",
			},
			PrDetailStatus: &maintpb.GithubIssueSyncStatus{
				ServerDate: timestamppb.Now(),
			},
		},
	})

	pr := gi.PullRequest
	if pr.Draft {
		t.Error("expected Draft to be false")
	}
	if !pr.Merged {
		t.Error("expected Merged to be true")
	}
	if !pr.MergedAt.Equal(t2) {
		t.Errorf("MergedAt = %v; want %v", pr.MergedAt, t2)
	}
	if pr.MergedBy == nil || pr.MergedBy.Login != "merger" {
		t.Errorf("MergedBy = %v; want merger", pr.MergedBy)
	}
	if pr.MergeCommitSHA != "abc123def456abc123def456abc123def456abc1" {
		t.Errorf("MergeCommitSHA = %q", pr.MergeCommitSHA)
	}
	if pr.Head.Ref != "feature-branch" || pr.Head.Owner != "contributor" {
		t.Errorf("Head = %+v", pr.Head)
	}
	if pr.Base.Ref != "main" || pr.Base.Owner != "golang" {
		t.Errorf("Base = %+v", pr.Base)
	}
	if gi.prDetailsSyncedAsOf.IsZero() {
		t.Error("prDetailsSyncedAsOf should be set")
	}
}

func TestProcessMutation_PullRequest_DraftChange(t *testing.T) {
	c := new(Corpus)

	// Create a draft PR.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:       "golang",
			Repo:        "go",
			Number:      11,
			PullRequest: true,
			User:        &maintpb.GithubUser{Id: 1, Login: "author"},
			Draft:       &maintpb.BoolChange{Val: true},
		},
	})

	gi := c.github.repos[GitHubRepoID{"golang", "go"}].issues[11]
	if !gi.PullRequest.Draft {
		t.Error("expected Draft to be true initially")
	}

	// Mark as ready for review.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:       "golang",
			Repo:        "go",
			Number:      11,
			PullRequest: true,
			Draft:       &maintpb.BoolChange{Val: false},
		},
	})

	if gi.PullRequest.Draft {
		t.Error("expected Draft to be false after change")
	}
}

func TestProcessMutation_NonPR_IgnoresPRFields(t *testing.T) {
	c := new(Corpus)

	// Create a regular issue (not a PR).
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "golang",
			Repo:   "go",
			Number: 12,
			User:   &maintpb.GithubUser{Id: 1, Login: "author"},
		},
	})

	gi := c.github.repos[GitHubRepoID{"golang", "go"}].issues[12]
	if gi.IsPullRequest() {
		t.Error("regular issue should not be a pull request")
	}

	// Apply a mutation with PR fields but pull_request=false.
	// The PR fields should be ignored since this isn't a PR.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "golang",
			Repo:   "go",
			Number: 12,
			Merged: &maintpb.BoolChange{Val: true},
			Head: &maintpb.GithubPullRequestBranch{
				Ref: "some-branch",
			},
		},
	})

	if gi.IsPullRequest() {
		t.Error("issue should still not be a pull request")
	}
}

func TestProcessMutation_Actions_WorkflowRun(t *testing.T) {
	c := new(Corpus)

	c.processMutationLocked(&maintpb.Mutation{
		GithubActions: &maintpb.GithubActionsMutation{
			Owner: "golang",
			Repo:  "go",
			Run: &maintpb.GithubWorkflowRun{
				Id:                 1001,
				Name:               "CI",
				HeadBranch:         "main",
				HeadHash:           "abc123",
				Event:              "push",
				Status:             "completed",
				Conclusion:         "success",
				WorkflowId:         42,
				RunNumber:          100,
				RunAttempt:         1,
				Created:            timestamppb.New(t1),
				Updated:            timestamppb.New(t2),
				RunStarted:         timestamppb.New(t1),
				ActorId:            99,
				Url:                "https://github.com/golang/go/actions/runs/1001",
				PullRequestNumbers: []int64{5, 10},
			},
		},
	})

	gr := c.github.repos[GitHubRepoID{"golang", "go"}]
	if gr == nil {
		t.Fatal("repo not created")
	}
	run := gr.workflowRuns[1001]
	if run == nil {
		t.Fatal("workflow run not created")
	}
	if run.Name != "CI" {
		t.Errorf("Name = %q; want CI", run.Name)
	}
	if run.HeadBranch != "main" {
		t.Errorf("HeadBranch = %q; want main", run.HeadBranch)
	}
	if run.Status != "completed" || run.Conclusion != "success" {
		t.Errorf("Status/Conclusion = %q/%q; want completed/success", run.Status, run.Conclusion)
	}
	if run.WorkflowID != 42 {
		t.Errorf("WorkflowID = %d; want 42", run.WorkflowID)
	}
	if len(run.PRNumbers) != 2 || run.PRNumbers[0] != 5 || run.PRNumbers[1] != 10 {
		t.Errorf("PRNumbers = %v; want [5 10]", run.PRNumbers)
	}
}

func TestProcessMutation_Actions_WorkflowJob(t *testing.T) {
	c := new(Corpus)

	// Create a run first.
	c.processMutationLocked(&maintpb.Mutation{
		GithubActions: &maintpb.GithubActionsMutation{
			Owner: "golang",
			Repo:  "go",
			Run: &maintpb.GithubWorkflowRun{
				Id:   2001,
				Name: "Tests",
			},
		},
	})

	// Add a job to the run.
	c.processMutationLocked(&maintpb.Mutation{
		GithubActions: &maintpb.GithubActionsMutation{
			Owner: "golang",
			Repo:  "go",
			Job: &maintpb.GithubWorkflowJob{
				Id:         3001,
				RunId:      2001,
				Name:       "test-linux",
				Status:     "completed",
				Conclusion: "success",
				Started:    timestamppb.New(t1),
				Completed:  timestamppb.New(t2),
				RunnerName: "ubuntu-latest",
				Labels:     []string{"ubuntu-latest"},
				Step: []*maintpb.GithubWorkflowStep{
					{Name: "Checkout", Status: "completed", Conclusion: "success", Number: 1},
					{Name: "Test", Status: "completed", Conclusion: "success", Number: 2},
				},
			},
		},
	})

	gr := c.github.repos[GitHubRepoID{"golang", "go"}]
	run := gr.workflowRuns[2001]
	if run == nil {
		t.Fatal("run not found")
	}
	job := run.Jobs[3001]
	if job == nil {
		t.Fatal("job not found")
	}
	if job.Name != "test-linux" {
		t.Errorf("job Name = %q; want test-linux", job.Name)
	}
	if job.RunnerName != "ubuntu-latest" {
		t.Errorf("RunnerName = %q; want ubuntu-latest", job.RunnerName)
	}
	if len(job.Steps) != 2 {
		t.Fatalf("got %d steps; want 2", len(job.Steps))
	}
	if job.Steps[0].Name != "Checkout" || job.Steps[1].Name != "Test" {
		t.Errorf("step names = %q, %q", job.Steps[0].Name, job.Steps[1].Name)
	}
}

func TestProcessMutation_Actions_JobBeforeRun(t *testing.T) {
	c := new(Corpus)

	// Job arrives before its run — should create a placeholder run.
	c.processMutationLocked(&maintpb.Mutation{
		GithubActions: &maintpb.GithubActionsMutation{
			Owner: "golang",
			Repo:  "go",
			Job: &maintpb.GithubWorkflowJob{
				Id:    4001,
				RunId: 5001,
				Name:  "build",
			},
		},
	})

	gr := c.github.repos[GitHubRepoID{"golang", "go"}]
	run := gr.workflowRuns[5001]
	if run == nil {
		t.Fatal("placeholder run not created for orphan job")
	}
	if run.Jobs[4001] == nil {
		t.Fatal("job not added to placeholder run")
	}
}

func TestDefaultGitHubSyncFilter(t *testing.T) {
	f := DefaultGitHubSyncFilter()
	if !f.Issues || !f.Comments || !f.Events || !f.Reviews || !f.PRDetails || !f.Reactions {
		t.Error("default filter should enable all standard sync categories")
	}
	if f.Actions {
		t.Error("default filter should not enable Actions")
	}
}

func TestForeachWorkflowRun(t *testing.T) {
	c := new(Corpus)

	for _, id := range []int64{300, 100, 200} {
		c.processMutationLocked(&maintpb.Mutation{
			GithubActions: &maintpb.GithubActionsMutation{
				Owner: "golang",
				Repo:  "go",
				Run:   &maintpb.GithubWorkflowRun{Id: id, Name: fmt.Sprintf("run-%d", id)},
			},
		})
	}

	gr := c.github.repos[GitHubRepoID{"golang", "go"}]
	var ids []int64
	gr.ForeachWorkflowRun(func(r *GitHubWorkflowRun) error {
		ids = append(ids, r.ID)
		return nil
	})
	want := []int64{100, 200, 300}
	if !reflect.DeepEqual(ids, want) {
		t.Errorf("ForeachWorkflowRun IDs = %v; want %v", ids, want)
	}
}

func TestProcessMutation_ProjectMetadata(t *testing.T) {
	c := new(Corpus)

	c.processMutationLocked(&maintpb.Mutation{
		GithubProject: &maintpb.GithubProjectMutation{
			Owner:         "tailscale",
			ProjectNodeId: "PVT_abc",
			ProjectNumber: 42,
			Title:         "My Project",
			StatusOptions: []*maintpb.GithubProjectStatusOption{
				{Id: "opt1", Name: "Todo"},
				{Id: "opt2", Name: "In Progress"},
				{Id: "opt3", Name: "Done"},
			},
		},
	})

	proj := c.github.projects["PVT_abc"]
	if proj == nil {
		t.Fatal("project not created")
	}
	if proj.Owner != "tailscale" {
		t.Errorf("Owner = %q, want tailscale", proj.Owner)
	}
	if proj.Number != 42 {
		t.Errorf("Number = %d, want 42", proj.Number)
	}
	if proj.Title != "My Project" {
		t.Errorf("Title = %q, want My Project", proj.Title)
	}
	if len(proj.StatusOptions) != 3 {
		t.Fatalf("StatusOptions len = %d, want 3", len(proj.StatusOptions))
	}
	if proj.StatusOptionName("opt2") != "In Progress" {
		t.Errorf("StatusOptionName(opt2) = %q, want In Progress", proj.StatusOptionName("opt2"))
	}

	// Update title.
	c.processMutationLocked(&maintpb.Mutation{
		GithubProject: &maintpb.GithubProjectMutation{
			Owner:         "tailscale",
			ProjectNodeId: "PVT_abc",
			Title:         "Renamed",
		},
	})
	if proj.Title != "Renamed" {
		t.Errorf("after update, Title = %q, want Renamed", proj.Title)
	}
	// Status options should be cleared (empty list in mutation replaces).
	// Actually empty list does not replace — only non-empty does.
	if len(proj.StatusOptions) != 3 {
		t.Errorf("StatusOptions should be unchanged, got %d", len(proj.StatusOptions))
	}
}

func TestProcessMutation_ProjectItems(t *testing.T) {
	c := new(Corpus)

	// Create an issue.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "tailscale",
			Repo:   "tailscale",
			Number: 100,
			User:   &maintpb.GithubUser{Id: 1, Login: "user1"},
		},
	})

	// Add project item.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "tailscale",
			Repo:   "tailscale",
			Number: 100,
			ProjectItem: []*maintpb.GithubIssueProjectItem{
				{
					ProjectNodeId:  "PVT_abc",
					ItemNodeId:     "PVTI_123",
					StatusOptionId: "opt1",
				},
			},
		},
	})

	gi := c.github.repos[GitHubRepoID{"tailscale", "tailscale"}].issues[100]
	if !gi.InProject("PVT_abc") {
		t.Error("issue should be in project PVT_abc")
	}
	if gi.InProject("PVT_other") {
		t.Error("issue should not be in project PVT_other")
	}

	item := gi.projectItems["PVT_abc"]
	if item == nil {
		t.Fatal("project item not found")
	}
	if item.ItemNodeID != "PVTI_123" {
		t.Errorf("ItemNodeID = %q, want PVTI_123", item.ItemNodeID)
	}
	if item.StatusOptionID != "opt1" {
		t.Errorf("StatusOptionID = %q, want opt1", item.StatusOptionID)
	}

	// Update status.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "tailscale",
			Repo:   "tailscale",
			Number: 100,
			ProjectItem: []*maintpb.GithubIssueProjectItem{
				{
					ProjectNodeId:  "PVT_abc",
					ItemNodeId:     "PVTI_123",
					StatusOptionId: "opt2",
				},
			},
		},
	})

	if item = gi.projectItems["PVT_abc"]; item.StatusOptionID != "opt2" {
		t.Errorf("after update, StatusOptionID = %q, want opt2", item.StatusOptionID)
	}

	// Remove from project.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:                "tailscale",
			Repo:                 "tailscale",
			Number:               100,
			RemovedProjectItemId: []string{"PVT_abc"},
		},
	})

	if gi.InProject("PVT_abc") {
		t.Error("issue should no longer be in project PVT_abc")
	}
}

func TestProcessMutation_ProjectEvents(t *testing.T) {
	c := new(Corpus)

	// Create an issue.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "tailscale",
			Repo:   "tailscale",
			Number: 200,
			User:   &maintpb.GithubUser{Id: 1, Login: "user1"},
		},
	})

	// Add project events.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "tailscale",
			Repo:   "tailscale",
			Number: 200,
			ProjectEvent: []*maintpb.GithubProjectEvent{
				{
					Id:            "ev1",
					EventType:     "added",
					ActorId:       42,
					ProjectNodeId: "PVT_abc",
					WasAutomated:  true,
				},
				{
					Id:             "ev2",
					EventType:      "status_changed",
					ActorId:        42,
					ProjectNodeId:  "PVT_abc",
					PreviousStatus: "Todo",
					Status:         "In Progress",
				},
			},
		},
	})

	gi := c.github.repos[GitHubRepoID{"tailscale", "tailscale"}].issues[200]
	if len(gi.projectEvents) != 2 {
		t.Fatalf("got %d project events, want 2", len(gi.projectEvents))
	}

	ev1 := gi.projectEvents["ev1"]
	if ev1.Type != "added" {
		t.Errorf("ev1 Type = %q, want added", ev1.Type)
	}
	if !ev1.WasAutomated {
		t.Error("ev1 should be automated")
	}
	if ev1.Actor == nil || ev1.Actor.ID != 42 {
		t.Error("ev1 actor should have ID 42")
	}

	ev2 := gi.projectEvents["ev2"]
	if ev2.Type != "status_changed" {
		t.Errorf("ev2 Type = %q, want status_changed", ev2.Type)
	}
	if ev2.PreviousStatus != "Todo" || ev2.Status != "In Progress" {
		t.Errorf("ev2 status = %q -> %q, want Todo -> In Progress", ev2.PreviousStatus, ev2.Status)
	}

	// Duplicate events should not be added (same ID).
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "tailscale",
			Repo:   "tailscale",
			Number: 200,
			ProjectEvent: []*maintpb.GithubProjectEvent{
				{
					Id:        "ev1",
					EventType: "added",
				},
			},
		},
	})
	// Should still have 2 events (ev1 was updated in place, not duplicated).
	if len(gi.projectEvents) != 2 {
		t.Fatalf("after re-adding ev1, got %d events, want 2", len(gi.projectEvents))
	}
}

func TestProjectStatusName(t *testing.T) {
	c := new(Corpus)

	// Create project with status options.
	c.processMutationLocked(&maintpb.Mutation{
		GithubProject: &maintpb.GithubProjectMutation{
			Owner:         "tailscale",
			ProjectNodeId: "PVT_abc",
			ProjectNumber: 1,
			Title:         "Test",
			StatusOptions: []*maintpb.GithubProjectStatusOption{
				{Id: "opt1", Name: "Todo"},
				{Id: "opt2", Name: "Done"},
			},
		},
	})

	// Create issue in the project.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "tailscale",
			Repo:   "tailscale",
			Number: 1,
			User:   &maintpb.GithubUser{Id: 1},
			ProjectItem: []*maintpb.GithubIssueProjectItem{
				{ProjectNodeId: "PVT_abc", ItemNodeId: "PVTI_1", StatusOptionId: "opt1"},
			},
		},
	})

	proj := c.github.projects["PVT_abc"]
	gi := c.github.repos[GitHubRepoID{"tailscale", "tailscale"}].issues[1]

	if got := gi.ProjectStatusName(proj); got != "Todo" {
		t.Errorf("ProjectStatusName = %q, want Todo", got)
	}
	if got := gi.ProjectStatusName(nil); got != "" {
		t.Errorf("ProjectStatusName(nil) = %q, want empty", got)
	}
}

func TestProcessMutation_SingleSelectFields(t *testing.T) {
	c := new(Corpus)

	// Emit project with Status and Pod single-select fields.
	c.processMutationLocked(&maintpb.Mutation{
		GithubProject: &maintpb.GithubProjectMutation{
			Owner:         "tailscale",
			ProjectNodeId: "PVT_abc",
			ProjectNumber: 127,
			Title:         "Backlog",
			SingleSelectFields: []*maintpb.GithubProjectSingleSelectField{
				{
					Name: "Status",
					Options: []*maintpb.GithubProjectStatusOption{
						{Id: "s1", Name: "Todo"},
						{Id: "s2", Name: "In Progress"},
						{Id: "s3", Name: "Done"},
					},
				},
				{
					Name: "Pod",
					Options: []*maintpb.GithubProjectStatusOption{
						{Id: "p1", Name: "Network"},
						{Id: "p2", Name: "Platform"},
					},
				},
			},
		},
	})

	proj := c.github.projects["PVT_abc"]
	if proj == nil {
		t.Fatal("project not created")
	}
	if len(proj.Fields) != 2 {
		t.Fatalf("Fields len = %d, want 2", len(proj.Fields))
	}

	status := proj.Fields["Status"]
	if status == nil {
		t.Fatal("Status field not found")
	}
	if len(status.Options) != 3 {
		t.Fatalf("Status options len = %d, want 3", len(status.Options))
	}
	if status.Options["s2"].Name != "In Progress" {
		t.Errorf("Status option s2 = %q, want In Progress", status.Options["s2"].Name)
	}

	pod := proj.Fields["Pod"]
	if pod == nil {
		t.Fatal("Pod field not found")
	}
	if len(pod.Options) != 2 {
		t.Fatalf("Pod options len = %d, want 2", len(pod.Options))
	}
	if pod.Options["p1"].Name != "Network" {
		t.Errorf("Pod option p1 = %q, want Network", pod.Options["p1"].Name)
	}

	// FieldOptionName on known and unknown IDs.
	if got := proj.FieldOptionName("Pod", "p2"); got != "Platform" {
		t.Errorf("FieldOptionName(Pod, p2) = %q, want Platform", got)
	}
	if got := proj.FieldOptionName("Pod", "unknown"); got != "" {
		t.Errorf("FieldOptionName(Pod, unknown) = %q, want empty", got)
	}
	if got := proj.FieldOptionName("NoSuchField", "p1"); got != "" {
		t.Errorf("FieldOptionName(NoSuchField, p1) = %q, want empty", got)
	}
	if got := (*GitHubProject)(nil).FieldOptionName("Status", "s1"); got != "" {
		t.Errorf("nil.FieldOptionName = %q, want empty", got)
	}
}

func TestProcessMutation_FieldValues(t *testing.T) {
	c := new(Corpus)

	// Create project with two fields.
	c.processMutationLocked(&maintpb.Mutation{
		GithubProject: &maintpb.GithubProjectMutation{
			Owner:         "tailscale",
			ProjectNodeId: "PVT_abc",
			ProjectNumber: 127,
			Title:         "Backlog",
			SingleSelectFields: []*maintpb.GithubProjectSingleSelectField{
				{
					Name:    "Status",
					Options: []*maintpb.GithubProjectStatusOption{{Id: "s1", Name: "In Progress"}},
				},
				{
					Name:    "Pod",
					Options: []*maintpb.GithubProjectStatusOption{{Id: "p1", Name: "Network"}},
				},
			},
		},
	})

	// Create issue with field values for both fields.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "tailscale",
			Repo:   "tailscale",
			Number: 1,
			User:   &maintpb.GithubUser{Id: 1},
			ProjectItem: []*maintpb.GithubIssueProjectItem{
				{
					ProjectNodeId:  "PVT_abc",
					ItemNodeId:     "PVTI_1",
					StatusOptionId: "s1",
					FieldValues: []*maintpb.GithubProjectItemFieldValue{
						{FieldName: "Status", OptionId: "s1"},
						{FieldName: "Pod", OptionId: "p1"},
					},
				},
			},
		},
	})

	proj := c.github.projects["PVT_abc"]
	gi := c.github.repos[GitHubRepoID{"tailscale", "tailscale"}].issues[1]

	item := gi.projectItems["PVT_abc"]
	if item == nil {
		t.Fatal("project item not found")
	}
	if item.FieldValues["Status"] != "s1" {
		t.Errorf("FieldValues[Status] = %q, want s1", item.FieldValues["Status"])
	}
	if item.FieldValues["Pod"] != "p1" {
		t.Errorf("FieldValues[Pod] = %q, want p1", item.FieldValues["Pod"])
	}
	// StatusOptionID should be set (directly from proto field).
	if item.StatusOptionID != "s1" {
		t.Errorf("StatusOptionID = %q, want s1", item.StatusOptionID)
	}

	// ProjectFieldValue resolves both fields.
	if got := gi.ProjectFieldValue(proj, "Status"); got != "In Progress" {
		t.Errorf("ProjectFieldValue(Status) = %q, want In Progress", got)
	}
	if got := gi.ProjectFieldValue(proj, "Pod"); got != "Network" {
		t.Errorf("ProjectFieldValue(Pod) = %q, want Network", got)
	}
	if got := gi.ProjectFieldValue(proj, "Priority"); got != "" {
		t.Errorf("ProjectFieldValue(Priority) = %q, want empty", got)
	}

	// Nil-safe.
	if got := (*GitHubIssue)(nil).ProjectFieldValue(proj, "Pod"); got != "" {
		t.Errorf("nil.ProjectFieldValue = %q, want empty", got)
	}
	if got := gi.ProjectFieldValue(nil, "Pod"); got != "" {
		t.Errorf("ProjectFieldValue(nil proj) = %q, want empty", got)
	}
}

// TestProjectFieldValue_BackwardCompat verifies that an old-style project item
// (StatusOptionId set, no FieldValues) still resolves correctly via
// ProjectFieldValue for the "Status" field.
func TestProjectFieldValue_BackwardCompat(t *testing.T) {
	c := new(Corpus)

	// Old-style project: only StatusOptions (no SingleSelectFields).
	c.processMutationLocked(&maintpb.Mutation{
		GithubProject: &maintpb.GithubProjectMutation{
			Owner:         "tailscale",
			ProjectNodeId: "PVT_old",
			ProjectNumber: 1,
			Title:         "Old Project",
			StatusOptions: []*maintpb.GithubProjectStatusOption{
				{Id: "opt1", Name: "Todo"},
				{Id: "opt2", Name: "Done"},
			},
		},
	})

	// Old-style item: only StatusOptionId, no FieldValues.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "tailscale",
			Repo:   "tailscale",
			Number: 2,
			User:   &maintpb.GithubUser{Id: 1},
			ProjectItem: []*maintpb.GithubIssueProjectItem{
				{
					ProjectNodeId:  "PVT_old",
					ItemNodeId:     "PVTI_old",
					StatusOptionId: "opt2",
					// No FieldValues — old record.
				},
			},
		},
	})

	proj := c.github.projects["PVT_old"]
	gi := c.github.repos[GitHubRepoID{"tailscale", "tailscale"}].issues[2]

	// ProjectStatusName (existing helper) still works.
	if got := gi.ProjectStatusName(proj); got != "Done" {
		t.Errorf("ProjectStatusName = %q, want Done", got)
	}
	// ProjectFieldValue for "Status" falls back to StatusOptionID.
	if got := gi.ProjectFieldValue(proj, "Status"); got != "Done" {
		t.Errorf("ProjectFieldValue(Status) backward-compat = %q, want Done", got)
	}
}

// TestProjectFieldValue_FieldValuesBackfillsStatus verifies that when only
// FieldValues is set (no explicit StatusOptionId in proto), the item's
// StatusOptionID is backfilled from FieldValues["Status"].
func TestProjectFieldValue_FieldValuesBackfillsStatus(t *testing.T) {
	c := new(Corpus)

	c.processMutationLocked(&maintpb.Mutation{
		GithubProject: &maintpb.GithubProjectMutation{
			Owner:         "tailscale",
			ProjectNodeId: "PVT_new",
			ProjectNumber: 2,
			Title:         "New Project",
			SingleSelectFields: []*maintpb.GithubProjectSingleSelectField{
				{
					Name:    "Status",
					Options: []*maintpb.GithubProjectStatusOption{{Id: "s9", Name: "Blocked"}},
				},
			},
		},
	})

	// New-style item: StatusOptionId is empty, value comes only via FieldValues.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "tailscale",
			Repo:   "tailscale",
			Number: 3,
			User:   &maintpb.GithubUser{Id: 1},
			ProjectItem: []*maintpb.GithubIssueProjectItem{
				{
					ProjectNodeId: "PVT_new",
					ItemNodeId:    "PVTI_new",
					// StatusOptionId intentionally omitted.
					FieldValues: []*maintpb.GithubProjectItemFieldValue{
						{FieldName: "Status", OptionId: "s9"},
					},
				},
			},
		},
	})

	proj := c.github.projects["PVT_new"]
	gi := c.github.repos[GitHubRepoID{"tailscale", "tailscale"}].issues[3]
	item := gi.projectItems["PVT_new"]

	// StatusOptionID should be backfilled.
	if item.StatusOptionID != "s9" {
		t.Errorf("StatusOptionID = %q, want s9 (backfilled from FieldValues)", item.StatusOptionID)
	}
	if got := gi.ProjectFieldValue(proj, "Status"); got != "Blocked" {
		t.Errorf("ProjectFieldValue(Status) = %q, want Blocked", got)
	}
}

func TestProcessMutation_IssueType(t *testing.T) {
	c := new(Corpus)

	// Create issue without a type.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  "tailscale",
			Repo:   "tailscale",
			Number: 10,
			User:   &maintpb.GithubUser{Id: 1},
		},
	})

	gi := c.github.repos[GitHubRepoID{"tailscale", "tailscale"}].issues[10]
	if gi.IssueType != "" {
		t.Errorf("IssueType = %q, want empty before any type mutation", gi.IssueType)
	}

	// Set the issue type.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:     "tailscale",
			Repo:      "tailscale",
			Number:    10,
			IssueType: "Bug",
		},
	})
	if gi.IssueType != "Bug" {
		t.Errorf("IssueType = %q, want Bug", gi.IssueType)
	}

	// Update to a different type.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:     "tailscale",
			Repo:      "tailscale",
			Number:    10,
			IssueType: "Feature",
		},
	})
	if gi.IssueType != "Feature" {
		t.Errorf("IssueType = %q, want Feature after update", gi.IssueType)
	}

	// Empty string in proto must not overwrite existing value (old records).
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:     "tailscale",
			Repo:      "tailscale",
			Number:    10,
			IssueType: "",
		},
	})
	if gi.IssueType != "Feature" {
		t.Errorf("IssueType = %q, want Feature (empty proto should not overwrite)", gi.IssueType)
	}
}

// TestSyncProjectsForIssue is an integration test that calls the real GitHub
// GraphQL API and verifies that FieldValues and IssueType are populated in the
// corpus after a sync.
//
// It requires a GitHub personal access token (read:project + repo scopes) in
// the GITHUB_TOKEN environment variable. The issue to sync is configured via:
//
//	MAINTNER_TEST_OWNER  (default: tailscale)
//	MAINTNER_TEST_REPO   (default: tailscale)
//	MAINTNER_TEST_ISSUE  (required — no default; supply a number for an issue
//	                      that belongs to a GitHub Projects V2 project)
//
// Example:
//
//	GITHUB_TOKEN=ghp_... MAINTNER_TEST_ISSUE=12345 \
//	  go test ./maintner/ -run TestSyncProjectsForIssue -v
func TestSyncProjectsForIssue(t *testing.T) {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		t.Skip("GITHUB_TOKEN not set")
	}
	issueStr := os.Getenv("MAINTNER_TEST_ISSUE")
	if issueStr == "" {
		t.Skip("MAINTNER_TEST_ISSUE not set (set to an issue number that belongs to a GitHub project)")
	}
	var issueNum int
	if _, err := fmt.Sscan(issueStr, &issueNum); err != nil || issueNum <= 0 {
		t.Fatalf("MAINTNER_TEST_ISSUE=%q: must be a positive integer", issueStr)
	}

	owner := os.Getenv("MAINTNER_TEST_OWNER")
	if owner == "" {
		owner = "tailscale"
	}
	repo := os.Getenv("MAINTNER_TEST_REPO")
	if repo == "" {
		repo = "tailscale"
	}

	hc := oauth2.NewClient(context.Background(),
		oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token}))

	c := new(Corpus)

	// Pre-seed the issue so syncProjectsForIssue doesn't bail out early.
	c.processMutationLocked(&maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  owner,
			Repo:   repo,
			Number: int32(issueNum),
			User:   &maintpb.GithubUser{Id: 1},
		},
	})

	ctx := context.Background()
	if err := c.SyncProjectsForIssue(ctx, hc, owner, repo, int32(issueNum)); err != nil {
		t.Fatalf("SyncProjectsForIssue: %v", err)
	}

	gr := c.github.repos[GitHubRepoID{owner, repo}]
	if gr == nil {
		t.Fatal("repo not in corpus after sync")
	}
	gi := gr.issues[int32(issueNum)]
	if gi == nil {
		t.Fatal("issue not in corpus after sync")
	}

	t.Logf("IssueType: %q", gi.IssueType)
	t.Logf("project items: %d", len(gi.projectItems))
	for projID, item := range gi.projectItems {
		t.Logf("  project %s: item=%s status=%s fieldValues=%v",
			projID, item.ItemNodeID, item.StatusOptionID, item.FieldValues)
	}

	// Verify project metadata (Fields) was populated for any projects seen.
	for projID := range gi.projectItems {
		proj := c.github.projects[projID]
		if proj == nil {
			t.Errorf("project %s has no metadata in corpus", projID)
			continue
		}
		t.Logf("  project %s title=%q fields=%v", projID, proj.Title, func() []string {
			var names []string
			for n := range proj.Fields {
				names = append(names, n)
			}
			return names
		}())

		item := gi.projectItems[projID]
		// Every FieldValue entry must resolve to a non-empty name via FieldOptionName.
		for fieldName, optionID := range item.FieldValues {
			name := proj.FieldOptionName(fieldName, optionID)
			if name == "" {
				t.Errorf("project %s field %q option %q: FieldOptionName returned empty (option not in project schema)",
					projID, fieldName, optionID)
			} else {
				t.Logf("    %s = %s (optionID=%s)", fieldName, name, optionID)
			}
		}

		// ProjectFieldValue must agree with FieldOptionName for every field.
		for fieldName := range item.FieldValues {
			want := proj.FieldOptionName(fieldName, item.FieldValues[fieldName])
			got := gi.ProjectFieldValue(proj, fieldName)
			if got != want {
				t.Errorf("ProjectFieldValue(%q) = %q, want %q", fieldName, got, want)
			}
		}
	}

	// IssueType should only be set for actual issues, not PRs.
	if gi.IsPullRequest() && gi.IssueType != "" {
		t.Errorf("IssueType = %q on a pull request; want empty", gi.IssueType)
	}
}

func TestFieldOptionName_StatusFallback(t *testing.T) {
	// Project with only legacy StatusOptions (no Fields populated).
	proj := &GitHubProject{
		StatusOptions: map[string]*GitHubProjectStatusOption{
			"s1": {ID: "s1", Name: "In Progress"},
		},
	}

	// FieldOptionName("Status", ...) falls back to StatusOptions.
	if got := proj.FieldOptionName("Status", "s1"); got != "In Progress" {
		t.Errorf("FieldOptionName(Status fallback) = %q, want In Progress", got)
	}
	// Non-Status field has no fallback.
	if got := proj.FieldOptionName("Pod", "s1"); got != "" {
		t.Errorf("FieldOptionName(Pod fallback) = %q, want empty", got)
	}
}
