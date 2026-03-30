// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package maintner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bradfitz/gxbm/maintner/maintpb"
	"github.com/google/go-github/v74/github"
	"github.com/gregjones/httpcache"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// xFromCache is the synthetic response header added by the httpcache
// package for responses fulfilled from cache due to a 304 from the server.
const xFromCache = "X-From-Cache"

// GitHubRepoID is a GitHub org & repo, lowercase.
type GitHubRepoID struct {
	Owner, Repo string
}

func (id GitHubRepoID) String() string { return id.Owner + "/" + id.Repo }

func (id GitHubRepoID) valid() bool {
	if id.Owner == "" || id.Repo == "" {
		// TODO: more validation. whatever GitHub requires.
		return false
	}
	return true
}

// GitHub holds data about a GitHub repo.
type GitHub struct {
	c     *Corpus
	users map[int64]*GitHubUser
	teams map[int64]*GitHubTeam
	repos map[GitHubRepoID]*GitHubRepo
}

// ForeachRepo calls fn serially for each GitHubRepo, stopping if fn
// returns an error. The function is called with lexically increasing
// repo IDs.
func (g *GitHub) ForeachRepo(fn func(*GitHubRepo) error) error {
	var ids []GitHubRepoID
	for id := range g.repos {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].Owner < ids[j].Owner {
			return true
		}
		return ids[i].Owner == ids[j].Owner && ids[i].Repo < ids[j].Repo
	})
	for _, id := range ids {
		if err := fn(g.repos[id]); err != nil {
			return err
		}
	}
	return nil
}

// Repo returns the repo if it's known. Otherwise it returns nil.
func (g *GitHub) Repo(owner, repo string) *GitHubRepo {
	return g.repos[GitHubRepoID{owner, repo}]
}

func (g *GitHub) getOrCreateRepo(owner, repo string) *GitHubRepo {
	if g == nil {
		panic("cannot call methods on nil GitHub")
	}
	id := GitHubRepoID{owner, repo}
	if !id.valid() {
		return nil
	}
	r, ok := g.repos[id]
	if ok {
		return r
	}
	r = &GitHubRepo{
		github: g,
		id:     id,
		issues: map[int32]*GitHubIssue{},
	}
	g.repos[id] = r
	return r
}

type GitHubRepo struct {
	github       *GitHub
	id           GitHubRepoID
	issues       map[int32]*GitHubIssue // num -> issue
	milestones   map[int64]*GitHubMilestone
	labels       map[int64]*GitHubLabel
	workflowRuns map[int64]*GitHubWorkflowRun // run ID -> run
}

func (gr *GitHubRepo) ID() GitHubRepoID { return gr.id }

// Issue returns the provided issue number, or nil if it's not known.
func (gr *GitHubRepo) Issue(n int32) *GitHubIssue { return gr.issues[n] }

// ForeachLabel calls fn for each label in the repo, in unsorted order.
//
// Iteration ends if fn returns an error, with that error.
func (gr *GitHubRepo) ForeachLabel(fn func(*GitHubLabel) error) error {
	for _, lb := range gr.labels {
		if err := fn(lb); err != nil {
			return err
		}
	}
	return nil
}

// ForeachMilestone calls fn for each milestone in the repo, in unsorted order.
//
// Iteration ends if fn returns an error, with that error.
func (gr *GitHubRepo) ForeachMilestone(fn func(*GitHubMilestone) error) error {
	for _, m := range gr.milestones {
		if err := fn(m); err != nil {
			return err
		}
	}
	return nil
}

// ForeachIssue calls fn for each issue in the repo.
//
// If fn returns an error, iteration ends and ForeachIssue returns
// with that error.
//
// The fn function is called serially, with increasingly numbered
// issues.
func (gr *GitHubRepo) ForeachIssue(fn func(*GitHubIssue) error) error {
	s := make([]*GitHubIssue, 0, len(gr.issues))
	for _, gi := range gr.issues {
		s = append(s, gi)
	}
	sort.Slice(s, func(i, j int) bool { return s[i].Number < s[j].Number })
	for _, gi := range s {
		if err := fn(gi); err != nil {
			return err
		}
	}
	return nil
}

// ForeachWorkflowRun calls fn for each workflow run in the repo,
// sorted by run ID.
func (gr *GitHubRepo) ForeachWorkflowRun(fn func(*GitHubWorkflowRun) error) error {
	s := make([]*GitHubWorkflowRun, 0, len(gr.workflowRuns))
	for _, r := range gr.workflowRuns {
		s = append(s, r)
	}
	sort.Slice(s, func(i, j int) bool { return s[i].ID < s[j].ID })
	for _, r := range s {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

// ForeachJob calls fn for each job in the workflow run, sorted by job ID.
func (r *GitHubWorkflowRun) ForeachJob(fn func(*GitHubWorkflowJob) error) error {
	s := make([]*GitHubWorkflowJob, 0, len(r.Jobs))
	for _, j := range r.Jobs {
		s = append(s, j)
	}
	sort.Slice(s, func(i, j int) bool { return s[i].ID < s[j].ID })
	for _, j := range s {
		if err := fn(j); err != nil {
			return err
		}
	}
	return nil
}

// ForeachReview calls fn for each review event on the issue
//
// If the issue is not a PullRequest, then it returns early with no error.
//
// If fn returns an error, iteration ends and ForeachReview returns
// with that error.
//
// The fn function is called serially, in chronological order.
func (pr *GitHubIssue) ForeachReview(fn func(*GitHubReview) error) error {
	if !pr.IsPullRequest() {
		return nil
	}
	s := make([]*GitHubReview, 0, len(pr.reviews))
	for _, rv := range pr.reviews {
		s = append(s, rv)
	}
	sort.Slice(s, func(i, j int) bool { return s[i].Created.Before(s[j].Created) })
	for _, rv := range s {
		if err := fn(rv); err != nil {
			return err
		}
	}

	return nil
}

func (g *GitHubRepo) getOrCreateMilestone(id int64) *GitHubMilestone {
	if id == 0 {
		panic("zero id")
	}
	m, ok := g.milestones[id]
	if ok {
		return m
	}
	if g.milestones == nil {
		g.milestones = map[int64]*GitHubMilestone{}
	}
	m = &GitHubMilestone{ID: id}
	g.milestones[id] = m
	return m
}

func (g *GitHubRepo) getOrCreateLabel(id int64) *GitHubLabel {
	if id == 0 {
		panic("zero id")
	}
	lb, ok := g.labels[id]
	if ok {
		return lb
	}
	if g.labels == nil {
		g.labels = map[int64]*GitHubLabel{}
	}
	lb = &GitHubLabel{ID: id}
	g.labels[id] = lb
	return lb
}

func (g *GitHubRepo) verbose() bool {
	return g.github != nil && g.github.c != nil && g.github.c.verbose
}

// GitHubUser represents a GitHub user.
// It is a subset of https://developer.github.com/v3/users/#get-a-single-user
type GitHubUser struct {
	ID    int64
	Login string
}

// GitHubTeam represents a GitHub team.
// It is a subset of https://developer.github.com/v3/orgs/teams/#get-team
type GitHubTeam struct {
	ID int64

	// Slug is a URL-friendly representation of the team name.
	// It is unique across a GitHub organization.
	Slug string
}

// GitHubIssueRef is a reference to an issue (or pull request) number
// in a repo. These are parsed from text making references such as
// "golang/go#1234" or just "#1234" (with an implicit Repo).
type GitHubIssueRef struct {
	Repo   *GitHubRepo // must be non-nil
	Number int32       // GitHubIssue.Number
}

func (r GitHubIssueRef) String() string { return fmt.Sprintf("%s#%d", r.Repo.ID(), r.Number) }

// GitHubIssue represents a GitHub issue.
// This is maintner's in-memory representation. It differs slightly
// from the API's *github.Issue type, notably in the lack of pointers
// for all fields.
// See https://developer.github.com/v3/issues/#get-a-single-issue
type GitHubIssue struct {
	ID          int64
	Number      int32
	NotExist    bool // if true, rest of fields should be ignored.
	Closed      bool
	Locked      bool
	PullRequest *GitHubPullRequest // non-nil if this issue is a pull request
	User        *GitHubUser
	Assignees   []*GitHubUser
	Created     time.Time
	Updated     time.Time
	ClosedAt    time.Time
	ClosedBy    *GitHubUser // TODO(dmitshur): Implement (see golang.org/issue/28745).
	Title       string
	Body        string
	Milestone   *GitHubMilestone       // nil for unknown, noMilestone for none
	Labels      map[int64]*GitHubLabel // label ID => label

	commentsUpdatedTil  time.Time                   // max comment modtime seen
	commentsSyncedAsOf  time.Time                   // as of server's Date header
	comments            map[int64]*GitHubComment    // by comment.ID
	eventMaxTime        time.Time                   // latest time of any event in events map
	eventsSyncedAsOf    time.Time                   // as of server's Date header
	reviewsSyncedAsOf   time.Time                   // as of server's Date header
	events              map[int64]*GitHubIssueEvent // by event.ID
	reviews             map[int64]*GitHubReview     // by event.ID
	reactions           map[int64]*GitHubReaction   // by reaction.ID, on the issue body
	reactionsSyncedAsOf time.Time                   // as of server's Date header
	prDetailsSyncedAsOf time.Time                   // as of server's Date header
}

// IsPullRequest reports whether the issue is a pull request.
func (gi *GitHubIssue) IsPullRequest() bool {
	return gi != nil && gi.PullRequest != nil
}

// GitHubPullRequest holds PR-specific metadata for a GitHub pull request.
// All PRs are also issues; the common fields (number, title, body, labels,
// assignees, created, updated, etc.) live on the parent GitHubIssue.
type GitHubPullRequest struct {
	Issue          *GitHubIssue // back-pointer to the parent issue; always non-nil
	Draft          bool
	Merged         bool
	MergedAt       time.Time
	MergedBy       *GitHubUser
	MergeCommitSHA GitHash
	Head           GitHubPullRequestBranch
	Base           GitHubPullRequestBranch
}

// GitHubPullRequestBranch identifies a branch in a pull request (head or base).
type GitHubPullRequestBranch struct {
	Ref   string  // branch name, e.g. "main" or "feature-x"
	Hash  GitHash // commit SHA
	Owner string  // repo owner (may differ from issue owner for fork PRs)
	Repo  string  // repo name
}

// LastModified reports the most recent time that any known metadata was updated.
// In contrast to the Updated field, LastModified includes comments and events.
//
// TODO(bradfitz): this seems to not be working, at least events
// aren't updating it. Investigate.
func (gi *GitHubIssue) LastModified() time.Time {
	ret := gi.Updated
	if gi.commentsUpdatedTil.After(ret) {
		ret = gi.commentsUpdatedTil
	}
	if gi.eventMaxTime.After(ret) {
		ret = gi.eventMaxTime
	}
	return ret
}

// HasEvent reports whether there's any GitHubIssueEvent in this
// issue's history of the given type.
func (gi *GitHubIssue) HasEvent(eventType string) bool {
	for _, e := range gi.events {
		if e.Type == eventType {
			return true
		}
	}
	return false
}

// ForeachEvent calls fn for each event on the issue.
//
// If fn returns an error, iteration ends and ForeachEvent returns
// with that error.
//
// The fn function is called serially, in order of the event's time.
func (gi *GitHubIssue) ForeachEvent(fn func(*GitHubIssueEvent) error) error {
	// TODO: keep these sorted in the corpus
	s := make([]*GitHubIssueEvent, 0, len(gi.events))
	for _, e := range gi.events {
		s = append(s, e)
	}
	sort.Slice(s, func(i, j int) bool {
		ci, cj := s[i].Created, s[j].Created
		if ci.Before(cj) {
			return true
		}
		return ci.Equal(cj) && s[i].ID < s[j].ID
	})
	for _, e := range s {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

// ForeachComment calls fn for each event on the issue.
//
// If fn returns an error, iteration ends and ForeachComment returns
// with that error.
//
// The fn function is called serially, in order of the comment's time.
func (gi *GitHubIssue) ForeachComment(fn func(*GitHubComment) error) error {
	// TODO: keep these sorted in the corpus
	s := make([]*GitHubComment, 0, len(gi.comments))
	for _, e := range gi.comments {
		s = append(s, e)
	}
	sort.Slice(s, func(i, j int) bool {
		ci, cj := s[i].Created, s[j].Created
		if ci.Before(cj) {
			return true
		}
		return ci.Equal(cj) && s[i].ID < s[j].ID
	})
	for _, e := range s {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

// ForeachReaction calls fn for each reaction on the issue body,
// sorted by creation time.
func (gi *GitHubIssue) ForeachReaction(fn func(*GitHubReaction) error) error {
	s := make([]*GitHubReaction, 0, len(gi.reactions))
	for _, r := range gi.reactions {
		s = append(s, r)
	}
	sort.Slice(s, func(i, j int) bool {
		ci, cj := s[i].Created, s[j].Created
		if ci.Before(cj) {
			return true
		}
		return ci.Equal(cj) && s[i].ID < s[j].ID
	})
	for _, r := range s {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

// ForeachReaction calls fn for each reaction on the comment,
// sorted by creation time.
func (gc *GitHubComment) ForeachReaction(fn func(*GitHubReaction) error) error {
	s := make([]*GitHubReaction, 0, len(gc.reactions))
	for _, r := range gc.reactions {
		s = append(s, r)
	}
	sort.Slice(s, func(i, j int) bool {
		ci, cj := s[i].Created, s[j].Created
		if ci.Before(cj) {
			return true
		}
		return ci.Equal(cj) && s[i].ID < s[j].ID
	})
	for _, r := range s {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

func (g *GitHub) newGithubReaction(p *maintpb.GithubReaction) *GitHubReaction {
	r := &GitHubReaction{
		ID:      p.Id,
		User:    g.getOrCreateUserID(p.UserId),
		Content: p.Content,
	}
	if p.Created != nil {
		r.Created = p.Created.AsTime()
	}
	return r
}

// HasLabel reports whether the issue is labeled with the given label.
func (gi *GitHubIssue) HasLabel(label string) bool {
	for _, lb := range gi.Labels {
		if lb.Name == label {
			return true
		}
	}
	return false
}

// HasLabelID returns whether the issue has a label with the given ID.
func (gi *GitHubIssue) HasLabelID(id int64) bool {
	_, ok := gi.Labels[id]
	return ok
}

func (gi *GitHubIssue) getCreatedAt() time.Time {
	if gi == nil {
		return time.Time{}
	}
	return gi.Created
}

func (gi *GitHubIssue) getUpdatedAt() time.Time {
	if gi == nil {
		return time.Time{}
	}
	return gi.Updated
}

func (gi *GitHubIssue) getClosedAt() time.Time {
	if gi == nil {
		return time.Time{}
	}
	return gi.ClosedAt
}

// noMilestone is a sentinel value to explicitly mean no milestone.
var noMilestone = new(GitHubMilestone)

type GitHubLabel struct {
	ID   int64
	Name string
	// TODO: color?
}

// GenMutationDiff generates a diff from in-memory state 'a' (which
// may be nil) to the current (non-nil) state b from GitHub. It
// returns nil if there's no difference.
func (a *GitHubLabel) GenMutationDiff(b *github.Label) *maintpb.GithubLabel {
	id := int64(b.GetID())
	if a != nil && a.ID == id && a.Name == b.GetName() {
		// No change.
		return nil
	}
	return &maintpb.GithubLabel{Id: id, Name: b.GetName()}
}

func (lb *GitHubLabel) processMutation(mut *maintpb.GithubLabel) {
	if lb.ID == 0 {
		panic("bogus label ID 0")
	}
	if lb.ID != mut.Id {
		panic(fmt.Sprintf("label ID = %v != mutation ID = %v", lb.ID, mut.Id))
	}
	if mut.Name != "" {
		lb.Name = mut.Name
	}
}

type GitHubMilestone struct {
	ID     int64
	Title  string
	Number int32
	Closed bool
}

// IsNone reports whether ms represents the sentinel "no milestone" milestone.
func (ms *GitHubMilestone) IsNone() bool { return ms == noMilestone }

// IsUnknown reports whether ms is nil, which represents the unknown
// state. Milestones should never be in this state, though.
func (ms *GitHubMilestone) IsUnknown() bool { return ms == nil }

// emptyMilestone is a non-nil *githubMilestone with zero values for
// all fields.
var emptyMilestone = new(GitHubMilestone)

// GenMutationDiff generates a diff from in-memory state 'a' (which
// may be nil) to the current (non-nil) state b from GitHub. It
// returns nil if there's no difference.
func (a *GitHubMilestone) GenMutationDiff(b *github.Milestone) *maintpb.GithubMilestone {
	var ret *maintpb.GithubMilestone // lazily inited by diff
	diff := func() *maintpb.GithubMilestone {
		if ret == nil {
			ret = &maintpb.GithubMilestone{Id: int64(b.GetID())}
		}
		return ret
	}
	if a == nil {
		a = emptyMilestone
	}
	if a.Title != b.GetTitle() {
		diff().Title = b.GetTitle()
	}
	if a.Number != int32(b.GetNumber()) {
		diff().Number = int64(b.GetNumber())
	}
	if closed := b.GetState() == "closed"; a.Closed != closed {
		diff().Closed = &maintpb.BoolChange{Val: closed}
	}
	return ret
}

func (ms *GitHubMilestone) processMutation(mut *maintpb.GithubMilestone) {
	if ms.ID == 0 {
		panic("bogus milestone ID 0")
	}
	if ms.ID != mut.Id {
		panic(fmt.Sprintf("milestone ID = %v != mutation ID = %v", ms.ID, mut.Id))
	}
	if mut.Title != "" {
		ms.Title = mut.Title
	}
	if mut.Number != 0 {
		ms.Number = int32(mut.Number)
	}
	if mut.Closed != nil {
		ms.Closed = mut.Closed.Val
	}
}

// GitHubReview represents a review on a Pull Request.
// For more details, see https://developer.github.com/v3/pulls/reviews/
type GitHubReview struct {
	ID               int64
	Actor            *GitHubUser
	Body             string
	State            string // COMMENTED, APPROVED, CHANGES_REQUESTED
	CommitID         string
	ActorAssociation string // CONTRIBUTOR
	Created          time.Time
	OtherJSON        string
}

// Proto converts GitHubReview to a protobuf
func (e *GitHubReview) Proto() *maintpb.GithubReview {
	p := &maintpb.GithubReview{
		Id:               e.ID,
		Body:             e.Body,
		State:            e.State,
		CommitId:         e.CommitID,
		ActorAssociation: e.ActorAssociation,
	}
	if e.OtherJSON != "" {
		p.OtherJson = []byte(e.OtherJSON)
	}
	if !e.Created.IsZero() {
		p.Created = timestamppb.New(e.Created)
	}
	if e.Actor != nil {
		p.ActorId = e.Actor.ID
	}

	return p
}

// r.github.c.mu must be held.
func (r *GitHubRepo) newGithubReview(p *maintpb.GithubReview) *GitHubReview {
	g := r.github
	e := &GitHubReview{
		ID:               p.Id,
		Actor:            g.getOrCreateUserID(p.ActorId),
		ActorAssociation: p.ActorAssociation,
		CommitID:         p.CommitId,
		Body:             p.Body,
		State:            p.State,
	}

	if p.Created != nil {
		e.Created = p.Created.AsTime()
	}
	if len(p.OtherJson) > 0 {
		// TODO: parse it and see if we've since learned how
		// to deal with it?
		if r.verbose() {
			log.Printf("newGithubReview: unknown JSON in log: %s", p.OtherJson)
		}
		e.OtherJSON = string(p.OtherJson)
	}

	return e
}

type GitHubComment struct {
	ID        int64
	User      *GitHubUser
	Created   time.Time
	Updated   time.Time
	Body      string
	reactions map[int64]*GitHubReaction // by reaction.ID
}

// GitHubReaction represents an emoji reaction on an issue or comment.
type GitHubReaction struct {
	ID      int64
	User    *GitHubUser
	Content string // "+1", "-1", "laugh", "confused", "heart", "hooray", "rocket", "eyes"
	Created time.Time
}

// GitHubWorkflowRun represents a GitHub Actions workflow run.
type GitHubWorkflowRun struct {
	ID         int64
	Name       string // workflow name
	HeadBranch string
	HeadSHA    GitHash
	Event      string // "push", "pull_request", etc.
	Status     string // "completed", "in_progress", "queued"
	Conclusion string // "success", "failure", "cancelled", etc.
	WorkflowID int64
	RunNumber  int64
	RunAttempt int64
	Created    time.Time
	Updated    time.Time
	RunStarted time.Time // when execution began (Created to RunStarted = queue time)
	ActorID    int64
	HTMLURL    string
	PRNumbers  []int32 // associated pull request numbers
	Jobs       map[int64]*GitHubWorkflowJob
}

// GitHubWorkflowJob represents an individual job within a workflow run.
type GitHubWorkflowJob struct {
	ID         int64
	RunID      int64
	Name       string
	Status     string // "queued", "in_progress", "completed"
	Conclusion string // "success", "failure", etc.
	Started    time.Time
	Completed  time.Time
	RunnerName string
	Labels     []string
	Steps      []*GitHubWorkflowStep
}

// GitHubWorkflowStep represents a step within a workflow job.
type GitHubWorkflowStep struct {
	Name       string
	Status     string
	Conclusion string
	Number     int64
	Started    time.Time
	Completed  time.Time
}

// GitHubDismissedReviewEvent is the contents of a dismissed review event. For more
// details, see https://developer.github.com/v3/issues/events/.
type GitHubDismissedReviewEvent struct {
	ReviewID         int64
	State            string // commented, approved, changes_requested
	DismissalMessage string
}

type GitHubIssueEvent struct {
	// TODO: this struct is a little wide. change it to an interface
	// instead?  Maybe later, if memory profiling suggests it would help.

	// ID is the ID of the event.
	ID int64

	// Type is one of:
	// * labeled, unlabeled
	// * milestoned, demilestoned
	// * assigned, unassigned
	// * locked, unlocked
	// * closed
	// * referenced
	// * renamed
	// * reopened
	// * comment_deleted
	// * head_ref_restored
	// * base_ref_changed
	// * subscribed
	// * mentioned
	// * review_requested, review_request_removed, review_dismissed
	Type string

	// OtherJSON optionally contains a JSON object of GitHub's API
	// response for any fields maintner was unable to extract at
	// the time. It is empty if maintner supported all the fields
	// when the mutation was created.
	OtherJSON string

	Created time.Time
	Actor   *GitHubUser

	Label               string      // for type: "unlabeled", "labeled"
	Assignee            *GitHubUser // for type: "assigned", "unassigned"
	Assigner            *GitHubUser // for type: "assigned", "unassigned"
	Milestone           string      // for type: "milestoned", "demilestoned"
	From, To            string      // for type: "renamed"
	CommitID, CommitURL string      // for type: "closed", "referenced" ... ?

	Reviewer        *GitHubUser
	TeamReviewer    *GitHubTeam
	ReviewRequester *GitHubUser
	DismissedReview *GitHubDismissedReviewEvent
}

func (e *GitHubIssueEvent) Proto() *maintpb.GithubIssueEvent {
	p := &maintpb.GithubIssueEvent{
		Id:         e.ID,
		EventType:  e.Type,
		RenameFrom: e.From,
		RenameTo:   e.To,
	}
	if e.OtherJSON != "" {
		p.OtherJson = []byte(e.OtherJSON)
	}
	if !e.Created.IsZero() {
		p.Created = timestamppb.New(e.Created)
	}
	if e.Actor != nil {
		p.ActorId = e.Actor.ID
	}
	if e.Assignee != nil {
		p.AssigneeId = e.Assignee.ID
	}
	if e.Assigner != nil {
		p.AssignerId = e.Assigner.ID
	}
	if e.Label != "" {
		p.Label = &maintpb.GithubLabel{Name: e.Label}
	}
	if e.Milestone != "" {
		p.Milestone = &maintpb.GithubMilestone{Title: e.Milestone}
	}
	if e.CommitID != "" {
		c := &maintpb.GithubCommit{CommitId: e.CommitID}
		if m := rxGithubCommitURL.FindStringSubmatch(e.CommitURL); m != nil {
			c.Owner = m[1]
			c.Repo = m[2]
		}
		p.Commit = c
	}
	if e.Reviewer != nil {
		p.ReviewerId = e.Reviewer.ID
	}
	if e.TeamReviewer != nil {
		p.TeamReviewer = &maintpb.GithubTeam{
			Id:   e.TeamReviewer.ID,
			Slug: e.TeamReviewer.Slug,
		}
	}
	if e.ReviewRequester != nil {
		p.ReviewRequesterId = e.ReviewRequester.ID
	}
	if e.DismissedReview != nil {
		p.DismissedReview = &maintpb.GithubDismissedReviewEvent{
			ReviewId:         e.DismissedReview.ReviewID,
			State:            e.DismissedReview.State,
			DismissalMessage: e.DismissedReview.DismissalMessage,
		}
	}
	return p
}

var rxGithubCommitURL = regexp.MustCompile(`^https://api\.github\.com/repos/([^/]+)/([^/]+)/commits/`)

// r.github.c.mu must be held.
func (r *GitHubRepo) newGithubEvent(p *maintpb.GithubIssueEvent) *GitHubIssueEvent {
	g := r.github
	e := &GitHubIssueEvent{
		ID:              p.Id,
		Type:            p.EventType,
		Actor:           g.getOrCreateUserID(p.ActorId),
		Assignee:        g.getOrCreateUserID(p.AssigneeId),
		Assigner:        g.getOrCreateUserID(p.AssignerId),
		Reviewer:        g.getOrCreateUserID(p.ReviewerId),
		TeamReviewer:    g.getTeam(p.TeamReviewer),
		ReviewRequester: g.getOrCreateUserID(p.ReviewRequesterId),
		From:            p.RenameFrom,
		To:              p.RenameTo,
	}
	if p.Created != nil {
		e.Created = p.Created.AsTime()
	}
	if len(p.OtherJson) > 0 {
		// TODO: parse it and see if we've since learned how
		// to deal with it?
		if r.verbose() {
			log.Printf("newGithubEvent: unknown JSON in log: %s", p.OtherJson)
		}
		e.OtherJSON = string(p.OtherJson)
	}
	if p.Label != nil {
		e.Label = g.c.str(p.Label.Name)
	}
	if p.Milestone != nil {
		e.Milestone = g.c.str(p.Milestone.Title)
	}
	if c := p.Commit; c != nil {
		e.CommitID = c.CommitId
		if c.Owner != "" && c.Repo != "" {
			// TODO: this field is dumb. break it down.
			e.CommitURL = "https://api.github.com/repos/" + c.Owner + "/" + c.Repo + "/commits/" + c.CommitId
		}
	}
	if d := p.DismissedReview; d != nil {
		e.DismissedReview = &GitHubDismissedReviewEvent{
			ReviewID:         d.ReviewId,
			State:            d.State,
			DismissalMessage: d.DismissalMessage,
		}
	}
	return e
}

// (requires corpus be locked for reads)
func (gi *GitHubIssue) commentsSynced() bool {
	if gi.NotExist {
		// Issue doesn't exist, so can't sync its non-issues,
		// so consider it done.
		return true
	}
	return gi.commentsSyncedAsOf.After(gi.Updated)
}

// (requires corpus be locked for reads)
func (gi *GitHubIssue) eventsSynced() bool {
	if gi.NotExist {
		// Issue doesn't exist, so can't sync its non-issues,
		// so consider it done.
		return true
	}
	return gi.eventsSyncedAsOf.After(gi.Updated)
}

// (requires corpus be locked for reads)
func (gi *GitHubIssue) reviewsSynced() bool {
	if gi.NotExist {
		// Issue doesn't exist, so can't sync its non-issues,
		// so consider it done.
		return true
	}
	return gi.reviewsSyncedAsOf.After(gi.Updated)
}

func (c *Corpus) initGithub() {
	if c.github != nil {
		return
	}
	c.github = &GitHub{
		c:     c,
		repos: map[GitHubRepoID]*GitHubRepo{},
	}
}

// SetGitHubLimiter sets a limiter that controls the rate of requests made
// to GitHub APIs. If nil, requests are not limited. Only valid in leader mode.
// The limiter must only be set before Sync or SyncLoop is called.
func (c *Corpus) SetGitHubLimiter(l *rate.Limiter) {
	c.githubLimiter = l
}

// SetGitHubBaseTransport sets the base HTTP transport used for all GitHub
// API requests. If non-nil, this transport is used as the foundation of
// the transport chain (under oauth2, rate limiting, and caching layers).
// This allows callers to inject custom behavior such as adaptive rate
// limiting based on response headers. Only valid before Sync or SyncLoop
// is called.
func (c *Corpus) SetGitHubBaseTransport(rt http.RoundTripper) {
	c.githubBaseTransport = rt
}

// TrackGitHub registers the named GitHub repo as a repo to
// watch and append to the mutation log. Only valid in leader mode.
// The token is the auth token to use to make API calls.
func (c *Corpus) TrackGitHub(owner, repo, token string) {
	c.TrackGitHubWithTokenSource(owner, repo, oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token}))
}

// TrackGitHubWithTokenSource registers the named GitHub repo as a repo to
// watch and append to the mutation log. Only valid in leader mode.
// The tokenSource is used to obtain auth tokens for GitHub API calls.
// All default sync categories are enabled (everything except Actions).
func (c *Corpus) TrackGitHubWithTokenSource(owner, repo string, tokenSource oauth2.TokenSource) {
	c.TrackGitHubWithOptions(owner, repo, tokenSource, nil)
}

// TrackGitHubWithOptions is like TrackGitHubWithTokenSource but accepts
// a GitHubSyncFilter to control which categories of data are synced.
// A nil filter uses DefaultGitHubSyncFilter.
func (c *Corpus) TrackGitHubWithOptions(owner, repo string, tokenSource oauth2.TokenSource, filter *GitHubSyncFilter) {
	if c.mutationLogger == nil {
		panic("can't TrackGitHub in non-leader mode")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.initGithub()
	gr := c.github.getOrCreateRepo(owner, repo)
	if gr == nil {
		log.Fatalf("invalid github owner/repo %q/%q", owner, repo)
	}
	c.watchedGithubRepos = append(c.watchedGithubRepos, watchedGithubRepo{
		gr:          gr,
		tokenSource: tokenSource,
		filter:      filter,
	})
}

type watchedGithubRepo struct {
	gr          *GitHubRepo
	tokenSource oauth2.TokenSource
	filter      *GitHubSyncFilter // nil means default
}

// GitHubSyncFilter controls which categories of GitHub data are synced
// for a repo. A nil filter uses DefaultGitHubSyncFilter (everything
// except Actions). Use DefaultGitHubSyncFilter to get a filter with
// the current defaults, then customize it.
type GitHubSyncFilter struct {
	Issues    bool // issues + PRs (the issue metadata itself)
	Comments  bool // issue/PR comments
	Events    bool // issue timeline events
	Reviews   bool // PR reviews
	PRDetails bool // PR merge/branch metadata
	Reactions bool // emoji reactions
	Actions   bool // workflow runs and jobs

	// ActionsSince, if non-zero, overrides the Actions sync start time
	// instead of deriving it from the corpus. Useful for backfilling gaps.
	ActionsSince time.Time

	// ActionsBackfillGaps, if true, uses a gap-filling strategy instead
	// of linear time-window walking. It finds the largest time gaps in
	// the corpus data and fills them first, repeatedly, until all gaps
	// are under 1 hour or no new data is found.
	// ActionsSince must be set to define the oldest boundary.
	ActionsBackfillGaps bool
}

// DefaultGitHubSyncFilter returns a filter matching the current default
// behavior: everything enabled except Actions.
func DefaultGitHubSyncFilter() *GitHubSyncFilter {
	return &GitHubSyncFilter{
		Issues:    true,
		Comments:  true,
		Events:    true,
		Reviews:   true,
		PRDetails: true,
		Reactions: true,
		Actions:   false,
	}
}

// g.c.mu must be held
func (g *GitHub) getUser(pu *maintpb.GithubUser) *GitHubUser {
	if pu == nil {
		return nil
	}
	if u := g.users[pu.Id]; u != nil {
		if pu.Login != "" && pu.Login != u.Login {
			u.Login = pu.Login
		}
		return u
	}
	if g.users == nil {
		g.users = make(map[int64]*GitHubUser)
	}
	u := &GitHubUser{
		ID:    pu.Id,
		Login: pu.Login,
	}
	g.users[pu.Id] = u
	return u
}

func (g *GitHub) getOrCreateUserID(id int64) *GitHubUser {
	if id == 0 {
		return nil
	}
	if u := g.users[id]; u != nil {
		return u
	}
	if g.users == nil {
		g.users = make(map[int64]*GitHubUser)
	}
	u := &GitHubUser{ID: id}
	g.users[id] = u
	return u
}

// g.c.mu must be held
func (g *GitHub) getTeam(pt *maintpb.GithubTeam) *GitHubTeam {
	if pt == nil {
		return nil
	}
	if g.teams == nil {
		g.teams = make(map[int64]*GitHubTeam)
	}

	t := g.teams[pt.Id]
	if t == nil {
		t = &GitHubTeam{
			ID: pt.Id,
		}
		g.teams[pt.Id] = t
	}
	if pt.Slug != "" {
		t.Slug = pt.Slug
	}
	return t
}

// newGithubUserProto creates a GithubUser with the minimum diff between
// existing and g. The return value is nil if there were no changes. existing
// may also be nil.
func newGithubUserProto(existing *GitHubUser, g *github.User) *maintpb.GithubUser {
	if g == nil {
		return nil
	}
	id := int64(g.GetID())
	if existing == nil {
		return &maintpb.GithubUser{
			Id:    id,
			Login: g.GetLogin(),
		}
	}
	hasChanges := false
	u := &maintpb.GithubUser{Id: id}
	if login := g.GetLogin(); existing.Login != login {
		u.Login = login
		hasChanges = true
	}
	// Add more fields here
	if hasChanges {
		return u
	}
	return nil
}

// deletedAssignees returns an array of user ID's that are present in existing
// but not present in new.
func deletedAssignees(existing []*GitHubUser, new []*github.User) []int64 {
	mp := make(map[int64]bool, len(existing))
	for _, u := range new {
		id := int64(u.GetID())
		mp[id] = true
	}
	toDelete := []int64{}
	for _, u := range existing {
		if _, ok := mp[u.ID]; !ok {
			toDelete = append(toDelete, u.ID)
		}
	}
	return toDelete
}

// newAssignees returns an array of diffs between existing and new. New users in
// new will be present in the returned array in their entirety. Modified users
// will appear containing only the ID field and changed fields. Unmodified users
// will not appear in the returned array.
func newAssignees(existing []*GitHubUser, new []*github.User) []*maintpb.GithubUser {
	mp := make(map[int64]*GitHubUser, len(existing))
	for _, u := range existing {
		mp[u.ID] = u
	}
	changes := []*maintpb.GithubUser{}
	for _, u := range new {
		if existingUser, ok := mp[int64(u.GetID())]; ok {
			diffUser := &maintpb.GithubUser{
				Id: int64(u.GetID()),
			}
			hasDiff := false
			if login := u.GetLogin(); existingUser.Login != login {
				diffUser.Login = login
				hasDiff = true
			}
			// check more User fields for diffs here, as we add them to the proto

			if hasDiff {
				changes = append(changes, diffUser)
			}
		} else {
			changes = append(changes, &maintpb.GithubUser{
				Id:    int64(u.GetID()),
				Login: u.GetLogin(),
			})
		}
	}
	return changes
}

// setAssigneesFromProto returns a new array of assignees according to the
// instructions in new (adds or modifies users in existing), and toDelete
// (deletes them). c.mu must be held.
func (g *GitHub) setAssigneesFromProto(existing []*GitHubUser, new []*maintpb.GithubUser, toDelete []int64) []*GitHubUser {
	c := g.c
	mp := make(map[int64]*GitHubUser)
	for _, u := range existing {
		mp[u.ID] = u
	}
	for _, u := range new {
		if existingUser, ok := mp[u.Id]; ok {
			if u.Login != "" {
				existingUser.Login = u.Login
			}
			// TODO: add other fields here when we add them for user.
		} else {
			c.debugf("adding assignee %q", u.Login)
			existing = append(existing, g.getUser(u))
		}
	}
	// this is quadratic but the number of assignees is very unlikely to exceed,
	// say, 5.
	existing = slices.DeleteFunc(existing, func(u *GitHubUser) bool {
		return slices.Contains(toDelete, u.ID)
	})
	return existing
}

// githubIssueDiffer generates a minimal diff (protobuf mutation) to
// get a GitHub Issue from its in-memory state 'a' to the current
// GitHub API state 'b'.
type githubIssueDiffer struct {
	gr *GitHubRepo
	a  *GitHubIssue  // may be nil if no current state
	b  *github.Issue // may NOT be nil
}

// Diff returns a minimal mutation and a summary of what changed.
// Returns (nil, "") if no changes.
func (d githubIssueDiffer) Diff() (*maintpb.GithubIssueMutation, string) {
	m := &maintpb.GithubIssueMutation{
		Owner:       d.gr.id.Owner,
		Repo:        d.gr.id.Repo,
		Number:      int32(d.b.GetNumber()),
		PullRequest: d.b.IsPullRequest(),
	}
	var fields []string
	for _, f := range issueDiffMethods {
		if f(d, m) {
			fname := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
			fname = strings.TrimPrefix(fname, "github.com/bradfitz/gxbm/maintner.githubIssueDiffer.")
			fname = strings.TrimPrefix(fname, "diff")
			fields = append(fields, fname)
		}
	}
	if len(fields) == 0 {
		return nil, ""
	}
	return m, strings.Join(fields, ",")
}

// issueDiffMethods are the different steps githubIssueDiffer.Diff
// goes through to compute a diff. The methods should return true if
// any change was made. The order is irrelevant unless otherwise
// documented in comments in the list below.
var issueDiffMethods = []func(githubIssueDiffer, *maintpb.GithubIssueMutation) bool{
	githubIssueDiffer.diffCreatedAt,
	githubIssueDiffer.diffUpdatedAt,
	githubIssueDiffer.diffUser,
	githubIssueDiffer.diffBody,
	githubIssueDiffer.diffTitle,
	githubIssueDiffer.diffMilestone,
	githubIssueDiffer.diffAssignees,
	githubIssueDiffer.diffClosedState,
	githubIssueDiffer.diffClosedAt,
	githubIssueDiffer.diffClosedBy,
	githubIssueDiffer.diffLockedState,
	githubIssueDiffer.diffLabels,
	githubIssueDiffer.diffDraft,
	githubIssueDiffer.diffMergedAt,
}

func (d githubIssueDiffer) diffCreatedAt(m *maintpb.GithubIssueMutation) bool {
	return d.diffTimeField(&m.Created, d.a.getCreatedAt(), d.b.GetCreatedAt().Time)
}

func (d githubIssueDiffer) diffUpdatedAt(m *maintpb.GithubIssueMutation) bool {
	return d.diffTimeField(&m.Updated, d.a.getUpdatedAt(), d.b.GetUpdatedAt().Time)
}

func (d githubIssueDiffer) diffClosedAt(m *maintpb.GithubIssueMutation) bool {
	return d.diffTimeField(&m.ClosedAt, d.a.getClosedAt(), d.b.GetClosedAt().Time)
}

func (d githubIssueDiffer) diffTimeField(dst **timestamppb.Timestamp, memTime, githubTime time.Time) bool {
	if githubTime.IsZero() || memTime.Equal(githubTime) {
		return false
	}
	*dst = timestamppb.New(githubTime)
	return true
}

func (d githubIssueDiffer) diffUser(m *maintpb.GithubIssueMutation) bool {
	var existing *GitHubUser
	if d.a != nil {
		existing = d.a.User
	}
	m.User = newGithubUserProto(existing, d.b.User)
	return m.User != nil
}

func (d githubIssueDiffer) diffClosedBy(m *maintpb.GithubIssueMutation) bool {
	var existing *GitHubUser
	if d.a != nil {
		existing = d.a.ClosedBy
	}
	m.ClosedBy = newGithubUserProto(existing, d.b.ClosedBy)
	return m.ClosedBy != nil
}

func (d githubIssueDiffer) diffBody(m *maintpb.GithubIssueMutation) bool {
	if d.a != nil && d.a.Body == d.b.GetBody() {
		return false
	}
	m.BodyChange = &maintpb.StringChange{Val: d.b.GetBody()}
	return true
}

func (d githubIssueDiffer) diffTitle(m *maintpb.GithubIssueMutation) bool {
	if d.a != nil && d.a.Title == d.b.GetTitle() {
		return false
	}
	m.Title = d.b.GetTitle()
	// TODO: emit a StringChange if we ever have a problem that we
	// legitimately need real issues with no titles reflected in
	// maintner's model. For now just ignore such changes, if
	// GitHub even permits the.
	return m.Title != ""
}

func (d githubIssueDiffer) diffMilestone(m *maintpb.GithubIssueMutation) bool {
	if d.a != nil && d.a.Milestone != nil {
		ma, mb := d.a.Milestone, d.b.Milestone
		if ma == noMilestone && d.b.Milestone == nil {
			// Unchanged. Still no milestone.
			return false
		}
		if mb != nil && ma.ID == int64(mb.GetID()) {
			// Unchanged. Same milestone.
			// TODO: detect milestone renames and emit mutation for that?
			return false
		}

	}
	if mb := d.b.Milestone; mb != nil {
		m.MilestoneId = int64(mb.GetID())
		m.MilestoneNum = int64(mb.GetNumber())
		m.MilestoneTitle = mb.GetTitle()
	} else {
		m.NoMilestone = true
	}
	return true
}

func (d githubIssueDiffer) diffAssignees(m *maintpb.GithubIssueMutation) bool {
	if d.a == nil {
		m.Assignees = newAssignees(nil, d.b.Assignees)
		return true
	}
	m.Assignees = newAssignees(d.a.Assignees, d.b.Assignees)
	m.DeletedAssignees = deletedAssignees(d.a.Assignees, d.b.Assignees)
	return len(m.Assignees) > 0 || len(m.DeletedAssignees) > 0
}

func (d githubIssueDiffer) diffLabels(m *maintpb.GithubIssueMutation) bool {
	// Common case: no changes. Return false quickly without allocations.
	if d.a != nil && len(d.a.Labels) == len(d.b.Labels) {
		missing := false
		for _, gl := range d.b.Labels {
			if _, ok := d.a.Labels[int64(gl.GetID())]; !ok {
				missing = true
				break
			}
		}
		if !missing {
			return false
		}
	}

	toAdd := map[int64]*maintpb.GithubLabel{}
	for _, gl := range d.b.Labels {
		id := int64(gl.GetID())
		if id == 0 {
			panic("zero label ID")
		}
		toAdd[id] = &maintpb.GithubLabel{Id: id, Name: gl.GetName()}
	}

	var toDelete []int64
	if d.a != nil {
		for id := range d.a.Labels {
			if _, ok := toAdd[id]; ok {
				// Already had it.
				delete(toAdd, id)
			} else {
				// We had it, but no longer.
				toDelete = append(toDelete, id)
			}
		}
	}

	m.RemoveLabel = toDelete
	for _, labpb := range toAdd {
		m.AddLabel = append(m.AddLabel, labpb)
	}

	return len(m.RemoveLabel) > 0 || len(m.AddLabel) > 0
}

func (d githubIssueDiffer) diffClosedState(m *maintpb.GithubIssueMutation) bool {
	bclosed := d.b.GetState() == "closed"
	if d.a != nil && d.a.Closed == bclosed {
		return false
	}
	m.Closed = &maintpb.BoolChange{Val: bclosed}
	return true
}

func (d githubIssueDiffer) diffLockedState(m *maintpb.GithubIssueMutation) bool {
	if d.a != nil && d.a.Locked == d.b.GetLocked() {
		return false
	}
	if d.a == nil && !d.b.GetLocked() {
		return false
	}
	m.Locked = &maintpb.BoolChange{Val: d.b.GetLocked()}
	return true
}

func (d githubIssueDiffer) diffDraft(m *maintpb.GithubIssueMutation) bool {
	if !d.b.IsPullRequest() {
		return false
	}
	bdraft := d.b.GetDraft()
	if d.a != nil && d.a.PullRequest != nil && d.a.PullRequest.Draft == bdraft {
		return false
	}
	if d.a != nil && d.a.PullRequest == nil && !bdraft {
		return false
	}
	m.Draft = &maintpb.BoolChange{Val: bdraft}
	return true
}

func (d githubIssueDiffer) diffMergedAt(m *maintpb.GithubIssueMutation) bool {
	if d.b.PullRequestLinks == nil {
		return false
	}
	mergedAt := d.b.PullRequestLinks.GetMergedAt().Time
	if mergedAt.IsZero() {
		return false
	}
	if d.a != nil && d.a.PullRequest != nil && d.a.PullRequest.MergedAt.Equal(mergedAt) {
		return false
	}
	m.MergedAt = timestamppb.New(mergedAt)
	return true
}

// newMutationFromIssue generates a GithubIssueMutation using the
// smallest possible diff between a (the state we have in memory in
// the corpus) and b (the current GitHub API state).
//
// If newMutationFromIssue returns nil, the provided github.Issue is no newer
// than the data we have in the corpus. 'a' may be nil.
//
// The returned summary describes which fields changed (e.g. "Title,Body,Labels").
func (r *GitHubRepo) newMutationFromIssue(a *GitHubIssue, b *github.Issue) (mut *maintpb.Mutation, summary string) {
	if b == nil || b.Number == nil {
		panic(fmt.Sprintf("github issue with nil number: %#v", b))
	}
	gim, summary := githubIssueDiffer{gr: r, a: a, b: b}.Diff()
	if gim == nil {
		return nil, ""
	}
	return &maintpb.Mutation{GithubIssue: gim}, summary
}

// processGithubMutation updates the corpus with the information in m.
func (c *Corpus) processGithubMutation(m *maintpb.GithubMutation) {
	if c == nil {
		panic("nil corpus")
	}
	c.initGithub()
	gr := c.github.getOrCreateRepo(m.Owner, m.Repo)
	if gr == nil {
		log.Printf("bogus Owner/Repo %q/%q in mutation: %v", m.Owner, m.Repo, m)
		return
	}
	for _, lp := range m.Labels {
		lb := gr.getOrCreateLabel(lp.Id)
		lb.processMutation(lp)
	}
	for _, mp := range m.Milestones {
		ms := gr.getOrCreateMilestone(mp.Id)
		ms.processMutation(mp)
	}
}

// processGithubIssueMutation updates the corpus with the information in m.
func (c *Corpus) processGithubIssueMutation(m *maintpb.GithubIssueMutation) {
	if c == nil {
		panic("nil corpus")
	}
	c.initGithub()
	gr := c.github.getOrCreateRepo(m.Owner, m.Repo)
	if gr == nil {
		log.Printf("bogus Owner/Repo %q/%q in mutation: %v", m.Owner, m.Repo, m)
		return
	}
	if m.Number == 0 {
		log.Printf("bogus zero Number in mutation: %v", m)
		return
	}
	gi, ok := gr.issues[m.Number]
	if !ok {
		gi = &GitHubIssue{
			// User added below
			Number: m.Number,
			ID:     m.Id,
		}
		if gr.issues == nil {
			gr.issues = make(map[int32]*GitHubIssue)
		}
		gr.issues[m.Number] = gi

		if m.NotExist {
			gi.NotExist = true
			return
		}

		gi.Created = m.Created.AsTime()
	}
	if m.NotExist != gi.NotExist {
		gi.NotExist = m.NotExist
	}
	if gi.NotExist {
		return
	}

	// Check Updated before all other fields so they don't update if this
	// Mutation is stale
	// (ignoring Created since it *should* never update)
	if m.Updated != nil {
		gi.Updated = m.Updated.AsTime()
	}
	if m.ClosedAt != nil {
		gi.ClosedAt = m.ClosedAt.AsTime()
	}
	if m.User != nil {
		gi.User = c.github.getUser(m.User)
	}
	if m.NoMilestone {
		gi.Milestone = noMilestone
	} else if m.MilestoneId != 0 {
		ms := gr.getOrCreateMilestone(m.MilestoneId)
		ms.processMutation(&maintpb.GithubMilestone{
			Id:     m.MilestoneId,
			Title:  m.MilestoneTitle,
			Number: m.MilestoneNum,
		})
		gi.Milestone = ms
	}
	if m.ClosedBy != nil {
		gi.ClosedBy = c.github.getUser(m.ClosedBy)
	}
	if b := m.Closed; b != nil {
		gi.Closed = b.Val
	}
	if b := m.Locked; b != nil {
		gi.Locked = b.Val
	}
	if m.PullRequest {
		if gi.PullRequest == nil {
			gi.PullRequest = &GitHubPullRequest{Issue: gi}
		}
	}
	if pr := gi.PullRequest; pr != nil {
		if b := m.Draft; b != nil {
			pr.Draft = b.Val
		}
		if b := m.Merged; b != nil {
			pr.Merged = b.Val
		}
		if m.MergedAt != nil {
			pr.MergedAt = m.MergedAt.AsTime()
		}
		if m.MergedBy != nil {
			pr.MergedBy = c.github.getUser(m.MergedBy)
		}
		if m.MergeCommitHash != "" {
			pr.MergeCommitSHA = GitHash(m.MergeCommitHash)
		}
		if m.Head != nil {
			pr.Head = GitHubPullRequestBranch{
				Ref:   m.Head.Ref,
				Hash:  GitHash(m.Head.Hash),
				Owner: m.Head.Owner,
				Repo:  m.Head.Repo,
			}
		}
		if m.Base != nil {
			pr.Base = GitHubPullRequestBranch{
				Ref:   m.Base.Ref,
				Hash:  GitHash(m.Base.Hash),
				Owner: m.Base.Owner,
				Repo:  m.Base.Repo,
			}
		}
		if m.PrDetailStatus != nil && m.PrDetailStatus.ServerDate != nil {
			gi.prDetailsSyncedAsOf = m.PrDetailStatus.ServerDate.AsTime().UTC()
		}
	}

	gi.Assignees = c.github.setAssigneesFromProto(gi.Assignees, m.Assignees, m.DeletedAssignees)

	if m.Body != "" {
		gi.Body = m.Body
	}
	if m.BodyChange != nil {
		gi.Body = m.BodyChange.Val
	}
	if m.Title != "" {
		gi.Title = m.Title
	}
	if len(m.RemoveLabel) > 0 || len(m.AddLabel) > 0 {
		if gi.Labels == nil {
			gi.Labels = make(map[int64]*GitHubLabel)
		}
		for _, lid := range m.RemoveLabel {
			delete(gi.Labels, lid)
		}
		for _, lp := range m.AddLabel {
			lb := gr.getOrCreateLabel(lp.Id)
			lb.processMutation(lp)
			gi.Labels[lp.Id] = lb
		}
	}

	for _, cmut := range m.Comment {
		if cmut.Id == 0 {
			log.Printf("Ignoring bogus comment mutation lacking Id: %v", cmut)
			continue
		}
		gc, ok := gi.comments[cmut.Id]
		if !ok {
			if gi.comments == nil {
				gi.comments = make(map[int64]*GitHubComment)
			}
			gc = &GitHubComment{ID: cmut.Id}
			gi.comments[gc.ID] = gc
		}
		if cmut.User != nil {
			gc.User = c.github.getUser(cmut.User)
		}
		if cmut.Created != nil {
			gc.Created = cmut.Created.AsTime().UTC()
		}
		if cmut.Updated != nil {
			gc.Updated = cmut.Updated.AsTime().UTC()
		}
		if cmut.Body != "" {
			gc.Body = cmut.Body
		}
		for _, rmut := range cmut.Reaction {
			if rmut.Id == 0 {
				continue
			}
			if gc.reactions == nil {
				gc.reactions = make(map[int64]*GitHubReaction)
			}
			gc.reactions[rmut.Id] = c.github.newGithubReaction(rmut)
		}
		for _, rid := range cmut.RemovedReactionId {
			delete(gc.reactions, rid)
		}
	}
	if m.CommentStatus != nil && m.CommentStatus.ServerDate != nil {
		gi.commentsSyncedAsOf = m.CommentStatus.ServerDate.AsTime().UTC()
	}

	for _, emut := range m.Event {
		if emut.Id == 0 {
			log.Printf("Ignoring bogus event mutation lacking Id: %v", emut)
			continue
		}
		if gi.events == nil {
			gi.events = make(map[int64]*GitHubIssueEvent)
		}
		gie := gr.newGithubEvent(emut)
		gi.events[emut.Id] = gie
		if gie.Created.After(gi.eventMaxTime) {
			gi.eventMaxTime = gie.Created
		}
	}
	if m.EventStatus != nil && m.EventStatus.ServerDate != nil {
		gi.eventsSyncedAsOf = m.EventStatus.ServerDate.AsTime().UTC()
	}

	for _, rmut := range m.Review {
		if rmut.Id == 0 {
			log.Printf("Ignoring bogus review mutation lacking Id: %v", rmut)
			continue
		}
		if gi.reviews == nil {
			gi.reviews = make(map[int64]*GitHubReview)
		}
		gre := gr.newGithubReview(rmut)
		gi.reviews[rmut.Id] = gre
		if gre.Created.After(gi.eventMaxTime) {
			gi.eventMaxTime = gre.Created
		}
	}
	if m.ReviewStatus != nil && m.ReviewStatus.ServerDate != nil {
		gi.reviewsSyncedAsOf = m.ReviewStatus.ServerDate.AsTime().UTC()
	}

	for _, rmut := range m.Reaction {
		if rmut.Id == 0 {
			continue
		}
		if gi.reactions == nil {
			gi.reactions = make(map[int64]*GitHubReaction)
		}
		gi.reactions[rmut.Id] = c.github.newGithubReaction(rmut)
	}
	for _, rid := range m.RemovedReactionId {
		delete(gi.reactions, rid)
	}
	if m.ReactionStatus != nil && m.ReactionStatus.ServerDate != nil {
		gi.reactionsSyncedAsOf = m.ReactionStatus.ServerDate.AsTime().UTC()
	}
}

// githubCache is an httpcache.Cache wrapper that only
// stores responses for:
//   - https://api.github.com/repos/$OWNER/$REPO/issues?direction=desc&page=1&sort=updated
//   - https://api.github.com/repos/$OWNER/$REPO/milestones?page=1
//   - https://api.github.com/repos/$OWNER/$REPO/labels?page=1
type githubCache struct {
	httpcache.Cache
}

var rxGithubCacheURLs = regexp.MustCompile(`^https://api.github.com/repos/\w+/\w+/(issues|milestones|labels)\?(.+)`)

func cacheableURL(urlStr string) bool {
	m := rxGithubCacheURLs.FindStringSubmatch(urlStr)
	if m == nil {
		return false
	}
	v, _ := url.ParseQuery(m[2])
	if v.Get("page") != "1" {
		return false
	}
	switch m[1] {
	case "issues":
		return v.Get("sort") == "updated" && v.Get("direction") == "desc"
	case "milestones", "labels":
		return true
	default:
		panic("unexpected cache key base " + m[1])
	}
}

func (c *githubCache) Set(urlKey string, res []byte) {
	// TODO: verify that the httpcache package guarantees that the
	// first string parameter to Set here is actually a
	// URL. Empirically they appear to be.
	if cacheableURL(urlKey) {
		c.Cache.Set(urlKey, res)
	}
}

// sync checks for new changes on a single GitHub repository and
// updates the Corpus with any changes. If loop is true, it runs
// forever.
func (gr *GitHubRepo) sync(ctx context.Context, tokenSource oauth2.TokenSource, filter *GitHubSyncFilter, loop bool) error {
	base := gr.github.c.githubBaseTransport
	if base == nil {
		base = http.DefaultTransport
	}
	authTransport := &oauth2.Transport{
		Source: tokenSource,
		Base:   base,
	}
	directTransport := http.RoundTripper(authTransport)
	if gr.github.c.githubLimiter != nil {
		directTransport = limitTransport{gr.github.c.githubLimiter, directTransport}
	}
	cachingTransport := &httpcache.Transport{
		Transport:           directTransport,
		Cache:               &githubCache{Cache: httpcache.NewMemoryCache()},
		MarkCachedResponses: true, // adds "X-From-Cache: 1" response header.
	}

	p := &githubRepoPoller{
		c:             gr.github.c,
		tokenSource:   tokenSource,
		gr:            gr,
		syncFilter:    filter,
		githubDirect:  github.NewClient(&http.Client{Transport: directTransport}),
		githubCaching: github.NewClient(&http.Client{Transport: cachingTransport}),
		client:        &http.Client{Transport: directTransport},
	}
	activityCh := gr.github.c.activityChan("github:" + gr.id.String())
	var expectChanges bool // got webhook update, but haven't seen new data yet
	var sleepDelay time.Duration
	for {
		prevLastUpdate := p.lastUpdate
		err := p.sync(ctx, expectChanges)
		if err == context.Canceled || !loop {
			return err
		}
		sawChanges := !p.lastUpdate.Equal(prevLastUpdate)
		if sawChanges {
			expectChanges = false
		}
		// If we got woken up by a webhook, sometimes
		// immediately polling GitHub for the data results in
		// a cache hit saying nothing's changed. Don't believe
		// it. Polling quickly with exponential backoff until
		// we see what we're expecting.
		if expectChanges {
			if sleepDelay == 0 {
				sleepDelay = 1 * time.Second
			} else {
				sleepDelay *= 2
				if sleepDelay > 15*time.Minute {
					sleepDelay = 15 * time.Minute
				}
			}
			p.logf("expect changes; re-polling in %v", sleepDelay)
		} else {
			sleepDelay = 15 * time.Minute
		}
		p.logf("sync = %v; sleeping", err)
		timer := time.NewTimer(sleepDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-activityCh:
			timer.Stop()
			expectChanges = true
			sleepDelay = 0
		case <-timer.C:
		}
	}
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// A githubRepoPoller updates the Corpus (gr.c) to have the latest
// version of the GitHub repo rp, using the GitHub client ghc.
type githubRepoPoller struct {
	c             *Corpus // shortcut for gr.github.c
	gr            *GitHubRepo
	tokenSource   oauth2.TokenSource
	syncFilter    *GitHubSyncFilter // nil means default
	lastUpdate    time.Time         // modified by sync
	githubCaching *github.Client
	githubDirect  *github.Client // not caching
	client        httpClient     // the client used to poll github

	// Reaction scanning state. These are transient per-sync-cycle maps
	// populated during syncIssues/syncComments to detect reaction changes
	// opportunistically from REST responses.
	staleReactionIssues map[int32]bool // issue numbers needing reaction detail fetch
	lastReactionScan    time.Time      // when the last full GraphQL scan ran
}

// filter returns the effective sync filter, defaulting if nil.
func (p *githubRepoPoller) filter() *GitHubSyncFilter {
	if p.syncFilter != nil {
		return p.syncFilter
	}
	return DefaultGitHubSyncFilter()
}

func (p *githubRepoPoller) Owner() string { return p.gr.id.Owner }
func (p *githubRepoPoller) Repo() string  { return p.gr.id.Repo }

func (p *githubRepoPoller) logf(format string, args ...any) {
	log.Printf("sync github "+p.gr.id.String()+": "+format, args...)
}

func (p *githubRepoPoller) sync(ctx context.Context, expectChanges bool) error {
	p.logf("Beginning sync.")
	f := p.filter()
	if f.Issues {
		p.staleReactionIssues = make(map[int32]bool) // reset per cycle
		if err := p.syncIssues(ctx, expectChanges); err != nil {
			return err
		}
	}
	if f.Comments {
		if err := p.syncComments(ctx); err != nil {
			return err
		}
	}
	if f.Events {
		if err := p.syncEvents(ctx); err != nil {
			return err
		}
	}
	if f.Reviews {
		if err := p.syncReviews(ctx); err != nil {
			return err
		}
	}
	if f.PRDetails {
		if err := p.syncPullRequests(ctx); err != nil {
			return err
		}
	}
	if f.Reactions {
		if err := p.syncReactions(ctx); err != nil {
			return err
		}
	}
	if f.Actions {
		if err := p.syncActions(ctx); err != nil {
			return err
		}
	}
	return nil
}

// syncPullRequests fetches PR-specific details (merge info, head/base branches)
// for pull requests that need updating. It uses the Pulls API which provides
// data not available from the Issues API.
func (p *githubRepoPoller) syncPullRequests(ctx context.Context) error {
	nums := p.issueNumbersWithStalePRDetails()
	if len(nums) == 0 {
		return nil
	}
	p.logf("syncing PR details for %d pull requests", len(nums))
	for _, num := range nums {
		if err := p.syncPullRequestDetails(ctx, num); err != nil {
			return err
		}
	}
	return nil
}

func (p *githubRepoPoller) issueNumbersWithStalePRDetails() (issueNums []int32) {
	p.c.mu.RLock()
	defer p.c.mu.RUnlock()
	for n, gi := range p.gr.issues {
		if gi.IsPullRequest() && !gi.prDetailsSyncedAsOf.After(gi.Updated) {
			issueNums = append(issueNums, n)
		}
	}
	slices.Sort(issueNums)
	return issueNums
}

func (p *githubRepoPoller) syncPullRequestDetails(ctx context.Context, issueNum int32) error {
	pr, resp, err := p.githubDirect.PullRequests.Get(ctx, p.Owner(), p.Repo(), int(issueNum))
	if isIssueGoneError(err) {
		p.logf("PR %d is gone (404/410/301), marking as NotExist", issueNum)
		p.c.addMutation(&maintpb.Mutation{
			GithubIssue: &maintpb.GithubIssueMutation{
				Owner:    p.Owner(),
				Repo:     p.Repo(),
				Number:   issueNum,
				NotExist: true,
			},
		})
		return nil
	}
	if err != nil {
		return fmt.Errorf("fetching PR %d: %w", issueNum, err)
	}

	serverDate, err := http.ParseTime(resp.Header.Get("Date"))
	if err != nil {
		serverDate = time.Now().UTC()
	}

	p.c.mu.RLock()
	gi := p.gr.issues[issueNum]
	p.c.mu.RUnlock()
	if gi == nil || !gi.IsPullRequest() {
		return nil
	}

	mut := &maintpb.GithubIssueMutation{
		Owner:       p.Owner(),
		Repo:        p.Repo(),
		Number:      issueNum,
		PullRequest: true,
	}

	var changed bool

	// Merged status.
	merged := pr.GetMerged()
	if gi.PullRequest.Merged != merged {
		mut.Merged = &maintpb.BoolChange{Val: merged}
		changed = true
	}

	// Draft status.
	draft := pr.GetDraft()
	if gi.PullRequest.Draft != draft {
		mut.Draft = &maintpb.BoolChange{Val: draft}
		changed = true
	}

	// Merge commit SHA.
	if sha := pr.GetMergeCommitSHA(); sha != "" && GitHash(sha) != gi.PullRequest.MergeCommitSHA {
		mut.MergeCommitHash = sha
		changed = true
	}

	// Merged by.
	if mb := pr.MergedBy; mb != nil {
		if gi.PullRequest.MergedBy == nil || gi.PullRequest.MergedBy.ID != mb.GetID() {
			mut.MergedBy = &maintpb.GithubUser{
				Id:    mb.GetID(),
				Login: mb.GetLogin(),
			}
			changed = true
		}
	}

	// Head branch.
	if h := pr.Head; h != nil {
		newHead := prBranchFromAPI(h)
		if newHead != gi.PullRequest.Head {
			mut.Head = newHead.proto()
			changed = true
		}
	}

	// Base branch.
	if b := pr.Base; b != nil {
		newBase := prBranchFromAPI(b)
		if newBase != gi.PullRequest.Base {
			mut.Base = newBase.proto()
			changed = true
		}
	}

	mut.PrDetailStatus = &maintpb.GithubIssueSyncStatus{
		ServerDate: timestamppb.New(serverDate),
	}

	if changed {
		p.c.addMutation(&maintpb.Mutation{GithubIssue: mut})
	} else {
		// Even with no data changes, record that we checked.
		p.c.addMutation(&maintpb.Mutation{GithubIssue: &maintpb.GithubIssueMutation{
			Owner:       p.Owner(),
			Repo:        p.Repo(),
			Number:      issueNum,
			PullRequest: true,
			PrDetailStatus: &maintpb.GithubIssueSyncStatus{
				ServerDate: timestamppb.New(serverDate),
			},
		}})
	}
	return nil
}

func prBranchFromAPI(b *github.PullRequestBranch) GitHubPullRequestBranch {
	br := GitHubPullRequestBranch{
		Ref:  b.GetRef(),
		Hash: GitHash(b.GetSHA()),
	}
	if r := b.Repo; r != nil {
		if owner := r.Owner; owner != nil {
			br.Owner = owner.GetLogin()
		}
		br.Repo = r.GetName()
	}
	return br
}

func (b GitHubPullRequestBranch) proto() *maintpb.GithubPullRequestBranch {
	return &maintpb.GithubPullRequestBranch{
		Ref:   b.Ref,
		Hash:  string(b.Hash),
		Owner: b.Owner,
		Repo:  b.Repo,
	}
}

// processGithubActionsMutation updates the corpus with Actions data.
func (c *Corpus) processGithubActionsMutation(m *maintpb.GithubActionsMutation) {
	if c == nil {
		panic("nil corpus")
	}
	c.initGithub()
	gr := c.github.getOrCreateRepo(m.Owner, m.Repo)
	if gr == nil {
		log.Printf("bogus Owner/Repo %q/%q in actions mutation", m.Owner, m.Repo)
		return
	}

	if rm := m.Run; rm != nil {
		if gr.workflowRuns == nil {
			gr.workflowRuns = make(map[int64]*GitHubWorkflowRun)
		}
		run, ok := gr.workflowRuns[rm.Id]
		if !ok {
			run = &GitHubWorkflowRun{ID: rm.Id}
			gr.workflowRuns[rm.Id] = run
		}
		if rm.Name != "" {
			run.Name = rm.Name
		}
		if rm.HeadBranch != "" {
			run.HeadBranch = rm.HeadBranch
		}
		if rm.HeadHash != "" {
			run.HeadSHA = GitHash(rm.HeadHash)
		}
		if rm.Event != "" {
			run.Event = rm.Event
		}
		if rm.Status != "" {
			run.Status = rm.Status
		}
		if rm.Conclusion != "" {
			run.Conclusion = rm.Conclusion
		}
		if rm.WorkflowId != 0 {
			run.WorkflowID = rm.WorkflowId
		}
		if rm.RunNumber != 0 {
			run.RunNumber = rm.RunNumber
		}
		if rm.RunAttempt != 0 {
			run.RunAttempt = rm.RunAttempt
		}
		if rm.Created != nil {
			run.Created = rm.Created.AsTime()
		}
		if rm.Updated != nil {
			run.Updated = rm.Updated.AsTime()
		}
		if rm.RunStarted != nil {
			run.RunStarted = rm.RunStarted.AsTime()
		}
		if rm.ActorId != 0 {
			run.ActorID = rm.ActorId
		}
		if rm.Url != "" {
			run.HTMLURL = rm.Url
		}
		if len(rm.PullRequestNumbers) > 0 {
			run.PRNumbers = make([]int32, len(rm.PullRequestNumbers))
			for i, n := range rm.PullRequestNumbers {
				run.PRNumbers[i] = int32(n)
			}
		}
	}

	if jm := m.Job; jm != nil {
		run := gr.workflowRuns[jm.RunId]
		if run == nil {
			// Job arrived before its run; create a placeholder.
			if gr.workflowRuns == nil {
				gr.workflowRuns = make(map[int64]*GitHubWorkflowRun)
			}
			run = &GitHubWorkflowRun{ID: jm.RunId}
			gr.workflowRuns[jm.RunId] = run
		}
		if run.Jobs == nil {
			run.Jobs = make(map[int64]*GitHubWorkflowJob)
		}
		job, ok := run.Jobs[jm.Id]
		if !ok {
			job = &GitHubWorkflowJob{ID: jm.Id, RunID: jm.RunId}
			run.Jobs[jm.Id] = job
		}
		if jm.Name != "" {
			job.Name = jm.Name
		}
		if jm.Status != "" {
			job.Status = jm.Status
		}
		if jm.Conclusion != "" {
			job.Conclusion = jm.Conclusion
		}
		if jm.Started != nil {
			job.Started = jm.Started.AsTime()
		}
		if jm.Completed != nil {
			job.Completed = jm.Completed.AsTime()
		}
		if jm.RunnerName != "" {
			job.RunnerName = jm.RunnerName
		}
		if len(jm.Labels) > 0 {
			job.Labels = jm.Labels
		}
		if len(jm.Step) > 0 {
			job.Steps = make([]*GitHubWorkflowStep, len(jm.Step))
			for i, sm := range jm.Step {
				job.Steps[i] = &GitHubWorkflowStep{
					Name:       sm.Name,
					Status:     sm.Status,
					Conclusion: sm.Conclusion,
					Number:     sm.Number,
				}
				if sm.Started != nil {
					job.Steps[i].Started = sm.Started.AsTime()
				}
				if sm.Completed != nil {
					job.Steps[i].Completed = sm.Completed.AsTime()
				}
			}
		}
	}
}

// syncActions fetches GitHub Actions workflow runs and jobs.
func (p *githubRepoPoller) syncActions(ctx context.Context) error {
	return p.syncWorkflowRuns(ctx)
}

// maxActionsAge is how far back to sync workflow runs on initial sync.
const maxActionsAge = 6 * 30 * 24 * time.Hour // ~6 months

func (p *githubRepoPoller) syncWorkflowRuns(ctx context.Context) error {
	p.logf("syncing Actions workflow runs")

	f := p.filter()
	if f.ActionsBackfillGaps {
		return p.syncWorkflowRunsBackfillGaps(ctx)
	}

	now := time.Now().UTC()

	// Determine start time: explicit override > corpus max > 6 months ago.
	var since time.Time
	if !f.ActionsSince.IsZero() {
		since = f.ActionsSince
		p.logf("Actions: backfill from %s (--actions-since override, %d runs in corpus)",
			since.Format("2006-01-02"), len(p.gr.workflowRuns))
	} else {
		p.c.mu.RLock()
		for _, run := range p.gr.workflowRuns {
			if run.Created.After(since) {
				since = run.Created
			}
		}
		p.c.mu.RUnlock()

		if since.IsZero() {
			since = now.Add(-maxActionsAge)
			p.logf("Actions: initial sync, fetching runs back to %s", since.Format("2006-01-02"))
		} else {
			// Back up one day to catch any runs created near the boundary
			// that might have been updated since.
			since = since.Add(-24 * time.Hour)
			p.logf("Actions: incremental sync from %s (%d runs in corpus)",
				since.Format("2006-01-02"), len(p.gr.workflowRuns))
		}
	}

	// Walk forward in 1-day windows from `since` to now.
	// The GitHub API caps results at 1000 per query (10 pages of 100),
	// so daily windows are needed for repos with high CI volume.
	var totalNew, totalSkipped int

	// Use a stack of time windows. Start with daily windows.
	// If any window hits the 1000-result API cap, split it in half.
	type window struct{ start, end time.Time }
	var windows []window
	for ws := since; ws.Before(now); ws = ws.Add(24 * time.Hour) {
		we := ws.Add(24 * time.Hour)
		if we.After(now) {
			we = now
		}
		windows = append(windows, window{ws, we})
	}

	for len(windows) > 0 {
		w := windows[0]
		windows = windows[1:]

		windowNew, windowTotal, err := p.syncWorkflowRunsWindow(ctx, w.start, w.end, &totalNew, &totalSkipped)
		if err != nil {
			return err
		}

		// If we hit the 1000-result cap and the window is wider than 1 hour,
		// split it in half and retry both halves.
		if windowTotal >= 1000 && w.end.Sub(w.start) > time.Hour {
			mid := w.start.Add(w.end.Sub(w.start) / 2)
			p.logf("Actions: window %s..%s had %d results (cap 1000), splitting",
				w.start.Format("2006-01-02T15:04"), w.end.Format("2006-01-02T15:04"), windowTotal)
			// Undo the mutations we already emitted? No — they're fine, we just
			// need to also fetch the ones we missed. The second pass will skip
			// already-known runs. Prepend both halves.
			windows = append([]window{{w.start, mid}, {mid, w.end}}, windows...)
			continue
		}
		_ = windowNew
	}

	p.logf("Actions: done. %d new runs synced, %d unchanged", totalNew, totalSkipped)
	return nil
}

// retryGitHubAPI retries fn on transient errors (5xx, network) with
// exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s.
// Returns the last error if all retries fail.
func retryGitHubAPI[T any](ctx context.Context, desc string, fn func() (T, *github.Response, error)) (T, *github.Response, error) {
	const maxAttempts = 7 // 1 initial + 6 retries (up to 32s backoff)
	var zero T
	for attempt := range maxAttempts {
		result, resp, err := fn()
		if err == nil {
			return result, resp, nil
		}
		if ctx.Err() != nil {
			return zero, nil, ctx.Err()
		}
		// Retry on 5xx or network errors, not on 4xx.
		if resp != nil && resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return zero, resp, err
		}
		if attempt < maxAttempts-1 {
			delay := time.Duration(1<<attempt) * time.Second // 1s, 2s, 4s, 8s, 16s, 32s
			log.Printf("%s: transient error (attempt %d/%d): %v; retrying in %v", desc, attempt+1, maxAttempts, err, delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return zero, nil, ctx.Err()
			}
			continue
		}
		return zero, resp, err
	}
	return zero, nil, fmt.Errorf("unreachable")
}

// syncWorkflowRunsBackfillGaps finds the largest time gaps in the corpus
// and fills them first, repeating until all gaps are under 1 hour or
// no new data is found in any gap over 1 hour.
func (p *githubRepoPoller) syncWorkflowRunsBackfillGaps(ctx context.Context) error {
	f := p.filter()
	if f.ActionsSince.IsZero() {
		return fmt.Errorf("ActionsBackfillGaps requires ActionsSince to be set")
	}

	oldest := f.ActionsSince
	now := time.Now().UTC()

	for round := 1; ; round++ {
		// Collect all known run Created times from the corpus.
		p.c.mu.RLock()
		times := make([]time.Time, 0, len(p.gr.workflowRuns)+2)
		times = append(times, oldest) // synthetic: left boundary
		times = append(times, now)    // synthetic: right boundary
		for _, run := range p.gr.workflowRuns {
			if !run.Created.IsZero() {
				times = append(times, run.Created)
			}
		}
		corpusSize := len(p.gr.workflowRuns)
		p.c.mu.RUnlock()

		sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })

		// Compute gaps between consecutive points, keep those > 1 hour.
		type gap struct {
			start, end time.Time
			dur        time.Duration
		}
		var gaps []gap
		for i := 1; i < len(times); i++ {
			d := times[i].Sub(times[i-1])
			if d > time.Hour {
				gaps = append(gaps, gap{times[i-1], times[i], d})
			}
		}

		if len(gaps) == 0 {
			p.logf("Actions backfill: round %d — no gaps > 1h remain (%d runs in corpus)", round, corpusSize)
			return nil
		}

		// Sort gaps by duration, largest first.
		sort.Slice(gaps, func(i, j int) bool { return gaps[i].dur > gaps[j].dur })

		p.logf("Actions backfill: round %d — %d gaps > 1h, largest: %s (%s to %s), %d runs in corpus",
			round, len(gaps), gaps[0].dur.Round(time.Minute), gaps[0].start.Format("2006-01-02T15:04"), gaps[0].end.Format("2006-01-02T15:04"), corpusSize)

		// Attack the largest gap using daily windows from the start.
		g := gaps[0]
		gapNew, err := p.backfillGap(ctx, g.start, g.end)
		if err != nil {
			return err
		}

		if gapNew == 0 {
			// No new data in the largest gap. Check other gaps.
			anyNew := false
			for _, g2 := range gaps[1:] {
				n2, err := p.backfillGap(ctx, g2.start, g2.end)
				if err != nil {
					return err
				}
				if n2 > 0 {
					anyNew = true
					p.logf("Actions backfill: gap %s..%s: %d new",
						g2.start.Format("2006-01-02T15:04"), g2.end.Format("2006-01-02T15:04"), n2)
					break
				}
			}
			if !anyNew {
				p.logf("Actions backfill: no new data in any gap > 1h; done (%d runs in corpus)", corpusSize)
				return nil
			}
		}
	}
}

// backfillGap probes a time range for runs, then walks it with daily windows.
// Returns the number of new runs found. Skips the gap entirely if the API
// reports 0 total runs in the range.
func (p *githubRepoPoller) backfillGap(ctx context.Context, start, end time.Time) (int, error) {
	// Probe: single API call to check if there's anything in this range.
	created := start.Format("2006-01-02T15:04:05Z") + ".." + end.Format("2006-01-02T15:04:05Z")
	probe, _, err := retryGitHubAPI(ctx, "probing gap", func() (*github.WorkflowRuns, *github.Response, error) {
		return p.githubDirect.Actions.ListRepositoryWorkflowRuns(ctx, p.Owner(), p.Repo(), &github.ListWorkflowRunsOptions{
			Created:     created,
			ListOptions: github.ListOptions{PerPage: 1},
		})
	})
	if err != nil {
		return 0, err
	}
	if probe.GetTotalCount() == 0 {
		p.logf("Actions backfill: gap %s..%s (%s): empty, skipping",
			start.Format("2006-01-02T15:04"), end.Format("2006-01-02T15:04"),
			end.Sub(start).Round(time.Minute))
		return 0, nil
	}
	p.logf("Actions backfill: gap %s..%s (%s): ~%d runs, walking daily",
		start.Format("2006-01-02T15:04"), end.Format("2006-01-02T15:04"),
		end.Sub(start).Round(time.Minute), probe.GetTotalCount())

	var totalNew, totalSkipped int
	ws := start
	for ws.Before(end) {
		we := ws.Add(24 * time.Hour)
		if we.After(end) {
			we = end
		}
		_, _, err := p.syncWorkflowRunsWindow(ctx, ws, we, &totalNew, &totalSkipped)
		if err != nil {
			return totalNew, err
		}
		ws = we
	}

	p.logf("Actions backfill: gap %s..%s: %d new, %d skipped",
		start.Format("2006-01-02T15:04"), end.Format("2006-01-02T15:04"),
		totalNew, totalSkipped)
	return totalNew, nil
}

// syncWorkflowRunsWindow fetches all workflow runs in [start, end) and returns
// the count of new runs, total runs seen, and any error.
func (p *githubRepoPoller) syncWorkflowRunsWindow(ctx context.Context, start, end time.Time, totalNew, totalSkipped *int) (windowNew, windowTotal int, _ error) {
	created := start.Format("2006-01-02T15:04:05Z") + ".." + end.Format("2006-01-02T15:04:05Z")
	opts := &github.ListWorkflowRunsOptions{
		Created:     created,
		ListOptions: github.ListOptions{PerPage: 100},
	}

	for {
		runs, runsResp, err := retryGitHubAPI(ctx, "listing workflow runs", func() (*github.WorkflowRuns, *github.Response, error) {
			return p.githubDirect.Actions.ListRepositoryWorkflowRuns(ctx, p.Owner(), p.Repo(), opts)
		})
		if err != nil {
			return windowNew, windowTotal, fmt.Errorf("listing workflow runs: %w", err)
		}

		pageNew, pageSkipped := 0, 0
		for _, run := range runs.WorkflowRuns {
			windowTotal++

			p.c.mu.RLock()
			existing := p.gr.workflowRuns[run.GetID()]
			p.c.mu.RUnlock()

			if existing != nil && existing.Updated.Equal(run.GetUpdatedAt().Time) {
				*totalSkipped++
				pageSkipped++
				continue
			}

			runMut := workflowRunToProto(run)
			p.c.addMutation(&maintpb.Mutation{
				GithubActions: &maintpb.GithubActionsMutation{
					Owner: p.Owner(),
					Repo:  p.Repo(),
					Run:   runMut,
				},
			})

			if err := p.syncWorkflowJobs(ctx, run.GetID()); err != nil {
				return windowNew, windowTotal, err
			}
			*totalNew++
			windowNew++
			pageNew++
		}

		p.logf("Actions: %s — page %d: %d new, %d skipped (total: %d new, %d skipped)",
			start.Format("2006-01-02T15:04"), opts.Page, pageNew, pageSkipped, *totalNew, *totalSkipped)

		if runsResp.NextPage == 0 {
			break
		}
		opts.Page = runsResp.NextPage
	}
	return windowNew, windowTotal, nil
}

func (p *githubRepoPoller) syncWorkflowJobs(ctx context.Context, runID int64) error {
	opts := &github.ListWorkflowJobsOptions{
		Filter:      "latest",
		ListOptions: github.ListOptions{PerPage: 100},
	}
	for {
		jobs, jobsResp, err := retryGitHubAPI(ctx, fmt.Sprintf("listing jobs for run %d", runID), func() (*github.Jobs, *github.Response, error) {
			return p.githubDirect.Actions.ListWorkflowJobs(ctx, p.Owner(), p.Repo(), runID, opts)
		})
		if err != nil {
			return fmt.Errorf("listing workflow jobs for run %d: %w", runID, err)
		}
		for _, job := range jobs.Jobs {
			jobMut := workflowJobToProto(job)
			p.c.addMutation(&maintpb.Mutation{
				GithubActions: &maintpb.GithubActionsMutation{
					Owner: p.Owner(),
					Repo:  p.Repo(),
					Job:   jobMut,
				},
			})
		}
		if jobsResp.NextPage == 0 {
			break
		}
		opts.Page = jobsResp.NextPage
	}
	return nil
}

func workflowRunToProto(run *github.WorkflowRun) *maintpb.GithubWorkflowRun {
	m := &maintpb.GithubWorkflowRun{
		Id:         run.GetID(),
		Name:       run.GetName(),
		HeadBranch: run.GetHeadBranch(),
		HeadHash:   run.GetHeadSHA(),
		Event:      run.GetEvent(),
		Status:     run.GetStatus(),
		Conclusion: run.GetConclusion(),
		WorkflowId: run.GetWorkflowID(),
		RunNumber:  int64(run.GetRunNumber()),
		RunAttempt: int64(run.GetRunAttempt()),
		Url:        run.GetHTMLURL(),
	}
	if run.Actor != nil {
		m.ActorId = run.Actor.GetID()
	}
	if t := run.GetCreatedAt(); !t.Time.IsZero() {
		m.Created = timestamppb.New(t.Time)
	}
	if t := run.GetUpdatedAt(); !t.Time.IsZero() {
		m.Updated = timestamppb.New(t.Time)
	}
	if t := run.GetRunStartedAt(); !t.Time.IsZero() {
		m.RunStarted = timestamppb.New(t.Time)
	}
	for _, pr := range run.PullRequests {
		if pr.Number != nil {
			m.PullRequestNumbers = append(m.PullRequestNumbers, int64(*pr.Number))
		}
	}
	return m
}

func workflowJobToProto(job *github.WorkflowJob) *maintpb.GithubWorkflowJob {
	m := &maintpb.GithubWorkflowJob{
		Id:         job.GetID(),
		RunId:      job.GetRunID(),
		Name:       job.GetName(),
		Status:     job.GetStatus(),
		Conclusion: job.GetConclusion(),
		RunnerName: job.GetRunnerName(),
		Labels:     job.Labels,
	}
	if t := job.StartedAt; t != nil {
		m.Started = timestamppb.New(t.Time)
	}
	if t := job.CompletedAt; t != nil {
		m.Completed = timestamppb.New(t.Time)
	}
	for _, step := range job.Steps {
		sm := &maintpb.GithubWorkflowStep{
			Name:       step.GetName(),
			Status:     step.GetStatus(),
			Conclusion: step.GetConclusion(),
			Number:     int64(step.GetNumber()),
		}
		if t := step.StartedAt; t != nil {
			sm.Started = timestamppb.New(t.Time)
		}
		if t := step.CompletedAt; t != nil {
			sm.Completed = timestamppb.New(t.Time)
		}
		m.Step = append(m.Step, sm)
	}
	return m
}

func (p *githubRepoPoller) syncMilestones(ctx context.Context) error {
	var mut *maintpb.GithubMutation // lazy init
	var changes int
	err := p.foreachItem(ctx, 1, p.getMilestonePage, func(e any) error {
		ms := e.(*github.Milestone)
		id := int64(ms.GetID())
		p.c.mu.RLock()
		diff := p.gr.milestones[id].GenMutationDiff(ms)
		p.c.mu.RUnlock()
		if diff == nil {
			return nil
		}
		if mut == nil {
			mut = &maintpb.GithubMutation{
				Owner: p.Owner(),
				Repo:  p.Repo(),
			}
		}
		mut.Milestones = append(mut.Milestones, diff)
		changes++
		return nil
	})
	if err != nil {
		return err
	}
	p.logf("%d milestone changes.", changes)
	if changes == 0 {
		return nil
	}
	p.c.addMutation(&maintpb.Mutation{Github: mut})
	return nil
}

func (p *githubRepoPoller) syncLabels(ctx context.Context) error {
	var mut *maintpb.GithubMutation // lazy init
	var changes int
	err := p.foreachItem(ctx, 1, p.getLabelPage, func(e any) error {
		lb := e.(*github.Label)
		id := int64(lb.GetID())
		p.c.mu.RLock()
		diff := p.gr.labels[id].GenMutationDiff(lb)
		p.c.mu.RUnlock()
		if diff == nil {
			return nil
		}
		if mut == nil {
			mut = &maintpb.GithubMutation{
				Owner: p.Owner(),
				Repo:  p.Repo(),
			}
		}
		mut.Labels = append(mut.Labels, diff)
		changes++
		return nil
	})
	if err != nil {
		return err
	}
	p.logf("%d label changes.", changes)
	if changes == 0 {
		return nil
	}
	p.c.addMutation(&maintpb.Mutation{Github: mut})
	return nil
}

func (p *githubRepoPoller) getMilestonePage(ctx context.Context, page int) ([]any, *github.Response, error) {
	ms, res, err := p.githubCaching.Issues.ListMilestones(ctx, p.Owner(), p.Repo(), &github.MilestoneListOptions{
		State:       "all",
		ListOptions: github.ListOptions{Page: page},
	})
	if err != nil {
		return nil, nil, err
	}
	its := make([]any, len(ms))
	for i, m := range ms {
		its[i] = m
	}
	return its, res, nil
}

func (p *githubRepoPoller) getLabelPage(ctx context.Context, page int) ([]any, *github.Response, error) {
	ls, res, err := p.githubCaching.Issues.ListLabels(ctx, p.Owner(), p.Repo(), &github.ListOptions{
		Page: page,
	})
	if err != nil {
		return nil, nil, err
	}
	its := make([]any, len(ls))
	for i, lb := range ls {
		its[i] = lb
	}
	return its, res, nil
}

// foreachItem walks over all pages of items from getPage and calls fn for each item.
// If the first page's response was cached, fn is never called.
func (p *githubRepoPoller) foreachItem(
	ctx context.Context,
	page int,
	getPage func(ctx context.Context, page int) ([]any, *github.Response, error),
	fn func(any) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		items, res, err := getPage(ctx, page)
		if err != nil {
			if canRetry(ctx, err) {
				continue
			}
			return err
		}
		if len(items) == 0 {
			return nil
		}
		fromCache := page == 1 && res.Response.Header.Get(xFromCache) == "1"
		if fromCache {
			log.Printf("no new items of type %T", items[0])
			// No need to walk over these again.
			return nil
		}
		// TODO: use res.Rate (sleep until Reset if Limit == 0)
		for _, it := range items {
			if err := fn(it); err != nil {
				return err
			}
		}
		if res.NextPage == 0 {
			return nil
		}
		page = res.NextPage
	}
}

func (p *githubRepoPoller) syncIssues(ctx context.Context, expectChanges bool) error {
	page := 0
	after := ""
	seen := make(map[int64]bool)
	keepGoing := true
	owner, repo := p.gr.id.Owner, p.gr.id.Repo
	for keepGoing {
		ghc := p.githubCaching
		if expectChanges {
			ghc = p.githubDirect
		}
		issues, res, err := ghc.Issues.ListByRepo(ctx, owner, repo, &github.IssueListByRepoOptions{
			State:     "all",
			Sort:      "updated",
			Direction: "desc",
			ListCursorOptions: github.ListCursorOptions{
				After:   after,
				PerPage: 100,
			},
		})
		if err != nil {
			if canRetry(ctx, err) {
				continue
			}
			return err
		}
		// See https://developer.github.com/v3/activity/events/ for X-Poll-Interval:
		if pi := res.Response.Header.Get("X-Poll-Interval"); pi != "" {
			nsec, _ := strconv.Atoi(pi)
			d := time.Duration(nsec) * time.Second
			p.logf("Requested to adjust poll interval to %v", d)
			// TODO: return an error type up that the sync loop can use
			// to adjust its default interval.
			// For now, ignore.
		}
		fromCache := res.Response.Header.Get(xFromCache) == "1"
		if len(issues) == 0 {
			p.logf("issues: reached end.")
			break
		}

		didMilestoneLabelSync := false
		changes := 0
		for _, is := range issues {
			id := int64(is.GetID())
			if seen[id] {
				// If an issue gets updated (and bumped to the top) while we
				// are paging, it's possible the last issue from page N can
				// appear as the first issue on page N+1. Don't process that
				// issue twice.
				// https://github.com/google/go-github/issues/566
				continue
			}
			seen[id] = true

			var mp *maintpb.Mutation
			var diffSummary string
			p.c.mu.RLock()
			{
				gi := p.gr.issues[int32(*is.Number)]
				mp, diffSummary = p.gr.newMutationFromIssue(gi, is)
				// Check if issue-body reaction counts changed.
				if gi != nil && is.Reactions.GetTotalCount() != len(gi.reactions) {
					p.staleReactionIssues[int32(*is.Number)] = true
				}
			}
			p.c.mu.RUnlock()

			if mp == nil {
				continue
			}

			// If there's something new (not a cached response),
			// then check for updated milestones and labels before
			// creating issue mutations below. Doesn't matter
			// much, but helps to have it all loaded.
			if !fromCache && !didMilestoneLabelSync {
				didMilestoneLabelSync = true
				group, ctx := errgroup.WithContext(ctx)
				group.Go(func() error { return p.syncMilestones(ctx) })
				group.Go(func() error { return p.syncLabels(ctx) })
				if err := group.Wait(); err != nil {
					return err
				}
			}

			changes++
			p.logf("changed issue %d [%s]: %s", is.GetNumber(), diffSummary, is.GetTitle())
			p.c.addMutation(mp)
			p.lastUpdate = time.Now()
		}

		if changes == 0 {
			p.logf("no changed issues; cached=%v", fromCache)
			return nil
		}

		p.c.mu.RLock()
		num := len(p.gr.issues)
		p.c.mu.RUnlock()
		p.logf("After page %d: %v issues, %v changes, %v issues in memory", page, len(issues), changes, num)

		page++
		after = res.After
	}

	return nil
}

func (p *githubRepoPoller) issueNumbersWithStaleCommentSync() (issueNums []int32) {
	p.c.mu.RLock()
	defer p.c.mu.RUnlock()

	for n, gi := range p.gr.issues {
		if !gi.commentsSynced() {
			issueNums = append(issueNums, n)
		}
	}
	slices.Sort(issueNums)
	return issueNums
}

func (p *githubRepoPoller) syncComments(ctx context.Context) error {
	for {
		nums := p.issueNumbersWithStaleCommentSync()
		if len(nums) == 0 {
			return nil
		}
		remain := len(nums)
		for _, num := range nums {
			p.logf("comment sync: %d issues remaining; syncing issue %v", remain, num)
			if err := p.syncCommentsOnIssue(ctx, num); err != nil {
				p.logf("comment sync on issue %d: %v", num, err)
				return err
			}
			remain--
		}
	}
}

func (p *githubRepoPoller) syncCommentsOnIssue(ctx context.Context, issueNum int32) error {
	p.c.mu.RLock()
	issue := p.gr.issues[issueNum]
	if issue == nil {
		p.c.mu.RUnlock()
		return fmt.Errorf("unknown issue number %v", issueNum)
	}
	since := issue.commentsUpdatedTil
	p.c.mu.RUnlock()

	owner, repo := p.gr.id.Owner, p.gr.id.Repo
	morePages := true // at least try the first. might be empty.
	for morePages {
		opt := &github.IssueListCommentsOptions{
			Direction:   github.String("asc"),
			Sort:        github.String("updated"),
			ListOptions: github.ListOptions{PerPage: 100},
		}
		if !since.IsZero() {
			opt.Since = &since
		}
		ics, res, err := p.githubDirect.Issues.ListComments(ctx, owner, repo, int(issueNum), opt)
		if canRetry(ctx, err) {
			continue
		} else if isIssueGoneError(err) {
			mut := &maintpb.Mutation{
				GithubIssue: &maintpb.GithubIssueMutation{
					Owner:    owner,
					Repo:     repo,
					Number:   issueNum,
					NotExist: true,
				},
			}
			p.logf("issue %d comments are gone, marking as NotExist", issueNum)
			p.c.addMutation(mut)
			return nil
		} else if err != nil {
			return err
		}
		serverDate, err := http.ParseTime(res.Header.Get("Date"))
		if err != nil {
			return fmt.Errorf("invalid server Date response: %v", err)
		}
		serverDate = serverDate.UTC()
		p.logf("Number of comments on issue %d since %v: %v", issueNum, since, len(ics))

		mut := &maintpb.Mutation{
			GithubIssue: &maintpb.GithubIssueMutation{
				Owner:  owner,
				Repo:   repo,
				Number: issueNum,
			},
		}

		p.c.mu.RLock()
		for _, ic := range ics {
			if ic.ID == nil || ic.Body == nil || ic.User == nil || ic.CreatedAt == nil || ic.UpdatedAt == nil {
				// Bogus.
				p.logf("bogus comment: %v", ic)
				continue
			}
			created := timestamppb.New(ic.CreatedAt.Time)
			updated := timestamppb.New(ic.UpdatedAt.Time)
			since = ic.UpdatedAt.Time // for next round

			id := int64(*ic.ID)
			cur := issue.comments[id]

			// Check if reaction counts changed on this comment.
			// Reaction changes don't update the comment's UpdatedAt, so we
			// detect them here by comparing API counts against corpus.
			if cur != nil && ic.Reactions.GetTotalCount() != len(cur.reactions) {
				p.staleReactionIssues[issueNum] = true
			}

			var cmut *maintpb.GithubIssueCommentMutation
			if cur == nil {
				cmut = &maintpb.GithubIssueCommentMutation{
					Id: id,
					User: &maintpb.GithubUser{
						Id:    int64(*ic.User.ID),
						Login: *ic.User.Login,
					},
					Body:    *ic.Body,
					Created: created,
					Updated: updated,
				}
			} else if !cur.Updated.Equal(ic.UpdatedAt.Time) || cur.Body != *ic.Body {
				cmut = &maintpb.GithubIssueCommentMutation{
					Id: id,
				}
				if !cur.Updated.Equal(ic.UpdatedAt.Time) {
					cmut.Updated = updated
				}
				if cur.Body != *ic.Body {
					cmut.Body = *ic.Body
				}
			}
			if cmut != nil {
				mut.GithubIssue.Comment = append(mut.GithubIssue.Comment, cmut)
			}
		}
		p.c.mu.RUnlock()

		if res.NextPage == 0 {
			mut.GithubIssue.CommentStatus = &maintpb.GithubIssueSyncStatus{
				ServerDate: timestamppb.New(serverDate),
			}
			morePages = false
		}

		p.c.addMutation(mut)
	}
	return nil
}

// graphqlRequest performs a GitHub GraphQL API request using the poller's
// authenticated HTTP client.
func (p *githubRepoPoller) graphqlRequest(ctx context.Context, query string, variables map[string]any, result any) error {
	body := struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables,omitempty"`
	}{query, variables}
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.github.com/graphql", bytes.NewReader(bodyJSON))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("graphql: HTTP %d: %s", resp.StatusCode, respBody)
	}
	var gqlResp struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return fmt.Errorf("graphql: decoding response: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		return fmt.Errorf("graphql: %s", gqlResp.Errors[0].Message)
	}
	return json.Unmarshal(gqlResp.Data, result)
}

// reactionCountScanQuery uses conservative page sizes (25 issues, 50
// comments) to stay within GitHub's GraphQL resource limits on large repos.
const reactionCountScanQuery = `
query($owner: String!, $name: String!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    issues(first: 25, after: $cursor, orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        reactions { totalCount }
        comments(first: 50) {
          pageInfo { hasNextPage endCursor }
          nodes {
            databaseId
            reactions { totalCount }
          }
        }
      }
    }
  }
}
`

// scanReactionCounts uses the GitHub GraphQL API to efficiently scan all
// issues and their comments for reaction count changes.
func (p *githubRepoPoller) scanReactionCounts(ctx context.Context) error {
	owner, repo := p.gr.id.Owner, p.gr.id.Repo
	p.logf("scanning reaction counts via GraphQL...")

	var cursor *string
	scanned := 0
	for {
		vars := map[string]any{
			"owner": owner,
			"name":  repo,
		}
		if cursor != nil {
			vars["cursor"] = *cursor
		}

		var result struct {
			Repository struct {
				Issues struct {
					PageInfo struct {
						HasNextPage bool   `json:"hasNextPage"`
						EndCursor   string `json:"endCursor"`
					} `json:"pageInfo"`
					Nodes []struct {
						Number    int `json:"number"`
						Reactions struct {
							TotalCount int `json:"totalCount"`
						} `json:"reactions"`
						Comments struct {
							PageInfo struct {
								HasNextPage bool   `json:"hasNextPage"`
								EndCursor   string `json:"endCursor"`
							} `json:"pageInfo"`
							Nodes []struct {
								DatabaseId int `json:"databaseId"`
								Reactions  struct {
									TotalCount int `json:"totalCount"`
								} `json:"reactions"`
							} `json:"nodes"`
						} `json:"comments"`
					} `json:"nodes"`
				} `json:"issues"`
			} `json:"repository"`
		}

		if err := p.graphqlRequest(ctx, reactionCountScanQuery, vars, &result); err != nil {
			return fmt.Errorf("reaction count scan: %w", err)
		}

		p.c.mu.RLock()
		for _, issue := range result.Repository.Issues.Nodes {
			num := int32(issue.Number)
			gi := p.gr.issues[num]

			// Check issue-body reactions.
			if gi == nil {
				continue
			}
			if issue.Reactions.TotalCount != len(gi.reactions) {
				p.staleReactionIssues[num] = true
			}
			// Also re-scan if never synced or stale (>24h).
			if gi.reactionsSyncedAsOf.IsZero() {
				if issue.Reactions.TotalCount > 0 || len(gi.reactions) > 0 {
					p.staleReactionIssues[num] = true
				}
			} else if time.Since(gi.reactionsSyncedAsOf) > 24*time.Hour && len(gi.reactions) > 0 {
				p.staleReactionIssues[num] = true
			}

			// Check comment reactions.
			for _, c := range issue.Comments.Nodes {
				gc := gi.comments[int64(c.DatabaseId)]
				if gc == nil {
					continue
				}
				if c.Reactions.TotalCount != len(gc.reactions) {
					p.staleReactionIssues[num] = true
				}
			}

			// TODO: handle comments.pageInfo.hasNextPage for issues with >100 comments.
		}
		p.c.mu.RUnlock()

		scanned += len(result.Repository.Issues.Nodes)
		if !result.Repository.Issues.PageInfo.HasNextPage {
			break
		}
		endCursor := result.Repository.Issues.PageInfo.EndCursor
		cursor = &endCursor
	}

	p.logf("reaction count scan complete: scanned %d issues, %d need updates", scanned, len(p.staleReactionIssues))
	p.lastReactionScan = time.Now()
	return nil
}

// syncReactions fetches detailed reactions for issues detected as having
// changed reaction counts (opportunistically during syncIssues/syncComments),
// or via the opt-in GraphQL count scan.
func (p *githubRepoPoller) syncReactions(ctx context.Context) error {
	// If GraphQL scanning is enabled and the scan interval has elapsed,
	// run scanReactionCounts and merge results into staleReactionIssues.
	if interval := p.c.reactionScanInterval; interval > 0 {
		if p.lastReactionScan.IsZero() || time.Since(p.lastReactionScan) >= interval {
			if err := p.scanReactionCounts(ctx); err != nil {
				return err
			}
		}
	}

	if len(p.staleReactionIssues) == 0 {
		return nil
	}

	issueNums := make([]int32, 0, len(p.staleReactionIssues))
	for n := range p.staleReactionIssues {
		issueNums = append(issueNums, n)
	}
	slices.Sort(issueNums)

	for _, num := range issueNums {
		if err := p.syncReactionsOnIssue(ctx, num); err != nil {
			return err
		}
	}
	return nil
}

// syncReactionsOnIssue fetches all reactions on the given issue (body + comments)
// and emits mutations for any changes.
func (p *githubRepoPoller) syncReactionsOnIssue(ctx context.Context, issueNum int32) error {
	owner, repo := p.gr.id.Owner, p.gr.id.Repo
	p.logf("syncing reactions on issue %d", issueNum)

	// Fetch issue-body reactions.
	var allIssueReactions []*github.Reaction
	opt := &github.ListOptions{PerPage: 100}
	for {
		reactions, res, err := p.githubDirect.Reactions.ListIssueReactions(ctx, owner, repo, int(issueNum), &github.ListReactionOptions{ListOptions: *opt})
		if err != nil {
			return fmt.Errorf("listing reactions on issue %d: %w", issueNum, err)
		}
		allIssueReactions = append(allIssueReactions, reactions...)
		if res.NextPage == 0 {
			break
		}
		opt.Page = res.NextPage
	}

	p.c.mu.RLock()
	gi := p.gr.issues[issueNum]
	if gi == nil {
		p.c.mu.RUnlock()
		return nil
	}

	mut := &maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  owner,
			Repo:   repo,
			Number: issueNum,
		},
	}

	// Diff issue-body reactions.
	seen := make(map[int64]bool)
	for _, r := range allIssueReactions {
		seen[r.GetID()] = true
		if _, ok := gi.reactions[r.GetID()]; !ok {
			mut.GithubIssue.Reaction = append(mut.GithubIssue.Reaction, githubReactionToProto(r))
		}
	}
	for id := range gi.reactions {
		if !seen[id] {
			mut.GithubIssue.RemovedReactionId = append(mut.GithubIssue.RemovedReactionId, id)
		}
	}

	// Fetch and diff comment reactions.
	for cid, gc := range gi.comments {
		var allCommentReactions []*github.Reaction
		copt := &github.ListOptions{PerPage: 100}
		for {
			reactions, res, err := p.githubDirect.Reactions.ListIssueCommentReactions(ctx, owner, repo, cid, &github.ListReactionOptions{ListOptions: *copt})
			if err != nil {
				p.c.mu.RUnlock()
				return fmt.Errorf("listing reactions on comment %d: %w", cid, err)
			}
			allCommentReactions = append(allCommentReactions, reactions...)
			if res.NextPage == 0 {
				break
			}
			copt.Page = res.NextPage
		}

		cseen := make(map[int64]bool)
		var cmut *maintpb.GithubIssueCommentMutation
		for _, r := range allCommentReactions {
			cseen[r.GetID()] = true
			if _, ok := gc.reactions[r.GetID()]; !ok {
				if cmut == nil {
					cmut = &maintpb.GithubIssueCommentMutation{Id: cid}
				}
				cmut.Reaction = append(cmut.Reaction, githubReactionToProto(r))
			}
		}
		for id := range gc.reactions {
			if !cseen[id] {
				if cmut == nil {
					cmut = &maintpb.GithubIssueCommentMutation{Id: cid}
				}
				cmut.RemovedReactionId = append(cmut.RemovedReactionId, id)
			}
		}
		if cmut != nil {
			mut.GithubIssue.Comment = append(mut.GithubIssue.Comment, cmut)
		}
	}
	p.c.mu.RUnlock()

	// Only emit if there are actual changes.
	hasChanges := len(mut.GithubIssue.Reaction) > 0 ||
		len(mut.GithubIssue.RemovedReactionId) > 0 ||
		len(mut.GithubIssue.Comment) > 0
	// Always set reaction_status to record that we synced.
	mut.GithubIssue.ReactionStatus = &maintpb.GithubIssueSyncStatus{
		ServerDate: timestamppb.Now(),
	}
	if hasChanges || gi.reactionsSyncedAsOf.IsZero() {
		p.c.addMutation(mut)
	}
	return nil
}

func githubReactionToProto(r *github.Reaction) *maintpb.GithubReaction {
	gr := &maintpb.GithubReaction{
		Id:      r.GetID(),
		Content: r.GetContent(),
	}
	if r.User != nil {
		gr.UserId = r.User.GetID()
	}
	if r.CreatedAt != nil {
		gr.Created = timestamppb.New(r.CreatedAt.Time)
	}
	return gr
}

func (p *githubRepoPoller) issueNumbersWithStaleEventSync() (issueNums []int32) {
	p.c.mu.RLock()
	defer p.c.mu.RUnlock()

	for n, gi := range p.gr.issues {
		if !gi.eventsSynced() {
			issueNums = append(issueNums, n)
		}
	}
	slices.Sort(issueNums)
	return issueNums
}

func (p *githubRepoPoller) syncEvents(ctx context.Context) error {
	for {
		nums := p.issueNumbersWithStaleEventSync()
		if len(nums) == 0 {
			return nil
		}
		remain := len(nums)
		for _, num := range nums {
			p.logf("event sync: %d issues remaining; syncing issue %v", remain, num)
			if err := p.syncEventsOnIssue(ctx, num); err != nil {
				if isIssueGoneError(err) {
					p.logf("issue %d events are gone, marking as NotExist", num)
					p.c.addMutation(&maintpb.Mutation{
						GithubIssue: &maintpb.GithubIssueMutation{
							Owner: p.Owner(), Repo: p.Repo(), Number: num, NotExist: true,
						},
					})
					remain--
					continue
				}
				p.logf("event sync on issue %d: %v", num, err)
				return err
			}
			remain--
		}
	}
}

func (p *githubRepoPoller) syncEventsOnIssue(ctx context.Context, issueNum int32) error {
	const perPage = 100
	p.c.mu.RLock()
	gi := p.gr.issues[issueNum]
	if gi == nil {
		panic(fmt.Sprintf("bogus issue %v", issueNum))
	}
	have := len(gi.events)
	p.c.mu.RUnlock()

	skipPages := have / perPage

	mut := &maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  p.Owner(),
			Repo:   p.Repo(),
			Number: issueNum,
		},
	}

	err := p.foreachItem(ctx,
		1+skipPages,
		func(ctx context.Context, page int) ([]any, *github.Response, error) {
			u := fmt.Sprintf("https://api.github.com/repos/%s/%s/issues/%v/events?per_page=%v&page=%v",
				p.Owner(), p.Repo(), issueNum, perPage, page)
			req, _ := http.NewRequest("GET", u, nil)

			tok, err := p.tokenSource.Token()
			if err != nil {
				return nil, nil, fmt.Errorf("getting token: %v", err)
			}
			req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
			req.Header.Set("User-Agent", "golang-x-build-maintner/1.0")
			ctx, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()
			req = req.WithContext(ctx)
			res, err := p.client.Do(req)
			if err != nil {
				log.Printf("Fetching %s: %v", u, err)
				return nil, nil, err
			}
			log.Printf("Fetching %s: %v", u, res.Status)
			ghResp := makeGithubResponse(res)
			if err := github.CheckResponse(res); err != nil {
				log.Printf("Fetching %s: %v: %+v", u, res.Status, res.Header)
				log.Printf("GitHub error %s: %v", u, ghResp)
				return nil, nil, err
			}

			evts, err := parseGithubEvents(res.Body)
			if err != nil {
				return nil, nil, fmt.Errorf("%s: parse github events: %v", u, err)
			}
			is := make([]any, len(evts))
			for i, v := range evts {
				is[i] = v
			}
			serverDate, err := http.ParseTime(res.Header.Get("Date"))
			if err != nil {
				return nil, nil, fmt.Errorf("invalid server Date response: %v", err)
			}
			mut.GithubIssue.EventStatus = &maintpb.GithubIssueSyncStatus{
				ServerDate: timestamppb.New(serverDate.UTC()),
			}

			return is, ghResp, err
		},
		func(v any) error {
			ge := v.(*GitHubIssueEvent)
			p.c.mu.RLock()
			_, ok := gi.events[ge.ID]
			p.c.mu.RUnlock()
			if ok {
				// Already have it. And they're
				// assumed to be immutable, so the
				// copy we already have should be
				// good. Don't add to mutation log.
				return nil
			}
			mut.GithubIssue.Event = append(mut.GithubIssue.Event, ge.Proto())
			return nil
		})
	if err != nil {
		return err
	}
	p.c.addMutation(mut)
	return nil
}

// parseGithubEvents parses the JSON array of GitHub issue events in r.
// It does this the very manual way (using map[string]interface{})
// instead of using nice types because https://golang.org/issue/15314
// isn't implemented yet and also because even if it were implemented,
// this code still wants to preserve any unknown fields to store in
// the "OtherJSON" field for future updates of the code to parse. (If
// GitHub adds new Event types in the future, we want to archive them,
// even if we don't understand them)
func parseGithubEvents(r io.Reader) ([]*GitHubIssueEvent, error) {
	var jevents []map[string]any
	jd := json.NewDecoder(r)
	jd.UseNumber()
	if err := jd.Decode(&jevents); err != nil {
		return nil, err
	}
	var evts []*GitHubIssueEvent
	for _, em := range jevents {
		for k, v := range em {
			if v == nil {
				delete(em, k)
			}
		}
		delete(em, "url")

		e := &GitHubIssueEvent{}

		e.Type, _ = em["event"].(string)
		delete(em, "event")

		e.ID = jint64(em["id"])
		delete(em, "id")

		// TODO: store these two more compactly:
		e.CommitID, _ = em["commit_id"].(string) // "5383ecf5a0824649ffcc0349f00f0317575753d0"
		delete(em, "commit_id")
		e.CommitURL, _ = em["commit_url"].(string) // "https://api.github.com/repos/bradfitz/go-issue-mirror/commits/5383ecf5a0824649ffcc0349f00f0317575753d0"
		delete(em, "commit_url")

		getUser := func(field string, gup **GitHubUser) {
			am, ok := em[field].(map[string]any)
			if !ok {
				return
			}
			delete(em, field)
			gu := &GitHubUser{ID: jint64(am["id"])}
			gu.Login, _ = am["login"].(string)
			*gup = gu
		}

		getUser("actor", &e.Actor)
		getUser("assignee", &e.Assignee)
		getUser("assigner", &e.Assigner)
		getUser("requested_reviewer", &e.Reviewer)
		getUser("review_requester", &e.ReviewRequester)

		if lm, ok := em["label"].(map[string]any); ok {
			delete(em, "label")
			e.Label, _ = lm["name"].(string)
		}

		if mm, ok := em["milestone"].(map[string]any); ok {
			delete(em, "milestone")
			e.Milestone, _ = mm["title"].(string)
		}

		if rm, ok := em["rename"].(map[string]any); ok {
			delete(em, "rename")
			e.From, _ = rm["from"].(string)
			e.To, _ = rm["to"].(string)
		}

		if createdStr, ok := em["created_at"].(string); ok {
			delete(em, "created_at")
			var err error
			e.Created, err = time.Parse(time.RFC3339, createdStr)
			if err != nil {
				return nil, err
			}
			e.Created = e.Created.UTC()
		}
		if dr, ok := em["dismissed_review"]; ok {
			delete(em, "dismissed_review")
			drm := dr.(map[string]any)
			dro := &GitHubDismissedReviewEvent{}
			dro.ReviewID = jint64(drm["review_id"])
			if state, ok := drm["state"].(string); ok {
				dro.State = state
			} else {
				log.Printf("got type %T for 'state' field, expected string in %+v", drm["state"], drm)
			}
			dro.DismissalMessage, _ = drm["dismissal_message"].(string)
			e.DismissedReview = dro
		}
		if rt, ok := em["requested_team"]; ok {
			delete(em, "requested_team")
			rtm, ok := rt.(map[string]any)
			if !ok {
				log.Printf("got value %+v for 'requested_team' field, wanted a map with 'id' and 'slug' fields", rt)
			} else {
				t := &GitHubTeam{}
				t.ID = jint64(rtm["id"])
				t.Slug, _ = rtm["slug"].(string)
				e.TeamReviewer = t
			}
		}
		delete(em, "node_id")                  // GitHub API v4 Global Node ID; don't store it.
		delete(em, "lock_reason")              // Not stored.
		delete(em, "performed_via_github_app") // Not stored; e.g. Copilot PR reviewer.

		otherJSON, _ := json.Marshal(em)
		e.OtherJSON = string(otherJSON)
		if e.OtherJSON == "{}" {
			e.OtherJSON = ""
		}
		if e.OtherJSON != "" {
			log.Printf("warning: storing unknown field(s) in GitHub issue event: %s", e.OtherJSON)
		}
		evts = append(evts, e)
	}
	return evts, nil
}

func (p *githubRepoPoller) issueNumbersWithStaleReviewsSync() (issueNums []int32) {
	p.c.mu.RLock()
	defer p.c.mu.RUnlock()

	for n, gi := range p.gr.issues {
		if gi.IsPullRequest() && !gi.reviewsSynced() {
			issueNums = append(issueNums, n)
		}
	}
	slices.Sort(issueNums)
	return issueNums
}

func (p *githubRepoPoller) syncReviews(ctx context.Context) error {
	for {
		nums := p.issueNumbersWithStaleReviewsSync()
		if len(nums) == 0 {
			return nil
		}
		remain := len(nums)
		for _, num := range nums {
			p.logf("reviews sync: %d issues remaining; syncing issue %v", remain, num)
			if err := p.syncReviewsOnPullRequest(ctx, num); err != nil {
				if isIssueGoneError(err) {
					p.logf("issue %d reviews are gone, marking as NotExist", num)
					p.c.addMutation(&maintpb.Mutation{
						GithubIssue: &maintpb.GithubIssueMutation{
							Owner: p.Owner(), Repo: p.Repo(), Number: num, NotExist: true,
						},
					})
					remain--
					continue
				}
				p.logf("review sync on issue %d: %v", num, err)
				return err
			}
			remain--
		}
	}
}

func (p *githubRepoPoller) syncReviewsOnPullRequest(ctx context.Context, issueNum int32) error {
	const perPage = 100
	p.c.mu.RLock()
	gi := p.gr.issues[issueNum]
	if gi == nil {
		p.c.mu.RUnlock()
		panic(fmt.Sprintf("bogus issue %v", issueNum))
	}

	if !gi.IsPullRequest() {
		p.c.mu.RUnlock()
		return nil
	}

	have := len(gi.reviews)
	p.c.mu.RUnlock()

	skipPages := have / perPage

	mut := &maintpb.Mutation{
		GithubIssue: &maintpb.GithubIssueMutation{
			Owner:  p.Owner(),
			Repo:   p.Repo(),
			Number: issueNum,
		},
	}

	err := p.foreachItem(ctx,
		1+skipPages,
		func(ctx context.Context, page int) ([]any, *github.Response, error) {
			u := fmt.Sprintf("https://api.github.com/repos/%s/%s/pulls/%v/reviews?per_page=%v&page=%v",
				p.Owner(), p.Repo(), issueNum, perPage, page)
			req, _ := http.NewRequest("GET", u, nil)

			tok, err := p.tokenSource.Token()
			if err != nil {
				return nil, nil, fmt.Errorf("getting token: %v", err)
			}
			req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
			req.Header.Set("User-Agent", "golang-x-build-maintner/1.0")
			ctx, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()
			req = req.WithContext(ctx)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("Fetching %s: %v", u, err)
				return nil, nil, err
			}
			log.Printf("Fetching %s: %v", u, res.Status)
			ghResp := makeGithubResponse(res)
			if err := github.CheckResponse(res); err != nil {
				log.Printf("Fetching %s: %v: %+v", u, res.Status, res.Header)
				log.Printf("GitHub error %s: %v", u, ghResp)
				return nil, nil, err
			}
			evts, err := parseGithubReviews(res.Body)
			if err != nil {
				return nil, nil, fmt.Errorf("%s: parse github pr reviews: %v", u, err)
			}
			is := make([]any, len(evts))
			for i, v := range evts {
				is[i] = v
			}
			serverDate, err := http.ParseTime(res.Header.Get("Date"))
			if err != nil {
				return nil, nil, fmt.Errorf("invalid server Date response: %v", err)
			}
			mut.GithubIssue.ReviewStatus = &maintpb.GithubIssueSyncStatus{
				ServerDate: timestamppb.New(serverDate.UTC()),
			}

			return is, ghResp, err
		},
		func(v any) error {
			ge := v.(*GitHubReview)
			p.c.mu.RLock()
			_, ok := gi.reviews[ge.ID]
			p.c.mu.RUnlock()
			if ok {
				// Already have it. And they're
				// assumed to be immutable, so the
				// copy we already have should be
				// good. Don't add to mutation log.
				return nil
			}
			mut.GithubIssue.Review = append(mut.GithubIssue.Review, ge.Proto())
			return nil
		})
	if err != nil {
		return err
	}
	p.c.addMutation(mut)
	return nil
}

// parseGithubReviews parses the JSON array of GitHub reviews in r.
// It does this the very manual way (using map[string]interface{})
// instead of using nice types because https://golang.org/issue/15314
// isn't implemented yet and also because even if it were implemented,
// this code still wants to preserve any unknown fields to store in
// the "OtherJSON" field for future updates of the code to parse. (If
// GitHub adds new Event types in the future, we want to archive them,
// even if we don't understand them)
func parseGithubReviews(r io.Reader) ([]*GitHubReview, error) {
	var jevents []map[string]any
	jd := json.NewDecoder(r)
	jd.UseNumber()
	if err := jd.Decode(&jevents); err != nil {
		return nil, err
	}
	var evts []*GitHubReview
	for _, em := range jevents {
		for k, v := range em {
			if v == nil {
				delete(em, k)
			}
		}

		e := &GitHubReview{}

		e.ID = jint64(em["id"])
		delete(em, "id")

		e.Body, _ = em["body"].(string)
		delete(em, "body")

		e.State, _ = em["state"].(string)
		delete(em, "state")

		// TODO: store these two more compactly:
		e.CommitID, _ = em["commit_id"].(string) // "5383ecf5a0824649ffcc0349f00f0317575753d0"
		delete(em, "commit_id")

		getUser := func(field string, gup **GitHubUser) {
			am, ok := em[field].(map[string]any)
			if !ok {
				return
			}
			delete(em, field)
			gu := &GitHubUser{ID: jint64(am["id"])}
			gu.Login, _ = am["login"].(string)
			*gup = gu
		}

		getUser("user", &e.Actor)

		e.ActorAssociation, _ = em["author_association"].(string)
		delete(em, "author_association")

		if createdStr, ok := em["submitted_at"].(string); ok {
			delete(em, "submitted_at")
			var err error
			e.Created, err = time.Parse(time.RFC3339, createdStr)
			if err != nil {
				return nil, err
			}
			e.Created = e.Created.UTC()
		}

		delete(em, "node_id")          // GitHub API v4 Global Node ID; don't store it.
		delete(em, "html_url")         // not needed.
		delete(em, "pull_request_url") // not needed.
		delete(em, "_links")           // not needed. (duplicate data of above two nodes)

		otherJSON, _ := json.Marshal(em)
		e.OtherJSON = string(otherJSON)
		if e.OtherJSON == "{}" {
			e.OtherJSON = ""
		}
		if e.OtherJSON != "" {
			log.Printf("warning: storing unknown field(s) in GitHub review: %s", e.OtherJSON)
		}
		evts = append(evts, e)
	}
	return evts, nil
}

// jint64 return an int64 from the provided JSON object value v.
func jint64(v any) int64 {
	switch v := v.(type) {
	case nil:
		return 0
	case json.Number:
		n, _ := strconv.ParseInt(string(v), 10, 64)
		return n
	default:
		panic(fmt.Sprintf("unexpected type %T", v))
	}
}

// copy of go-github's parseRate, basically.
func parseRate(r *http.Response) github.Rate {
	var rate github.Rate
	// Note: even though the header names below are not canonical (the
	// canonical form would be X-Ratelimit-Limit), this particular
	// casing is what GitHub returns. See headerRateRemaining in
	// package go-github.
	if limit := r.Header.Get("X-RateLimit-Limit"); limit != "" {
		rate.Limit, _ = strconv.Atoi(limit)
	}
	if remaining := r.Header.Get("X-RateLimit-Remaining"); remaining != "" {
		rate.Remaining, _ = strconv.Atoi(remaining)
	}
	if reset := r.Header.Get("X-RateLimit-Reset"); reset != "" {
		if v, _ := strconv.ParseInt(reset, 10, 64); v != 0 {
			rate.Reset = github.Timestamp{Time: time.Unix(v, 0)}
		}
	}
	return rate
}

// Copy of go-github's func newResponse, basically.
func makeGithubResponse(res *http.Response) *github.Response {
	gr := &github.Response{Response: res}
	gr.Rate = parseRate(res)
	for _, lv := range res.Header["Link"] {
		for link := range strings.SplitSeq(lv, ",") {
			segs := strings.Split(strings.TrimSpace(link), ";")
			if len(segs) < 2 {
				continue
			}
			// ensure href is properly formatted
			if !strings.HasPrefix(segs[0], "<") || !strings.HasSuffix(segs[0], ">") {
				continue
			}

			// try to pull out page parameter
			u, err := url.Parse(segs[0][1 : len(segs[0])-1])
			if err != nil {
				continue
			}
			page := u.Query().Get("page")
			if page == "" {
				continue
			}

			for _, seg := range segs[1:] {
				switch strings.TrimSpace(seg) {
				case `rel="next"`:
					gr.NextPage, _ = strconv.Atoi(page)
				case `rel="prev"`:
					gr.PrevPage, _ = strconv.Atoi(page)
				case `rel="first"`:
					gr.FirstPage, _ = strconv.Atoi(page)
				case `rel="last"`:
					gr.LastPage, _ = strconv.Atoi(page)
				}
			}
		}
	}
	return gr
}

var rxReferences = regexp.MustCompile(`(?:\b([\w\-]+)/([\w\-]+))?\#(\d+)\b`)

// parseGithubRefs parses references to GitHub issues from commit message commitMsg.
// Multiple references to the same issue are deduplicated.
func (c *Corpus) parseGithubRefs(gerritProj string, commitMsg string) []GitHubIssueRef {
	// Use of rxReferences by itself caused this function to take 20% of the CPU time.
	// TODO(bradfitz): stop using regexps here.
	// But in the meantime, help the regexp engine with this one weird trick:
	// Reduce the length of the string given to FindAllStringSubmatch.
	// Discard all lines before the first line containing a '#'.
	// The "Fixes #nnnn" is usually at the end, so this discards most of the input.
	// Now CPU is only 2% instead of 20%.
	hash := strings.IndexByte(commitMsg, '#')
	if hash == -1 {
		return nil
	}
	nl := strings.LastIndexByte(commitMsg[:hash], '\n')
	commitMsg = commitMsg[nl+1:]

	// TODO: use FindAllStringSubmatchIndex instead, so we can
	// back up and see what's behind it and ignore "#1", "#2",
	// "#3" 'references' which are actually bullets or ARM
	// disassembly, and only respect them as real if they have the
	// word "Fixes " or "Issue " or similar before them.
	ms := rxReferences.FindAllStringSubmatch(commitMsg, -1)
	if len(ms) == 0 {
		return nil
	}
	/* e.g.
	2017/03/30 21:42:07 matches: [["golang/go#9327" "golang" "go" "9327"]]
	2017/03/30 21:42:07 matches: [["golang/go#16512" "golang" "go" "16512"] ["golang/go#18404" "golang" "go" "18404"]]
	2017/03/30 21:42:07 matches: [["#1" "" "" "1"]]
	2017/03/30 21:42:07 matches: [["#10234" "" "" "10234"]]
	2017/03/30 21:42:31 matches: [["GoogleCloudPlatform/gcloud-golang#262" "GoogleCloudPlatform" "gcloud-golang" "262"]]
	2017/03/30 21:42:31 matches: [["GoogleCloudPlatform/google-cloud-go#481" "GoogleCloudPlatform" "google-cloud-go" "481"]]
	*/
	c.initGithub()
	github := c.GitHub()
	refs := make([]GitHubIssueRef, 0, len(ms))
	for _, m := range ms {
		owner, repo, numStr := strings.ToLower(m[1]), strings.ToLower(m[2]), m[3]
		num, err := strconv.ParseInt(numStr, 10, 32)
		if err != nil {
			continue
		}
		if owner == "" {
			if gerritProj == "go.googlesource.com/go" {
				owner, repo = "golang", "go"
			} else {
				continue
			}
		}
		ref := GitHubIssueRef{github.getOrCreateRepo(owner, repo), int32(num)}
		if slices.Contains(refs, ref) {
			continue
		}
		refs = append(refs, ref)
	}
	return refs
}

type limitTransport struct {
	limiter *rate.Limiter
	base    http.RoundTripper
}

func (t limitTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	limiter := t.limiter
	// NOTE(cbro): limiter should not be nil, but check defensively.
	if limiter != nil {
		if err := limiter.Wait(r.Context()); err != nil {
			return nil, err
		}
	}
	return t.base.RoundTrip(r)
}

// isIssueGoneError reports whether err from a GitHub API call indicates the
// issue no longer exists in this repo. This covers HTTP 404 (Not Found),
// 410 (Gone), and 301 (Moved Permanently, used for transferred issues).
// The error may be a *github.ErrorResponse or a *github.RedirectionError.
func isIssueGoneError(err error) bool {
	if ge, ok := err.(*github.ErrorResponse); ok {
		return ge.Response.StatusCode == http.StatusNotFound ||
			ge.Response.StatusCode == http.StatusGone ||
			ge.Response.StatusCode == http.StatusMovedPermanently
	}
	var re *github.RedirectionError
	if errors.As(err, &re) {
		return re.StatusCode == http.StatusMovedPermanently
	}
	return false
}

// canRetry reports whether ctx hasn't been canceled and err is a non-nil retryable error.
// If so, it blocks until enough time passes so that it's acceptable to retry immediately.
func canRetry(ctx context.Context, err error) bool {
	switch e := err.(type) {
	case *github.RateLimitError:
		log.Printf("GitHub rate limit error: %s, waiting until %s", e.Message, e.Rate.Reset.Time)
		ctx, cancel := context.WithDeadline(ctx, e.Rate.Reset.Time)
		defer cancel()
		<-ctx.Done()
		return ctx.Err() != context.Canceled
	case *github.AbuseRateLimitError:
		if e.RetryAfter != nil {
			log.Printf("GitHub rate abuse error: %s, waiting for %s", e.Message, *e.RetryAfter)
			ctx, cancel := context.WithTimeout(ctx, *e.RetryAfter)
			defer cancel()
			<-ctx.Done()
			return ctx.Err() != context.Canceled
		}
		log.Printf("GitHub rate abuse error: %s", e.Message)
	}
	return false
}
