// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package maintner mirrors, searches, syncs, and serves Git, Github,
// and Gerrit metadata.
//
// Maintner is short for "Maintainer". This package is intended for
// use by many tools. The name of the daemon that serves the maintner
// data to other tools is "maintnerd".
package maintner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gxbm/maintner/maintpb"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// Corpus holds all of a project's metadata.
type Corpus struct {
	mutationLogger       MutationLogger // non-nil when this is a self-updating corpus
	mutationSource       MutationSource // from Initialize
	verbose              bool
	dataDir              string
	sawErrSplit          bool
	reactionScanInterval time.Duration // if >0, enable GraphQL reaction scanning

	mu sync.RWMutex // guards all following fields
	// corpus state:
	didInit   bool // true after Initialize completes successfully
	debug     bool
	strIntern map[string]string // interned strings, including binary githashes

	// pubsub:
	activityChans map[string]chan struct{} // keyed by topic

	// github-specific
	github              *GitHub
	gerrit              *Gerrit
	watchedGithubRepos  []watchedGithubRepo
	watchedGerritRepos  []watchedGerritRepo
	githubLimiter       *rate.Limiter
	githubBaseTransport http.RoundTripper

	// git-specific:
	lastGitCount          time.Time // last time of log spam about loading status
	pollGitDirs           []polledGitCommits
	watchedGitRepoConfigs []watchedGitRepoState
	gitPeople             map[string]*GitPerson
	gitCommit             map[GitHash]*GitCommit
	gitRepos              map[string]bool               // repo names seen in git mutations
	gitRefs               map[string]map[string]GitHash // repo name -> ref name -> hash
	gitTags               map[GitHash]*GitTag           // tag object hash -> parsed tag
	gitCommitTodo         map[GitHash]bool              // -> true
	gitOfHg               map[string]GitHash            // hg hex hash -> git hash
	zoneCache             map[string]*time.Location     // "+0530" => location
}

// RLock grabs the corpus's read lock. Grabbing the read lock prevents
// any concurrent writes from mutating the corpus. This is only
// necessary if the application is querying the corpus and calling its
// Update method concurrently.
func (c *Corpus) RLock() { c.mu.RLock() }

// RUnlock unlocks the corpus's read lock.
func (c *Corpus) RUnlock() { c.mu.RUnlock() }

type polledGitCommits struct {
	repo *maintpb.GitRepo
	dir  string
}

// EnableLeaderMode prepares c to be the leader. This should only be
// called by the maintnerd process.
//
// The provided scratchDir will store git checkouts.
func (c *Corpus) EnableLeaderMode(logger MutationLogger, scratchDir string) {
	c.mutationLogger = logger
	c.dataDir = scratchDir
}

// SetVerbose enables or disables verbose logging.
func (c *Corpus) SetVerbose(v bool) { c.verbose = v }

// EnableReactionScanning enables periodic scanning for reaction changes
// using the GitHub GraphQL API. This is necessary to detect reactions on
// issues that have no other activity, because GitHub does not update an
// issue's or comment's UpdatedAt timestamp when reactions are added or
// removed. Without this, reactions are only captured opportunistically
// when an issue is re-synced for another reason (e.g. comment edit,
// label change).
//
// The interval controls how often the full count scan runs. A reasonable
// default is 24 hours. Each scan pages through all issues via GraphQL
// (~100 per request) to compare reaction counts, then fetches detailed
// reactions only for issues where counts diverge.
func (c *Corpus) EnableReactionScanning(interval time.Duration) {
	c.reactionScanInterval = interval
}

func (c *Corpus) getDataDir() string {
	if c.dataDir == "" {
		panic("getDataDir called before Corpus.EnableLeaderMode")
	}
	return c.dataDir
}

// GitHub returns the corpus's github data.
func (c *Corpus) GitHub() *GitHub {
	if c.github != nil {
		return c.github
	}
	return new(GitHub)
}

// Gerrit returns the corpus's Gerrit data.
func (c *Corpus) Gerrit() *Gerrit {
	if c.gerrit != nil {
		return c.gerrit
	}
	return new(Gerrit)
}

// Check verifies the internal structure of the Corpus data structures.
// It is intended for tests and debugging.
func (c *Corpus) Check() error {
	if err := c.Gerrit().check(); err != nil {
		return fmt.Errorf("gerrit: %v", err)
	}

	for hash, gc := range c.gitCommit {
		if gc.Committer == placeholderCommitter {
			return fmt.Errorf("corpus git commit %v has placeholder committer", hash)
		}
		if gc.Hash != hash {
			return fmt.Errorf("git commit for key %q had GitCommit.Hash %q", hash, gc.Hash)
		}
		for _, pc := range gc.Parents {
			if _, ok := c.gitCommit[pc.Hash]; !ok {
				return fmt.Errorf("git commit %q exists but its parent %q does not", gc.Hash, pc.Hash)
			}
		}
	}

	return nil
}

// requires c.mu be held for writing
func (c *Corpus) str(s string) string {
	if v, ok := c.strIntern[s]; ok {
		return v
	}
	if c.strIntern == nil {
		c.strIntern = make(map[string]string)
	}
	c.strIntern[s] = s
	return s
}

func (c *Corpus) strb(b []byte) string {
	if v, ok := c.strIntern[string(b)]; ok {
		return v
	}
	return c.str(string(b))
}

func (c *Corpus) SetDebug() {
	c.debug = true
}

func (c *Corpus) debugf(format string, v ...any) {
	if c.debug {
		log.Printf(format, v...)
	}
}

// gerritProjNameRx is the pattern describing a Gerrit project name.
// TODO: figure out if this is accurate.
var gerritProjNameRx = regexp.MustCompile(`^[a-z0-9]+[a-z0-9\-\_]*$`)

// TrackGoGitRepo registers a git directory to have its metadata slurped into the corpus.
// The goRepo is a name like "go" or "net". The dir is a path on disk.
func (c *Corpus) TrackGoGitRepo(goRepo, dir string) {
	if c.mutationLogger == nil {
		panic("can't TrackGoGitRepo in non-leader mode")
	}
	if !gerritProjNameRx.MatchString(goRepo) {
		panic(fmt.Sprintf("bogus goRepo value %q", goRepo))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pollGitDirs = append(c.pollGitDirs, polledGitCommits{
		repo: &maintpb.GitRepo{GoRepo: goRepo},
		dir:  dir,
	})
}

// WatchedGitRepo configures a git repository to be tracked by the corpus.
type WatchedGitRepo struct {
	// Name is a short name like "foo" or "org/repo" (e.g. "tailscale/tailscale").
	Name string
	// Remote is the git URL for ls-remote and fetch (e.g. "git://rogitproxy/tailscale/tailscale").
	Remote string
	// Refs are exact ref names to watch (e.g. "refs/heads/main").
	Refs []string
	// RefRegexps are patterns matched against ref names (e.g. `refs/tags/v1\..*`).
	RefRegexps []string
	// PollInterval is how often to run git ls-remote. Default 30s.
	PollInterval time.Duration
}

type watchedGitRepoState struct {
	conf       WatchedGitRepo
	refRegexps []*regexp.Regexp
}

func (ws *watchedGitRepoState) matchesRef(refName string) bool {
	for _, r := range ws.conf.Refs {
		if r == refName {
			return true
		}
	}
	for _, re := range ws.refRegexps {
		if re.MatchString(refName) {
			return true
		}
	}
	return false
}

// TrackGitRepo registers a git repository to have its commits tracked.
// The conf.Name must be non-empty and conf.Remote must be non-empty.
// At least one of Refs or RefRegexps must be provided.
func (c *Corpus) TrackGitRepo(conf WatchedGitRepo) {
	if c.mutationLogger == nil {
		panic("can't TrackGitRepo in non-leader mode")
	}
	if conf.Name == "" {
		panic("TrackGitRepo: Name is required")
	}
	if conf.Remote == "" {
		panic("TrackGitRepo: Remote is required")
	}
	if len(conf.Refs) == 0 && len(conf.RefRegexps) == 0 {
		panic("TrackGitRepo: at least one of Refs or RefRegexps is required")
	}
	if conf.PollInterval == 0 {
		conf.PollInterval = 30 * time.Second
	}
	ws := watchedGitRepoState{
		conf: conf,
	}
	for _, pat := range conf.RefRegexps {
		re, err := regexp.Compile(pat)
		if err != nil {
			panic(fmt.Sprintf("TrackGitRepo: bad RefRegexp %q: %v", pat, err))
		}
		ws.refRegexps = append(ws.refRegexps, re)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.watchedGitRepoConfigs = append(c.watchedGitRepoConfigs, ws)
}

// A MutationSource yields a log of mutations that will catch a corpus
// back up to the present.
type MutationSource interface {
	// GetMutations returns a channel of mutations or related events.
	// The channel will never be closed.
	// All sends on the returned channel should select
	// on the provided context.
	GetMutations(context.Context) <-chan MutationStreamEvent
}

// MutationStreamEvent represents one of three possible events while
// reading mutations from disk or another source.
// An event is either a mutation, an error, or reaching the current
// end of the log. Exactly one of the three fields will be non-zero.
type MutationStreamEvent struct {
	Mutation *maintpb.Mutation

	// Err is a fatal error reading the log. No other events will
	// follow an Err.
	Err error

	// End, if true, means that all mutations have been sent and
	// the next event might take some time to arrive (it might not
	// have occurred yet). The End event is not a terminal state
	// like Err. There may be multiple Ends.
	End bool
}

// Initialize populates the Corpus using the data from the
// MutationSource. It returns once it's up-to-date. To incrementally
// update it later, use the Update method.
func (c *Corpus) Initialize(ctx context.Context, src MutationSource) error {
	if c.mutationSource != nil {
		panic("duplicate call to Initialize")
	}
	c.mutationSource = src
	log.Printf("Loading data from log %T ...", src)
	return c.update(ctx, nil)
}

// ErrSplit is returned when the client notices the leader's
// mutation log has changed. This can happen if the leader restarts
// with uncommitted transactions. (The leader only commits mutations
// periodically.)
var ErrSplit = errors.New("maintner: leader server's history split, process out of sync")

// Update incrementally updates the corpus from its current state to
// the latest state from the MutationSource passed earlier to
// Initialize. It does not return until there's either a new change or
// the context expires.
// If Update returns ErrSplit, the corpus can no longer be updated.
//
// Update must not be called concurrently with any other Update calls. If
// reading the corpus concurrently while the corpus is updating, you must hold
// the read lock using Corpus.RLock.
//
// Deprecated: Update holds the corpus write lock for the entire duration,
// including while blocking on the network for new data. Use
// [Corpus.RunUpdateLoop] instead, which only holds the lock while processing
// mutations.
func (c *Corpus) Update(ctx context.Context) error {
	if c.mutationSource == nil {
		panic("Update called without call to Initialize")
	}
	if c.sawErrSplit {
		panic("Update called after previous call returned ErrSplit")
	}
	log.Printf("Updating data from log %T ...", c.mutationSource)
	err := c.update(ctx, nil)
	if err == ErrSplit {
		c.sawErrSplit = true
	}
	return err
}

// UpdateWithLocker behaves just like Update, but holds lk when processing
// mutation events.
//
// Deprecated: Use [Corpus.RunUpdateLoop] instead.
func (c *Corpus) UpdateWithLocker(ctx context.Context, lk sync.Locker) error {
	if c.mutationSource == nil {
		panic("UpdateWithLocker called without call to Initialize")
	}
	if c.sawErrSplit {
		panic("UpdateWithLocker called after previous call returned ErrSplit")
	}
	log.Printf("Updating data from log %T ...", c.mutationSource)
	err := c.update(ctx, lk)
	if err == ErrSplit {
		c.sawErrSplit = true
	}
	return err
}

// RunUpdateLoop continuously polls the MutationSource for new mutations and
// applies them to the corpus. Unlike [Corpus.Update], it does not hold the
// corpus write lock while waiting for data. Instead, it only briefly locks the corpus
// while processing each mutation. This allows concurrent readers holding
// [Corpus.RLock] to proceed unblocked while the loop waits for new data.
//
// RunUpdateLoop runs until ctx is canceled or a fatal error (such as
// [ErrSplit]) occurs. It is intended to be run in a goroutine for the
// lifetime of the process.
//
// RunUpdateLoop must not be called concurrently with itself or with Update.
func (c *Corpus) RunUpdateLoop(ctx context.Context) error {
	if c.mutationSource == nil {
		panic("RunUpdateLoop called without call to Initialize")
	}
	if c.sawErrSplit {
		panic("RunUpdateLoop called after previous call returned ErrSplit")
	}
	src := c.mutationSource
	done := ctx.Done()
	for {
		ch := src.GetMutations(ctx)
		for {
			// Wait for an event WITHOUT holding any lock.
			var e MutationStreamEvent
			select {
			case <-done:
				return ctx.Err()
			case e = <-ch:
			}

			if e.Err != nil {
				log.Printf("Corpus GetMutations: %v", e.Err)
				if e.Err == ErrSplit {
					c.sawErrSplit = true
				}
				return e.Err
			}

			// Lock only while mutating the corpus.
			c.mu.Lock()
			if e.End {
				c.didInit = true
				c.finishProcessing()
				c.mu.Unlock()
				break // inner loop; long-poll for next batch
			}
			c.processMutationLocked(e.Mutation)
			c.mu.Unlock()
		}
	}
}

type noopLocker struct{}

func (noopLocker) Lock()   {}
func (noopLocker) Unlock() {}

// lk optionally specifies a locker to use while processing mutations.
func (c *Corpus) update(ctx context.Context, lk sync.Locker) error {
	src := c.mutationSource
	ch := src.GetMutations(ctx)
	done := ctx.Done()
	c.mu.Lock()
	defer c.mu.Unlock()
	if lk == nil {
		lk = noopLocker{}
	}
	for {
		select {
		case <-done:
			err := ctx.Err()
			log.Printf("Context expired while loading data from log %T: %v", src, err)
			return err
		case e := <-ch:
			if e.Err != nil {
				log.Printf("Corpus GetMutations: %v", e.Err)
				return e.Err
			}
			if e.End {
				c.didInit = true
				lk.Lock()
				c.finishProcessing()
				lk.Unlock()
				log.Printf("Reloaded data from log %T.", src)
				return nil
			}
			lk.Lock()
			c.processMutationLocked(e.Mutation)
			lk.Unlock()
		}
	}
}

// addMutation adds a mutation to the log and immediately processes it.
func (c *Corpus) addMutation(m *maintpb.Mutation) {
	if c.verbose {
		log.Printf("mutation: %v", m)
	}
	c.mu.Lock()
	c.processMutationLocked(m)
	c.finishProcessing()
	c.mu.Unlock()

	if c.mutationLogger == nil {
		return
	}
	err := c.mutationLogger.Log(m)
	if err != nil {
		// TODO: handle errors better? failing is only safe option.
		log.Fatalf("could not log mutation %v: %v\n", m, err)
	}
}

// c.mu must be held.
func (c *Corpus) processMutationLocked(m *maintpb.Mutation) {
	if im := m.GithubIssue; im != nil {
		c.processGithubIssueMutation(im)
	}
	if gm := m.Github; gm != nil {
		c.processGithubMutation(gm)
	}
	if gm := m.Git; gm != nil {
		c.processGitMutation(gm)
	}
	if gm := m.Gerrit; gm != nil {
		c.processGerritMutation(gm)
	}
	if am := m.GithubActions; am != nil {
		c.processGithubActionsMutation(am)
	}
}

// finishProcessing fixes up invariants and data structures before
// returning the Corpus from the Update loop back to the user.
//
// c.mu must be held.
func (c *Corpus) finishProcessing() {
	c.gerrit.finishProcessing()
}

// SyncLoop runs forever (until an error or context expiration) and
// updates the corpus as the tracked sources change.
func (c *Corpus) SyncLoop(ctx context.Context) error {
	return c.sync(ctx, true)
}

// Sync updates the corpus from its tracked sources.
func (c *Corpus) Sync(ctx context.Context) error {
	return c.sync(ctx, false)
}

func (c *Corpus) sync(ctx context.Context, loop bool) error {
	if _, ok := c.mutationSource.(*netMutSource); ok {
		return errors.New("maintner: can't run Corpus.Sync on a Corpus using NetworkMutationSource (did you mean Update?)")
	}

	group, ctx := errgroup.WithContext(ctx)
	for _, w := range c.watchedGithubRepos {
		gr, ts, filter := w.gr, w.tokenSource, w.filter
		group.Go(func() error {
			log.Printf("Polling %v ...", gr.id)
			for {
				err := gr.sync(ctx, ts, filter, loop)
				if loop && isTempErr(err) {
					log.Printf("Temporary error from github %v: %v", gr.ID(), err)
					time.Sleep(30 * time.Second)
					continue
				}
				log.Printf("github sync ending for %v: %v", gr.ID(), err)
				return err
			}
		})
	}
	for _, rp := range c.pollGitDirs {
		group.Go(func() error {
			for {
				err := c.syncGitCommits(ctx, rp, loop)
				if loop && isTempErr(err) {
					log.Printf("Temporary error from git repo %v: %v", rp.dir, err)
					time.Sleep(30 * time.Second)
					continue
				}
				log.Printf("git sync ending for %v: %v", rp.dir, err)
				return err
			}
		})
	}
	for i := range c.watchedGitRepoConfigs {
		ws := &c.watchedGitRepoConfigs[i]
		group.Go(func() error {
			log.Printf("Polling git repo %v ...", ws.conf.Name)
			for {
				err := c.syncGitRepo(ctx, ws, loop)
				if loop && isTempErr(err) {
					log.Printf("Temporary error from git repo %v: %v", ws.conf.Name, err)
					time.Sleep(30 * time.Second)
					continue
				}
				log.Printf("git repo sync ending for %v: %v", ws.conf.Name, err)
				return err
			}
		})
	}
	for _, w := range c.watchedGerritRepos {
		gp := w.project
		group.Go(func() error {
			log.Printf("Polling gerrit %v ...", gp.proj)
			for {
				err := gp.sync(ctx, loop)
				if loop && isTempErr(err) {
					log.Printf("Temporary error from gerrit %v: %v", gp.proj, err)
					time.Sleep(30 * time.Second)
					continue
				}
				log.Printf("gerrit sync ending for %v: %v", gp.proj, err)
				return err
			}
		})
	}
	return group.Wait()
}

func isTempErr(err error) bool {
	log.Printf("IS TEMP ERROR? %T %v", err, err)
	return true
}

// GitRefInfo represents a tracked git ref.
type GitRefInfo struct {
	Repo string  // repo name, e.g. "tailscale/tailscale"
	Ref  string  // ref name, e.g. "refs/heads/main"
	Hash GitHash // current hash
}

// GitRepos returns the set of git repo names seen in the corpus.
func (c *Corpus) GitRepos() []string {
	repos := make([]string, 0, len(c.gitRepos))
	for name := range c.gitRepos {
		repos = append(repos, name)
	}
	return repos
}

// GitRef returns the commit hash for a given repo and ref name, or
// the zero GitHash if not known.
func (c *Corpus) GitRef(repoName, refName string) GitHash {
	if refs := c.gitRefs[repoName]; refs != nil {
		return refs[refName]
	}
	return ""
}

// ForeachGitCommit calls fn for each git commit in the corpus.
// Placeholder commits (with no committer) are skipped.
// If fn returns an error, iteration stops and that error is returned.
// The caller must hold the read lock if the corpus may be updated concurrently.
func (c *Corpus) ForeachGitCommit(fn func(*GitCommit) error) error {
	for _, gc := range c.gitCommit {
		if gc.Committer == placeholderCommitter {
			continue
		}
		if err := fn(gc); err != nil {
			return err
		}
	}
	return nil
}

// ForeachGitCommitForRepo calls fn for each git commit in the corpus
// that belongs to the named repo. Placeholder commits are skipped.
func (c *Corpus) ForeachGitCommitForRepo(repoName string, fn func(*GitCommit) error) error {
	for _, gc := range c.gitCommit {
		if gc.Committer == placeholderCommitter {
			continue
		}
		if gc.Repo != repoName {
			continue
		}
		if err := fn(gc); err != nil {
			return err
		}
	}
	return nil
}

// ForeachGitRef calls fn for each tracked git ref across all watched repos.
func (c *Corpus) ForeachGitRef(fn func(GitRefInfo) error) error {
	for repo, refs := range c.gitRefs {
		for ref, hash := range refs {
			if err := fn(GitRefInfo{Repo: repo, Ref: ref, Hash: hash}); err != nil {
				return err
			}
		}
	}
	return nil
}

// ForeachGitRepoRef calls fn for each tracked git ref in the named repo.
func (c *Corpus) ForeachGitRepoRef(repoName string, fn func(GitRefInfo) error) error {
	for ref, hash := range c.gitRefs[repoName] {
		if err := fn(GitRefInfo{Repo: repoName, Ref: ref, Hash: hash}); err != nil {
			return err
		}
	}
	return nil
}

// GitTagInfo represents a tag ref in a git repo.
type GitTagInfo struct {
	Repo       string  // repo name
	Ref        string  // e.g. "refs/tags/v1.0.0"
	Hash       GitHash // what the ref points to (commit for lightweight, tag object for annotated)
	CommitHash GitHash // the underlying commit (same as Hash for lightweight tags)
	Tag        *GitTag // non-nil for annotated tags; nil for lightweight tags
}

// ForeachGitTag calls fn for each tag ref in the named repo.
// Both lightweight and annotated tags are included.
func (c *Corpus) ForeachGitTag(repoName string, fn func(GitTagInfo) error) error {
	for ref, hash := range c.gitRefs[repoName] {
		if !strings.HasPrefix(ref, "refs/tags/") {
			continue
		}
		info := GitTagInfo{
			Repo: repoName,
			Ref:  ref,
			Hash: hash,
		}
		if gt := c.gitTags[hash]; gt != nil {
			info.Tag = gt
			info.CommitHash = gt.Target
		} else {
			info.CommitHash = hash
		}
		if err := fn(info); err != nil {
			return err
		}
	}
	return nil
}

// GitTagByHash returns the parsed annotated tag object for the given hash,
// or nil if the hash is not a known tag object.
func (c *Corpus) GitTagByHash(hash GitHash) *GitTag {
	return c.gitTags[hash]
}
