// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package maintner

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bradfitz/gxbm/maintner/maintpb"
)

func TestParsePerson(t *testing.T) {
	var c Corpus

	p, ct, err := c.parsePerson([]byte(" Foo Bar <foo@bar.com> 1257894000 -0800"))
	if err != nil {
		t.Fatal(err)
	}
	wantp := &GitPerson{Str: "Foo Bar <foo@bar.com>"}
	if !reflect.DeepEqual(p, wantp) {
		t.Errorf("person = %+v; want %+v", p, wantp)
	}
	wantct := time.Unix(1257894000, 0)
	if !ct.Equal(wantct) {
		t.Errorf("commit time = %v; want %v", ct, wantct)
	}
	zoneName, off := ct.Zone()
	if want := "-0800"; zoneName != want {
		t.Errorf("zone name = %q; want %q", zoneName, want)
	}
	if want := -28800; off != want {
		t.Errorf("offset = %v; want %v", off, want)
	}

	p2, ct2, err := c.parsePerson([]byte("Foo Bar <foo@bar.com> 1257894001 -0800"))
	if err != nil {
		t.Fatal(err)
	}
	if p != p2 {
		t.Errorf("gitPerson pointer values differ; not sharing memory")
	}
	if !ct2.Equal(ct.Add(time.Second)) {
		t.Errorf("wrong time")
	}
}

func BenchmarkParsePerson(b *testing.B) {
	b.ReportAllocs()
	in := []byte(" Foo Bar <foo@bar.com> 1257894000 -0800")
	var c Corpus
	for i := 0; i < b.N; i++ {
		_, _, err := c.parsePerson(in)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// testGitRepo is a test helper that manages a temporary git repository
// and a maintner Corpus that tracks it.
type testGitRepo struct {
	t         testing.TB
	dir       string // the "remote" repo
	scratch   string // bare scratch repo for fetching
	corpus    *Corpus
	ws        *watchedGitRepoState
	mutations []*maintpb.Mutation
}

// newTestGitRepo creates a temporary git repository and a Corpus configured
// to track it. The caller uses Git to run commands in the repo, then calls
// Sync to poll and process mutations.
func newTestGitRepo(t testing.TB) *testGitRepo {
	t.Helper()

	dir := t.TempDir()
	repoDir := filepath.Join(dir, "repo")
	scratchDir := filepath.Join(dir, "scratch")

	// Init a non-bare repo as the "remote".
	run(t, dir, "git", "init", repoDir)
	run(t, repoDir, "git", "config", "user.email", "test@example.com")
	run(t, repoDir, "git", "config", "user.name", "Test User")

	r := &testGitRepo{
		t:       t,
		dir:     repoDir,
		scratch: scratchDir,
	}

	logger := &mutationCollector{}
	c := new(Corpus)
	c.mutationLogger = logger

	conf := WatchedGitRepo{
		Name:         "testgit",
		Remote:       repoDir,
		RefRegexps:   []string{`refs/heads/.*`, `refs/tags/.*`},
		PollInterval: time.Hour, // we poll manually
	}
	ws := watchedGitRepoState{conf: conf}
	for _, pat := range conf.RefRegexps {
		re, err := regexp.Compile(pat)
		if err != nil {
			t.Fatal(err)
		}
		ws.refRegexps = append(ws.refRegexps, re)
	}
	c.watchedGitRepoConfigs = append(c.watchedGitRepoConfigs, ws)

	r.corpus = c
	r.ws = &c.watchedGitRepoConfigs[0]

	return r
}

// Git runs a git command in the test repo.
func (r *testGitRepo) Git(args ...string) string {
	r.t.Helper()
	return run(r.t, r.dir, "git", args...)
}

// Sync runs one poll cycle, syncing the corpus with the repo.
// It returns the mutations generated during this sync.
func (r *testGitRepo) Sync() []*maintpb.Mutation {
	r.t.Helper()

	// Init bare scratch repo if needed.
	if _, err := os.Stat(filepath.Join(r.scratch, "HEAD")); os.IsNotExist(err) {
		if err := os.MkdirAll(r.scratch, 0755); err != nil {
			r.t.Fatal(err)
		}
		run(r.t, r.scratch, "git", "init", "--bare", r.scratch)
	}

	logger := r.corpus.mutationLogger.(*mutationCollector)
	before := len(logger.mutations)

	ctx := context.Background()
	if err := r.corpus.syncGitRepoOnce(ctx, r.ws, r.scratch); err != nil {
		r.t.Fatalf("syncGitRepoOnce: %v", err)
	}

	newMuts := logger.mutations[before:]
	r.mutations = append(r.mutations, newMuts...)
	return newMuts
}

func run(t testing.TB, dir string, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_DATE=2024-01-15T12:00:00Z",
		"GIT_COMMITTER_DATE=2024-01-15T12:00:00Z",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// mutationCollector is a MutationLogger that collects mutations in memory.
// Log may be called concurrently from multiple sync goroutines (addMutation
// invokes the logger outside c.mu), so the slice needs its own lock.
type mutationCollector struct {
	mu        sync.Mutex
	mutations []*maintpb.Mutation
}

func (mc *mutationCollector) Log(m *maintpb.Mutation) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.mutations = append(mc.mutations, m)
	return nil
}

func TestGitSyncBasicCommit(t *testing.T) {
	r := newTestGitRepo(t)

	// Create initial commit on main.
	r.Git("commit", "--allow-empty", "-m", "initial commit")

	muts := r.Sync()
	if len(muts) == 0 {
		t.Fatal("expected mutations from initial sync")
	}

	// Should have at least one commit mutation and one ref update.
	var hasCommit, hasRef bool
	for _, m := range muts {
		if g := m.Git; g != nil {
			if g.Commit != nil {
				hasCommit = true
			}
			if g.RefUpdate != nil {
				hasRef = true
			}
		}
	}
	if !hasCommit {
		t.Error("no commit mutation found")
	}
	if !hasRef {
		t.Error("no ref update mutation found")
	}

	// Verify corpus state.
	repos := r.corpus.GitRepos()
	if len(repos) != 1 || repos[0] != "testgit" {
		t.Errorf("GitRepos = %v; want [testgit]", repos)
	}

	// Should have a ref for the main branch.
	var refs []GitRefInfo
	r.corpus.ForeachGitRepoRef("testgit", func(ri GitRefInfo) error {
		refs = append(refs, ri)
		return nil
	})
	if len(refs) == 0 {
		t.Fatal("no refs found in corpus")
	}
	found := false
	for _, ri := range refs {
		if strings.HasPrefix(ri.Ref, "refs/heads/") {
			found = true
		}
	}
	if !found {
		t.Errorf("no branch ref found; refs = %v", refs)
	}
}

func TestGitSyncRefUpdate(t *testing.T) {
	r := newTestGitRepo(t)

	r.Git("commit", "--allow-empty", "-m", "first")
	r.Sync()

	// Get the ref hash after first sync.
	var hash1 GitHash
	r.corpus.ForeachGitRepoRef("testgit", func(ri GitRefInfo) error {
		if strings.HasPrefix(ri.Ref, "refs/heads/") {
			hash1 = ri.Hash
		}
		return nil
	})
	if hash1 == "" {
		t.Fatal("no branch ref after first sync")
	}

	// Add another commit.
	r.Git("commit", "--allow-empty", "-m", "second")
	muts := r.Sync()
	if len(muts) == 0 {
		t.Fatal("expected mutations from second sync")
	}

	// Ref should have changed.
	var hash2 GitHash
	r.corpus.ForeachGitRepoRef("testgit", func(ri GitRefInfo) error {
		if strings.HasPrefix(ri.Ref, "refs/heads/") {
			hash2 = ri.Hash
		}
		return nil
	})
	if hash2 == "" {
		t.Fatal("no branch ref after second sync")
	}
	if hash1 == hash2 {
		t.Error("ref hash did not change after new commit")
	}

	// Third sync with no changes should produce no mutations.
	muts = r.Sync()
	if len(muts) != 0 {
		t.Errorf("expected 0 mutations on no-op sync; got %d", len(muts))
	}
}

func TestGitSyncLightweightTag(t *testing.T) {
	r := newTestGitRepo(t)

	r.Git("commit", "--allow-empty", "-m", "initial")
	r.Sync()

	// Create a lightweight tag.
	r.Git("tag", "v1.0")
	muts := r.Sync()

	// Should see a ref update for the tag, but no tag object mutation.
	var hasTagRef bool
	for _, m := range muts {
		if g := m.Git; g != nil {
			if g.RefUpdate != nil && g.RefUpdate.Ref == "refs/tags/v1.0" {
				hasTagRef = true
			}
			if g.Tag != nil {
				t.Error("lightweight tag should not produce a tag object mutation")
			}
		}
	}
	if !hasTagRef {
		t.Error("no ref update for lightweight tag")
	}

	// ForeachGitTag should list it with Tag == nil.
	var tags []GitTagInfo
	r.corpus.ForeachGitTag("testgit", func(ti GitTagInfo) error {
		tags = append(tags, ti)
		return nil
	})
	if len(tags) != 1 {
		t.Fatalf("ForeachGitTag returned %d tags; want 1", len(tags))
	}
	tag := tags[0]
	if tag.Ref != "refs/tags/v1.0" {
		t.Errorf("tag ref = %q; want refs/tags/v1.0", tag.Ref)
	}
	if tag.Tag != nil {
		t.Error("lightweight tag should have Tag == nil")
	}
	if tag.Hash != tag.CommitHash {
		t.Error("lightweight tag Hash and CommitHash should be equal")
	}
}

func TestGitSyncAnnotatedTag(t *testing.T) {
	r := newTestGitRepo(t)

	r.Git("commit", "--allow-empty", "-m", "initial")
	r.Sync()

	// Create an annotated tag.
	r.Git("tag", "-a", "v2.0", "-m", "release v2.0")
	muts := r.Sync()

	// Should see both a tag object mutation and a ref update.
	var hasTagObj, hasTagRef bool
	var tagMut *maintpb.GitTag
	for _, m := range muts {
		if g := m.Git; g != nil {
			if g.Tag != nil {
				hasTagObj = true
				tagMut = g.Tag
			}
			if g.RefUpdate != nil && g.RefUpdate.Ref == "refs/tags/v2.0" {
				hasTagRef = true
			}
		}
	}
	if !hasTagObj {
		t.Error("no tag object mutation for annotated tag")
	}
	if !hasTagRef {
		t.Error("no ref update for annotated tag")
	}
	if tagMut != nil && tagMut.TargetHash == "" {
		t.Error("tag mutation missing target_sha1")
	}

	// ForeachGitTag should list it with Tag != nil.
	var tags []GitTagInfo
	r.corpus.ForeachGitTag("testgit", func(ti GitTagInfo) error {
		tags = append(tags, ti)
		return nil
	})
	if len(tags) != 1 {
		t.Fatalf("ForeachGitTag returned %d tags; want 1", len(tags))
	}
	tag := tags[0]
	if tag.Ref != "refs/tags/v2.0" {
		t.Errorf("tag ref = %q; want refs/tags/v2.0", tag.Ref)
	}
	if tag.Tag == nil {
		t.Fatal("annotated tag should have Tag != nil")
	}
	if tag.Tag.Name != "v2.0" {
		t.Errorf("tag name = %q; want v2.0", tag.Tag.Name)
	}
	if tag.Tag.Msg == "" {
		t.Error("tag message should not be empty")
	}
	if !strings.Contains(tag.Tag.Msg, "release v2.0") {
		t.Errorf("tag message = %q; want to contain 'release v2.0'", tag.Tag.Msg)
	}
	if tag.Tag.Tagger == nil {
		t.Error("annotated tag should have a tagger")
	}
	if tag.Hash == tag.CommitHash {
		t.Error("annotated tag Hash (tag object) and CommitHash (commit) should differ")
	}

	// The commit the tag points to should be in the corpus.
	gc := r.corpus.gitCommit[tag.CommitHash]
	if gc == nil {
		t.Error("tagged commit not found in corpus")
	}
}

func TestGitSyncAnnotatedTagUpdate(t *testing.T) {
	r := newTestGitRepo(t)

	r.Git("commit", "--allow-empty", "-m", "first")
	r.Sync()

	// Create an annotated tag.
	r.Git("tag", "-a", "v3.0", "-m", "release v3.0")
	r.Sync()

	// Get the old tag hash.
	oldHash := r.corpus.GitRef("testgit", "refs/tags/v3.0")
	if oldHash == "" {
		t.Fatal("tag ref not found after first tag")
	}

	// Add another commit and force-move the tag.
	r.Git("commit", "--allow-empty", "-m", "second")
	r.Git("tag", "-f", "-a", "v3.0", "-m", "release v3.0 updated")
	muts := r.Sync()
	if len(muts) == 0 {
		t.Fatal("expected mutations after tag update")
	}

	// The ref should now point to a different tag object.
	newHash := r.corpus.GitRef("testgit", "refs/tags/v3.0")
	if newHash == "" {
		t.Fatal("tag ref not found after update")
	}
	if oldHash == newHash {
		t.Error("tag ref hash should have changed after force update")
	}

	// The new tag object should be in the corpus.
	gt := r.corpus.GitTagByHash(newHash)
	if gt == nil {
		t.Fatal("new tag object not found in corpus")
	}
	if !strings.Contains(gt.Msg, "updated") {
		t.Errorf("tag message = %q; want to contain 'updated'", gt.Msg)
	}
}

func TestGitSyncMixedTags(t *testing.T) {
	r := newTestGitRepo(t)

	r.Git("commit", "--allow-empty", "-m", "initial")
	r.Sync()

	// Create one of each.
	r.Git("tag", "light")
	r.Git("tag", "-a", "annotated", "-m", "annotated tag")
	r.Sync()

	var tags []GitTagInfo
	r.corpus.ForeachGitTag("testgit", func(ti GitTagInfo) error {
		tags = append(tags, ti)
		return nil
	})
	if len(tags) != 2 {
		t.Fatalf("ForeachGitTag returned %d tags; want 2", len(tags))
	}

	byRef := map[string]GitTagInfo{}
	for _, ti := range tags {
		byRef[ti.Ref] = ti
	}

	light, ok := byRef["refs/tags/light"]
	if !ok {
		t.Fatal("missing lightweight tag")
	}
	if light.Tag != nil {
		t.Error("lightweight tag should have nil Tag")
	}

	ann, ok := byRef["refs/tags/annotated"]
	if !ok {
		t.Fatal("missing annotated tag")
	}
	if ann.Tag == nil {
		t.Error("annotated tag should have non-nil Tag")
	}

	// Both should resolve to the same commit (the initial commit).
	if light.CommitHash != ann.CommitHash {
		t.Errorf("tags point to different commits: light=%s, annotated=%s",
			light.CommitHash, ann.CommitHash)
	}
}

func TestGitTagByHash(t *testing.T) {
	r := newTestGitRepo(t)

	r.Git("commit", "--allow-empty", "-m", "initial")
	r.Git("tag", "-a", "v1.0", "-m", "first release")
	r.Sync()

	refHash := r.corpus.GitRef("testgit", "refs/tags/v1.0")
	if refHash == "" {
		t.Fatal("tag ref not found")
	}

	gt := r.corpus.GitTagByHash(refHash)
	if gt == nil {
		t.Fatal("GitTagByHash returned nil for annotated tag")
	}
	if gt.Name != "v1.0" {
		t.Errorf("tag name = %q; want v1.0", gt.Name)
	}

	// Lookup with a random hash should return nil.
	var fakeHash GitHash = "01234567890123456789"
	if r.corpus.GitTagByHash(fakeHash) != nil {
		t.Error("GitTagByHash should return nil for unknown hash")
	}
}

func TestGitSyncNoOpProducesNoMutations(t *testing.T) {
	r := newTestGitRepo(t)

	r.Git("commit", "--allow-empty", "-m", "initial")
	r.Sync()

	// Sync again with no changes.
	muts := r.Sync()
	if len(muts) != 0 {
		for i, m := range muts {
			t.Logf("unexpected mutation %d: %v", i, m)
		}
		t.Errorf("expected 0 mutations on no-op sync; got %d", len(muts))
	}
}

func TestGitRefQuery(t *testing.T) {
	r := newTestGitRepo(t)

	r.Git("commit", "--allow-empty", "-m", "initial")
	r.Sync()

	// GitRef for a known ref should return non-empty.
	var branchRef string
	r.corpus.ForeachGitRepoRef("testgit", func(ri GitRefInfo) error {
		if strings.HasPrefix(ri.Ref, "refs/heads/") {
			branchRef = ri.Ref
		}
		return nil
	})
	if branchRef == "" {
		t.Fatal("no branch ref found")
	}
	h := r.corpus.GitRef("testgit", branchRef)
	if h == "" {
		t.Error("GitRef returned empty for known ref")
	}

	// Unknown repo or ref should return empty.
	if r.corpus.GitRef("nonexistent", branchRef) != "" {
		t.Error("GitRef should return empty for unknown repo")
	}
	if r.corpus.GitRef("testgit", "refs/heads/nonexistent") != "" {
		t.Error("GitRef should return empty for unknown ref")
	}
}

// TestRepoTodoIsolation regresses a bug where the pending-commit todo
// queue was a single corpus-wide map. With many watched repos, each
// sync goroutine drained the same map and tried to "git cat-file" the
// other repos' hashes in its own scratch dir, failing with exit 128.
func TestRepoTodoIsolation(t *testing.T) {
	var c Corpus

	c.mu.Lock()
	hA := c.gitHashFromHexStr("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	hB := c.gitHashFromHexStr("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	c.enqueueCommitLocked("repoA", hA)
	c.enqueueCommitLocked("repoB", hB)
	c.mu.Unlock()

	if got := c.gitCommitToIndex("repoA"); got != hA {
		t.Errorf("gitCommitToIndex(repoA) = %x; want %x", got, hA)
	}
	if got := c.gitCommitToIndex("repoB"); got != hB {
		t.Errorf("gitCommitToIndex(repoB) = %x; want %x", got, hB)
	}
	if got := c.gitCommitToIndex("repoC"); got != "" {
		t.Errorf("gitCommitToIndex(repoC) = %x; want empty (unknown repo)", got)
	}
}

// TestRepoTagIsolation verifies that tag storage is per-repo so that
// fork relationships (shared commits, distinct tag namespaces) don't
// conflate tags across repos.
func TestRepoTagIsolation(t *testing.T) {
	var c Corpus

	tag := &maintpb.GitTag{
		Hash:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Raw:        []byte("object bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\ntype commit\ntag v1.0\ntagger Tagger <t@example.com> 0 +0000\n\nmsg\n"),
	}

	c.mu.Lock()
	gtA := c.processGitTag("repoA", tag)
	gtB := c.processGitTag("repoB", tag)
	c.mu.Unlock()

	if gtA == gtB {
		t.Error("processGitTag returned the same *GitTag for two different repos; tags must be per-repo")
	}
	if got := len(c.gitRepoState["repoA"].tags); got != 1 {
		t.Errorf("repoA tags = %d; want 1", got)
	}
	if got := len(c.gitRepoState["repoB"].tags); got != 1 {
		t.Errorf("repoB tags = %d; want 1", got)
	}
}

// TestMultiRepoSyncDisjoint is an end-to-end regression for the same
// cross-pollution bug: two watched repos with disjoint histories share
// a Corpus and a Sync; neither's syncGitRepoOnce should error.
func TestMultiRepoSyncDisjoint(t *testing.T) {
	a := newTestGitRepo(t)
	b := addTestGitRepo(t, a.corpus, "testgitB")

	a.Git("commit", "--allow-empty", "-m", "a1")
	a.Git("commit", "--allow-empty", "-m", "a2")
	b.Git("commit", "--allow-empty", "-m", "b1")
	b.Git("commit", "--allow-empty", "-m", "b2")
	b.Git("commit", "--allow-empty", "-m", "b3")

	a.Sync()
	b.Sync()

	aHash := a.corpus.GitRef("testgit", "refs/heads/main")
	if aHash == "" {
		aHash = a.corpus.GitRef("testgit", "refs/heads/master")
	}
	bHash := a.corpus.GitRef("testgitB", "refs/heads/main")
	if bHash == "" {
		bHash = a.corpus.GitRef("testgitB", "refs/heads/master")
	}
	if aHash == "" || bHash == "" {
		t.Fatalf("missing refs: a=%x b=%x", aHash, bHash)
	}
	if aHash == bHash {
		t.Errorf("repos unexpectedly share a HEAD hash %x", aHash)
	}

	// Each repo's per-repo state should have drained cleanly.
	for _, name := range []string{"testgit", "testgitB"} {
		s := a.corpus.gitRepoState[name]
		if s == nil {
			t.Errorf("%s: no per-repo state", name)
			continue
		}
		for h := range s.todo {
			gc, ok := a.corpus.gitCommit[h]
			if !ok || gc.Committer == placeholderCommitter {
				t.Errorf("%s: pending todo %x after sync", name, h)
			}
		}
	}
}

// TestMultiRepoSyncConcurrent is the strongest regression for the
// cross-pollution bug: two goroutines drive syncGitRepoOnce on the same
// Corpus at the same time, racing to enqueue commits and process
// mutations. With a corpus-global todo queue, the goroutines stole each
// other's hashes and the syncs would fail with "git cat-file" exit 128.
func TestMultiRepoSyncConcurrent(t *testing.T) {
	a := newTestGitRepo(t)
	b := addTestGitRepo(t, a.corpus, "testgitB")

	const commitsPerRepo = 20
	for i := 0; i < commitsPerRepo; i++ {
		a.Git("commit", "--allow-empty", "-m", fmt.Sprintf("a%d", i))
		b.Git("commit", "--allow-empty", "-m", fmt.Sprintf("b%d", i))
	}

	// Prime each repo's scratch dir so the goroutines don't race on
	// "git init --bare".
	for _, r := range []*testGitRepo{a, b} {
		if err := os.MkdirAll(r.scratch, 0o755); err != nil {
			t.Fatal(err)
		}
		run(t, r.scratch, "git", "init", "--bare", r.scratch)
	}

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	for _, r := range []*testGitRepo{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- a.corpus.syncGitRepoOnce(context.Background(), r.ws, r.scratch)
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Errorf("concurrent syncGitRepoOnce: %v", err)
		}
	}

	// Both repos should be fully indexed: no pending hashes that aren't
	// already in gitCommit, and the commit count should be at least
	// commitsPerRepo for each (commits made above; initial branch commit
	// behavior varies by git version).
	for _, name := range []string{"testgit", "testgitB"} {
		if got := a.corpus.GitRepoCommitCount(name); got < commitsPerRepo {
			t.Errorf("%s: indexed %d commits; want >= %d", name, got, commitsPerRepo)
		}
	}
}

// TestMultiRepoForkOverlap covers the case the cross-repo isolation was
// strengthened for: a repo (b) is a fork of another tracked repo (a), so
// the two share part of their commit history but maintain independent
// tag namespaces (e.g. a "v1.0" annotated tag in each pointing at
// different commits).
//
// The shared commits should be deduplicated in the global gitCommit map
// (commit hashes are content-addressed; storing twice would waste
// memory), but the tags must NOT be conflated — each repo's
// ForeachGitTag/GitRef should see only its own v1.0.
func TestMultiRepoForkOverlap(t *testing.T) {
	a := newTestGitRepo(t)

	a.Git("commit", "--allow-empty", "-m", "shared 1")
	a.Git("commit", "--allow-empty", "-m", "shared 2")
	a.Git("commit", "--allow-empty", "-m", "shared 3")

	b := forkTestGitRepo(t, a, "testgitFork")

	b.Git("commit", "--allow-empty", "-m", "fork only 1")
	b.Git("commit", "--allow-empty", "-m", "fork only 2")

	// Same tag name in each repo, different content -> different tag
	// object hashes. Annotated so they're actual tag objects.
	a.Git("tag", "-a", "v1.0", "-m", "upstream v1.0")
	b.Git("tag", "-a", "v1.0", "-m", "fork v1.0")

	a.Sync()
	b.Sync()

	branchRef := defaultBranchRef(t, a.dir)
	aHead := a.corpus.GitRef("testgit", branchRef)
	bBranchRef := defaultBranchRef(t, b.dir)
	bHead := a.corpus.GitRef("testgitFork", bBranchRef)
	if aHead == "" || bHead == "" {
		t.Fatalf("missing HEAD refs: a=%x b=%x", aHead, bHead)
	}
	if aHead == bHead {
		t.Errorf("a and b HEADs should differ (b has fork-only commits): both = %x", aHead)
	}

	// b's HEAD chain must reach a's HEAD (shared history).
	bGc := a.corpus.gitCommit[bHead]
	if bGc == nil {
		t.Fatalf("b's HEAD commit %x not indexed", bHead)
	}
	if !reaches(bGc, aHead) {
		t.Errorf("b's HEAD chain does not include a's HEAD %x — shared history not deduplicated", aHead)
	}

	// v1.0 tag refs must point at different objects.
	aTagRef := a.corpus.GitRef("testgit", "refs/tags/v1.0")
	bTagRef := a.corpus.GitRef("testgitFork", "refs/tags/v1.0")
	if aTagRef == "" || bTagRef == "" {
		t.Fatalf("missing v1.0 refs: a=%x b=%x", aTagRef, bTagRef)
	}
	if aTagRef == bTagRef {
		t.Errorf("a's and b's v1.0 tags collapsed to one object %x; per-repo tag namespaces were not preserved", aTagRef)
	}

	// Each repo's tag map must contain its own tag and not the other's.
	aTags := a.corpus.gitRepoState["testgit"].tags
	bTags := a.corpus.gitRepoState["testgitFork"].tags
	if _, ok := aTags[aTagRef]; !ok {
		t.Errorf("testgit tag map missing its own v1.0 (%x)", aTagRef)
	}
	if _, ok := aTags[bTagRef]; ok {
		t.Errorf("testgit tag map leaked fork's v1.0 (%x)", bTagRef)
	}
	if _, ok := bTags[bTagRef]; !ok {
		t.Errorf("testgitFork tag map missing its own v1.0 (%x)", bTagRef)
	}
	if _, ok := bTags[aTagRef]; ok {
		t.Errorf("testgitFork tag map leaked upstream's v1.0 (%x)", aTagRef)
	}

	// Sanity: ForeachGitTag for each repo returns its own v1.0 and not the other's.
	for _, name := range []string{"testgit", "testgitFork"} {
		var seen []GitHash
		a.corpus.ForeachGitTag(name, func(ti GitTagInfo) error {
			if ti.Ref == "refs/tags/v1.0" {
				seen = append(seen, ti.Hash)
			}
			return nil
		})
		if len(seen) != 1 {
			t.Errorf("%s: ForeachGitTag returned %d v1.0 refs; want 1", name, len(seen))
		}
	}
}

// reaches reports whether starting from gc and walking parents reaches
// the commit with hash target.
func reaches(gc *GitCommit, target GitHash) bool {
	seen := map[GitHash]bool{}
	var stack []*GitCommit
	stack = append(stack, gc)
	for len(stack) > 0 {
		c := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if c.Hash == target {
			return true
		}
		if seen[c.Hash] {
			continue
		}
		seen[c.Hash] = true
		stack = append(stack, c.Parents...)
	}
	return false
}

// defaultBranchRef returns the ref name (e.g. "refs/heads/main" or
// "refs/heads/master") that the given repo's HEAD points at. Different
// git versions default to different branch names.
func defaultBranchRef(t *testing.T, repoDir string) string {
	t.Helper()
	out := run(t, repoDir, "git", "symbolic-ref", "HEAD")
	return strings.TrimSpace(out)
}

// forkTestGitRepo clones the underlying git repo of base into a new
// tracked repo named name under the same Corpus, so the two repos share
// their history up to the fork point.
func forkTestGitRepo(t *testing.T, base *testGitRepo, name string) *testGitRepo {
	t.Helper()
	dir := t.TempDir()
	repoDir := filepath.Join(dir, "repo")
	scratchDir := filepath.Join(dir, "scratch")
	run(t, dir, "git", "clone", "--quiet", base.dir, repoDir)
	run(t, repoDir, "git", "config", "user.email", "test@example.com")
	run(t, repoDir, "git", "config", "user.name", "Test User")

	conf := WatchedGitRepo{
		Name:         name,
		Remote:       repoDir,
		RefRegexps:   []string{`refs/heads/.*`, `refs/tags/.*`},
		PollInterval: time.Hour,
	}
	ws := watchedGitRepoState{conf: conf}
	for _, pat := range conf.RefRegexps {
		re, err := regexp.Compile(pat)
		if err != nil {
			t.Fatal(err)
		}
		ws.refRegexps = append(ws.refRegexps, re)
	}
	base.corpus.watchedGitRepoConfigs = append(base.corpus.watchedGitRepoConfigs, ws)

	return &testGitRepo{
		t:       t,
		dir:     repoDir,
		scratch: scratchDir,
		corpus:  base.corpus,
		ws:      &base.corpus.watchedGitRepoConfigs[len(base.corpus.watchedGitRepoConfigs)-1],
	}
}

// addTestGitRepo creates a second test repo backed by the same Corpus as
// an existing testGitRepo, so that multi-repo behavior can be exercised.
func addTestGitRepo(t *testing.T, c *Corpus, name string) *testGitRepo {
	t.Helper()
	dir := t.TempDir()
	repoDir := filepath.Join(dir, "repo")
	scratchDir := filepath.Join(dir, "scratch")
	run(t, dir, "git", "init", repoDir)
	run(t, repoDir, "git", "config", "user.email", "test@example.com")
	run(t, repoDir, "git", "config", "user.name", "Test User")

	conf := WatchedGitRepo{
		Name:         name,
		Remote:       repoDir,
		RefRegexps:   []string{`refs/heads/.*`, `refs/tags/.*`},
		PollInterval: time.Hour,
	}
	ws := watchedGitRepoState{conf: conf}
	for _, pat := range conf.RefRegexps {
		re, err := regexp.Compile(pat)
		if err != nil {
			t.Fatal(err)
		}
		ws.refRegexps = append(ws.refRegexps, re)
	}
	c.watchedGitRepoConfigs = append(c.watchedGitRepoConfigs, ws)

	return &testGitRepo{
		t:       t,
		dir:     repoDir,
		scratch: scratchDir,
		corpus:  c,
		ws:      &c.watchedGitRepoConfigs[len(c.watchedGitRepoConfigs)-1],
	}
}
