// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package maintner

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bradfitz/gxbm/internal/envutil"
	"github.com/bradfitz/gxbm/internal/foreach"
	"github.com/bradfitz/gxbm/maintner/maintpb"
	"google.golang.org/protobuf/proto"
)

// GitHash is a git commit in binary form (NOT hex form).
// They are currently always 20 bytes long. (for SHA-1 refs)
// That may change in the future.
type GitHash string

func (h GitHash) String() string { return fmt.Sprintf("%x", string(h)) }

const (
	maxHexHashLen = 64 // SHA-256; SHA-1 is 40
	maxBinHashLen = 32 // SHA-256; SHA-1 is 20
)

// ValidHexHashLen reports whether s has a plausible length for a
// hex-encoded git object hash: 40 hex bytes (SHA-1) or 64 hex bytes (SHA-256).
func ValidHexHashLen(s string) bool {
	return len(s) == 40 || len(s) == 64
}

// decodeHexHash decodes a hex-encoded git hash into buf (which must be
// at least maxBinHashLen bytes) and returns the number of bytes written.
// This avoids allocating by using a caller-provided stack buffer.
func decodeHexHash(buf []byte, s string) (int, error) {
	n := hex.DecodedLen(len(s))
	if n > len(buf) {
		return 0, fmt.Errorf("git hash %q too long", s)
	}
	return hex.Decode(buf[:n], []byte(s))
}

// requires c.mu be held for writing.
func (c *Corpus) gitHashFromHexStr(s string) GitHash {
	if !ValidHexHashLen(s) {
		panic(fmt.Sprintf("bogus git hash %q", s))
	}
	var buf [maxBinHashLen]byte
	n, err := decodeHexHash(buf[:], s)
	if err != nil {
		panic(fmt.Sprintf("bogus git hash %q: %v", s, err))
	}
	return GitHash(c.strb(buf[:n]))
}

// requires c.mu be held for writing.
func (c *Corpus) gitHashFromHex(s []byte) GitHash {
	if !ValidHexHashLen(string(s)) {
		panic(fmt.Sprintf("bogus git hash %q", s))
	}
	var buf [maxBinHashLen]byte
	n, err := hex.Decode(buf[:], s)
	if err != nil {
		panic(fmt.Sprintf("bogus git hash %q: %v", s, err))
	}
	return GitHash(c.strb(buf[:n]))
}

// placeholderCommitter is a sentinel value for GitCommit.Committer to
// mean that the GitCommit is a placeholder. It's used for commits we
// know should exist (because they're referenced as parents) but we
// haven't yet seen in the log.
var placeholderCommitter = new(GitPerson)

// GitCommit represents a single commit in a git repository.
type GitCommit struct {
	Hash       GitHash
	Tree       GitHash
	Parents    []*GitCommit
	Author     *GitPerson
	AuthorTime time.Time
	Committer  *GitPerson
	Reviewer   *GitPerson
	CommitTime time.Time
	Msg        string // Commit message subject and body
	Files      []*maintpb.GitDiffTreeFile
	GerritMeta *GerritMeta // non-nil if it's a Gerrit NoteDB meta commit
	Repo       string      // repo name from GitRepo.Name or GitRepo.GoRepo
}

// GitTag represents an annotated tag object in a git repository.
type GitTag struct {
	Hash       GitHash    // the tag object's own hash
	Target     GitHash    // the object (usually commit) this tag points at
	Tagger     *GitPerson // may be nil for unusual tags
	TaggerTime time.Time
	Name       string // tag name from the "tag" header line
	Msg        string // tag message
	Repo       string // repo name
}

func (gc *GitCommit) String() string {
	if gc == nil {
		return "<nil *GitCommit>"
	}
	return fmt.Sprintf("{GitCommit %s}", gc.Hash)
}

// HasAncestor reports whether gc contains the provided ancestor
// commit in gc's history.
func (gc *GitCommit) HasAncestor(ancestor *GitCommit) bool {
	return gc.hasAncestor(ancestor, make(map[*GitCommit]bool))
}

func (gc *GitCommit) hasAncestor(ancestor *GitCommit, checked map[*GitCommit]bool) bool {
	if v, ok := checked[gc]; ok {
		return v
	}
	checked[gc] = false
	for _, pc := range gc.Parents {
		if pc == nil {
			panic("nil parent")
		}
		if pc.Committer == placeholderCommitter {
			log.Printf("WARNING: hasAncestor(%q, %q) found parent %q with placeholder parent", gc.Hash, ancestor.Hash, pc.Hash)
		}
		if pc.Hash == ancestor.Hash || pc.hasAncestor(ancestor, checked) {
			checked[gc] = true
			return true
		}
	}
	return false
}

// Summary returns the first line of the commit message.
func (gc *GitCommit) Summary() string {
	s := gc.Msg
	if i := strings.IndexByte(s, '\n'); i != -1 {
		s = s[:i]
	}
	s = strings.TrimSpace(s)
	return s
}

// SameDiffStat reports whether gc has the same diff stat numbers as b.
// If either is unknown, false is returned.
func (gc *GitCommit) SameDiffStat(b *GitCommit) bool {
	if len(gc.Files) != len(b.Files) {
		return false
	}
	for i, af := range gc.Files {
		bf := b.Files[i]
		if af == nil || bf == nil {
			return false
		}
		if !proto.Equal(af, bf) {
			return false
		}
	}
	return true
}

// GitPerson is a person in a git commit.
type GitPerson struct {
	Str string // "Foo Bar <foo@bar.com>"
}

// Email returns the GitPerson's email address only, without the name
// or angle brackets.
func (p *GitPerson) Email() string {
	lt := strings.IndexByte(p.Str, '<')
	gt := strings.IndexByte(p.Str, '>')
	if lt < 0 || gt < lt {
		return ""
	}
	return p.Str[lt+1 : gt]
}

func (p *GitPerson) Name() string {
	i := strings.IndexByte(p.Str, '<')
	if i < 0 {
		return p.Str
	}
	return strings.TrimSpace(p.Str[:i])
}

// String implements fmt.Stringer.
func (p *GitPerson) String() string { return p.Str }

// enqueueCommitLocked records that h needs to be indexed by the goroutine
// syncing repo (the watched repo name, e.g. "tailscale/tailscale", or a
// Gerrit "server/project"). The per-repo todo queue prevents goroutines
// from stealing each other's hashes and failing with "git cat-file"
// errors because the object isn't in their scratch dir.
//
// Requires c.mu be held for writing.
func (c *Corpus) enqueueCommitLocked(repo string, h GitHash) {
	if _, ok := c.gitCommit[h]; ok {
		return
	}
	c.repoStateLocked(repo).todo[h] = true
}

// syncGitCommits polls for git commits in a directory.
func (c *Corpus) syncGitCommits(ctx context.Context, conf polledGitCommits, loop bool) error {
	cmd := exec.CommandContext(ctx, "git", "show-ref", "refs/remotes/origin/master")
	envutil.SetDir(cmd, conf.dir)
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	outs := strings.TrimSpace(string(out))
	if outs == "" {
		return fmt.Errorf("no remote found for refs/remotes/origin/master")
	}
	ref := strings.Fields(outs)[0]
	repoName := gitRepoNameFromPB(conf.repo)
	c.mu.Lock()
	refHash := c.gitHashFromHexStr(ref)
	c.enqueueCommitLocked(repoName, refHash)
	c.mu.Unlock()

	idle := false
	for {
		hash := c.gitCommitToIndex(repoName)
		if hash == "" {
			if !loop {
				return nil
			}
			if !idle {
				c.logf("All git commits index for %v; idle.", conf.repo)
				idle = true
			}
			time.Sleep(5 * time.Second)
			continue
		}
		if err := c.indexCommit(conf, hash); err != nil {
			c.logf("Error indexing %v: %v", hash, err)
			select {
			case <-ctx.Done():
				return ctx.Err()
				// TODO: temporary vs permanent failure? reschedule? fail hard?
				// For now just loop with a sleep.
			case <-time.After(5 * time.Second):
			}
		}
	}
}

// gitCommitToIndex returns a hash queued for indexing in repo, or "" if
// none. Per-repo todo queues prevent goroutines from stealing each
// other's pending hashes; see enqueueCommitLocked.
func (c *Corpus) gitCommitToIndex(repo string) GitHash {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.gitRepoState[repo]
	if !ok {
		return ""
	}
	for hash := range s.todo {
		gc, ok := c.gitCommit[hash]
		if !ok || gc.Committer == placeholderCommitter {
			return hash
		}
	}
	return ""
}

var (
	nlnl           = []byte("\n\n")
	parentSpace    = []byte("parent ")
	authorSpace    = []byte("author ")
	committerSpace = []byte("committer ")
	treeSpace      = []byte("tree ")
	golangHgSpace  = []byte("golang-hg ")
	gpgSigSpace    = []byte("gpgsig ")
	encodingSpace  = []byte("encoding ")
	objectSpace    = []byte("object ")
	typeSpace      = []byte("type ")
	tagSpace       = []byte("tag ")
	taggerSpace    = []byte("tagger ")
	space          = []byte(" ")
)

func parseCommitFromGit(dir string, hash GitHash) (*maintpb.GitCommit, error) {
	cmd := exec.Command("git", "cat-file", "commit", hash.String())
	envutil.SetDir(cmd, dir)
	catFile, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git cat-file -p %v: %v", hash, err)
	}
	cmd = exec.Command("git", "diff-tree", "--numstat", hash.String())
	envutil.SetDir(cmd, dir)
	diffTreeOut, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git diff-tree --numstat %v: %v", hash, err)
	}

	diffTree := &maintpb.GitDiffTree{}
	bs := bufio.NewScanner(bytes.NewReader(diffTreeOut))
	lineNum := 0
	for bs.Scan() {
		line := strings.TrimSpace(bs.Text())
		lineNum++
		if lineNum == 1 && line == hash.String() {
			continue
		}
		f := strings.Fields(line)
		// A line is like: <added> WS+ <deleted> WS+ <filename>
		// Where <added> or <deleted> can be '-' to mean binary.
		// The filename could contain spaces.
		// 49      8       maintner/maintner.go
		// Or:
		// 49      8       some/name with spaces.txt
		if len(f) < 3 {
			continue
		}
		binary := f[0] == "-" || f[1] == "-"
		added, _ := strconv.ParseInt(f[0], 10, 64)
		deleted, _ := strconv.ParseInt(f[1], 10, 64)
		file := strings.TrimPrefix(line, f[0])
		file = strings.TrimSpace(file)
		file = strings.TrimPrefix(file, f[1])
		file = strings.TrimSpace(file)

		diffTree.File = append(diffTree.File, &maintpb.GitDiffTreeFile{
			File:    file,
			Added:   added,
			Deleted: deleted,
			Binary:  binary,
		})
	}
	if err := bs.Err(); err != nil {
		return nil, err
	}
	commit := &maintpb.GitCommit{
		Raw:      catFile,
		DiffTree: diffTree,
	}
	hexHash := hash.String()
	if !ValidHexHashLen(hexHash) {
		return nil, fmt.Errorf("unsupported git hash length %d for %q", len(hexHash), hexHash)
	}
	commit.Hash = hexHash
	return commit, nil
}

func parseTagFromGit(dir string, hash GitHash) (*maintpb.GitTag, error) {
	cmd := exec.Command("git", "cat-file", "tag", hash.String())
	envutil.SetDir(cmd, dir)
	catFile, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git cat-file tag %v: %v", hash, err)
	}

	i := bytes.Index(catFile, nlnl)
	if i < 0 {
		return nil, fmt.Errorf("tag %v lacks double newline", hash)
	}
	hdr := catFile[:i]

	var targetHash string
	for _, ln := range bytes.Split(hdr, []byte("\n")) {
		if bytes.HasPrefix(ln, objectSpace) {
			targetHash = string(ln[len(objectSpace):])
		}
	}
	if targetHash == "" {
		return nil, fmt.Errorf("tag %v missing object header", hash)
	}

	tag := &maintpb.GitTag{
		Hash:       hash.String(),
		Raw:        catFile,
		TargetHash: targetHash,
	}
	return tag, nil
}

func (c *Corpus) indexCommit(conf polledGitCommits, hash GitHash) error {
	if conf.repo == nil {
		panic("bogus config; nil repo")
	}
	commit, err := parseCommitFromGit(conf.dir, hash)
	if err != nil {
		return err
	}
	m := &maintpb.Mutation{
		Git: &maintpb.GitMutation{
			Repo:   conf.repo,
			Commit: commit,
		},
	}
	c.addMutation(m)
	return nil
}

// c.mu is held for writing.
func (c *Corpus) processGitMutation(m *maintpb.GitMutation) {
	var repoName string
	if m.Repo != nil {
		if m.Repo.Name != "" {
			repoName = m.Repo.Name
		} else if m.Repo.GoRepo != "" {
			repoName = m.Repo.GoRepo
		}
	}
	if repoName != "" {
		if c.gitRepos == nil {
			c.gitRepos = make(map[string]bool)
		}
		c.gitRepos[repoName] = true
	}

	if commit := m.Commit; commit != nil {
		gc, err := c.processGitCommit(repoName, commit)
		if err != nil {
			return
		}
		if gc.Repo == "" && repoName != "" {
			gc.Repo = repoName
			// Count newly-assigned commits. Placeholders have no Repo,
			// so this only fires once per real commit.
			if c.gitCommitCount == nil {
				c.gitCommitCount = make(map[string]int)
			}
			c.gitCommitCount[repoName]++
		}
	}

	if ru := m.RefUpdate; ru != nil && repoName != "" {
		c.repoStateLocked(repoName).refs[ru.Ref] = c.gitHashFromHexStr(ru.Hash)
	}

	if tag := m.Tag; tag != nil {
		gt := c.processGitTag(repoName, tag)
		if gt.Repo == "" && repoName != "" {
			gt.Repo = repoName
		}
	}
}

// processGitTag stores the tag under repo's per-repo tag namespace. Two
// repos may legitimately have tags with the same content (identical hash)
// or the same tag name pointing at different content; per-repo storage
// keeps them disjoint.
//
// c.mu is held for writing.
func (c *Corpus) processGitTag(repo string, tag *maintpb.GitTag) *GitTag {
	if !ValidHexHashLen(tag.Hash) || !ValidHexHashLen(tag.TargetHash) {
		c.logf("bogus git tag hash %q / target %q", tag.Hash, tag.TargetHash)
		return &GitTag{}
	}
	hash := c.gitHashFromHexStr(tag.Hash)
	s := c.repoStateLocked(repo)
	if gt, ok := s.tags[hash]; ok {
		return gt
	}

	catFile := tag.Raw
	hdr, msg, _ := bytes.Cut(catFile, nlnl)

	gt := &GitTag{
		Hash:   hash,
		Target: c.gitHashFromHexStr(tag.TargetHash),
		Msg:    c.strb(msg),
	}

	for ln := range bytes.SplitSeq(hdr, []byte("\n")) {
		if bytes.HasPrefix(ln, taggerSpace) {
			p, t, err := c.parsePerson(ln[len(taggerSpace):])
			if err == nil {
				gt.Tagger = p
				gt.TaggerTime = t
			}
		} else if bytes.HasPrefix(ln, tagSpace) {
			gt.Name = c.strb(ln[len(tagSpace):])
		}
	}

	s.tags[hash] = gt
	return gt
}

// c.mu is held for writing.
// processGitCommit processes a GitCommit mutation. repo is the name of
// the repository being indexed (e.g. "tailscale/tailscale" or a Gerrit
// "server/project"); it is used to tag any newly-enqueued parent commits
// so the right goroutine picks them up.
func (c *Corpus) processGitCommit(repo string, commit *maintpb.GitCommit) (*GitCommit, error) {
	if c.gitCommit == nil {
		c.gitCommit = map[GitHash]*GitCommit{}
	}
	if !ValidHexHashLen(commit.Hash) {
		return nil, fmt.Errorf("bogus git hash %q", commit.Hash)
	}
	hash := c.gitHashFromHexStr(commit.Hash)

	catFile := commit.Raw
	i := bytes.Index(catFile, nlnl)
	if i == 0 {
		return nil, fmt.Errorf("commit %v lacks double newline", hash)
	}
	hdr, msg := catFile[:i], catFile[i+2:]
	gc := &GitCommit{
		Hash:    hash,
		Parents: make([]*GitCommit, 0, bytes.Count(hdr, parentSpace)),
		Msg:     c.strb(msg),
	}

	// The commit message contains the reviewer email address. Sample commit message:
	// Update patch set 1
	//
	// Patch Set 1: Code-Review+2
	//
	// Patch-set: 1
	// Reviewer: Ian Lance Taylor <5206@62eb7196-b449-3ce5-99f1-c037f21e1705>
	// Label: Code-Review=+2
	if reviewer := lineValue(c.strb(msg), "Reviewer: "); reviewer != "" {
		gc.Reviewer = &GitPerson{Str: reviewer}
	}

	if commit.DiffTree != nil {
		gc.Files = commit.DiffTree.File
	}
	for _, f := range gc.Files {
		f.File = c.str(f.File) // intern the string
	}
	sort.Slice(gc.Files, func(i, j int) bool { return gc.Files[i].File < gc.Files[j].File })
	parents := 0
	err := foreach.Line(hdr, func(ln []byte) error {
		if bytes.HasPrefix(ln, parentSpace) {
			parents++
			parentHash := c.gitHashFromHex(ln[len(parentSpace):])
			parent := c.gitCommit[parentHash]
			if parent == nil {
				// Enqueue for indexing before installing placeholder,
				// since enqueueCommitLocked skips hashes already in gitCommit.
				c.enqueueCommitLocked(repo, parentHash)
				// Install a placeholder to be filled in later.
				parent = &GitCommit{
					Hash:      parentHash,
					Committer: placeholderCommitter,
				}
				c.gitCommit[parentHash] = parent
			}
			gc.Parents = append(gc.Parents, parent)
			return nil
		}
		if bytes.HasPrefix(ln, authorSpace) {
			p, t, err := c.parsePerson(ln[len(authorSpace):])
			if err != nil {
				return fmt.Errorf("unrecognized author line %q: %v", ln, err)
			}
			gc.Author = p
			gc.AuthorTime = t
			return nil
		}
		if bytes.HasPrefix(ln, committerSpace) {
			p, t, err := c.parsePerson(ln[len(committerSpace):])
			if err != nil {
				return fmt.Errorf("unrecognized committer line %q: %v", ln, err)
			}
			gc.Committer = p
			gc.CommitTime = t
			return nil
		}
		if bytes.HasPrefix(ln, treeSpace) {
			gc.Tree = c.gitHashFromHex(ln[len(treeSpace):])
			return nil
		}
		if bytes.HasPrefix(ln, golangHgSpace) {
			if c.gitOfHg == nil {
				c.gitOfHg = map[string]GitHash{}
			}
			c.gitOfHg[string(ln[len(golangHgSpace):])] = hash
			return nil
		}
		if bytes.HasPrefix(ln, gpgSigSpace) || bytes.HasPrefix(ln, space) {
			// Jessie Frazelle is a unique butterfly.
			return nil
		}
		if bytes.HasPrefix(ln, encodingSpace) {
			// Also ignore this. In practice this has only
			// been seen to declare that a commit's
			// metadata is utf-8 when the author name has
			// non-ASCII.
			return nil
		}
		// Ignore unrecognized header lines (e.g. change-id trailers).
		return nil
	})
	if err != nil {
		c.logf("Unparseable commit %q: %v", hash, err)
		return nil, fmt.Errorf("Unparseable commit %q: %v", hash, err)
	}
	if ph, ok := c.gitCommit[hash]; ok {
		// Update placeholder.
		*ph = *gc
	} else {
		c.gitCommit[hash] = gc
	}
	// Drop this hash from every repo's todo. Usually only repo's todo had
	// it, but if other repos enqueued it concurrently (fork scenario, or
	// shared parent chains) clean up the stale entries so gitCommitToIndex
	// doesn't iterate them forever.
	for _, s := range c.gitRepoState {
		delete(s.todo, hash)
	}
	if c.verbose {
		now := time.Now()
		if now.After(c.lastGitCount.Add(time.Second)) {
			c.lastGitCount = now
			c.logf("Num git commits = %v", len(c.gitCommit))
		}
	}
	return gc, nil
}

// parsePerson parses an "author" or "committer" value from "git cat-file -p COMMIT"
// The values are like:
//
//	Foo Bar <foobar@gmail.com> 1488624439 +0900
//
// c.mu must be held for writing.
func (c *Corpus) parsePerson(v []byte) (*GitPerson, time.Time, error) {
	v = bytes.TrimSpace(v)

	lastSpace := bytes.LastIndexByte(v, ' ')
	if lastSpace < 0 {
		return nil, time.Time{}, errors.New("failed to match person")
	}
	tz := v[lastSpace+1:] // "+0800"
	v = v[:lastSpace]     // now v is "Foo Bar <foobar@gmail.com> 1488624439"

	lastSpace = bytes.LastIndexByte(v, ' ')
	if lastSpace < 0 {
		return nil, time.Time{}, errors.New("failed to match person")
	}
	unixTime := v[lastSpace+1:]
	nameEmail := v[:lastSpace] // now v is "Foo Bar <foobar@gmail.com>"

	ut, err := strconv.ParseInt(string(unixTime), 10, 64)
	if err != nil {
		return nil, time.Time{}, err
	}
	t := time.Unix(ut, 0).In(c.gitLocation(tz))

	p, ok := c.gitPeople[string(nameEmail)]
	if !ok {
		p = &GitPerson{Str: string(nameEmail)}
		if c.gitPeople == nil {
			c.gitPeople = map[string]*GitPerson{}
		}
		c.gitPeople[p.Str] = p
	}
	return p, t, nil

}

// GitCommit returns the provided git commit, or nil if it's unknown.
func (c *Corpus) GitCommit(hash string) *GitCommit {
	if !ValidHexHashLen(hash) {
		// TODO: support prefix lookups. build a trie. But
		// for now just avoid panicking in gitHashFromHexStr.
		return nil
	}
	var buf [maxBinHashLen]byte
	n, err := decodeHexHash(buf[:], hash)
	if err != nil {
		return nil
	}
	return c.gitCommit[GitHash(buf[:n])]
}

// v is like '[+-]hhmm'
// c.mu must be held for writing.
func (c *Corpus) gitLocation(v []byte) *time.Location {
	if loc, ok := c.zoneCache[string(v)]; ok {
		return loc
	}
	s := string(v)
	h, _ := strconv.Atoi(s[1:3])
	m, _ := strconv.Atoi(s[3:5])
	east := 1
	if v[0] == '-' {
		east = -1
	}
	loc := time.FixedZone(s, east*(h*3600+m*60))
	if c.zoneCache == nil {
		c.zoneCache = map[string]*time.Location{}
	}
	c.zoneCache[s] = loc
	return loc
}

// gitScratchDir returns the path for a bare git scratch repo for the given name.
func gitScratchDir(name string) string {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		cacheDir = filepath.Join(os.Getenv("HOME"), ".cache")
	}
	return filepath.Join(cacheDir, "maintner-git-scratch", url.PathEscape(name))
}

// syncGitRepo is the outer loop for syncing a watched git repo.
func (c *Corpus) syncGitRepo(ctx context.Context, ws *watchedGitRepoState, loop bool) error {
	dir := gitScratchDir(ws.conf.Name)

	// Init bare repo if needed.
	if _, err := os.Stat(filepath.Join(dir, "HEAD")); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("creating scratch dir: %w", err)
		}
		cmd := exec.CommandContext(ctx, "git", "init", "--bare", dir)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("git init --bare %s: %v\n%s", dir, err, out)
		}
		c.logf("Created bare scratch repo at %s", dir)
	}

	for {
		if err := c.syncGitRepoOnce(ctx, ws, dir); err != nil {
			return err
		}
		if !loop {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(ws.conf.PollInterval):
		}
	}
}

// syncGitRepoOnce performs a single poll cycle for a watched git repo.
func (c *Corpus) syncGitRepoOnce(ctx context.Context, ws *watchedGitRepoState, dir string) error {
	// 1. ls-remote to discover refs
	cmd := exec.CommandContext(ctx, "git", "ls-remote", ws.conf.Remote)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("git ls-remote %s: %w", ws.conf.Remote, err)
	}

	// 2. Parse and filter refs
	type refUpdate struct {
		name       string
		hash       string // 40-char hex, what the ref points to (tag object or commit)
		commitHash string // 40-char hex, the commit to index (same as hash for non-tags)
	}
	var updates []refUpdate

	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		hash, refName := fields[0], fields[1]
		if !ValidHexHashLen(hash) {
			continue
		}
		// Skip peeled tag refs (e.g. "refs/tags/v1.0^{}") from ls-remote;
		// we dereference tag objects ourselves.
		if strings.HasSuffix(refName, "^{}") {
			continue
		}
		if !ws.matchesRef(refName) {
			continue
		}

		// 3. Compare against known refs from mutation log
		c.mu.RLock()
		var knownHash GitHash
		if s := c.gitRepoState[ws.conf.Name]; s != nil {
			knownHash = s.refs[refName]
		}
		c.mu.RUnlock()

		newHash := GitHash(mustDecodeHex(hash))
		if knownHash == newHash {
			continue
		}
		updates = append(updates, refUpdate{name: refName, hash: hash})
	}

	if len(updates) == 0 {
		return nil
	}

	// 4. Fetch changed refs
	fetchArgs := []string{"fetch", "--no-tags", ws.conf.Remote}
	for _, u := range updates {
		fetchArgs = append(fetchArgs, "+"+u.name+":"+u.name)
	}
	cmd = exec.CommandContext(ctx, "git", fetchArgs...)
	envutil.SetDir(cmd, dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git fetch: %v\n%s", err, out)
	}

	// 5. Classify objects, index tag objects, determine commit hashes
	repo := &maintpb.GitRepo{Name: ws.conf.Name}
	for i := range updates {
		u := &updates[i]
		objType, err := gitObjectType(ctx, dir, u.hash)
		if err != nil {
			return fmt.Errorf("git cat-file -t %s: %w", u.hash, err)
		}
		if objType == "tag" {
			tag, err := parseTagFromGit(dir, GitHash(mustDecodeHex(u.hash)))
			if err != nil {
				return fmt.Errorf("parsing tag %s: %w", u.hash, err)
			}
			c.addMutation(&maintpb.Mutation{
				Git: &maintpb.GitMutation{
					Repo: repo,
					Tag:  tag,
				},
			})
			u.commitHash = tag.TargetHash
		} else {
			u.commitHash = u.hash
		}
	}

	// 6. Enqueue commit tips
	c.mu.Lock()
	for _, u := range updates {
		h := c.gitHashFromHexStr(u.commitHash)
		c.enqueueCommitLocked(ws.conf.Name, h)
	}
	c.mu.Unlock()

	// 7. Walk commit graph: index all queued commits
	conf := polledGitCommits{repo: repo, dir: dir}
	indexed := 0
	for {
		hash := c.gitCommitToIndex(ws.conf.Name)
		if hash == "" {
			break
		}
		if err := c.indexCommit(conf, hash); err != nil {
			return fmt.Errorf("indexing commit %v: %w", hash, err)
		}
		indexed++
		if indexed%1000 == 0 {
			c.logf("Indexed %d git commits so far for %s ...", indexed, ws.conf.Name)
		}
	}
	if indexed > 0 {
		c.logf("Indexed %d new git commits for %s", indexed, ws.conf.Name)
	}

	// 8. Emit ref update mutations (after commits are indexed).
	// u.hash is the original hash from ls-remote (tag object for annotated tags).
	for _, u := range updates {
		c.addMutation(&maintpb.Mutation{
			Git: &maintpb.GitMutation{
				Repo: repo,
				RefUpdate: &maintpb.GitRef{
					Ref:  u.name,
					Hash: u.hash,
				},
			},
		})
	}
	return nil
}

// gitObjectType returns the git object type ("commit", "tag", "tree", "blob")
// for the given hex hash.
func gitObjectType(ctx context.Context, dir, hash string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "cat-file", "-t", hash)
	envutil.SetDir(cmd, dir)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func mustDecodeHex(s string) string {
	var buf [maxBinHashLen]byte
	n, err := decodeHexHash(buf[:], s)
	if err != nil {
		panic(fmt.Sprintf("bad hex %q: %v", s, err))
	}
	return string(buf[:n])
}
