// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package maintner

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gxbm/maintner/maintpb"
	"github.com/bradfitz/gxbm/maintner/reclog"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// A MutationLogger logs mutations.
type MutationLogger interface {
	Log(*maintpb.Mutation) error
}

// DiskMutationLogger logs mutations to disk.
type DiskMutationLogger struct {
	directory string

	mu   sync.Mutex
	done bool // true after first GetMutations
}

// NewDiskMutationLogger creates a new DiskMutationLogger, which will create
// mutations in the given directory.
func NewDiskMutationLogger(directory string) *DiskMutationLogger {
	if directory == "" {
		panic("empty directory")
	}
	return &DiskMutationLogger{directory: directory}
}

// filename returns the filename to write to. The oldest filename must come
// first in lexical order.
func (d *DiskMutationLogger) filename() string {
	now := time.Now().UTC()
	return filepath.Join(d.directory, fmt.Sprintf("maintner-%s.mutlog", now.Format("2006-01-02")))
}

// Log will write m to disk. If a mutation file does not exist for the current
// day, it will be created.
func (d *DiskMutationLogger) Log(m *maintpb.Mutation) error {
	data, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return reclog.AppendRecordToFile(d.filename(), data)
}

func (d *DiskMutationLogger) ForeachFile(fn func(fullPath string, fi os.FileInfo) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.directory == "" {
		panic("empty directory")
	}
	// Walk guarantees that files are walked in lexical order, which we depend on.
	return filepath.Walk(d.directory, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() && path != filepath.Clean(d.directory) {
			return filepath.SkipDir
		}
		if !strings.HasPrefix(fi.Name(), "maintner-") {
			return nil
		}
		if !strings.HasSuffix(fi.Name(), ".mutlog") {
			return nil
		}
		return fn(path, fi)
	})
}

func (d *DiskMutationLogger) GetMutations(ctx context.Context) <-chan MutationStreamEvent {
	d.mu.Lock()
	wasDone := d.done
	d.done = true
	d.mu.Unlock()

	if wasDone {
		// TODO: support subsequent Update? for now we only
		// support the initial loading.  The network mutation
		// source is the new implementation with Update
		// support.
		return nil
	}

	ch := make(chan MutationStreamEvent, 50) // buffered: overlap gunzip/unmarshal with loading

	go func() {
		err := d.ForeachFile(func(fullPath string, fi os.FileInfo) error {
			return reclog.ForeachFileRecord(fullPath, func(off int64, hdr, rec []byte) error {
				m := new(maintpb.Mutation)
				if err := proto.Unmarshal(rec, m); err != nil {
					return err
				}
				select {
				case ch <- MutationStreamEvent{Mutation: m}:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		})
		final := MutationStreamEvent{Err: err}
		if err == nil {
			final.End = true
		}
		select {
		case ch <- final:
		case <-ctx.Done():
		}
	}()
	return ch
}

// DevLogger wraps an existing MutationLogger and additionally writes
// each mutation to a separate dev log file in human-readable prototext
// format. The dev log file is truncated at creation time, so it only
// contains mutations from the current run.
//
// This is intended for development: you can run the production sync
// pipeline but divert new mutations to a dev file for inspection
// without risking corruption of the production mutation log.
//
// If wrap is nil, mutations are only written to the dev file (not to
// any production log).
type DevLogger struct {
	wrap MutationLogger // may be nil
	f    *os.File
	mu   sync.Mutex
	n    int
}

// NewDevLogger creates a DevLogger that writes pretty-printed mutations
// to the given file path (truncated at startup). If wrap is non-nil,
// each mutation is also forwarded to wrap.Log.
func NewDevLogger(path string, wrap MutationLogger) *DevLogger {
	f, err := os.Create(path)
	if err != nil {
		log.Fatalf("creating dev mutation log: %v", err)
	}
	fmt.Fprintf(f, "# Dev mutation log started at %s\n\n", time.Now().UTC().Format(time.RFC3339))
	return &DevLogger{wrap: wrap, f: f}
}

func (d *DevLogger) Log(m *maintpb.Mutation) error {
	text, err := prototext.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(m)
	if err != nil {
		return fmt.Errorf("dev logger: marshaling prototext: %w", err)
	}

	d.mu.Lock()
	d.n++
	n := d.n
	fmt.Fprintf(d.f, "--- mutation #%d at %s ---\n%s\n", n, time.Now().UTC().Format(time.RFC3339), text)
	d.f.Sync()
	d.mu.Unlock()

	log.Printf("[dev] mutation #%d:\n%s", n, text)

	if d.wrap != nil {
		return d.wrap.Log(m)
	}
	return nil
}
