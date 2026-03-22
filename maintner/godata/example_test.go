// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package godata_test

import (
	"context"
	"os"
	"testing"

	"github.com/bradfitz/gxbm/maintner"
	"github.com/bradfitz/gxbm/maintner/godata"
)

func TestGet_numComments(t *testing.T) {
	if os.Getenv("TEST_GODATA") == "" {
		t.Skip("skipping test requiring large download; set TEST_GODATA=1 to enable")
	}
	corpus, err := godata.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	num := 0
	corpus.GitHub().ForeachRepo(func(gr *maintner.GitHubRepo) error {
		return gr.ForeachIssue(func(gi *maintner.GitHubIssue) error {
			return gi.ForeachComment(func(*maintner.GitHubComment) error {
				num++
				return nil
			})
		})
	})
	t.Logf("%d GitHub comments on Go repos.", num)
}
