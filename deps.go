//go:build go_mod_tidy_deps

// Package gxbm's deps.go depends on tool packages so their versions stay in
// go.mod after a 'go mod tidy'.
// See https://github.com/golang/go/wiki/Modules#how-can-i-track-tool-dependencies-for-a-module
package gxbm

import _ "github.com/bufbuild/buf/cmd/buf"
