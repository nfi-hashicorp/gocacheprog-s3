// Forked from https://github.com/bradfitz/go-tool-cache/blob/main/wire/wire.go

// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package wire contains the JSON types that cmd/go uses
// to communicate with child processes implementing
// the cache interface.
package wire

import (
	"io"
	"time"
)

// Cmd is a command that can be issued to a child process.
//
// If the interface needs to grow, we can add new commands or new versioned
// commands like "get2".
type Cmd string

// from https://pkg.go.dev/cmd/go/internal/cacheprog@master#pkg-constants
const (
	CmdGet   = Cmd("get")
	CmdPut   = Cmd("put")
	CmdClose = Cmd("close")
)

// from https://pkg.go.dev/cmd/go/internal/cacheprog@master#Request
type Request struct {
	// ID is a unique number per process across all requests.
	// It must be echoed in the Response from the child.
	ID int64

	// Command is the type of request.
	// The go command will only send commands that were declared
	// as supported by the child.
	Command Cmd

	// ActionID is the cache key for "put" and "get" requests.
	ActionID []byte `json:",omitempty"` // or nil if not used

	// OutputID is stored with the body for "put" requests.
	OutputID []byte `json:",omitempty"` // or nil if not used

	// Body is the body for "put" requests. It's sent after the JSON object
	// as a base64-encoded JSON string when BodySize is non-zero.
	// It's sent as a separate JSON value instead of being a struct field
	// send in this JSON object so large values can be streamed in both directions.
	// The base64 string body of a Request will always be written
	// immediately after the JSON object and a newline.
	Body io.Reader `json:"-"`

	// BodySize is the number of bytes of Body. If zero, the body isn't written.
	BodySize int64 `json:",omitempty"`
}

// from https://pkg.go.dev/cmd/go/internal/cacheprog@master#Response
type Response struct {
	ID  int64  // that corresponds to Request; they can be answered out of order
	Err string `json:",omitempty"` // if non-empty, the error

	// KnownCommands is included in the first message that cache helper program
	// writes to stdout on startup (with ID==0). It includes the
	// Request.Command types that are supported by the program.
	//
	// This lets the go command extend the protocol gracefully over time (adding
	// "get2", etc), or fail gracefully when needed. It also lets the go command
	// verify the program wants to be a cache helper.
	KnownCommands []Cmd `json:",omitempty"`

	Miss     bool       `json:",omitempty"` // cache miss
	OutputID []byte     `json:",omitempty"` // the OutputID stored with the body
	Size     int64      `json:",omitempty"` // body size in bytes
	Time     *time.Time `json:",omitempty"` // when the object was put in the cache (optional; used for cache expiration)

	// DiskPath is the absolute path on disk of the body corresponding to a
	// "get" (on cache hit) or "put" request's ActionID.
	DiskPath string `json:",omitempty"`
}
