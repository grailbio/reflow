// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow

import "context"

var contextKey struct{}

// Context is a context.Context that is used for background
// operations within reflow. In addition to providing a common
// background context for operations, it also carries a WaitGroup, so
// that the caller can wait for background operation completion.
type Context struct {
	context.Context
	wg WaitGroup
}

// Background returns the Context associated with the given / parent
// context.Context. If there is no associated context, it returns a
// fresh Context without an affiliated WaitGroup.
func Background(ctx context.Context) Context {
	if ctx, ok := ctx.Value(&contextKey).(Context); ok {
		ctx.wg.Add(1)
		return ctx
	}
	return Context{context.Background(), nopWaitGroup{}}
}

// Complete should be called when the operation is complete.
func (c Context) Complete() {
	c.wg.Done()
}

// WaitGroup defines a subset of sync.WaitGroup's interface
// for use with Context.
type WaitGroup interface {
	Add(int)
	Done()
}

type nopWaitGroup struct{}

func (nopWaitGroup) Add(int) {}
func (nopWaitGroup) Done()   {}

// WithBackground returns a new context.Context with an affiliated
// Context, accessible via Background. The background context may be
// canceled with the returned cancellation function. The supplied
// WaitGroup is used to inform the caller of pending background
// operations: wg.Add(1) is called for each call to Background;
// wg.Done is called when the context returned from Background is
// disposed of through (Context).Complete.
func WithBackground(ctx context.Context, wg WaitGroup) (context.Context, context.CancelFunc) {
	bg, cancel := context.WithCancel(context.Background())
	return context.WithValue(ctx, &contextKey, Context{bg, wg}), cancel
}
