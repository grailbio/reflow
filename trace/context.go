// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package trace

type key int

var (
	tracerKey key = 0
	spanKey   key = 1
)
