// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package context

type key int

const (
	TracerKey key = 0
	SpanKey   key = 1

	MetricsClientKey key = 2
)
