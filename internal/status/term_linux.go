//+build linux

// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package status

import "syscall"

const termios = syscall.TCGETS
