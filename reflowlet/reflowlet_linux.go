// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build linux

package reflowlet

import (
	"time"

	"golang.org/x/sys/unix"
)

func uptime() time.Duration {
	var si unix.Sysinfo_t
	if err := unix.Sysinfo(&si); err != nil {
		return 5 * time.Minute
	}
	return time.Duration(si.Uptime) * time.Second
}
