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
