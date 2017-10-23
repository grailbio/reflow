// +build !linux

package reflowlet

import "time"

func uptime() time.Duration {
	return 5 * time.Minute
}
