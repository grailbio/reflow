package iputil

import (
	"errors"
	"net"
)

// ExternalIP4 retrieves the machine's external (not necessarily public)
// IP address.
//
// TODO(marius): use http://169.254.169.254/latest/meta-data/public-ipv4 on
// AWS to get the public address as well.
func ExternalIP4() (net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue
			}
			return ip, nil
		}
	}
	return nil, errors.New("no external IPs found")
}
