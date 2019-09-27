// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package volume

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

var (
	sepln    = []byte("\n")
	sepcolon = []byte(":")
)

// Device is an abstraction of a physical or logical device
type Device interface {

	// Usage returns the amount of disk space in-use for this device as a percentage.
	Usage() (float64, error)

	// ResizeFS resizes the filesystem for this device
	ResizeFS() error
}

type device string

// Usage implements Device.
func (d device) Usage() (float64, error) {
	cmd := exec.Command("/bin/df", "--sync", "--output=pcent", string(d))
	out, err := cmd.Output()
	if err != nil {
		return 0.0, err
	}
	parts := strings.Split(string(out), "\n")
	if len(parts) < 2 {
		return 0.0, fmt.Errorf("unexpected output: %s\n%v", strings.Join(cmd.Args, " "), out)
	}
	pct, err := strconv.Atoi(strings.TrimSpace(strings.TrimSuffix(parts[1], "%")))
	return float64(pct), err
}

// ResizeFS implements Device.
func (d device) ResizeFS() error {
	device := string(d)
	if !strings.HasPrefix(device, "/dev/mapper/") {
		// We have a regular device.
		return exec.Command("/usr/bin/sudo", "/sbin/resize2fs", device).Run()
	}

	// We have an LVM, so determine the underlying physical devices to run the appropriate commands.
	physDevices, err := physicalDevices(device)
	if err != nil {
		return err
	}
	// Run pvresize on all devices:
	//   pvresize /dev/xvdb /dev/xvdc /dev/xvdd /dev/xvde
	args := []string{"/sbin/pvresize", "--debug"}
	args = append(args, physDevices...)
	stdout, err := exec.Command("/usr/bin/sudo", args...).Output()
	if err != nil {
		return fmt.Errorf("pvresize: %v\n stdout: %s", err, stdout)
	}
	// Run lvextend
	//   lvextend -r -l +100%FREE /dev/data_group/data_vol
	stdout, err = exec.Command("/usr/bin/sudo", "/sbin/lvextend", "--debug", "--nofsck", "--resizefs", "--extents", "+100%FREE", device).Output()
	if err != nil {
		return fmt.Errorf("lvextend: %v\n stdout: %s", err, stdout)
	}
	return nil
}

// physicalDevices returns the list of physical devices that back this device.
// Works either for a physical device itself or a logical volume setup using LVM.
func physicalDevices(device string) ([]string, error) {
	if !strings.HasPrefix(device, "/dev/mapper/") {
		return []string{device}, nil
	}
	/* lvdisplay -c /dev/data_group-data_vol
	   /dev/data_group/data_vol:data_group:3:1:-1:1:3590258688:438264:-1:0:-1:254:1 */
	stdout, err := exec.Command("/usr/bin/sudo", "/sbin/lvdisplay", "-c", device).Output()
	if err != nil {
		return nil, err
	}
	groupName := ""
	for _, line := range strings.Split(strings.TrimSpace(string(stdout)), "\n") {
		parts := strings.Split(line, ":")
		groupName = parts[1]
		if groupName != "" {
			break
		}
	}
	if groupName == "" {
		return nil, fmt.Errorf("couldn't find group in lvdisplay output for %s: %s", device, stdout)
	}
	/* pvdisplay -c
	   /dev/xvdf:data_group:448790528:-1:8:8:-1:4096:54783:0:54783:tSuUlo-vTQQ-2BKa-pC6i-GQVS-Po27-9DF1R6
	   /dev/xvdg:data_group:448790528:-1:8:8:-1:4096:54783:0:54783:yYNAv1-7qts-b1GC-ow3c-vZXI-mJ6E-afcsKN
	   /dev/xvdh:data_group:448790528:-1:8:8:-1:4096:54783:0:54783:yy5P2D-018a-S4hC-KNCY-cJWL-iwBF-J8udV8
	   /dev/xvdi:data_group:448790528:-1:8:8:-1:4096:54783:0:54783:lNRy19-74Gi-3PGH-NHQ6-Ynme-Wee2-V0zyQT
	   /dev/xvdj:data_group:448790528:-1:8:8:-1:4096:54783:0:54783:CWFc2p-fWng-aUfj-vZSF-a9IQ-DmS6-rIFNoN
	   /dev/xvdk:data_group:448790528:-1:8:8:-1:4096:54783:0:54783:NSqF0h-dU8p-5Lj1-kikn-oDQK-M1II-z1NAPr
	   /dev/xvdl:data_group:448790528:-1:8:8:-1:4096:54783:0:54783:VVFpfe-wtKG-t1Ee-A8AG-ONwX-LOck-69jXW9
	   /dev/xvdm:data_group:448790528:-1:8:8:-1:4096:54783:0:54783:iwfFgQ-Vhgj-C1xh-BDD1-eqnW-8QwK-D9DQi0 */
	stdout, err = exec.Command("/usr/bin/sudo", "/sbin/pvdisplay", "-c").Output()
	if err != nil {
		return nil, err
	}
	var devices []string
	for _, line := range bytes.Split(bytes.TrimSpace(stdout), sepln) {
		parts := bytes.Split(line, sepcolon)
		if groupName == string(parts[1]) {
			devices = append(devices, string(bytes.TrimSpace(parts[0])))
		}
	}
	return devices, nil
}

// deviceFor returns the device for the given path.
func deviceFor(path string) (device, error) {
	cmd := exec.Command("/bin/df", "--output=source", path)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	parts := bytes.Split(out, sepln)
	if len(parts) < 2 {
		return "", fmt.Errorf("unexpected output: %s\n%v", strings.Join(cmd.Args, " "), out)
	}
	return device(parts[1]), nil
}
