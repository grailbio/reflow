// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

import yaml "gopkg.in/yaml.v2"

// CloudFile is a component of the cloudConfig configuration for CoreOS.
// It represents a file that will be written to the filesystem.
type CloudFile struct {
	Path        string `yaml:"path,omitempty"`
	Permissions string `yaml:"permissions,omitempty"`
	Owner       string `yaml:"owner,omitempty"`
	Content     string `yaml:"content,omitempty"`
}

// CloudUnit is a component of the cloudConfig configuration for CoreOS.
// It represents a CoreOS unit.
type CloudUnit struct {
	Name    string `yaml:"name,omitempty"`
	Command string `yaml:"command,omitempty"`
	Enable  bool   `yaml:"enable,omitempty"`
	Content string `yaml:"content,omitempty"`
}

// cloudConfig represents a cloud cloud configuration as
// accepted by CoreOS. It can be incrementally defined and
// then rendered by its Marshal method.
type cloudConfig struct {
	WriteFiles []CloudFile `yaml:"write_files,omitempty"`
	CoreOS     struct {
		Update struct {
			RebootStrategy string `yaml:"reboot-strategy,omitempty"`
		} `yaml:"update,omitempty"`
		Units []CloudUnit `yaml:"units,omitempty"`
	} `yaml:"coreos,omitempty"`
	SshAuthorizedKeys []string `yaml:"ssh-authorized-keys,omitempty"`
}

// Merge merges cloudConfig d into c. List entries from c are
// appended to d, and key-values are overwritten.
func (c *cloudConfig) Merge(d *cloudConfig) {
	for _, f := range d.WriteFiles {
		c.WriteFiles = append(c.WriteFiles, f)
	}
	if s := d.CoreOS.Update.RebootStrategy; s != "" {
		c.CoreOS.Update.RebootStrategy = s
	}
	for _, u := range d.CoreOS.Units {
		c.CoreOS.Units = append(c.CoreOS.Units, u)
	}
	for _, k := range d.SshAuthorizedKeys {
		c.SshAuthorizedKeys = append(c.SshAuthorizedKeys, k)
	}
}

// AppendFile appends the file f to the cloudConfig c.
func (c *cloudConfig) AppendFile(f CloudFile) {
	c.WriteFiles = append(c.WriteFiles, f)
}

// AppendUnit appends the systemd unit u to the cloudConfig c.
func (c *cloudConfig) AppendUnit(u CloudUnit) {
	c.CoreOS.Units = append(c.CoreOS.Units, u)
}

// Marshal renders the cloudConfig into YAML, with the prerequisite
// cloud-config header.
func (c *cloudConfig) Marshal() ([]byte, error) {
	b, err := yaml.Marshal(c)
	if err != nil {
		return nil, err
	}
	return append([]byte("#cloud-config\n"), b...), nil
}
