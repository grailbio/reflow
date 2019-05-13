package reflow

import (
	"flag"
	"io/ioutil"
	"os"

	"github.com/grailbio/infra"
)

func init() {
	infra.Register(new(User))
	infra.Register(new(ReflowletVersion))
	infra.Register(new(ReflowVersion))
	infra.Register(new(SshKey))
}

// User is the infrastructure provider for username.
type User string

// Init implements infra.Provider.
func (u *User) Init() error {
	return nil
}

// Flags implements infra.Provider.
func (u *User) Flags(flags *flag.FlagSet) {
	flags.StringVar((*string)(u), "user", "", "user name")
}

// User returns the username.
func (u User) User() string {
	return (string)(u)
}

// ReflowletVersion is the infrastructure provider for the reflowlet version.
type ReflowletVersion string

// Init implements infra.Provider.
func (r *ReflowletVersion) Init() error {
	if *r == "" {
		*r = "bootstrap"
	}
	return nil
}

// Flags implements infra.Provider.
func (r *ReflowletVersion) Flags(flags *flag.FlagSet) {
	flags.StringVar((*string)(r), "version", "", "version")
}

// Value returns the reflowlet version name.
func (r *ReflowletVersion) Value() string {
	return string(*r)
}

// ReflowVersion is the infrastructure provider for the reflow version.
type ReflowVersion string

// Init implements infra.Provider.
func (r *ReflowVersion) Init() error {
	if *r == "" {
		*r = "broken"
	}
	return nil
}

// Flags implements infra.Provider.
func (r *ReflowVersion) Flags(flags *flag.FlagSet) {
	flags.StringVar((*string)(r), "version", "", "version")
}

// Value returns the reflow version name.
func (r *ReflowVersion) Value() string {
	return string(*r)
}

const sshKeyFile = "$HOME/.ssh/id_rsa.pub"

// SshKey is the infrastructure provider for ssh key
type SshKey []byte

// Init implements infra.Provider.
func (s *SshKey) Init() error {
	// Ignore error: SSH key is optional.
	*s, _ = ioutil.ReadFile(os.ExpandEnv(sshKeyFile))
	return nil
}

// Value returns the ssh key bytes.
func (s *SshKey) Value() []byte {
	return *s
}
