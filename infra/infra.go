package infra

import (
	"flag"
	"io/ioutil"
	golog "log"
	"os"
	"os/user"

	"github.com/grailbio/infra"
	"github.com/grailbio/reflow/log"
)

func init() {
	infra.Register(new(User))
	infra.Register(new(ReflowletVersion))
	infra.Register(new(ReflowVersion))
	infra.Register(new(SshKey))
	infra.Register(new(CacheProvider))
	infra.Register(new(Logger))
}

// Reflow infra schema key names.
const (
	AWSCreds   = "awscreds"
	AWSRegion  = "awsregion"
	Assoc      = "assoc"
	AWSTool    = "awstool"
	Cache      = "cache"
	Cluster    = "cluster"
	Labels     = "labels"
	Log        = "logger"
	Repository = "repository"
	Reflow     = "reflow"
	Reflowlet  = "reflowlet"
	Session    = "session"
	SSHKey     = "sshkey"
	Username   = "user"
	TLS        = "tls"
	Tracer     = "tracer"
)

// User is the infrastructure provider for username.
type User string

// Init implements infra.Provider.
func (u *User) Init() error {
	if (string)(*u) != "" {
		return nil
	}
	user, err := user.Current()
	if err != nil {
		log.Errorf("failed to retrieve user: %v", err)
		*u = User("unknown")
	} else {
		*u = User(user.Username)
	}
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
type SshKey struct {
	Key string `yaml:"key"`
}

// Init implements infra.Provider.
func (s *SshKey) Init() error {
	if len(s.Key) == 0 {
		// Ignore error: SSH key is optional.
		b, err := ioutil.ReadFile(os.ExpandEnv(sshKeyFile))
		if err == nil {
			s.Key = string(b)
		}
	}
	return nil
}

// Config implements infra.Provider
func (s *SshKey) Config() interface{} {
	return s
}

// Value returns the ssh key.
func (s *SshKey) Value() string {
	return s.Key
}

// CacheMode is a bitmask that determines how caching is to be used in the evaluator.
type CacheMode int

const (
	// CacheOff is CacheMode's default value and indicates
	// no caching (read or write) is to be performed.
	CacheOff CacheMode = 0
	// CacheRead indicates that cache lookups should be performed
	// during evaluation.
	CacheRead CacheMode = 1 << iota
	// CacheWrite indicates that the evaluator should write evaluation
	// results to the cache.
	CacheWrite
)

// Reading returns whether the cache mode contains CacheRead.
func (c CacheMode) Reading() bool {
	return c&CacheRead == CacheRead
}

// Writing returns whether the cache mode contains CacheWrite.
func (c CacheMode) Writing() bool {
	return c&CacheWrite == CacheWrite
}

// CacheProvider is an infra provider for cache mode.
type CacheProvider struct {
	// CacheMode is the configured cache mode of this CacheMode infra provider.
	CacheMode
	mode string
}

// Init implements infra.Provider
func (c *CacheProvider) Init() error {
	switch c.mode {
	case "off":
		c.CacheMode = CacheOff
	case "read":
		c.CacheMode = CacheRead
	case "write":
		c.CacheMode = CacheWrite
	case "read+write":
		c.CacheMode = CacheRead | CacheWrite
	}
	return nil
}

// Flags implements infra.Provider
func (c *CacheProvider) Flags(flags *flag.FlagSet) {
	flags.StringVar(&c.mode, "cache", "read+write", "cache can be off, read, write, or read+write.")
}

// Logger is the infra provider for logger.
type Logger struct {
	*log.Logger
	level string
}

// Init implements infra.Provider
func (l *Logger) Init() error {
	var (
		level     log.Level
		logflags  int
		logprefix = "reflow: "
	)
	switch string(l.level) {
	case "off":
		level = log.OffLevel
	case "error":
		level = log.ErrorLevel
	case "info":
		level = log.InfoLevel
	case "debug":
		level = log.DebugLevel
	default:
		log.Fatalf("unrecognized log level %v", level)
	}
	if level > log.InfoLevel {
		logflags = golog.LstdFlags
		logprefix = ""
	}
	l.Logger = log.New(golog.New(os.Stderr, logprefix, logflags), level)
	return nil
}

func (l *Logger) Flags(flags *flag.FlagSet) {
	flags.StringVar(&l.level, "level", "info", "level of logging: off, error, info, debug.")
}
