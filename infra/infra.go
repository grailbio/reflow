package infra

import (
	"flag"
	"fmt"
	"io/ioutil"
	golog "log"
	"os"
	"os/user"
	"strings"

	"github.com/grailbio/reflow/pool"

	"github.com/grailbio/infra"
	"github.com/grailbio/reflow/log"
)

func init() {
	infra.Register("user", new(User))
	infra.Register("reflowletversion", new(ReflowletVersion))
	infra.Register("reflowversion", new(ReflowVersion))
	infra.Register("key", new(SshKey))
	infra.Register("off", new(CacheProviderOff))
	infra.Register("read", new(CacheProviderRead))
	infra.Register("write", new(CacheProviderWrite))
	infra.Register("readwrite", new(CacheProviderReadWrite))
	infra.Register("logger", new(Logger))
	infra.Register("kv", new(KV))
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
	TaskDB     = "taskdb"
)

// User is the infrastructure provider for username.
type User string

// Help implements infra.Provider
func (u *User) Help() string {
	return "provide a username (or use OS user by default)"
}

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

// Help implements infra.Provider
func (r ReflowletVersion) Help() string {
	return "provide a reflowlet version"
}

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

// Help implements infra.Provider
func (r ReflowVersion) Help() string {
	return "provide a reflow version"
}

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
	Key string `yaml:"key,omitempty"`
}

// Help implements infra.Provider
func (SshKey) Help() string {
	return "provide a ssh public key"
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

func (s *SshKey) Flags(flags *flag.FlagSet) {
	flags.StringVar(&s.Key, "key", "", "public key")
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
}

// CacheProviderOff is the provider to turn off cache.
type CacheProviderOff struct {
	*CacheProvider
}

// Help implements infra.Provider
func (CacheProviderOff) Help() string {
	return "turn caching off"
}

// Init implements infra.Provider
func (c *CacheProviderOff) Init() error {
	c.CacheProvider = &CacheProvider{CacheMode: CacheOff}
	return nil
}

// CacheProviderRead is the provider to only read from cache.
type CacheProviderRead struct {
	*CacheProvider
}

// Init implements infra.Provider
func (c *CacheProviderRead) Init() error {
	c.CacheProvider = &CacheProvider{CacheMode: CacheRead}
	return nil
}

// Help implements infra.Provider
func (CacheProviderRead) Help() string {
	return "read-only cache"
}

// CacheProviderWrite is the provider to only write to cache.
type CacheProviderWrite struct {
	*CacheProvider
}

// Init implements infra.Provider
func (c *CacheProviderWrite) Init() error {
	c.CacheProvider = &CacheProvider{CacheMode: CacheWrite}
	return nil
}

// Help implements infra.Provider
func (CacheProviderWrite) Help() string {
	return "write-only cache"
}

// CacheProviderReadWrite is the provider to read and write to cache.
type CacheProviderReadWrite struct {
	*CacheProvider
}

// Help implements infra.Provider
func (CacheProviderReadWrite) Help() string {
	return "read write cache"
}

// Init implements infra.Provider
func (c *CacheProviderReadWrite) Init() error {
	c.CacheProvider = &CacheProvider{CacheMode: CacheRead | CacheWrite}
	return nil
}

// Logger is the infra provider for logger.
type Logger struct {
	*log.Logger
	level string
}

// Help implements infra.Provider
func (Logger) Help() string {
	return "provide a logger"
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

// KV is provider that takes a semicolon separated key=value list.
type KV struct {
	pool.Labels
	flags string
}

// Help implements infra.Provider
func (KV) Help() string {
	return "semicolon separated list of key=value labels"
}

// Flags implements infra.Provider
func (l *KV) Flags(flags *flag.FlagSet) {
	flags.StringVar(&l.flags, "labels", "", "key=value;...")
}

// Init implements infra.Provider
func (l *KV) Init(user *User) error {
	l.Labels = make(pool.Labels)
	l.Labels["user"] = string(*user)
	if l.flags != "" {
		split := strings.Split(l.flags, ";")
		for _, kv := range split {
			kvs := strings.Split(kv, "=")
			if len(kvs) != 2 || len(kvs[0]) == 0 || len(kvs[1]) == 0 {
				return fmt.Errorf("malformed label: %v", kv)
			}
			l.Labels[kvs[0]] = kvs[1]
		}
	}
	return nil
}

// Instance implements infra.Provider
func (l *KV) Instance() interface{} {
	return l
}
