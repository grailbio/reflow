package infra

import (
	"flag"
	"fmt"
	"io/ioutil"
	golog "log"
	"os"
	"os/user"
	"strings"
	"time"

	"github.com/grailbio/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
)

func init() {
	infra.Register("user", new(User))
	infra.Register("bootstrapimage", new(BootstrapImage))
	infra.Register("reflowversion", new(ReflowVersion))
	infra.Register("key", new(SshKey))
	infra.Register("off", new(CacheProviderOff))
	infra.Register("read", new(CacheProviderRead))
	infra.Register("write", new(CacheProviderWrite))
	infra.Register("readwrite", new(CacheProviderReadWrite))
	infra.Register("logger", new(Logger))
	infra.Register("kv", new(KV))
	infra.Register("reflowletconfig", new(ReflowletConfig))
	infra.Register("docker", new(DockerConfig))
	infra.Register("predictorconfig", new(PredictorConfig))
	infra.Register("testpredictorconfig", new(PredictorTestConfig))
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
	Bootstrap  = "bootstrap"
	Session    = "session"
	SSHKey     = "sshkey"
	Username   = "user"
	TLS        = "tls"
	Tracer     = "tracer"
	TaskDB     = "taskdb"
	Docker     = "docker"
	Predictor  = "predictor"
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

// BootstrapImage is the URL of the image used for instance bootstrap.
type BootstrapImage string

// Help implements infra.Provider
func (r BootstrapImage) Help() string {
	return "provide a bootstrap image URL"
}

// Init implements infra.Provider.
func (r *BootstrapImage) Init() error {
	if *r == "" {
		*r = "bootstrap"
	}
	return nil
}

// Flags implements infra.Provider.
func (r *BootstrapImage) Flags(flags *flag.FlagSet) {
	flags.StringVar((*string)(r), "uri", "", "uri")
}

// Value returns the bootstrap image uri.
func (r *BootstrapImage) Value() string {
	return string(*r)
}

// Set sets this BootstrapImage's value with the given one (if different)
// but only if its current value is "bootstrap".  Returns true if the value was set.
func (r *BootstrapImage) Set(image string) bool {
	if *r != "bootstrap" && string(*r) != image {
		return false
	}
	*r = BootstrapImage(image)
	return true
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

// VolumeWatcher represents the set of parameters that govern the behavior of a volume watcher.
// Every WatcherSleepDuration, the watcher will check the disk usage and keep track of the
// last time at which the usage was below the LowThresholdPct. If the disk usage goes
// above HighThresholdPct, then a resize is triggered.  The volume size will be increased to
// 2x the current size unless the time taken to go from below LowThresholdPct to above HighThresholdPct
// was within FastThresholdDuration, in which case the size will be increased to 4x the current size.
// Once the underlying volume is resized, a filesystem resize will be attempted every ResizeSleepDuration
// until success.
type VolumeWatcher struct {
	// LowThresholdPct defines how full the filesystem needs to be
	// to trigger the low threshold.
	LowThresholdPct float64 `yaml:"lowthresholdpct,omitempty"`

	// HighThresholdPct defines how full the filesystem needs to be
	// to trigger the high threshold.
	// The time it takes for the filesystem to fill from low threshold to high threshold
	// selects between a lower (time is longer) or higher (time is shorter)
	// amount of extra space to get added to the volume.
	HighThresholdPct float64 `yaml:"highthresholdpct,omitempty"`

	// WatcherSleepDuration is the frequency with which to check
	// whether disk are full over threshold and need resizing
	WatcherSleepDuration time.Duration `yaml:"watchersleepduration,omitempty"`

	// ResizeSleepDuration is the frequency with which to attempt
	// resizing the volume and filesystem once we've hit above HighThresholdPct
	ResizeSleepDuration time.Duration `yaml:"resizesleepduration,omitempty"`

	// FastThresholdDuration is the time duration within which if the disk usage
	// went from below LowThresholdPct to above HighThresholdPct, then
	// we quadruple the disk size (otherwise we just double)
	FastThresholdDuration time.Duration `yaml:"fastthresholdduration,omitempty"`
}

// ReflowletConfig is a provider for reflowlet configuration parameters which control its behavior.
type ReflowletConfig struct {
	// MaxIdleDuration is the maximum duration the reflowlet will be idle waiting to receive work after which it dies.
	MaxIdleDuration time.Duration `yaml:"maxidleduration,omitempty"`
	// LogMemStatsDuration is the periodicity with which the reflowlet will log memory usage.
	LogMemStatsDuration time.Duration `yaml:"logmemstatsduration,omitempty"`
	// VolumeWatcher defines a set of parameters for the volume watcher on the reflowlet.
	VolumeWatcher VolumeWatcher `yaml:"volumewatcher,omitempty"`
}

var DefaultReflowletConfig = ReflowletConfig{
	MaxIdleDuration:     10 * time.Minute,
	LogMemStatsDuration: 1 * time.Minute,
	VolumeWatcher:       DefaultVolumeWatcher,
}

// DefaultVolumeWatcher are a default set of volume watcher parameters which will double the disk size
// if the disk usage >75%, or quadruple if the usage went from <55% to >75% within 24 hours.
var DefaultVolumeWatcher = VolumeWatcher{
	LowThresholdPct:       55.0,
	HighThresholdPct:      75.0,
	WatcherSleepDuration:  1 * time.Minute,
	ResizeSleepDuration:   5 * time.Second,
	FastThresholdDuration: 24 * time.Hour,
}

// Init implements infra.Provider.
func (rp *ReflowletConfig) Init() error {
	if rp == nil || *rp == (ReflowletConfig{}) {
		*rp = DefaultReflowletConfig
	}
	return nil
}

// Instance implements infra.Provider.
func (rp *ReflowletConfig) InstanceConfig() interface{} {
	return rp
}

// DockerConfig sets the docker memory limit to either be hard or soft.
type DockerConfig string

// Help implements infra.Provider.
func (m DockerConfig) Help() string {
	return "set hard/soft memory limits for docker containers"
}

// Init implements infra.Provider.
func (m *DockerConfig) Init() error {
	if m.Value() != "soft" && m.Value() != "hard" {
		return fmt.Errorf("invalid memlimit: %v: memlimit is soft or hard only", m.Value())
	}
	return nil
}

// Flags implements infra.Provider.
func (m *DockerConfig) Flags(flags *flag.FlagSet) {
	flags.StringVar((*string)(m), "memlimit", "soft", "memlimit")
}

// Value returns the docker memory limit mode.
func (m *DockerConfig) Value() string {
	return string(*m)
}

// PredictorConfig represents the set of parameters that govern the behavior
// of the resource prediction system. The resource prediction system predicts
// the resource usage of an exec based on previous runs. A prediction requires
// at least MinData datapoints--profiles containing the resource to be predicted.
// No prediction will look at more than MaxInspect ExecInspects to obtain theses
// profiles. An exec's predicted memory usage is the MemPercentile'th percentile
// of the maximum memory usage of the exec.
type PredictorConfig struct {
	// MinData is the minimum number of datapoints
	// required to make a resource prediction.
	MinData int `yaml:"mindata"`
	// MaxInspect is the maximum number of ExecInspects
	// that can be used to make a prediction.
	MaxInspect int `yaml:"maxinspect"`
	// MemPercentile is the percentile that will
	// be used to predict memory usage for all tasks.
	MemPercentile float64 `yaml:"mempercentile"`
	// NonEC2Ok determines if the predictor should
	// run on any machine. If set to false, the predictor
	// will only work when 'reflow run' is invoked from
	// an EC2 instance
	NonEC2Ok bool `yaml:"alwaysrun"`
}

var DefaultPredictorConfig = PredictorConfig{
	MinData:       5,
	MaxInspect:    50,
	MemPercentile: 95,
	NonEC2Ok:      false,
}

// Help implements infra.Provider.
func (p PredictorConfig) Help() string {
	return "config for the reflow resource prediction system"
}

// Init implements infra.Provider.
func (p *PredictorConfig) Init() error {
	if p == nil || *p == (PredictorConfig{}) {
		*p = DefaultPredictorConfig
	}
	if p.MinData < 1 {
		return fmt.Errorf("mindata %d is less than 1", p.MinData)
	}
	if p.MaxInspect < p.MinData {
		return fmt.Errorf("maxinspect is less than mindata")
	}
	if p.MemPercentile < 0 || p.MemPercentile > 100 {
		return fmt.Errorf("percentile %v is outside of range [0, 100]", p.MemPercentile)
	}
	return nil
}

// InstanceConfig implements infra.Provider.
func (p *PredictorConfig) InstanceConfig() interface{} {
	return p
}

type PredictorTestConfig struct {
	*PredictorConfig
}

var DefaultPredictorTestConfig = PredictorTestConfig{
	&PredictorConfig{
		MinData:       1,
		MaxInspect:    50,
		MemPercentile: 95,
		NonEC2Ok:      true,
	},
}

// Help implements infra.Provider.
func (p PredictorTestConfig) Help() string {
	return "config for testing the reflow resource prediction system"
}

// Init implements infra.Provider.
func (p *PredictorTestConfig) Init() error {
	if p == nil || *p == (PredictorTestConfig{}) {
		*p = DefaultPredictorTestConfig
	}
	return p.PredictorConfig.Init()
}

// InstanceConfig implements infra.Provider.
func (p *PredictorTestConfig) InstanceConfig() interface{} {
	return p
}
