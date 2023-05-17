// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflowlet

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"docker.io/go-docker"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/base/cloud/ec2util"
	"github.com/grailbio/infra"
	infratls "github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/ec2authenticator"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/ec2cluster/instances"
	"github.com/grailbio/reflow/ec2cluster/volume"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/local"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/metrics"
	"github.com/grailbio/reflow/metrics/monitoring"
	"github.com/grailbio/reflow/metrics/prometrics"
	"github.com/grailbio/reflow/pool/server"
	"github.com/grailbio/reflow/repository/blobrepo"
	repositoryhttp "github.com/grailbio/reflow/repository/http"
	"github.com/grailbio/reflow/rest"
	"github.com/grailbio/reflow/taskdb"
	"golang.org/x/net/http2"
)

const (
	// maxConcurrentStreams is the number of concurrent http/2 streams we support.
	maxConcurrentStreams = 20000

	// metricsUpdatePeriodicity is the periodicity at which reflowlet metrics are updated.
	metricsUpdatePeriodicity = 1 * time.Minute
)

// A Server is a reflow server, exposing a local pool over an HTTP server.
type Server struct {
	// Config is the server's config.
	Config infra.Config

	// Addr is the address on which to listen.
	Addr string
	// Prefix is the prefix used for directory lookup; permits reflowlet
	// to run inside of Docker.
	Prefix string
	// Insecure listens on HTTP, not HTTPS.
	Insecure bool
	// Dir is the runtime data directory.
	Dir string
	// EC2Cluster tells whether this reflowlet is part of an EC2cluster.
	// When true, the reflowlet shuts down if it is idle after 10 minutes.
	EC2Cluster  bool
	ec2Identity ec2metadata.EC2InstanceIdentityDocument

	// HTTPDebug determines whether HTTP debug logging is turned on.
	HTTPDebug bool

	// NodeExporterMetricsPort determines whether to run a prometheus node_exporter daemon
	// on each Reflowlet. Setting a value runs the node_exporter daemon and configures it to
	// output prometheus metrics on the given port. Passing a non-zero value also adds an
	// additional route to the general Reflowlet server, such that metrics are made available
	// via proxy over the existing HTTPS connection and the following Reflow command:
	// $ reflow http https://${EC2_INST_PUBLIC_DNS}:9000/v1/node/metrics
	// If the user wishes to use other scrapers to fetch metrics from the Reflowlet over HTTP,
	// they may additionally choose to expose the port via the AWS settings for their Reflow
	// cluster.
	NodeExporterMetricsPort int

	// MetricsPort is the port where (if set to non-zero) prometheus metrics are being served.
	// If set to a non-zero value, an additional handler is always enabled (in the reflowlet server) such that,
	// metrics are available via proxy from the client side, accessible via the following Reflow command:
	// $ reflow http https://${EC2_INST_PUBLIC_DNS}:9000/v1/metrics
	MetricsPort int

	// server is the underlying HTTP server
	server *http.Server

	configFlag string

	// version of the reflowlet instance.
	version string
}

// NewServer returns a new server with specified version and config.
func NewServer(version string, config infra.Config) *Server {
	return &Server{version: version, Config: config}
}

// AddFlags adds flags configuring various Reflowlet parameters to
// the provided FlagSet.
func (s *Server) AddFlags(flags *flag.FlagSet) {
	flags.StringVar(&s.Addr, "addr", ":9000", "HTTPS server address")
	flags.StringVar(&s.Prefix, "prefix", "", "prefix used for directory lookup")
	flags.BoolVar(&s.Insecure, "insecure", false, "listen on HTTP, not HTTPS")
	flags.StringVar(&s.Dir, "dir", "/mnt/data/reflow", "runtime data directory")
	flags.BoolVar(&s.EC2Cluster, "ec2cluster", false, "this reflowlet is part of an ec2cluster")
	flags.BoolVar(&s.HTTPDebug, "httpdebug", false, "turn on HTTP debug logging")
}

// spotNoticeWatcher watches for a spot termination notice and logs if found.
// See https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-interruptions.html#instance-action-metadata
// TODO(swami):  When flagged for termination, put server in lameduck mode.
func (s *Server) spotNoticeWatcher(ctx context.Context) {
	logger := log.Std.Tee(nil, "spot notice: ")
	tick := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		resp, err := http.Get("http://169.254.169.254/latest/meta-data/spot/instance-action")
		if err != nil {
			continue
		}
		// The following is done in a func to defer closing the response body.
		func() {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return
			}
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				logger.Debugf("read %v", err)
				return
			}
			logger.Print(string(b))
		}()
	}
}

// setTags sets the reflowlet version tag on the EC2 instance (if running on one).
func (s *Server) setTags(sess *session.Session) error {
	if !s.EC2Cluster {
		return nil
	}
	var err error
	s.ec2Identity, err = ec2util.GetInstanceIdentityDocument(sess)
	if err != nil {
		return err
	}
	svc := ec2.New(sess, &aws.Config{MaxRetries: aws.Int(3)})
	input := &ec2.CreateTagsInput{
		Resources: []*string{aws.String(s.ec2Identity.InstanceID)},
		Tags:      []*ec2.Tag{{Key: aws.String("reflowlet:version"), Value: aws.String(s.version)}}}
	_, err = svc.CreateTags(input)
	return err
}

// getVolumeWatcher returns a volume watcher for the given path (or an error).
// This does not start the volume watcher, which must be done explicitly by calling `Watch()` on it.
func (s *Server) getVolumeWatcher(sess *session.Session, path string, vw infra2.VolumeWatcher) (*volume.Watcher, error) {
	if vw == (infra2.VolumeWatcher{}) {
		log.Print("volume watcher not configured, skipping\n")
		return nil, nil
	}
	logger := log.Std.Tee(nil, fmt.Sprintf("watcher %s: ", path))
	v, err := volume.NewEbsLvmVolume(sess, logger, path)
	if err != nil {
		return nil, fmt.Errorf("create volume for path %s: %v", path, err)
	}
	return volume.NewWatcher(v, vw, logger)
}

// loopUntilIdleOrBad loops forever while the given pool is in use and healthy.
// It returns if the pool is idle for rc.MaxIdleDuration or if any of the pool's
// executors encounter a file integrity error.
func (s *Server) loopUntilIdleOrBad(p *local.Pool, rc *infra2.ReflowletConfig, logger *log.Logger) {
	t := time.NewTimer(rc.MaxIdleDuration)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			// Always give the instance an expiry period to receive work,
			// then check periodically if the instance has been idle for more
			// than the expiry time.
			if stopped, _ := p.StopIfIdleFor(rc.MaxIdleDuration); stopped {
				logger.Printf("idle for %s; shutting down", rc.MaxIdleDuration)
				return
			}
			t.Reset(time.Minute)
		case <-p.IntegrityErrSignal:
			logger.Error("received integrity error signal; shutting down")
			return
		}
	}
}

// ListenAndServe serves the Reflowlet server on the configured address.
func (s *Server) ListenAndServe() error {
	defer s.waitForCloudwatchLogs()

	addr := os.Getenv("DOCKER_HOST")
	if addr == "" {
		addr = "unix:///var/run/docker.sock"
	}
	client, err := docker.NewClient(
		addr, "1.22",
		nil, map[string]string{"user-agent": "reflow"})
	if err != nil {
		return err
	}
	var rc *infra2.ReflowletConfig
	if err = s.Config.Instance(&rc); err != nil {
		return err
	}
	var sess *session.Session
	if err = s.Config.Instance(&sess); err != nil {
		return err
	}
	var tlsa infratls.Certs
	if err = s.Config.Instance(&tlsa); err != nil {
		return err
	}
	clientConfig, serverConfig, terr := tlsa.HTTPS()
	if terr != nil {
		return terr
	}
	var creds *credentials.Credentials
	if err = s.Config.Instance(&creds); err != nil {
		return err
	}

	var mclient metrics.Client
	if err = s.Config.Instance(&mclient); err != nil {
		return err
	}
	if pclient, ok := mclient.(*prometrics.Client); ok {
		s.NodeExporterMetricsPort = pclient.NodeExporterPort
		s.MetricsPort = pclient.Port
	}

	var (
		dockerconfig *infra2.DockerConfig
		hardMemLimit bool
	)
	if err = s.Config.Instance(&dockerconfig); err != nil {
		return err
	} else if dockerconfig.Value() == "hard" {
		hardMemLimit = true
	}

	if err = s.setTags(sess); err != nil {
		return fmt.Errorf("set tags: %v", err)
	}

	var (
		tdb                    taskdb.TaskDB
		poolId                 reflow.StringDigest
		expectedUsableMemBytes int64
	)
	if s.EC2Cluster {
		if err = s.Config.Instance(&tdb); err != nil {
			log.Debugf("taskdb: %v", err)
		}
		poolId = reflow.NewStringDigest(s.ec2Identity.InstanceID)
		verifiedStatus := instances.VerifiedByRegion[s.ec2Identity.Region][s.ec2Identity.InstanceType]
		if !verifiedStatus.Verified {
			log.Debugf("WARNING: using an unverified instance type: %s", s.ec2Identity.InstanceType)
		}
		expectedUsableMemBytes = verifiedStatus.ExpectedMemoryBytes()
	}

	// Default HTTPS and s3 clients for repository dialers.
	// TODO(marius): handle this more elegantly, perhaps by
	// avoiding global registration altogether.
	blobrepo.Register("s3", s3blob.New(sess))
	transport := &http.Transport{TLSClientConfig: clientConfig}
	if err = http2.ConfigureTransport(transport); err != nil {
		return err
	}
	repositoryhttp.HTTPClient = &http.Client{Transport: transport}
	p := &local.Pool{
		Client:             client,
		Dir:                s.Dir,
		Prefix:             s.Prefix,
		Authenticator:      ec2authenticator.New(sess),
		AWSCreds:           creds,
		Session:            sess,
		Blob:               blob.Mux{"s3": s3blob.New(sess)},
		TaskDBPoolId:       poolId,
		TaskDB:             tdb,
		Log:                log.Std.Tee(nil, "executor: "),
		HardMemLimit:       hardMemLimit,
		IntegrityErrSignal: make(chan struct{}),
	}
	if err = p.Start(expectedUsableMemBytes); err != nil {
		return err
	}

	var (
		logType = "local"
		w       *volume.Watcher
		wg      sync.WaitGroup
	)
	if s.EC2Cluster {
		logType = s.ec2Identity.InstanceType

		if w, err = s.getVolumeWatcher(sess, filepath.Join(s.Prefix, s.Dir), rc.VolumeWatcher); err != nil {
			return fmt.Errorf("getVolumeWatcher: %v", err)
		}
	}

	reflowletLog := log.Std.Tee(nil, fmt.Sprintf("reflowlet (%s) ", logType))
	reflowletLog.Printf("started (version %s)", s.version)

	ctx, cancel := context.WithCancel(context.Background())
	if s.EC2Cluster {
		// Start the volume watcher.
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Watch(ctx)
		}()

		// Start the spot notice watcher.
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.spotNoticeWatcher(ctx)
		}()

		// Maintain the pool's TaskDB row.
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.MaintainTaskDBRow(ctx)
		}()
	}

	// Start periodic stats logging
	wg.Add(1)
	go func() {
		defer wg.Done()
		logStats(ctx, p, reflowletLog.Tee(nil, "stats: "), rc.LogStatsDuration)
	}()

	// Start periodic prometheus metrics updation.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx := metrics.WithClient(ctx, mclient)
		updateMetrics(ctx, p, metricsUpdatePeriodicity)
	}()

	// Start the loop to exit reflowlet if idle or unhealthy.
	go func() {
		s.loopUntilIdleOrBad(p, rc, reflowletLog)
		// Cancel and wait for other goroutines.
		cancel()
		wg.Wait()
		s.waitForCloudwatchLogs()
		os.Exit(0)
	}()

	var httpLog *log.Logger
	if s.HTTPDebug {
		httpLog = reflowletLog.Tee(nil, "http: ")
		httpLog.Level = log.DebugLevel
		log.Std.Level = log.DebugLevel
	}

	http.Handle("/", rest.Handler(server.NewNode(p), httpLog))
	// Create a servlet node for this reflowlet's config.
	var cfgNode rest.DoFunc
	if cfgNode, err = newConfigNode(s.Config); err != nil {
		return fmt.Errorf("read config: %v", err)
	}
	http.Handle("/v1/config", rest.DoFuncHandler(cfgNode, httpLog))
	if s.NodeExporterMetricsPort != 0 {
		url, proxyPath := fmt.Sprintf("http://localhost:%d/metrics", s.NodeExporterMetricsPort), "/v1/node/metrics"
		http.Handle(proxyPath, rest.DoProxyHandler(url, httpLog))
		reflowletLog.Printf("proxying node metrics %s -> %s", proxyPath, url)

		// Start node exporter metrics logging
		m := monitoring.NewMetricsLogger(url, rc.LogStatsDuration, monitoring.NodeMetricsNames, log.Std.Tee(nil, "node_exporter metric: "))
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Go(ctx)
		}()
		d := monitoring.NewNodeOomDetector(url, log.Std.Tee(nil, "node oom detector: "))
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.Go(ctx)
		}()
		p.NodeOomDetector = d
	}
	if s.MetricsPort != 0 {
		url, proxyPath := fmt.Sprintf("http://localhost:%d", s.MetricsPort), "/v1/metrics"
		http.Handle(proxyPath, rest.DoProxyHandler(url, httpLog))
		reflowletLog.Printf("proxying node metrics %s -> %s", proxyPath, url)
		// Start reflowlet process metrics logging
		m := monitoring.NewMetricsLogger(url, rc.LogStatsDuration, nil, reflowletLog.Tee(nil, "metric: "))
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Go(ctx)
		}()
	}

	s.server = &http.Server{Addr: s.Addr}
	if s.Insecure {
		return s.server.ListenAndServe()
	}
	serverConfig.ClientAuth = tls.RequireAndVerifyClientCert
	s.server.TLSConfig = serverConfig
	if err = http2.ConfigureServer(s.server, &http2.Server{
		MaxConcurrentStreams: maxConcurrentStreams,
	}); err != nil {
		return err
	}
	return s.server.ListenAndServeTLS("", "")
}

// waitForCloudwatchLogs sleeps for enough time to allow logs to be flushed to
// CloudWatch during server shutdown.
func (s *Server) waitForCloudwatchLogs() {
	time.Sleep(time.Second + ec2cluster.ReflowletCloudwatchFlushMs*time.Millisecond)
}

func (s *Server) Shutdown() {
	_ = s.server.Shutdown(context.Background())
}

// IgnoreSigpipe consumes (and ignores) SIGPIPE signals. As of Go
// 1.6, these are generated only for stdout and stderr.
//
// This is useful where a reflowlet's standard output is closed while
// running, as can happen when journald restarts on systemd managed
// systems.
//
// See the following for more information:
//	https://bugzilla.redhat.com/show_bug.cgi?id=1300076
func IgnoreSigpipe() {
	c := make(chan os.Signal, 1024)
	signal.Notify(c, os.Signal(syscall.SIGPIPE))
	for {
		<-c
	}
}

func newConfigNode(cfg infra.Config) (rest.DoFunc, error) {
	b, err := cfg.Marshal(false)
	if err != nil {
		return nil, fmt.Errorf("serialize keys: %v", err)
	}
	return func(ctx context.Context, call *rest.Call) {
		if !call.Allow("GET") {
			return
		}
		call.Reply(http.StatusOK, string(b))
	}, nil
}

// logStats logs various stats to the given logger every d duration.
func logStats(ctx context.Context, p *local.Pool, log *log.Logger, d time.Duration) {
	iter := time.NewTicker(d)
	for {
		tot, free := p.Resources(), p.Available()
		freeFrac := free.Div(tot)
		var usedPcts []string
		for _, k := range []string{"cpu", "mem"} {
			usedPcts = append(usedPcts, fmt.Sprintf("%s: %.1f%%", k, 100.0-math.Round(100*freeFrac[k])))
		}
		log.Printf("Allocated resources %s; total %s free %s", strings.Join(usedPcts, " "), tot, free)

		select {
		case <-ctx.Done():
			return
		case <-iter.C:
		}
	}
}

// updateMetrics updates prometheus metrics every d duration.
func updateMetrics(ctx context.Context, p *local.Pool, d time.Duration) {
	iter := time.NewTicker(d)
	for {
		tot, free := p.Resources(), p.Available()

		metrics.GetPoolTotalSizeGauge(ctx).Set(tot.ScaledDistance(nil))
		metrics.GetPoolAvailSizeGauge(ctx).Set(free.ScaledDistance(nil))
		for _, r := range reflow.ResourcesKeys {
			metrics.GetPoolTotalResourcesGauge(ctx, r).Set(tot[r])
			metrics.GetPoolAvailResourcesGauge(ctx, r).Set(free[r])
		}

		select {
		case <-ctx.Done():
			return
		case <-iter.C:
		}
	}
}
