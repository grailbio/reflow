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
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/grailbio/base/data"

	"docker.io/go-docker"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/infra"
	infraaws "github.com/grailbio/infra/aws"
	infratls "github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/ec2authenticator"
	"github.com/grailbio/reflow/ec2cluster/volume"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/local"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool/server"
	"github.com/grailbio/reflow/repository/blobrepo"
	repositoryhttp "github.com/grailbio/reflow/repository/http"
	"github.com/grailbio/reflow/rest"
	"golang.org/x/net/http2"
)

// maxConcurrentStreams is the number of concurrent http/2 streams we
// support.
const maxConcurrentStreams = 20000

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
	msvc := ec2metadata.New(sess)
	s.ec2Identity, err = msvc.GetInstanceIdentityDocument()
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

// setupWatcher sets up a volume watcher for the given path.
func (s *Server) setupWatcher(ctx context.Context, sess *session.Session, path string, vw infra2.VolumeWatcher) error {
	if vw == (infra2.VolumeWatcher{}) {
		log.Print("volume watcher not configured, skipping\n")
		return nil
	}
	logger := log.Std.Tee(nil, fmt.Sprintf("watcher %s: ", path))
	v, err := volume.NewEbsLvmVolume(sess, logger, path)
	if err != nil {
		return fmt.Errorf("create volume for path %s: %v", path, err)
	}
	w, err := volume.NewWatcher(ctx, logger, v, vw)
	if err != nil {
		return err
	}
	go w.Watch(ctx)
	return nil
}

// ListenAndServe serves the Reflowlet server on the configured address.
func (s *Server) ListenAndServe() error {
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
	err = s.Config.Instance(&rc)
	if err != nil {
		return err
	}
	var sess *session.Session
	err = s.Config.Instance(&sess)
	if err != nil {
		return err
	}
	var tlsa infratls.Certs
	err = s.Config.Instance(&tlsa)
	if err != nil {
		return err
	}
	clientConfig, serverConfig, err := tlsa.HTTPS()
	if err != nil {
		return err
	}
	var creds *credentials.Credentials
	err = s.Config.Instance(&creds)
	if err != nil {
		return err
	}
	var tool *infraaws.AWSTool
	err = s.Config.Instance(&tool)
	if err != nil {
		return err
	}
	var (
		dockerconfig *infra2.DockerConfig
		hardMemLimit bool
	)
	err = s.Config.Instance(&dockerconfig)
	if err != nil {
		return err
	} else if dockerconfig.Value() == "hard" {
		hardMemLimit = true
	}
	if err := s.setTags(sess); err != nil {
		return fmt.Errorf("set tags: %v", err)
	}

	// Default HTTPS and s3 clients for repository dialers.
	// TODO(marius): handle this more elegantly, perhaps by
	// avoiding global registration altogether.
	blobrepo.Register("s3", s3blob.New(sess))
	transport := &http.Transport{TLSClientConfig: clientConfig}
	http2.ConfigureTransport(transport)
	repositoryhttp.HTTPClient = &http.Client{Transport: transport}

	p := &local.Pool{
		Client:        client,
		Dir:           s.Dir,
		Prefix:        s.Prefix,
		Authenticator: ec2authenticator.New(sess),
		AWSImage:      string(*tool),
		AWSCreds:      creds,
		Blob: blob.Mux{
			"s3": s3blob.New(sess),
		},
		Log:          log.Std.Tee(nil, "executor: "),
		HardMemLimit: hardMemLimit,
	}
	if err := p.Start(); err != nil {
		return err
	}
	if s.EC2Cluster {
		ec2Log := log.Std.Tee(nil, fmt.Sprintf("reflowlet (%s) ", s.ec2Identity.InstanceType))
		ec2Log.Printf("started")
		ctx, cancel := context.WithCancel(context.Background())
		if err := s.setupWatcher(ctx, sess, filepath.Join(s.Prefix, s.Dir), rc.VolumeWatcher); err != nil {
			log.Fatal(err)
		}
		go s.spotNoticeWatcher(ctx)
		go func() {
			const period = time.Minute
			// Always give the instance an expiry period to receive work,
			// then check periodically if the instance has been idle for more
			// than the expiry time.
			time.Sleep(rc.MaxIdleDuration)
			for {
				if stopped, tte := p.StopIfIdleFor(rc.MaxIdleDuration); stopped {
					ec2Log.Printf("idle for %s; shutting down", rc.MaxIdleDuration)
					cancel()
					// Exit normally
					os.Exit(0)
				} else {
					tot, free, freePct := p.Resources(), p.Available(), 1.0
					freeFrac := free.Div(tot)
					for _, k := range []string{"mem", "cpu"} {
						if v := freeFrac[k]; v < freePct {
							freePct = v
						}
					}
					busyPct := 100.0 * (1.0 - freePct)
					ec2Log.Printf("%.2f%% busy for %s; resources total %s free %s", busyPct, tte, tot, free)
				}
				time.Sleep(period)
			}
		}()
	}

	var httpLog *log.Logger
	if s.HTTPDebug {
		httpLog = log.Std.Tee(nil, "http: ")
		httpLog.Level = log.DebugLevel
		log.Std.Level = log.DebugLevel
	}

	http.Handle("/", rest.Handler(server.NewNode(p), httpLog))
	// Create a servlet node for this reflowlet's config.
	cfgNode, err := newConfigNode(s.Config)
	if err != nil {
		return fmt.Errorf("read config: %v", err)
	}
	http.Handle("/v1/config", rest.DoFuncHandler(cfgNode, httpLog))
	var repo reflow.Repository
	err = s.Config.Instance(&repo)
	if err != nil {
		return fmt.Errorf("repo: %v", err)
	}
	s.server = &http.Server{Addr: s.Addr}
	if s.Insecure {
		return s.server.ListenAndServe()
	}
	serverConfig.ClientAuth = tls.RequireAndVerifyClientCert
	s.server.TLSConfig = serverConfig
	http2.ConfigureServer(s.server, &http2.Server{
		MaxConcurrentStreams: maxConcurrentStreams,
	})
	go logMemStats(context.Background(), log.Std.Tee(nil, "memstats: "), rc.LogMemStatsDuration)
	return s.server.ListenAndServeTLS("", "")
}

func (s *Server) Shutdown() {
	s.server.Shutdown(context.Background())
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

// logMemStats logs runtime memstats to the given logger every d duration.
func logMemStats(ctx context.Context, log *log.Logger, d time.Duration) {
	iter := time.NewTicker(d)
	for {
		memStats := new(runtime.MemStats)
		runtime.ReadMemStats(memStats)
		log.Printf("Heap %s, Sys %s", data.Size(memStats.HeapAlloc), data.Size(memStats.Sys))
		select {
		case <-ctx.Done():
			return
		case <-iter.C:
		}
	}
}
