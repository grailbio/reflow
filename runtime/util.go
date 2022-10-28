package runtime

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/infra"
	"github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/errors"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/repository/blobrepo"
	repositoryhttp "github.com/grailbio/reflow/repository/http"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/syntax"
	"github.com/grailbio/reflow/taskdb"
	"golang.org/x/net/http2"
)

// ClusterInstance returns a configured cluster and sets up repository
// credentials so that remote repositories can be dialed.
func ClusterInstance(config infra.Config) (runner.Cluster, error) {
	var cluster runner.Cluster
	err := config.Instance(&cluster)
	if err != nil {
		return nil, err
	}
	if ec, ok := cluster.(*ec2cluster.Cluster); ok {
		ec.Configuration = config
		ec.ExportStats()
		if err = ec.Verify(); err != nil {
			return nil, err
		}
	} else {
		return cluster, nil
	}
	var sess *session.Session
	err = config.Instance(&sess)
	if err != nil {
		return nil, err
	}
	// TODO(marius): handle this more elegantly, perhaps by avoiding
	// such global registration altogether. The current way of doing this
	// also ties the binary to specific implementations (e.g., s3), which
	// should be avoided.
	blobrepo.Register("s3", s3blob.New(sess))
	// TODO(swami): Why is this needed and can we avoid this?
	repositoryhttp.HTTPClient, err = HttpClient(config)
	if err != nil {
		return nil, err
	}
	return cluster, nil
}

func HttpClient(config infra.Config) (*http.Client, error) {
	var ca tls.Certs
	err := config.Instance(&ca)
	if err != nil {
		return nil, err
	}
	clientConfig, _, err := ca.HTTPS()
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{TLSClientConfig: clientConfig}
	if err := http2.ConfigureTransport(transport); err != nil {
		return nil, err
	}
	return &http.Client{Transport: transport}, nil
}

// PredictorConfig returns a PredictorConfig (possibly nil) and an error (if any).
func PredictorConfig(cfg infra.Config, validate bool) (*infra2.PredictorConfig, error) {
	var (
		predConfig *infra2.PredictorConfig
		sess       *session.Session
		tdb        taskdb.TaskDB
	)
	// Predictor config is optional.
	if err := cfg.Instance(&predConfig); err != nil {
		return nil, nil
	}
	if err := cfg.Instance(&tdb); err != nil {
		return nil, errors.E("predictor config: no taskdb", err)
	}
	if err := cfg.Instance(&sess); err != nil {
		return nil, errors.E("predictor config: session", err)
	}
	if validate {
		return predConfig, validatePredictorConfig(sess, tdb, predConfig)
	}
	return predConfig, nil
}

// validatePredictorConfig validates if the Predictor can be used by reflow.
// The Predictor can only be used if the following conditions are true:
// 1. A taskdb is present in the provided config, for querying tasks.
//    (and the taskdb must return a valid `Repository()`)
// 2. Reflow is being run from an ec2 instance OR the Predictor config (using NonEC2Ok)
//    gives explicit permission to run the Predictor on non-ec2-instance machines.
//    This is because the Predictor is network-intensive and its performance will be hampered by poor network.
func validatePredictorConfig(sess *session.Session, tdb taskdb.TaskDB, predConfig *infra2.PredictorConfig) error {
	if tdb == nil {
		return fmt.Errorf("validatePredictorConfig: no taskdb")
	}
	if tdb.Repository() == nil {
		return fmt.Errorf("validatePredictorConfig: no repo")
	}
	if predConfig == nil {
		return fmt.Errorf("validatePredictorConfig: no predconfig")
	}
	if !predConfig.NonEC2Ok {
		if sess == nil {
			return fmt.Errorf("validatePredictorConfig: no session")
		}
		if md := ec2metadata.New(sess, &aws.Config{MaxRetries: aws.Int(3)}); !md.Available() {
			return fmt.Errorf("not running on ec2 instance (and nonEc2Ok is not true)")
		}
	}
	return nil
}

// asserter returns a reflow.Assert based on the given name.
func asserter(name string) (reflow.Assert, error) {
	switch name {
	case "never":
		return reflow.AssertNever, nil
	case "exact":
		return reflow.AssertExact, nil
	default:
		return nil, fmt.Errorf("unknown Assert policy %s", name)
	}
}

func getBundle(file string) (io.ReadCloser, digest.Digest, error) {
	dw := reflow.Digester.NewWriter()
	f, err := os.Open(file)
	if err != nil {
		return nil, digest.Digest{}, err
	}
	if _, err = io.Copy(dw, f); err != nil {
		return nil, digest.Digest{}, err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, digest.Digest{}, err
	}
	return f, dw.Digest(), nil
}

func makeBundle(b *syntax.Bundle) (io.ReadCloser, digest.Digest, string, error) {
	dw := reflow.Digester.NewWriter()
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, digest.Digest{}, "", err
	}
	if err = b.WriteTo(io.MultiWriter(dw, f)); err != nil {
		return nil, digest.Digest{}, "", err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, digest.Digest{}, "", err
	}
	return f, dw.Digest(), f.Name(), nil
}
