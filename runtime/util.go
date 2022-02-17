package runtime

import (
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/infra"
	"github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/errors"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/repository/blobrepo"
	repositoryhttp "github.com/grailbio/reflow/repository/http"
	"github.com/grailbio/reflow/runner"
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
func PredictorConfig(cfg infra.Config) (*infra2.PredictorConfig, error) {
	var (
		predConfig *infra2.PredictorConfig
		sess       *session.Session
		tdb        taskdb.TaskDB
	)
	if err := cfg.Instance(&tdb); err != nil {
		return nil, errors.E("predictor config: no taskdb", err)
	}
	if err := cfg.Instance(&predConfig); err != nil {
		return nil, errors.E("predictor config: no predconfig", err)
	}
	if err := cfg.Instance(&sess); err != nil || sess == nil {
		return nil, errors.E("predictor config: no session", err)
	}
	return predConfig, validatePredictorConfig(sess, tdb, predConfig)
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
		if md := ec2metadata.New(sess, &aws.Config{MaxRetries: aws.Int(3)}); !md.Available() {
			return fmt.Errorf("not running on ec2 instance (and nonEc2Ok is not true)")
		}
	}
	return nil
}
