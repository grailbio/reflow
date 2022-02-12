package runtime

import (
	"net/http"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/infra"
	"github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/repository/blobrepo"
	repositoryhttp "github.com/grailbio/reflow/repository/http"
	"github.com/grailbio/reflow/runner"
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
