// Package ec2config defines and registers configuration providers
// using Amazon's EC2 metadata service. It is imported for its side
// effects. Note that ec2config does not marshal its key material.
// In this configuration, it's expected that reflowlets can also derive
// their credentials directly from the ec2 metdata service.
package ec2metadataconfig

import (
	awspkg "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/reflow/config"
)

func init() {
	config.Register(config.AWS, "ec2metadata", "", "use EC2/IAM role credentials",
		func(cfg config.Config, arg string) (config.Config, error) {
			return &aws{cfg}, nil
		},
	)
}

type aws struct {
	config.Config
}

func (*aws) AWS() (*session.Session, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	metaClient := ec2metadata.New(sess)
	provider := &ec2rolecreds.EC2RoleProvider{Client: metaClient}
	creds := credentials.NewCredentials(provider)
	doc, err := metaClient.GetInstanceIdentityDocument()
	if err != nil {
		return nil, err
	}
	sess, err = session.NewSession(&awspkg.Config{
		Credentials: creds,
		// TODO(marius): what if region and (*Config).Region conflict?
		Region: awspkg.String(doc.Region),
	})
	if err != nil {
		return nil, err
	}
	return sess, err
}

func (*aws) AWSCreds() (*credentials.Credentials, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	metaClient := ec2metadata.New(sess)
	provider := &ec2rolecreds.EC2RoleProvider{Client: metaClient}
	creds := credentials.NewCredentials(provider)
	return creds, nil
}
