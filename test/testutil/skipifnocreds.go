package testutil

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

// SkipIfNoCreds allows a test to be skipped if no credentials are found.
// Caution: Renaming/removing this will prevent execution of tests (with credentials) which call this.
func SkipIfNoCreds(t *testing.T) {
	t.Helper()
	provider := &credentials.ChainProvider{
		VerboseErrors: true,
		Providers: []credentials.Provider{
			&credentials.EnvProvider{},
			&credentials.SharedCredentialsProvider{},
		},
	}
	_, err := provider.Retrieve()
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NoCredentialProviders" {
			t.Skip("no credentials in environment; skipping")
		}
		t.Fatal(err)
	}
}
