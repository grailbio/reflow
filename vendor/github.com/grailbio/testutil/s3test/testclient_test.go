package s3test_test

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/testutil/s3test"
)

const (
	testBucket    = "test-bucket"
	contentLength = 100
)

// populateS3 writes 20 keys to an S3 client using SetFile
// A manifest of written keys is returned
// File contents are contentLength bytes long and are mocked
// so that no files are written.
func populateS3(t *testing.T, client *s3test.Client) []string {

	manifest := make([]string, 0, 100)

	content := make([]byte, contentLength)
	for i := 0; i < len(content); i++ {
		content[i] = byte('a' + byte(i%14))
	}

	h := sha256.New()

	hashValue := h.Sum(content)

	bucketKeyParts := []string{"who", "what", "when", "where", "may", "must", "might", "merrily"}

	for _, part := range bucketKeyParts {
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("knights/%s/say/ni%d", part, i)
			client.SetFile(key, content, string(hashValue))
			manifest = append(manifest, key)
			t.Logf("Populating %s", key)
		}

	}
	// now add some files
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("knights/sword%d", i)
		client.SetFile(key, content, string(hashValue))
		manifest = append(manifest, key)
		t.Logf("Populating %s", key)
	}
	return manifest
}

// testClientListObjects wraps a call to listObjectsV2 with expected values
// In particular, we expect a certain number of objects and a certain number of
// CommonPrefixes, based on the semantics of ListObjectsV2 from S3
func testClientListOjbects(t *testing.T, client *s3test.Client, manifest []string,
	prefix string, delim string, expObjects int, expPrefixes int) {

	// test a lookup  without
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(testBucket),
		MaxKeys: aws.Int64(500),
		Prefix:  aws.String(prefix),
	}
	if delim != "" {
		input.Delimiter = aws.String(delim)
	}

	result, err := client.ListObjectsV2(input)
	if err != nil {
		t.Errorf("Cannot list objects in test Bucket: %v", err)
	}

	if len(result.Contents) != expObjects {
		t.Errorf("Expected %d objects, got %d", expObjects, len(result.Contents))
	}

	if expPrefixes == 0 {
		if result.CommonPrefixes != nil && len(result.CommonPrefixes) > 0 {
			for _, val := range result.CommonPrefixes {
				t.Logf("Prefix: %s", val)
			}
			t.Errorf("expected no prefixes, but we have %d instead: ",
				len(result.Contents))
		}
	} else {

		if result.CommonPrefixes == nil || len(result.CommonPrefixes) < expPrefixes {
			if result.CommonPrefixes == nil {
				t.Errorf("Expected %d prefixes, but common prefixes was nil", expPrefixes)
			} else {
				for _, val := range result.CommonPrefixes {
					t.Logf("Prefix: %s", val.GoString())
				}
				t.Errorf("Expected %d prefixes, but we have %d instead: ", expPrefixes,
					len(result.CommonPrefixes))
			}
		}
	}

}

// TestClientListObjectsV2 sets up the mocked data and calls other tests
func TestClientListObjectsV2(t *testing.T) {

	mockS3Client := s3test.NewClient(t, testBucket)
	if mockS3Client == nil {
		t.Error("s3test.NewClient returned a null client")
	}

	manifest := populateS3(t, mockS3Client)
	t.Log("Len(manifest)=", len(manifest))

	// should return everything we put in, since every line starts with knights
	t.Run("all-bucket", func(t *testing.T) {
		testClientListOjbects(t, mockS3Client, manifest, "knights/", "", len(manifest), 0)
	})

	// only 1/2 of the objects start with knights/w
	t.Run("half-match", func(t *testing.T) {
		testClientListOjbects(t, mockS3Client, manifest, "knights/w", "", (len(manifest)-3)/2, 0)
	})

	// with a delimiter, there are knights/who, knights/what, knights/when and knights/where
	t.Run("four directories", func(t *testing.T) {
		testClientListOjbects(t, mockS3Client, manifest, "knights/", "/", 3, 4)
	})

	// with a delimiter, there are knights/who, knights/what, knights/when and knights/where
	t.Run("no trailing slash", func(t *testing.T) {
		testClientListOjbects(t, mockS3Client, manifest, "knights", "/", 0, 1)
	})

	// corner case -- one group of "/"
	t.Run("entire-prefix", func(t *testing.T) {
		testClientListOjbects(t, mockS3Client, manifest, "knights/what", "/", 0, 1)
	})
	// corner case -- prefix matches nothing
	t.Run("nomatch", func(t *testing.T) {
		testClientListOjbects(t, mockS3Client, manifest, "nomatch", "", 0, 0)
	})

}
