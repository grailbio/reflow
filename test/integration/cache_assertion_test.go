package cache_assertion_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/cloud/awssession"
	"github.com/grailbio/base/vcontext"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/testutil"
)

const (
	sampleA      = "Sample a contents"
	sampleB      = "Contents of b sample"
	sampleC      = "Changed contents of sample a"
	reflowModule = "testdata/cache_assertion.rf"
	testBucket   = "reflow-unittest"
)

var (
	ticket  = flag.String("cache_assertion_test.ticket", "tickets/eng/dev/aws", "path to AWS ticket")
	region  = flag.String("cache_assertion_test.region", "us-west-2", "aws region")
	binary  = flag.String("cache_assertion_test.binary", "", "reflow binary to use for the test")
	local   = flag.Bool("cache_assertion_test.local", true, "whether to run the tests locally")
	debug   = flag.Bool("cache_assertion_test.debug", false, "whether to run the reflow runs with debug")
	testDir string
)

// TestCacheAssertions runs a large integration test for cache assertions.
func TestCacheAssertions(t *testing.T) {
	if *binary == "" {
		fmt.Fprintln(os.Stderr, `usage: go test -cache_assertion_test.binary </path/to/binary> [-cache_assertion_test.local true|false] [-cache_assertion_test.debug true|false].`)
		t.Fatalf("-cache_assertion_test.binary must be specified for test")
	}
	rand.Seed(time.Now().UnixNano())
	testDir = randString()

	t.Logf("Running tests with binary: %s testDir: %s\n", *binary, testDir)

	// TODO(swami): Use infra package for provisioning once its ready to use.
	awsSession, err := getSession(*region)
	if err != nil {
		log.Fatal(err)
	}
	mux := blob.Mux{"s3": s3blob.New(awsSession)}

	for _, tc := range []testConfig{
		{mux, "EvalTopDown", []string{"-eval", "topdown"}, *local},
		{mux, "EvalBottomUp", []string{"-eval", "bottomup"}, *local},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.local {
				// Run tests in parallel when running locally.
				t.Parallel()
			}
			defer tc.cleanup(t)
			commonScenarios(t, tc)
			evalOnlyScenarios(t, tc)
		})
	}
	// Skip -sched tests in local mode.
	// TODO(swami): Enable once -sched can run with -local.
	if *local {
		return
	}
	for _, tc := range []testConfig{
		{mux, "SchedTopDown", []string{"-sched", "-eval", "topdown"}, false},
		{mux, "SchedBottomUp", []string{"-sched", "-eval", "bottomup"}, false}} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			defer tc.cleanup(t)
			commonScenarios(t, tc)
			schedOnlyScenarios(t, tc)
		})
	}
}

// evalOnlyScenarios runs scenarios where the files do change.
func evalOnlyScenarios(t *testing.T, tc testConfig) {
	// Setup paths and input files.
	in, out, allout := tc.basePath()+"/input/", tc.basePath()+"/output/", tc.basePath()+"/all/out"

	wants := tc.assertContents(t, map[string]string{"/output/a": sampleA, "/output/b": sampleB, "/all/out": sampleA + sampleB})
	// Write the same contents again
	if err := tc.createInput(map[string]string{"/input/a": sampleA, "/input/b": sampleB}); err != nil {
		t.Fatal(err)
	}
	// Should hit cached results.
	tc.runReflow(t, append([]string{"-assert", "never"}, tc.runArgs...), in, out, allout)
	gots := tc.assertContents(t, map[string]string{"/output/a": sampleA, "/output/b": sampleB, "/all/out": sampleA + sampleB})
	assertSame(t, gots, wants)

	// With assertions, should copy all the files (though contents didn't change) in to out and concat into allout
	tc.runReflow(t, append([]string{"-assert", "exact"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": sampleA, "/output/b": sampleB, "/all/out": sampleA + sampleB})
	assertDifferent(t, gots, wants)

	// Should hit cached results.
	wants = gots
	tc.runReflow(t, append([]string{"-assert", "exact"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": sampleA, "/output/b": sampleB, "/all/out": sampleA + sampleB})
	assertSame(t, gots, wants)

	// Change contents of one of the inputs
	if err := tc.createInput(map[string]string{"/input/a": sampleC}); err != nil {
		t.Fatal(err)
	}
	// Should hit cached results.
	tc.runReflow(t, append([]string{"-assert", "never"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": sampleA, "/output/b": sampleB, "/all/out": sampleA + sampleB})
	assertSame(t, gots, wants)

	// With assertions, should copy only the changed file from in to out and concat into allout
	tc.runReflow(t, append([]string{"-assert", "exact"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": sampleC, "/all/out": sampleC + sampleB})
	assertDifferent(t, gots, wants)
	gots = tc.assertContents(t, map[string]string{"/output/b": sampleB})
	assertSame(t, gots, wants)
}

// schedOnlyScenarios runs scenarios where the files do change but for -sched mode.
// Since scheduler uses snapshotter, changes are picked up even without assertions.
func schedOnlyScenarios(t *testing.T, tc testConfig) {
	// Setup paths and input files.
	in, out, allout := tc.basePath()+"/input/", tc.basePath()+"/output/", tc.basePath()+"/all/out"
	wants := tc.assertContents(t, map[string]string{"/output/a": sampleA, "/output/b": sampleB, "/all/out": sampleA + sampleB})

	// Write the same contents again
	if err := tc.createInput(map[string]string{"/input/a": sampleA, "/input/b": sampleB}); err != nil {
		t.Fatal(err)
	}
	// Since scheduler uses snapshotter, changes are picked up even without assertions.
	tc.runReflow(t, append([]string{"-assert", "never"}, tc.runArgs...), in, out, allout)
	gots := tc.assertContents(t, map[string]string{"/output/a": sampleA, "/output/b": sampleB, "/all/out": sampleA + sampleB})
	assertDifferent(t, gots, wants)

	wants = gots
	tc.runReflow(t, append([]string{"-assert", "exact"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": sampleA, "/output/b": sampleB, "/all/out": sampleA + sampleB})
	assertSame(t, gots, wants)

	// Change contents of one of the inputs
	if err := tc.createInput(map[string]string{"/input/a": sampleC}); err != nil {
		t.Fatal(err)
	}
	// Since scheduler uses snapshotter, changes are picked up even without assertions.
	tc.runReflow(t, append([]string{"-assert", "never"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": sampleC, "/all/out": sampleC + sampleB})
	assertDifferent(t, gots, wants)
	gots = tc.assertContents(t, map[string]string{"/output/b": sampleB})
	assertSame(t, gots, wants)

	// Should hit cached results.
	wants = tc.assertContents(t, map[string]string{"/output/a": sampleC, "/output/b": sampleB, "/all/out": sampleC + sampleB})
	tc.runReflow(t, append([]string{"-assert", "exact"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": sampleC, "/output/b": sampleB, "/all/out": sampleC + sampleB})
	assertSame(t, gots, wants)
}

// commonScenarios runs scenarios where the files don't change.
func commonScenarios(t *testing.T, tc testConfig) {
	// Setup paths and input files.
	in, out, allout := tc.basePath()+"/input/", tc.basePath()+"/output/", tc.basePath()+"/all/out"
	if err := tc.createInput(map[string]string{"/input/a": sampleA, "/input/b": sampleB}); err != nil {
		t.Fatal(err)
	}
	// Should copy all files from in to out and concat into allout
	tc.runReflow(t, append([]string{"-assert", "never"}, tc.runArgs...), in, out, allout)
	wants := tc.assertContents(t, map[string]string{"/output/a": sampleA, "/output/b": sampleB, "/all/out": sampleA + sampleB})

	// Should hit cached results.
	tc.runReflow(t, append([]string{"-assert", "never"}, tc.runArgs...), in, out, allout)
	gots := tc.assertContents(t, map[string]string{"/output/a": sampleA, "/output/b": sampleB, "/all/out": sampleA + sampleB})
	assertSame(t, gots, wants)

	// With assertions, should *still* hit cached results.
	tc.runReflow(t, append([]string{"-assert", "exact"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": sampleA, "/output/b": sampleB, "/all/out": sampleA + sampleB})
	assertSame(t, gots, wants)
}

type testConfig struct {
	mux     blob.Mux
	name    string
	runArgs []string
	local   bool
}

// cleanup removes all in/out dir contents
func (tc testConfig) cleanup(t *testing.T) {
	path := tc.basePath()
	ctx := context.Background()
	bucket, prefix, err := tc.mux.Bucket(ctx, path)
	if err != nil {
		t.Errorf("cleanup %s: %v", path, err)
	}
	scan := bucket.Scan(prefix)
	var keys []string
	for scan.Scan(ctx) {
		keys = append(keys, scan.Key())
	}
	if err = bucket.Delete(ctx, keys...); err != nil {
		t.Errorf("remove keys: %v\n%s\n", err, strings.Join(keys, "\n"))
	}
}

func (tc testConfig) createInput(dir map[string]string) error {
	base := tc.basePath()
	for name, contents := range dir {
		path := base + name
		if err := tc.mux.Put(context.Background(), path, int64(len(contents)), strings.NewReader(contents)); err != nil {
			return fmt.Errorf("write %s: %v", path, err)
		}
	}
	return nil
}

func (tc testConfig) assertContents(t *testing.T, files map[string]string) map[string]fileInfo {
	t.Helper()
	base := tc.basePath()
	infos := make(map[string]fileInfo, len(files))
	for name, contents := range files {
		path := base + name
		rc, file, err := tc.mux.Get(context.Background(), path, "")
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		got, err := ioutil.ReadAll(rc)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		want := []byte(contents)
		if !bytes.Equal(got, want) {
			t.Errorf("content mismatch: %s got:\n%s\nwant:\n%s", path, got, want)
		}
		infos[name] = fileInfo{file.ETag, file.LastModified}
	}
	return infos
}

func (tc testConfig) dirSuffix() string {
	return testDir + "/" + tc.name
}

func (tc testConfig) runReflow(t *testing.T, runArgs []string, in, out, allout string) {
	if tc.local {
		dir, cleanup := testutil.TempDir(t, "/tmp", "")
		defer cleanup()
		runArgs = append(runArgs, "-local", "-localdir", dir)
	}
	args := []string{
		"-labels", fmt.Sprintf("kv,cache_assertion_test=%s", tc.dirSuffix()),
	}
	if *debug {
		args = append(args, "-log", "debug")
	}
	args = append(args, "run")
	args = append(args, runArgs...)
	args = append(args, reflowModule, "-in", in, "-out", out, "-allout", allout)
	cmd := exec.Command(*binary, args...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s %s: %v", cmd.Path, strings.Join(cmd.Args[1:], " "), err)
	}
}

func (tc testConfig) basePath() string {
	return fmt.Sprintf("s3://%s/integration/cache_assertion/%s", testBucket, tc.dirSuffix())
}

type fileInfo struct {
	etag    string
	modTime time.Time
}

// assertSame asserts that for each key in a, the value matches b.
func assertSame(t *testing.T, a, b map[string]fileInfo) {
	t.Helper()
	for k, v := range a {
		if got, want := v, b[k]; got != want {
			t.Errorf("%s: got %v, want %v", k, got, want)
		}
	}
}

// assertDifferent asserts that for each key in a, the value does not matche b.
func assertDifferent(t *testing.T, a, b map[string]fileInfo) {
	t.Helper()
	for k, v := range a {
		if got, want := v, b[k]; got == want {
			t.Errorf("%s: got same %v, want different", k, v)
		}
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randString() string {
	b := make([]byte, 10)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// getSession returns an AWS session.
func getSession(region string) (*session.Session, error) {
	credProvider := &credentials.ChainProvider{
		VerboseErrors: true,
		Providers: []credentials.Provider{
			&credentials.EnvProvider{},
			&credentials.SharedCredentialsProvider{},
			&awssession.Provider{Ctx: vcontext.Background(), Timeout: 10 * time.Second, TicketPath: *ticket},
		},
	}
	// We do a retrieval here to catch NoCredentialProviders errors
	// early on.
	if _, err := credProvider.Retrieve(); err != nil {
		return nil, fmt.Errorf("aws.Session: failed to retrieve AWS credentials: %v", err)
	}
	return session.NewSessionWithOptions(session.Options{
		Config: aws.Config{Region: aws.String(region), Credentials: credentials.NewCredentials(credProvider)},
	})
}
