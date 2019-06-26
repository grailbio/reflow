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
	"github.com/grailbio/base/iofmt"
	"github.com/grailbio/base/vcontext"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/testutil"
)

const (
	reflowModule = "testdata/cache_assertion.rf"
	testBucket   = "reflow-unittest"
	timeFormat   = "2006_01_02_15_04"
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
		{mux: mux, name: "EvalTopDown", runArgs: []string{"-eval", "topdown"}, local: *local},
		{mux: mux, name: "EvalBottomUp", runArgs: []string{"-eval", "bottomup"}, local: *local},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.local {
				// Run tests in parallel when running locally.
				t.Parallel()
			}
			tc.init()
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
		{mux: mux, name: "SchedTopDown", runArgs: []string{"-sched", "-eval", "topdown"}, local: false},
		{mux: mux, name: "SchedBottomUp", runArgs: []string{"-sched", "-eval", "bottomup"}, local: false},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tc.init()
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

	wants := tc.assertContents(t, map[string]string{"/output/a": tc.sampleA, "/output/b": tc.sampleB, "/all/out": tc.sampleA + tc.sampleB})
	// Write the same contents again
	if err := tc.writeFiles(map[string]string{"/input/a": tc.sampleA, "/input/b": tc.sampleB}); err != nil {
		t.Fatal(err)
	}
	// Should hit cached results.
	tc.runReflow(t, "eval_touched_never", append([]string{"-assert", "never"}, tc.runArgs...), in, out, allout)
	gots := tc.assertContents(t, map[string]string{"/output/a": tc.sampleA, "/output/b": tc.sampleB, "/all/out": tc.sampleA + tc.sampleB})
	assertSame(t, gots, wants)

	// With assertions, should copy all the files (though contents didn't change) in to out and concat into allout
	tc.runReflow(t, "eval_touched_exact", append([]string{"-assert", "exact"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": tc.sampleA, "/output/b": tc.sampleB, "/all/out": tc.sampleA + tc.sampleB})
	assertDifferent(t, gots, wants)

	// Should hit cached results.
	wants = gots
	tc.runReflow(t, "eval_touched_exact_again", append([]string{"-assert", "exact"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": tc.sampleA, "/output/b": tc.sampleB, "/all/out": tc.sampleA + tc.sampleB})
	assertSame(t, gots, wants)

	// Change contents of one of the inputs
	if err := tc.writeFiles(map[string]string{"/input/a": tc.sampleC}); err != nil {
		t.Fatal(err)
	}
	// Should hit cached results.
	tc.runReflow(t, "eval_changed_never", append([]string{"-assert", "never"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": tc.sampleA, "/output/b": tc.sampleB, "/all/out": tc.sampleA + tc.sampleB})
	assertSame(t, gots, wants)

	// With assertions, should copy only the changed file from in to out and concat into allout
	tc.runReflow(t, "eval_changed_exact", append([]string{"-assert", "exact"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": tc.sampleC, "/all/out": tc.sampleC + tc.sampleB})
	assertDifferent(t, gots, wants)
	gots = tc.assertContents(t, map[string]string{"/output/b": tc.sampleB})
	assertSame(t, gots, wants)
}

// schedOnlyScenarios runs scenarios where the files do change but for -sched mode.
// Since scheduler uses snapshotter, changes are picked up even without assertions.
func schedOnlyScenarios(t *testing.T, tc testConfig) {
	// Setup paths and input files.
	in, out, allout := tc.basePath()+"/input/", tc.basePath()+"/output/", tc.basePath()+"/all/out"
	wants := tc.assertContents(t, map[string]string{"/output/a": tc.sampleA, "/output/b": tc.sampleB, "/all/out": tc.sampleA + tc.sampleB})

	// Write the same contents again
	if err := tc.writeFiles(map[string]string{"/input/a": tc.sampleA, "/input/b": tc.sampleB}); err != nil {
		t.Fatal(err)
	}
	// Since scheduler uses snapshotter, changes are picked up even without assertions.
	tc.runReflow(t, "sched_touched_never", append([]string{"-assert", "never"}, tc.runArgs...), in, out, allout)
	gots := tc.assertContents(t, map[string]string{"/output/a": tc.sampleA, "/output/b": tc.sampleB, "/all/out": tc.sampleA + tc.sampleB})
	assertDifferent(t, gots, wants)

	wants = gots
	tc.runReflow(t, "sched_touched_exact", append([]string{"-assert", "exact"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": tc.sampleA, "/output/b": tc.sampleB, "/all/out": tc.sampleA + tc.sampleB})
	assertSame(t, gots, wants)

	// Change contents of one of the inputs
	if err := tc.writeFiles(map[string]string{"/input/a": tc.sampleC}); err != nil {
		t.Fatal(err)
	}
	// Since scheduler uses snapshotter, changes are picked up even without assertions.
	tc.runReflow(t, "sched_changed_never", append([]string{"-assert", "never"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": tc.sampleC, "/all/out": tc.sampleC + tc.sampleB})
	assertDifferent(t, gots, wants)
	gots = tc.assertContents(t, map[string]string{"/output/b": tc.sampleB})
	assertSame(t, gots, wants)

	// Should hit cached results.
	wants = tc.assertContents(t, map[string]string{"/output/a": tc.sampleC, "/output/b": tc.sampleB, "/all/out": tc.sampleC + tc.sampleB})
	tc.runReflow(t, "sched_changed_exact", append([]string{"-assert", "exact"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": tc.sampleC, "/output/b": tc.sampleB, "/all/out": tc.sampleC + tc.sampleB})
	assertSame(t, gots, wants)
}

// commonScenarios runs scenarios where the files don't change.
func commonScenarios(t *testing.T, tc testConfig) {
	// Setup paths and input files.
	in, out, allout := tc.basePath()+"/input/", tc.basePath()+"/output/", tc.basePath()+"/all/out"
	if err := tc.writeFiles(map[string]string{"/input/a": tc.sampleA, "/input/b": tc.sampleB}); err != nil {
		t.Fatal(err)
	}
	// Should copy all files from in to out and concat into allout
	tc.runReflow(t, "common_first_run", append([]string{"-assert", "never"}, tc.runArgs...), in, out, allout)
	wants := tc.assertContents(t, map[string]string{"/output/a": tc.sampleA, "/output/b": tc.sampleB, "/all/out": tc.sampleA + tc.sampleB})

	// Should hit cached results.
	tc.runReflow(t, "common_never", append([]string{"-assert", "never"}, tc.runArgs...), in, out, allout)
	gots := tc.assertContents(t, map[string]string{"/output/a": tc.sampleA, "/output/b": tc.sampleB, "/all/out": tc.sampleA + tc.sampleB})
	assertSame(t, gots, wants)

	// With assertions, should *still* hit cached results.
	tc.runReflow(t, "common_exact", append([]string{"-assert", "exact"}, tc.runArgs...), in, out, allout)
	gots = tc.assertContents(t, map[string]string{"/output/a": tc.sampleA, "/output/b": tc.sampleB, "/all/out": tc.sampleA + tc.sampleB})
	assertSame(t, gots, wants)
}

type testConfig struct {
	mux     blob.Mux
	name    string
	runArgs []string
	local   bool

	sampleA, sampleB, sampleC string
}

func (tc *testConfig) init() {
	tc.sampleA = "Sample a contents:" + tc.dirSuffix()
	tc.sampleB = "Contents of b sample:" + tc.dirSuffix()
	tc.sampleC = "Changed contents of sample a:" + tc.dirSuffix()
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

func (tc testConfig) writeFiles(dir map[string]string) error {
	base := tc.basePath()
	// We add a delay before writing to S3 to ensure that metadata will always differ
	// (ie, even if contents are same, last-modified would be different)
	time.Sleep(time.Second)
	for name, contents := range dir {
		path := base + name
		if err := tc.mux.Put(context.Background(), path, int64(len(contents)), strings.NewReader(contents)); err != nil {
			return fmt.Errorf("write %s: %v", path, err)
		}
	}
	// We add a delay after writing to S3 to wait for S3's eventual consistency since
	// overwriting an existing key with new data can sometimes yield the old result.
	// TODO(swami):  Perhaps Get after Put to ensure contents/metadata changed
	time.Sleep(time.Second)
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

func (tc testConfig) runReflow(t *testing.T, logPrefix string, runArgs []string, in, out, allout string) {
	if tc.local {
		dir, cleanup := testutil.TempDir(t, "/tmp", "")
		defer cleanup()
		runArgs = append(runArgs, "-local", "-localdir", dir)
	}
	args := []string{
		"-labels", fmt.Sprintf("kv,cache_assertion_test=%s_%s", time.Now().Format(timeFormat), tc.dirSuffix()),
	}
	if *debug {
		args = append(args, "-log", "debug")
	}
	args = append(args, "run")
	args = append(args, runArgs...)
	args = append(args, reflowModule, "-in", in, "-out", out, "-allout", allout)
	cmd := exec.Command(*binary, args...)
	cmd.Stderr = iofmt.PrefixWriter(os.Stderr, tc.name+":"+logPrefix+": \t")
	cmd.Stdout = iofmt.PrefixWriter(os.Stdout, tc.name+":"+logPrefix+": \t")
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
	b := make([]byte, 32)
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
