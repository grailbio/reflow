package runtime

import (
	"context"
	"fmt"
	golog "log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/status"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/testblob"
	"github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/taskdb/inmemorytaskdb"
)

func TestRunnerImplClosesStateFile(t *testing.T) {
	ctx := context.Background()
	// Upload input file.
	url := "test://inputs/foo.txt"
	content := "foo"
	mux := blob.Mux{"test": testblob.New("test")}
	if err := mux.Put(ctx, url, int64(len(content)), strings.NewReader(content), ""); err != nil {
		t.Fatal(err)
	}
	// Set up reflow program and runner.
	path := filepath.Join(t.TempDir(), "main.rf")
	prog := []byte(fmt.Sprintf("val Main = len(dir(%q))", url))
	if err := os.WriteFile(path, prog, 0744); err != nil {
		t.Fatal(err)
	}
	rc := RunConfig{
		Program: path,
		RunFlags: RunFlags{
			CommonRunFlags: CommonRunFlags{
				EvalStrategy: "topdown",
				Assert:       "never",
			},
			// Allow cache writes to complete. Exceeding this timeout will not cause the test to fail.
			BackgroundTimeout: 100 * time.Millisecond,
		},
	}
	tdb := inmemorytaskdb.NewInmemoryTaskDB(fmt.Sprintf("tdb_scheduler_test_%d", rand.Int63()),
		fmt.Sprintf("taskrepo_scheduler_test_%d", rand.Int63()))
	rr := runnerImpl{
		RunID:     taskdb.NewRunID(),
		Log:       log.New(golog.New(os.Stdout, "", golog.LstdFlags), log.InfoLevel),
		RunConfig: rc,
		scheduler: &sched.Scheduler{
			Mux:    mux,
			TaskDB: tdb,
		},
		sess: &session.Session{
			Config: &aws.Config{
				Region: aws.String("fakeregion"),
			},
		},
		cache:  new(infra.CacheProvider),
		status: new(status.Status),
	}
	// Run program and assert that the state file is closed.
	if _, err := rr.Go(ctx); err != nil {
		t.Fatal(err)
	}
	stateFilename, err := rr.Runbase()
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	switch goruntime.GOOS {
	// OS-specific behavior is needed because "lsof" is not installed by default on all Linux distributions.
	case "linux":
		fdPath := fmt.Sprintf("/proc/%v/fd/", os.Getpid())
		dir, err := os.ReadDir(fdPath)
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range dir {
			// TODO(pfialho): can we skip files whose symbolic link evaluation will not return a file path
			//  (e.g. pipe:[XXXXXXXXXX]) instead of ignoring the error?
			f, err := filepath.EvalSymlinks(filepath.Join(fdPath, file.Name()))
			if err != nil {
				continue
			}
			if strings.Contains(f, stateFilename) {
				found = true
				break
			}
		}
	case "darwin":
		out, err := exec.Command("lsof", "-p", strconv.Itoa(os.Getpid())).Output()
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(out), "\n")
		for _, l := range lines {
			if strings.Contains(l, stateFilename) {
				found = true
				break
			}
		}
	default:
		panic(fmt.Sprintf("OS %q is not supported", goruntime.GOOS))
	}
	if found {
		t.Fatalf("state file %s was not closed", stateFilename)
	}
}
