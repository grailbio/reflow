// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package testutil

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/expect"
)

// MockTB is a mock implementation of gosh.TB. FailNow and Fatalf will
// set Failed to true. Logf and Fatalf write their log message to Result.
// MockTB is intended for building negatived tests.
type MockTB struct {
	Failed bool
	Result string
}

// FailNow implements TB.
func (m *MockTB) FailNow() {
	m.Failed = true
}

// Logf implements TB.
func (m *MockTB) Logf(format string, args ...interface{}) {
	m.Result = fmt.Sprintf(format, args...)
}

// Fatalf implements TB.
func (m *MockTB) Fatalf(format string, args ...interface{}) {
	m.Failed = true
	m.Result = fmt.Sprintf(format, args...)
}

// Caller returns a string of the form <file>:<line> for the caller
// at the specified depth.
func Caller(depth int) string {
	_, file, line, _ := runtime.Caller(depth + 1)
	return fmt.Sprintf("%s:%d", filepath.Base(file), line)
}

// NoCleanupOnError avoids calling the supplied cleanup function when
// a test has failed or paniced. The Log function is called with args
// when the test has failed and is typically used to log the location
// of the state that would have been removed by the cleanup function.
// Common usage would be:
//
// tempdir, cleanup := testutil.TempDir(t, "", "scandb-state-")
// defer testutil.NoCleanupOnError(t, cleanup, "tempdir:", tempdir)
func NoCleanupOnError(t testing.TB, cleanup func(), args ...interface{}) {
	if t.Failed() {
		if len(args) > 0 {
			t.Log(args...)
		}
		return
	}
	if recover() != nil {
		debug.PrintStack()
		if len(args) > 0 {
			t.Log(args...)
		}
		t.Fail()
		return
	}
	cleanup()
}

// GetFilePath detects if we're running under "bazel test". If so, it builds
// a path to the test data file based on Bazel environment variables.
// Otherwise, it tries to build a path relative to $GRAIL.
// If that fails, it returns the input path unchanged.
//
// relativePath will need to be prefixed with a Bazel workspace designation if
// the paths go across workspaces. Only certain workspaces are recognized when
// running tests for the Go tool.  Add more workspaces as necessary in the map
// below.
func GetFilePath(relativePath string) string {
	if strings.HasPrefix(relativePath, "//") {
		relativePath = relativePath[1:]
	}
	if bazelPath, ok := os.LookupEnv("TEST_SRCDIR"); ok {
		return filepath.Join(bazelPath, os.Getenv("TEST_WORKSPACE"), relativePath)
	}
	if grailPath, ok := os.LookupEnv("GRAIL"); ok {
		return filepath.Join(grailPath, relativePath)
	}
	panic("Unexpected test environment. Should be running with either $GRAIL or in a bazel build space.")
}

// GetTmpDir will retrieve/generate a test-specific directory appropriate
// for writing scratch data. When running under Bazel, Bazel should clean
// up the directory. However, when running under vanilla Go tooling, it will
// not be cleaned up. Thus, it's probably best for a test to clean up
// any test directories itself.
func GetTmpDir() string {
	bazelPath, hasBazelPath := os.LookupEnv("TEST_TMPDIR")
	if hasBazelPath {
		return bazelPath
	}

	tmpPath, err := ioutil.TempDir("/tmp", "go_test_")
	if err != nil {
		panic(err.Error)
	}
	return tmpPath
}

// WriteTmp writes the supplied contents to a temporary file and returns the
// name of that file.
func writeTmp(t testing.TB, contents string) string {
	f, err := ioutil.TempFile("", "WriteTmp-")
	assert.NoError(t, err)
	_, err = f.Write([]byte(contents))
	assert.NoError(t, err)
	return f.Name()
}

// CompareFile compares the supplied contents against the contents of the
// specified file and if they differ calls t.Errorf and displays a diff -u of
// them. If specified the strip function can be used to cleanup the contents to
// be compared to remove things such as dates or other spurious information
// that's not relevant to the comparison.
func CompareFile(t testing.TB, contents string, golden string, strip func(string) string) {
	data, err := ioutil.ReadFile(golden)
	assert.NoError(t, err)
	got, want := contents, string(data)
	if strip != nil {
		got, want = strip(got), strip(want)
	}
	if got != want {
		gf := writeTmp(t, got)
		defer os.Remove(gf) // nolint: errcheck
		cmd := exec.Command("diff", "-u", gf, golden)
		diff, _ := cmd.CombinedOutput()
		t.Logf("got %v", got)
		t.Logf("diff %v %v", gf, golden)
		expect.True(t, false, "Golden: %v, diff: %v", golden, string(diff))
	}
}

// CompareFiles compares 2 files in the same manner as CompareFile.
func CompareFiles(t testing.TB, a, golden string, strip func(string) string) {
	ac, err := ioutil.ReadFile(a)
	assert.NoError(t, err)
	CompareFile(t, string(ac), golden, strip)
}

// IsBazel checks if the current process is started by "bazel test".
func IsBazel() bool {
	return os.Getenv("TEST_TMPDIR") != "" && os.Getenv("RUNFILES_DIR") != ""
}

// GoExecutable returns the Go executable for "path", or builds the executable
// and returns its path. The latter happens when the caller is not running under
// Bazel. "path" must start with "//go/src/grail.com/".  For example,
// "//go/src/grail.com/cmd/bio-metrics/bio-metrics".
func GoExecutable(t testing.TB, path string) string {
	return GoExecutableEnv(t, path, nil)
}

// GoExecutableEnv is like GoExecutable but allows environment variables
// to be specified.
func GoExecutableEnv(t testing.TB, path string, env []string) string {
	re := regexp.MustCompile("^//go/src/(.*/([^/]+))/([^/]+)$")
	match := re.FindStringSubmatch(path)
	if match == nil || match[2] != match[3] {
		t.Fatalf("%v: target must be of format \"//go/src/path/target/target\"",
			path)
	}
	if IsBazel() {
		expandedPath := GetFilePath(path)
		if _, err := os.Stat(expandedPath); err == nil {
			return expandedPath
		}
		pattern := GetFilePath(fmt.Sprintf("//go/src/%s/*/%s", match[1], match[2]))
		paths, err := filepath.Glob(pattern)
		assert.NoError(t, err, "glob %v", pattern)
		assert.EQ(t, len(paths), 1, "Pattern %s must match exactly one executable, but found %v", pattern, paths)
		return paths[0]
	}
	pkg := match[1]
	hashString := func(s string) string {
		b := sha256.Sum256([]byte(s))
		return fmt.Sprintf("%x", b[:16])
	}
	tempDir := fmt.Sprintf("/tmp/go_build/%s", hashString(os.Getenv("GOPATH")+pkg))
	os.MkdirAll(tempDir, 0700) // nolint: errcheck
	xpath := filepath.Join(tempDir, filepath.Base(pkg))
	cmd := exec.Command("go", "build", "-o", xpath, pkg)
	if env != nil {
		cmd.Env = env
	}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build %s: %v\n%s\n", pkg, err, string(output))
	}
	return xpath
}
