package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/grail"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/base/vcontext"
	"github.com/grailbio/testutil/benchmark"
)

// RunConfig is the format of the benchmark config file.
type RunConfig struct {
	// RootDir specifies the root directory used when resolving
	// @file:relativepath.
	RootDir string
	// CacheDir specifies the directory to cache inputs to the application.
	// If empty, the value of -cache-dir flag is used.
	CacheDir string
	// OutputDir specifies the value of "@out" in the commandline spec.  If empty,
	// the value of -output-dir flag is used.
	OutputDir string
	// Targets is the list of benchmark applications to run.
	Targets []RunTarget
}

// RunTarget is a configuration of one benchmark application.
type RunTarget struct {
	// Name is any human-readable label for the application.
	Name string
	// Commandline is the commandline string. Each argument can contain the following special labels:
	//
	// - If an arg is of form "@bazel:target", the target is compiled using bazel
	// and the string is replaced with the resulting executable path. The target
	// must be a *_binary or *_test.
	//
	// - If an arg is of form "@cache:path", the given file is copied under
	// RunConfig.CacheDir using grail-file.
	//
	// - If an arg is of form "@cachedir:path", the given directory is recursively
	// copied under RunConfig.CacheDir using grail-file.
	//
	// - If an arg is of form "@out:path", it is replaced with <RunConfig.OutputDir>/path
	//
	// - If an arg is of form "@file:relpath", it is replaced with <RunConfig.RootDir>/relpath
	Commandline []string
}

// Results is the result of a benchmark run. It is printed as JSON to stdout at
// the end.
type Results struct {
	Results []benchmark.Result
}

type benchmarkRunner struct {
	conf    RunConfig
	tempDir string
}

// readRunConfig reads a json-encoded RunConfig from the given file.
func readRunConfig(ctx context.Context, path, defaultCacheDir, defaultOutputDir, defaultRootDir string) (conf RunConfig, err error) {
	in, err := file.Open(ctx, path)
	if err != nil {
		return conf, err
	}
	defer file.CloseAndReport(ctx, in, &err)
	dec := json.NewDecoder(in.Reader(ctx))
	if err := dec.Decode(&conf); err != nil {
		return conf, err
	}
	if conf.CacheDir == "" {
		conf.CacheDir = defaultCacheDir
	}
	if conf.OutputDir == "" {
		conf.OutputDir = defaultOutputDir
	}
	if conf.RootDir == "" {
		conf.RootDir = defaultRootDir
	}
	return conf, err
}

// workspaceRoot returns the repository root dir.
func workspaceRoot(ctx context.Context) (string, error) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, gitPath, "rev-parse", "--show-toplevel")
	out, err := cmd.Output()
	if err != nil {
		return "", errors.E(err, fmt.Sprintf("%s rev-parse --show-toplevel failed", gitPath))
	}
	return strings.TrimSpace(string(out)), err
}

func jsonString(obj interface{}) string {
	out := bytes.Buffer{}
	enc := json.NewEncoder(&out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(obj); err != nil {
		panic(err)
	}
	return out.String()
}

func newBenchmarkRunner(ctx context.Context, conf RunConfig) (*benchmarkRunner, error) {
	b := &benchmarkRunner{
		conf: conf,
	}
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}
	b.tempDir = tempDir

	err = traverse.Each(len(b.conf.Targets), func(i int) error {
		target := &b.conf.Targets[i]
		return traverse.Each(len(target.Commandline), func(j int) error {
			var err error
			target.Commandline[j], err = b.translateArg(ctx, target.Commandline[j])
			return err
		})
	})
	return b, err
}

func (b *benchmarkRunner) newTempPath() string {
	f, err := ioutil.TempFile(b.tempDir, "")
	if err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	return f.Name()
}

func (b *benchmarkRunner) translateBazelTarget(ctx context.Context, target string) (string, error) {
	bazelPath, err := exec.LookPath("bazel")
	if err != nil {
		return "", errors.E(err, "lookpath bazel")
	}
	// Determine if the target is a *_test or a *_binary.
	out, err := exec.CommandContext(ctx, bazelPath, "query", "--output=label_kind", target).Output()
	if err != nil {
		return "", errors.E(err, "bazel query "+target)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 1 {
		return "", fmt.Errorf("bazel query %s: exactly one target expected, but found %s (%d)", target, lines, len(lines))
	}
	m := regexp.MustCompile(`(\S+)+\s+rule`).FindStringSubmatch(lines[0])
	if m == nil {
		return "", fmt.Errorf("bazel query %s: invalid output %s", target, lines)
	}
	// Generate the shell script that invokes the target.
	targetType := m[1]
	scriptPath := b.newTempPath()
	runArgs := []string{"run", "--script_path=" + scriptPath}
	if strings.HasSuffix(targetType, "_test") {
		runArgs = append(runArgs, "--test_sharding_strategy=disabled")
	}
	runArgs = append(runArgs, target)
	cmd := exec.CommandContext(ctx, bazelPath, runArgs...)
	cmd.Dir, err = workspaceRoot(ctx)
	if err != nil {
		return "", err
	}
	if err := cmd.Run(); err != nil {
		return "", errors.E(err,
			fmt.Sprintf("%s run --script_path %s %s failed", bazelPath, scriptPath, m[1]))
	}
	return scriptPath, nil
}

func (b *benchmarkRunner) translateArg(ctx context.Context, arg string) (string, error) {
	if m := regexp.MustCompile("@bazel:(.*)").FindStringSubmatch(arg); m != nil {
		return b.translateBazelTarget(ctx, m[1])
	}
	if m := regexp.MustCompile("@cache:(.*)").FindStringSubmatch(arg); m != nil {
		path, err := benchmark.CacheFile(ctx, m[1], b.conf.CacheDir)
		if err != nil {
			return "", errors.E(err, fmt.Sprintf("cache '%s' failed", m[1]))
		}
		return path, nil
	}
	if m := regexp.MustCompile("@cachedir:(.*)").FindStringSubmatch(arg); m != nil {
		path, err := benchmark.CacheDir(ctx, m[1], b.conf.CacheDir)
		if err != nil {
			return "", errors.E(err, fmt.Sprintf("cachedir '%s' failed", m[1]))
		}
		return path, nil
	}
	if m := regexp.MustCompile("@file:(.+)").FindStringSubmatch(arg); m != nil {
		if strings.HasPrefix(m[1], "/") {
			return m[1], nil
		}
		return file.Join(b.conf.RootDir, m[1]), nil
	}
	if m := regexp.MustCompile("@out:(.*)").FindStringSubmatch(arg); m != nil {
		return file.Join(b.conf.OutputDir, m[1]), nil
	}
	return arg, nil
}

func (b *benchmarkRunner) Run() Results {
	fmt.Fprintf(os.Stderr, "Benchmark: Running with config:\n%s", jsonString(b.conf))
	targets := make([]benchmark.Target, len(b.conf.Targets))
	for i := range b.conf.Targets {
		t := b.conf.Targets[i]
		targets[i].Name = t.Name
		targets[i].Callback = func() error {
			cmd := exec.Command(t.Commandline[0], t.Commandline[1:]...)
			return cmd.Run()
		}
	}
	opts := benchmark.Opts{}
	return Results{Results: benchmark.Run(targets, opts)}
}

func (b *benchmarkRunner) Cleanup() {
	if err := os.RemoveAll(b.tempDir); err != nil {
		log.Error.Printf("benchmark cleanup: %v", err)
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] config\n", os.Args[0])
		fmt.Fprintln(os.Stderr, `
The config file is a json file that encodes the following RunConfig type:

        type RunConfig struct {
                // CacheDir specifies the directory to cache inputs to the application.
                // If empty, the value of -cache-dir flag is used.
                CacheDir string
                // OutputDir specifies the value of "@out" in the commandline spec.  If empty,
                // the value of -output-dir flag is used.
                OutputDir string
                // Targets is the list of benchmark applications to run.
                Targets []RunTarget
        }

        // RunTarget is a configuration of one benchmark application.
        type RunTarget struct {
                // Name is any human-readable label for the application.
                Name string
                // Commandline is the commandline string. Each argument can contain the following special labels:
                //
                // - If an arg is of form "@bazel:target", the target is compiled using bazel
                // and the string is replaced with the resulting executable path. The target
                // must be a *_binary or *_test.
                //
                // - If an arg is of form "@cache:path", the given file is copied under
                // RunConfig.CacheDir using grail-file.
                //
                // - If an arg is of form "@cachedir:path", the given directory is recursively
                // copied under RunConfig.CacheDir using grail-file.
                //
                // - If an arg is of form "@out:path", it is replaced with <RunConfig.OutputDir>/path
                //
              	// - If an arg is of form "@file:relpath", it is replaced with <RunConfig.RootDir>/relpath
                Commandline []string
        }

Example:
        {
            "Targets": [
                {
                    "Name": "bio-metrics-1.10",
                    "Commandline": [
                        "@bazel://go/src/grail.com/cmd/bio-binmetrics",
                        "-bam", "@cachedir:s3://grail-clinical-results/48402/S00J5QB/pam/dupmarked.merged",
                        "-sample-id", "S00KZ4A",
                        "-analysis-id", "methylation-v0",
                        "-output", "@out:binmetrics-1.10.grail-rpk"
                    ]
                }
            ]
        }`)

		flag.PrintDefaults()
	}

	log.AddFlags()
	cacheDirFlag := flag.String("cache-dir", "/tmp/benchmark-cache", "Directory used to cache files")
	outputDirFlag := flag.String("output-dir", "/tmp/benchmark-output", "Directory used to output files")
	rootDirFlag := flag.String("root-dir", "", "Root dir for @file directives")
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	cleanup := grail.Init()
	defer cleanup()
	if len(flag.Args()) != 1 {
		flag.Usage()
		os.Exit(1)
	}
	if *rootDirFlag == "" {
		root, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
		if err != nil {
			log.Panicf("git rev-parse --show-toplevel: %s", err)
		}
		*rootDirFlag = strings.TrimSpace(string(root))
	}
	ctx := vcontext.Background()
	conf, err := readRunConfig(ctx, flag.Arg(0), *cacheDirFlag, *outputDirFlag, *rootDirFlag)
	if err != nil {
		log.Panicf("read %s: failed to read and parse config: %v", flag.Arg(0), err)
	}
	b, err := newBenchmarkRunner(ctx, conf)
	if err != nil {
		log.Panicf("failed to initialize benchmark runner: %v", err)
	}
	r := b.Run()
	fmt.Println(jsonString(r))
	b.Cleanup()
}
