# Grail-benchmark

Grail-benchmark is a tool for measuring performance of a large application.  It
runs a set of applications repeatedly on the local machine until until their
performance variance falls within a reasonable range. The results are printed as
JSON to stdout.

Usage:

  grail-benchmark _configfile_

The configfile is a JSON file that encodes `RunConfig` type as defined below:

    type RunConfig struct {
        // CacheDir defaults the value of -cache-dir flag.
        CacheDir  string
        // OutputDir defaults the value of -output-dir flag.
        OutputDir string
        Targets   []RunTarget
    }

    type RunTarget struct {
        Name        string
        Commandline []string
    }

For example:

    {
        "Targets": [
            {
                "Name": "bio-metrics-1.10",
                "Commandline": [
                    "@bazel://go/src/grail.com/cmd/bio-metrics",
                    "-reference", "@cache:s3://grail-reference/hg19_lambda_pUC19_viral.fa",
                    "-bam", "@cachedir:s3://grail-results/bam/dupmarked.merged",
                    "-calculate-bisulfite-conversion",
                    "-output", "@out:binmetrics-1.10.result"
                ]
            }
        ]
    }

A string in RunTarget.Commandline can contain special characters:

- If an arg is of form "@bazel:target", the target is compiled using bazel
  and the string is replaced with the resulting executable path.

- If an arg is of form "@cache:path", the given file is copied under
  RunConfig.CacheDir using grail-file.

- If an arg is of form "@cachedir:path", the given directory is recursively
  copied under RunConfig.CacheDir using grail-file.

- If an arg is of form "@out:path", it is replaced with <RunConfig.OutputDir>/path


The benchmark results are in the following JSON format:

    type Results struct {
        Results []Result
    }

    // Result is the result of runs of an applications.
    type Result struct {
        // Name is copied from Target.Name
        Name string
        // Runs is the list of all runs.
        Runs []RunResult
    }

    // RunResult is the result of one run of an application.
    type RunResult struct {
        Start    time.Time
        Duration time.Duration
        Err      error
    }
