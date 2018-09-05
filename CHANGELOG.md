## HEAD

## [reflow0.6](https://github.com/grailbio/reflow/releases/tag/reflow0.6)

This release introduces many improvements to Reflow's user experience
and performance and, as always, many bug fixes and smaller
improvements.

The major changes in this release are: *bottom up evaluation*, which
allows Reflow to use both logical and physical cache keys in order to
improve cache utilization and hit rates; *lazy object fetching* which
minimizes the amount of data transfer Reflow needs to perform from
cache; and *status displays* which, when Reflow is run in a capable
terminal, displays current evaluation status to the user, so that
it's always simple to understand what Reflow is doing at a glance.
Many of Reflow's commands are also clarified and simplified. For
example "reflow info" is now capable of displaying information about
any valid Reflow identifier (e.g., a cached object, a running exec,
an alloc, or a cached fileset).

Reflow 0.6 also introduces some changes in how cache entries are
stored. This is in order to support abbreviated lookups of cache
entries (e.g., "reflow info abcdef"). Command "reflow migrate"
migrates the current database setup to support this feature.

The changes and improvements are detailed below.

### Core

- [bottom-up evaluation](https://github.com/grailbio/reflow/commit/2093b25253069dc99e996a3e4080ef522d783080): 
  With "reflow run -eval=bottomup", reflow
  performs pure, cache-assisted bottom-up evaluation. This allows
  Reflow to compute physical cache keys in addition to logical ones,
  and can thus (safely) cache more widely. Bottom-up evaluation logs
  also never skip any part of the evaluation, thus providing a more
  sensible program trace. In bottom-up mode, Reflow looks up both
  keys.
- precise, lazy object fetching: Reflow now fetches objects from 
  cache by need, and not when the corresponding cache entry is
  retrieved. This means that Reflow now never fetches more data
  than is absolutely required.
- many improvements to evaluation debug logging (reflow -log debug run)
- add configuration provider for static clusters
- extensible resource types (beyond cpu, mem, disk)
- use pure concurrency limits for transfers
- a number of cluster scheduler tweaks and improvements
- add a [status reporting](https://github.com/grailbio/reflow/commit/9568976b561eb35a14f2923b60f70ddcb814b810)
  facility that is used to report the current set of evaluation activities
  (executions, interns, data transfers, cluster management activities) to the user in
  a comprehensible way  
- identifier-based invalidation: 'reflow run -invalidate=regexp ..." will invalidate 
  cache entries produced by nodes whose identifiers match the provided regular
  expression. For example, the command "reflow run -invalidate=[aA]lign wgs.rf ..."
  will run wgs.rf as normal, but recompute all alignments.

### Tool

- unified handling of sha256s: "reflow info" can now resolve 
  any valid Reflow identifier
- "reflow info" can now resolve sha256 prefixes; this requires adding
  indices to DynamoDB table with new command "reflow migrate"
- command "reflow cat" writes a cached object to stdout
- command "reflow sync" synchronizes a cached fileset to a local destination
- "reflow doc" displays all available system modules when invoked without
  arguments
- "reflow doc" now renders all default parameters by pretty-printing their 
  expressions in an abbreviated form
- handle interrupts (control-c) properly, so that allocs can be reclaimed
  much more quickly after an interrupted run

### Syntax

- [comprehension support](https://github.com/grailbio/reflow/commit/f8f0b300d9dc98fe88a8464fe3249409a51dc4f6)
  for cross products and filters
- $/regexp.Match: regular expression matching
- $/regexp.Replace: regular expression matching with replacement
- $/strings.FromFloat: convert floating point numbers to strings
- $/strings.Join, $/strings.HasPrefix, $/strings.HasSuffix: string utilities
- support missing arithmetic binary operations for ints and floats:
  %, <<, >>, -, /
- improve error messages in various standard library funcs
- various improvements to type and syntax error messages
- add registration mechanism for custom intrinsics
- support for expressing cpu features in resource requirements
  (e.g., "intel_avx", "intel_avx2", and "intel_avx512" as supported by various
  EC2 instance types)
- [trace builtin](https://github.com/grailbio/reflow/commit/dfcbf744852276e8f6d6ac912c26c4d7b139a515)
  to aid debugging
- support fractional CPUs in resource requirements
- [range builtin](https://github.com/grailbio/reflow/commit/9f09974ead5e4748aa013fb9e13e09ec236c00c3):
  create integer ranges
- support rich(er) key types in maps: string, int, float, bool, and file are now supported

### Cache

- record latest access times counts to permit for LRU collection
- support digest abbreviations

### Local executor

- don't attempt to hardlink across devices
- more careful about parsing Docker repository names to avoid
  unnecessary pulls

### ec2cluster

- update definitions from Amazon
- change resource fudge factor; instance-reported resource 
  availability changed with recent virtualization changes
- remove idle boundary computation no longer needed due to EC2
  billing changes; instead use a straightforward idle timeout
- [user-mergeable cloud-config](https://github.com/grailbio/reflow/commit/f1768adc0915821dbb7c5efb6e9a6f71a6347f3a)
- instances launched by ec2cluster may now be assigned a (configurable) 
  EC2 instance profile
- batch DescribeInstance calls with paging to reduce EC2 API load
- support for avx, avx2, and avx512 resource constraints
- allocation improvements resulting in better instance packing
- configurabhle [striping of multiple EBS volumes](https://github.com/grailbio/reflow/commit/53696ed3c3f7aba305dc38945118dba9e85f4c99)
  to improve throughput; with this change we can push >2GiB/s on larger instance types

## [reflow0.5.3](https://github.com/grailbio/reflow/releases/tag/reflow0.5.3)

- ec2cluster: support for the new c5 instance types
- local/s3: remove digest.WriterAt until we have the new S3 downloader
- syntax: fix miscellaneous delayed evaluation bugs
- tool: check if shell returns an error
- local: use the return code from Docker's ContainerInspect call
- syntax: increase local file limit from 100MB to 200MB
- local: fix invalid sequence token assignment in cloudwatchlogs
- local: restore s3 state across restarts

## [reflow0.5.2](https://github.com/grailbio/reflow/releases/tag/reflow0.5.2)

- reflowlet: terminate on 10-minute idle time (since AWS does secondly billing)
- syntax: fix digest for ExprApply
- syntax: {files,dirs}.Copy: return a helpful error message if URL scheme is missing
- runner: improve stealer allocation
- eval: scramble digest when an Val carries an error
- syntax,types: fix type environment for recursive module instantiations
- `reflow shell`: new command to shell into an exec
- tool: use stdout for reflow doc output
- tool: disable -gc for v1 flows, -dir alias for -localdir, for backwards compatibility
- `reflow logs -f`: support for tailing ongoing exec logs
- tool: restore noexterncache flag
- syntax: always sort map keys carefully before digesting
- syntax: fix environment persistence for declarations
- doc: add language overview docs
- config: add "local" configuration provider for user
- eval: print full image name in debug logs
- syntax: add few missing cases for trailing commas
- syntax: add a semicolon at the end of a file if there is a missing newline
- syntax: add arbitrary precision float, conversion between int and float
- eval: add option/flag -recomputeempty to recompute empty cache values

## [reflow0.5.1](https://github.com/grailbio/reflow/releases/tag/reflow0.5.1)

Bugfix release to include proper library vendoring, causing intermittent Docker issues.

- Fix `vendor/` directory to include the correct Docker dependencies.
- tool: create the config directory if it does not exist

## [reflow0.5](https://github.com/grailbio/reflow/releases/tag/reflow0.5)

Initial public release of Reflow.
