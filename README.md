![Reflow](reflow.svg)

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/grailbio/reflow?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![Build Status](https://travis-ci.org/grailbio/reflow.svg?branch=master)](https://travis-ci.org/grailbio/reflow)

Reflow is a system for incremental data processing in the cloud.
Reflow enables scientists and engineers to compose existing tools
(packaged in Docker images) using ordinary programming constructs.
Reflow then evaluates these programs in a cloud environment,
transparently parallelizing work and memoizing results. Reflow was
created at [GRAIL](http://grail.com/) to manage our NGS (next
generation sequencing) bioinformatics workloads on
[AWS](https://aws.amazon.com), but has also been used for many other
applications, including model training and ad-hoc data analyses.

Reflow comprises:

- a functional, lazy, type-safe domain specific language for writing workflow programs;
- a runtime for evaluating Reflow programs [incrementally](https://en.wikipedia.org/wiki/Incremental_computing), coordinating cluster execution, and transparent memoization;
- a cluster scheduler to dynamically provision and tear down resources from a cloud provider (AWS currently supported).

Reflow thus allows scientists and engineers to write straightforward
programs and then have them transparently executed in a cloud
environment. Programs are automatically parallelized and distributed
across multiple machines, and redundant computations (even across
runs and users) are eliminated by its memoization cache. Reflow
evaluates its programs
[incrementally](https://en.wikipedia.org/wiki/Incremental_computing):
whenever the input data or program changes, only those outputs that
depend on the changed data or code are recomputed.

In addition to the default cluster computing mode, Reflow programs
can also be run locally, making use of the local machine's Docker
daemon (including Docker for Mac).

Reflow was designed to support sophisticated, large-scale
bioinformatics workflows, but should be widely applicable to
scientific and engineering computing workloads. It was built 
using [Go](https://golang.org).

Reflow joins a [long
list](https://github.com/pditommaso/awesome-pipeline) of systems
designed to tackle bioinformatics workloads, but differ from these in
important ways:

- it is a vertically integrated system with a minimal set of external dependencies; this allows Reflow to be "plug-and-play": bring your cloud credentials, and you're off to the races;
- it defines a strict data model which is used for transparent memoization and other optimizations;
- it takes workflow software seriously: the Reflow DSL provides type checking, modularity, and other constructors that are commonplace in general purpose programming languages;
- because of its high level data model and use of caching, Reflow computes [incrementally](https://en.wikipedia.org/wiki/Incremental_computing): it is always able to compute the smallest set of operations given what has been computed previously.

## Table of Contents

- [Quickstart - AWS](#quickstart---aws)
- [Simple bioinformatics workflow](#simple-bioinformatics-workflow)
- [1000align](#1000align)
- [Documentation](#documentation)
- [Developing and building Reflow](#developing-and-building-reflow)
- [Debugging Reflow runs](#debugging-reflow-runs)
- [A note on Reflow's EC2 cluster manager](#a-note-on-reflows-ec2-cluster-manager)
- [Setting up a TaskDB](#setting-up-a-taskdb)
- [Support and community](#support-and-community)


## Getting Reflow

You can get binaries (macOS/amd64, Linux/amd64) for the latest
release at the [GitHub release
page](https://github.com/grailbio/reflow/releases).

If you are developing Reflow,
or would like to build it yourself,
please follow the instructions in the section
"[Developing and building Reflow](#developing-and-building-reflow)."

## Quickstart - AWS

Reflow is distributed with an EC2 cluster manager, and a memoization
cache implementation based on S3. These must be configured before
use. Reflow maintains a configuration file in `$HOME/.reflow/config.yaml`
by default (this can be overridden with the `-config` option). Reflow's
setup commands modify this file directly. After each step, the current 
configuration can be examined by running `reflow config`.

Note Reflow must have access to AWS credentials and configuration in the
environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`) while
running these commands.

	% reflow setup-ec2
	% reflow config
	cluster: ec2cluster
	ec2cluster:
	  ami: ami-d0e54eb0
	  diskspace: 100
	  disktype: gp2
	  instancetypes:
	  - c1.medium
	  - c1.xlarge
	  - c3.2xlarge
	  - c3.4xlarge
	  - c3.8xlarge
	  - c3.large
	  - c3.xlarge
	  - c4.2xlarge
	  - c4.4xlarge
	  - c4.8xlarge
	  - c4.large
	  - c4.xlarge
	  - cc2.8xlarge
	  - m1.large
	  - m1.medium
	  - m1.small
	  - m1.xlarge
	  - m2.2xlarge
	  - m2.4xlarge
	  - m2.xlarge
	  - m3.2xlarge
	  - m3.large
	  - m3.medium
	  - m3.xlarge
	  - m4.16xlarge
	  - m4.4xlarge
	  - m4.xlarge
	  - r4.xlarge
	  - t1.micro
	  - t2.large
	  - t2.medium
	  - t2.micro
	  - t2.nano
	  - t2.small
	  keyname: ""
	  maxinstances: 10
	  region: us-west-2
	  securitygroup: <a newly created security group here>
	  sshkey: <your public SSH key here>
	https: httpsca,$HOME/.reflow/reflow.pem

After running `reflow setup-ec2`, we see that Reflow created a new
security group (associated with the account's default VPC), and
configured the cluster to use some default settings. Feel free to
edit the configuration file (`$HOME/.reflow/config.yaml`) to your
taste. If you want to use spot instances, add a new key under `ec2cluster`:
`spot: true`.

Reflow only configures one security group per account: Reflow will reuse
a previously created security group if `reflow setup-ec2` is run anew.
See `reflow setup-ec2 -help` for more details.

Next, we'll set up a cache. This isn't strictly necessary, but we'll
need it in order to use many of Reflow's sophisticated caching and
incremental computation features. On AWS, Reflow implements a cache
based on S3 and DynamoDB. A new S3-based cache is provisioned by
`reflow setup-s3-repository` and `reflow setup-dynamodb-assoc`, each
of which takes one argument naming the S3 bucket and DynamoDB table
name to be used, respectively. The S3 bucket is used to store file
objects while the DynamoDB table is used to store associations
between logically named computations and their concrete output. Note
that S3 bucket names are global, so pick a name that's likely to be
unique.

	% reflow setup-s3-repository reflow-quickstart-cache
	reflow: creating s3 bucket reflow-quickstart-cache
	reflow: created s3 bucket reflow-quickstart-cache
	% reflow setup-dynamodb-assoc reflow-quickstart
	reflow: creating DynamoDB table reflow-quickstart
	reflow: created DynamoDB table reflow-quickstart
	% reflow config
	assoc: dynamodb,reflow-quickstart
	repository: s3,reflow-quickstart-cache

	<rest is same as before>

The setup commands created the S3 bucket and DynamoDB table as
needed, and modified the configuration accordingly.

Advanced users can also optionally [setup a taskdb](#setting-up-a-taskdb).

We're now ready to run our first "hello world" program!

Create a file called "hello.rf" with the following contents:

	val Main = exec(image := "ubuntu", mem := GiB) (out file) {"
		echo hello world >>{{out}}
	"}

and run it:

	% reflow run hello.rf
	reflow: run ID: 6da656d1
		ec2cluster: 0 instances:  (<=$0.0/hr), total{}, waiting{mem:1.0GiB cpu:1 disk:1.0GiB
	reflow: total n=1 time=0s
			ident      n   ncache transfer runtime(m) cpu mem(GiB) disk(GiB) tmp(GiB)
			hello.Main 1   1      0B

	a948904f

Here, Reflow started a new `t2.small` instance (Reflow matches the workload with
available instance types), ran `echo hello world` inside of an Ubuntu container,
placed the output in a file, and returned its SHA256 digest. (Reflow represents
file contents using their SHA256 digest.)

We're now ready to explore Reflow more fully.

## Simple bioinformatics workflow

Let's explore some of Reflow's features through a simple task:
aligning NGS read data from the 1000genomes project. Create
a file called "align.rf" with the following. The code is commented
inline for clarity.

	// In order to align raw NGS data, we first need to construct an index
	// against which to perform the alignment. We're going to be using
	// the BWA aligner, and so we'll need to retrieve a reference sequence
	// and create an index that's usable from BWA.

	// g1kv37 is a human reference FASTA sequence. (All
	// chromosomes.) Reflow has a static type system, but most type
	// annotations can be omitted: they are inferred by Reflow. In this
	// case, we're creating a file: a reference to the contents of the
	// named URL. We're retrieving data from the public 1000genomes S3
	// bucket.
	val g1kv37 = file("s3://1000genomes/technical/reference/human_g1k_v37.fasta.gz")
	
	// Here we create an indexed version of the g1kv37 reference. It is
	// created using the "bwa index" command with the raw FASTA data as
	// input. Here we encounter another way to produce data in reflow:
	// the exec. An exec runs a (Bash) script inside of a Docker image,
	// placing the output in files or directories (or both: execs can
	// return multiple values). In this case, we're returning a
	// directory since BWA stores multiple index files alongside the raw
	// reference. We also declare that the image to be used is
	// "biocontainers/bwa" (the BWA image maintained by the
	// biocontainers project).
	//
	// Inside of an exec template (delimited by {" and "}) we refer to
	// (interpolate) values in our environment by placing expressions
	// inside of the {{ and }} delimiters. In this case we're referring
	// to the file g1kv37 declared above, and our output, named out.
	//
	// Many types of expressions can be interpolated inside of an exec,
	// for example strings, integers, files, and directories. Strings
	// and integers are rendered using their normal representation,
	// files and directories are materialized to a local path before
	// starting execution. Thus, in this case, {{g1kv37}} is replaced at
	// runtime by a path on disk with a file with the contents of the
	// file g1kv37 (i.e.,
	// s3://1000genomes/technical/reference/human_g1k_v37.fasta.gz)
	val reference = exec(image := "biocontainers/bwa:v0.7.15_cv3", mem := 6*GiB, cpu := 1) (out dir) {"
		# Ignore failures here. The file from 1000genomes has a trailer
		# that isn't recognized by gunzip. (This is not recommended practice!)
		gunzip -c {{g1kv37}} > {{out}}/g1k_v37.fa || true
		cd {{out}}
		bwa index -a bwtsw g1k_v37.fa
	"}
	
	// Now that we have defined a reference, we can define a function to
	// align a pair of reads against the reference, producing an output
	// SAM-formatted file. Functions compute expressions over a set of
	// abstract parameters, in this case, a pair of read files. Unlike almost
	// everywhere else in Reflow, function parameters must be explicitly
	// typed.
	//
	// (Note that we're using a syntactic short-hand here: parameter lists can 
	// be abbreviated. "r1, r2 file" is equivalent to "r1 file, r2 file".)
	//
	// The implementation of align is a straightforward invocation of "bwa mem".
	// Note that "r1" and "r2" inside of the exec refer to the function arguments,
	// thus align can be invoked for any set of r1, r2.
	func align(r1, r2 file) = 
		exec(image := "biocontainers/bwa:v0.7.15_cv3", mem := 20*GiB, cpu := 16) (out file) {"
			bwa mem -M -t 16 {{reference}}/g1k_v37.fa {{r1}} {{r2}} > {{out}}
		"}

	// We're ready to test our workflow now. We pick an arbitrary read
	// pair from the 1000genomes data set, and invoke align. There are a
	// few things of note here. First is the identifier "Main". This
	// names the expression that's evaluated by `reflow run` -- the
	// entry point of the computation. Second, we've defined Main to be
	// a block. A block is an expression that contains one or more
	// definitions followed by an expression. The value of a block is the
	// final expression. Finally, Main contains a @requires annotation.
	// This instructs Reflow how many resources to reserve for the work
	// being done. Note that, because Reflow is able to distribute work,
	// if a single instance is too small to execute fully in parallel,
	// Reflow will provision additional compute instances to help along.
	// @requires thus denotes the smallest possible instance
	// configuration that's required for the program.
	@requires(cpu := 16, mem := 24*GiB, disk := 50*GiB)	
	val Main = {
		r1 := file("s3://1000genomes/phase3/data/HG00103/sequence_read/SRR062640_1.filt.fastq.gz")
		r2 := file("s3://1000genomes/phase3/data/HG00103/sequence_read/SRR062640_2.filt.fastq.gz")
		align(r1, r2)
	}

Now we're ready to run our module. First, let's run `reflow doc`.
This does two things. First, it typechecks the module (and any
dependent modules), and second, it prints documentation for the
public declarations in the module. Identifiers that begin with an
uppercase letter are public (and may be used from other modules);
others are not.

	% reflow doc align.rf
	Declarations
	
	val Main (out file)
	    We're ready to test our workflow now. We pick an arbitrary read pair from the
	    1000genomes data set, and invoke align. There are a few things of note here.
	    First is the identifier "Main". This names the expression that's evaluated by
	    `reflow run` -- the entry point of the computation. Second, we've defined Main
	    to be a block. A block is an expression that contains one or more definitions
	    followed by an expression. The value of a block is the final expression. Finally,
	    Main contains a @requires annotation. This instructs Reflow how many resources
	    to reserve for the work being done. Note that, because Reflow is able to
	    distribute work, if a single instance is too small to execute fully in parallel,
	    Reflow will provision additional compute instances to help along. @requires thus
	    denotes the smallest possible instance configuration that's required for the
	    program.

Then let's run it:

	% reflow run align.rf
	reflow: run ID: 82e63a7a
	ec2cluster: 1 instances: c5.4xlarge:1 (<=$0.7/hr), total{mem:29.8GiB cpu:16 disk:250.0GiB intel_avx512:16}, waiting{}, pending{}
	82e63a7a: elapsed: 2m30s, executing:1, completed: 3/5
	  align.reference:  exec ..101f9a082e1679c16d23787c532a0107537c9c # Ignore failures here. The f..bwa index -a bwtsw g1k_v37.fa  2m4s

Reflow launched a new instance: the previously launched instance (a
`t2.small`) was not big enough to fit the requirements of align.rf.
Note also that Reflow assigns a run name for each `reflow run`
invocation. This can be used to look up run details with the `reflow
info` command. In this case:

	% reflow info 82e63a7a
	82e63a7aee201d137f8ade3d584c234b856dc6bdeba00d5d6efc9627bd988a68 (run)
	    time:      Wed Dec 12 10:45:04 2018
	    program:   /Users/you/align.rf
	    phase:     Eval
	    alloc:     ec2-34-213-42-76.us-west-2.compute.amazonaws.com:9000/5a0adaf6c879efb1
	    resources: {mem:28.9GiB cpu:16 disk:245.1GiB intel_avx:16 intel_avx2:16 intel_avx512:16}
	    log:       /Users/you/.reflow/runs/82e63a7aee201d137f8ade3d584c234b856dc6bdeba00d5d6efc9627bd988a68.execlog

Here we see that the run is currently being performed on the alloc named
`ec2-34-213-42-76.us-west-2.compute.amazonaws.com:9000/5a0adaf6c879efb1`.
An alloc is a resource reservation on a single machine. A run can
make use of multiple allocs to distribute work across multiple
machines. The alloc is a URI, and the first component is the real 
hostname. You can ssh into the host in order to inspect what's going on.
Reflow launched the instance with your public SSH key (as long as it was
set up by `reflow setup-ec2`, and `$HOME/.ssh/id_rsa.pub` existed at that time).

	% ssh core@ec2-34-213-42-76.us-west-2.compute.amazonaws.com
	...

As the run progresses, Reflow prints execution status of each task on the
console.

	...
	align.Main.r2:    intern s3://1000genomes/phase3/data/HG00103/sequence_read/SRR062640_2.filt.fastq.gz                         23s
	align.Main.r1:    intern done 1.8GiB                                                                                          23s
	align.g1kv37:     intern done 851.0MiB                                                                                        23s
	align.reference:  exec ..101f9a082e1679c16d23787c532a0107537c9c # Ignore failures here. The f..bwa index -a bwtsw g1k_v37.fa  6s

Here, Reflow started downloading r1 and r2 in parallel with creating the reference.
Creating the reference is an expensive operation. We can examine it while it's running
with `reflow ps`:

	% reflow ps 
	3674721e align.reference 10:46AM 0:00 running 4.4GiB 1.0 6.5GiB bwa

This tells us that the only task that's currently running is bwa to compute the reference.
It's currently using 4.4GiB of memory, 1 cores, and 6.5GiB GiB of disk space. By passing the -l
option, reflow ps also prints the task's exec URI.

	% reflow ps -l
	3674721e align.reference 10:46AM 0:00 running 4.4GiB 1.0 6.5GiB bwa ec2-34-213-42-76.us-west-2.compute.amazonaws.com:9000/5a0adaf6c879efb1/3674721e2d9e80b325934b08973fb3b1d3028b2df34514c9238be466112eb86e

An exec URI is a handle to the actual task being executed. It
globally identifies all tasks, and can be examined with `reflow info`:

	% reflow info ec2-34-213-42-76.us-west-2.compute.amazonaws.com:9000/5a0adaf6c879efb1/3674721e2d9e80b325934b08973fb3b1d3028b2df34514c9238be466112eb86e
	ec2-34-213-42-76.us-west-2.compute.amazonaws.com:9000/5a0adaf6c879efb1/3674721e2d9e80b325934b08973fb3b1d3028b2df34514c9238be466112eb86e (exec)
	    state: running
	    type:  exec
	    ident: align.reference
	    image: index.docker.io/biocontainers/bwa@sha256:0529e39005e35618c4e52f8f56101f9a082e1679c16d23787c532a0107537c9c
	    cmd:   "\n\t# Ignore failures here. The file from 1000genomes has a trailer\n\t# that isn't recognized by gunzip. (This is not recommended practice!)\n\tgunzip -c {{arg[0]}} > {{arg[1]}}/g1k_v37.fa || true\n\tcd {{arg[2]}}\n\tbwa index -a bwtsw g1k_v37.fa\n"
	      arg[0]:
	        .: sha256:8b6c538abf0dd92d3f3020f36cc1dd67ce004ffa421c2781205f1eb690bdb442 (851.0MiB)
	      arg[1]: output 0
	      arg[2]: output 0
	    top:
	         bwa index -a bwtsw g1k_v37.fa

Here, Reflow tells us that the currently running process is "bwa
index...", its template command, and the SHA256 digest of its inputs.
Programs often print helpful output to standard error while working;
this output can be examined with `reflow logs`:

	% reflow logs ec2-34-221-0-157.us-west-2.compute.amazonaws.com:9000/0061a20f88f57386/3674721e2d9e80b325934b08973fb3b1d3028b2df34514c9238be466112eb86e
	
	gzip: /arg/0/0: decompression OK, trailing garbage ignored
	[bwa_index] Pack FASTA... 18.87 sec
	[bwa_index] Construct BWT for the packed sequence...
	[BWTIncCreate] textLength=6203609478, availableWord=448508744
	[BWTIncConstructFromPacked] 10 iterations done. 99999990 characters processed.
	[BWTIncConstructFromPacked] 20 iterations done. 199999990 characters processed.
	[BWTIncConstructFromPacked] 30 iterations done. 299999990 characters processed.
	[BWTIncConstructFromPacked] 40 iterations done. 399999990 characters processed.
	[BWTIncConstructFromPacked] 50 iterations done. 499999990 characters processed.
	[BWTIncConstructFromPacked] 60 iterations done. 599999990 characters processed.
	[BWTIncConstructFromPacked] 70 iterations done. 699999990 characters processed.
	[BWTIncConstructFromPacked] 80 iterations done. 799999990 characters processed.
	[BWTIncConstructFromPacked] 90 iterations done. 899999990 characters processed.
	[BWTIncConstructFromPacked] 100 iterations done. 999999990 characters processed.
	[BWTIncConstructFromPacked] 110 iterations done. 1099999990 characters processed.
	[BWTIncConstructFromPacked] 120 iterations done. 1199999990 characters processed.
	[BWTIncConstructFromPacked] 130 iterations done. 1299999990 characters processed.
	[BWTIncConstructFromPacked] 140 iterations done. 1399999990 characters processed.
	[BWTIncConstructFromPacked] 150 iterations done. 1499999990 characters processed.
	[BWTIncConstructFromPacked] 160 iterations done. 1599999990 characters processed.
	[BWTIncConstructFromPacked] 170 iterations done. 1699999990 characters processed.
	[BWTIncConstructFromPacked] 180 iterations done. 1799999990 characters processed.
	[BWTIncConstructFromPacked] 190 iterations done. 1899999990 characters processed.
	[BWTIncConstructFromPacked] 200 iterations done. 1999999990 characters processed.
	[BWTIncConstructFromPacked] 210 iterations done. 2099999990 characters processed.
	[BWTIncConstructFromPacked] 220 iterations done. 2199999990 characters processed.
	[BWTIncConstructFromPacked] 230 iterations done. 2299999990 characters processed.
	[BWTIncConstructFromPacked] 240 iterations done. 2399999990 characters processed.
	[BWTIncConstructFromPacked] 250 iterations done. 2499999990 characters processed.
	[BWTIncConstructFromPacked] 260 iterations done. 2599999990 characters processed.
	[BWTIncConstructFromPacked] 270 iterations done. 2699999990 characters processed.
	[BWTIncConstructFromPacked] 280 iterations done. 2799999990 characters processed.
	[BWTIncConstructFromPacked] 290 iterations done. 2899999990 characters processed.
	[BWTIncConstructFromPacked] 300 iterations done. 2999999990 characters processed.
	[BWTIncConstructFromPacked] 310 iterations done. 3099999990 characters processed.
	[BWTIncConstructFromPacked] 320 iterations done. 3199999990 characters processed.
	[BWTIncConstructFromPacked] 330 iterations done. 3299999990 characters processed.
	[BWTIncConstructFromPacked] 340 iterations done. 3399999990 characters processed.
	[BWTIncConstructFromPacked] 350 iterations done. 3499999990 characters processed.
	[BWTIncConstructFromPacked] 360 iterations done. 3599999990 characters processed.
	[BWTIncConstructFromPacked] 370 iterations done. 3699999990 characters processed.
	[BWTIncConstructFromPacked] 380 iterations done. 3799999990 characters processed.
	[BWTIncConstructFromPacked] 390 iterations done. 3899999990 characters processed.
	[BWTIncConstructFromPacked] 400 iterations done. 3999999990 characters processed.
	[BWTIncConstructFromPacked] 410 iterations done. 4099999990 characters processed.
	[BWTIncConstructFromPacked] 420 iterations done. 4199999990 characters processed.
	[BWTIncConstructFromPacked] 430 iterations done. 4299999990 characters processed.
	[BWTIncConstructFromPacked] 440 iterations done. 4399999990 characters processed.
	[BWTIncConstructFromPacked] 450 iterations done. 4499999990 characters processed.

At this point, it looks like everything is running as expected.
There's not much more to do than wait. Note that, while creating an
index takes a long time, Reflow only has to compute it once. When
it's done, Reflow memoizes the result, uploading the resulting data
directly to the configured S3 cache bucket. The next time the
reference expression is encountered, Reflow will use the previously
computed result. If the input file changes (e.g., we decide to use
another reference sequence), Reflow will recompute the index again.
The same will happen if the command (or Docker image) that's used to
compute the index changes. Reflow keeps track of all the dependencies
for a particular sub computation, and recomputes them only when
dependencies have changed. This way, we always know what is being
computed is correct (the result is the same as if we had computed the
result from scratch), but avoid paying the cost of redundant
computation.

After a little while, the reference will have finished generating,
and Reflow begins alignment. Here, Reflow reports that the reference
took 52 minutes to compute, and produced 8 GiB of output.

      align.reference:  exec done 8.0GiB                                                                                            52m37s
      align.align:      exec ..101f9a082e1679c16d23787c532a0107537c9c bwa mem -M -t 16 {{reference}..37.fa {{r1}} {{r2}} > {{out}}  4s

If we query ("info") the reference exec again, Reflow reports precisely what
was produced:

	% reflow info ec2-34-221-0-157.us-west-2.compute.amazonaws.com:9000/0061a20f88f57386/3674721e2d9e80b325934b08973fb3b1d3028b2df34514c9238be466112eb86e
	ec2-34-221-0-157.us-west-2.compute.amazonaws.com:9000/0061a20f88f57386/3674721e2d9e80b325934b08973fb3b1d3028b2df34514c9238be466112eb86e (exec)
	    state: complete
	    type:  exec
	    ident: align.reference
	    image: index.docker.io/biocontainers/bwa@sha256:0529e39005e35618c4e52f8f56101f9a082e1679c16d23787c532a0107537c9c
	    cmd:   "\n\t# Ignore failures here. The file from 1000genomes has a trailer\n\t# that isn't recognized by gunzip. (This is not recommended practice!)\n\tgunzip -c {{arg[0]}} > {{arg[1]}}/g1k_v37.fa || true\n\tcd {{arg[2]}}\n\tbwa index -a bwtsw g1k_v37.fa\n"
	      arg[0]:
	        .: sha256:8b6c538abf0dd92d3f3020f36cc1dd67ce004ffa421c2781205f1eb690bdb442 (851.0MiB)
	      arg[1]: output 0
	      arg[2]: output 0
	    result:
	      list[0]:
	        g1k_v37.fa:     sha256:2f9cd9e853a9284c53884e6a551b1c7284795dd053f255d630aeeb114d1fa81f (2.9GiB)
	        g1k_v37.fa.amb: sha256:dd51a07041a470925c1ebba45c2f534af91d829f104ade8fc321095f65e7e206 (6.4KiB)
	        g1k_v37.fa.ann: sha256:68928e712ef48af64c5b6a443f2d2b8517e392ae58b6a4ab7191ef7da3f7930e (6.7KiB)
	        g1k_v37.fa.bwt: sha256:2aec938930b8a2681eb0dfbe4f865360b98b2b6212c1fb9f7991bc74f72d79d8 (2.9GiB)
	        g1k_v37.fa.pac: sha256:d62039666da85d859a29ea24af55b3c8ffc61ddf02287af4d51b0647f863b94c (739.5MiB)
	        g1k_v37.fa.sa:  sha256:99eb6ff6b54fba663c25e2642bb2a6c82921c931338a7144327c1e3ee99a4447 (1.4GiB)

In this case, "bwa index" produced a number of auxiliary index
files. These are the contents of the "reference" directory.

We can again query Reflow for running execs, and examine the
alignment. We see now that the reference is passed in (argument 0),
along side the read pairs (arguments 1 and 2). 

	% reflow ps -l
	6a6c36f5 align.align 5:12PM 0:00 running 5.9GiB 12.3 0B  bwa ec2-34-221-0-157.us-west-2.compute.amazonaws.com:9000/0061a20f88f57386/6a6c36f5da6ee387510b0b61d788d7e4c94244d61e6bc621b43f59a73443a755
	% reflow info ec2-34-221-0-157.us-west-2.compute.amazonaws.com:9000/0061a20f88f57386/6a6c36f5da6ee387510b0b61d788d7e4c94244d61e6bc621b43f59a73443a755
	ec2-34-221-0-157.us-west-2.compute.amazonaws.com:9000/0061a20f88f57386/6a6c36f5da6ee387510b0b61d788d7e4c94244d61e6bc621b43f59a73443a755 (exec)
	    state: running
	    type:  exec
	    ident: align.align
	    image: index.docker.io/biocontainers/bwa@sha256:0529e39005e35618c4e52f8f56101f9a082e1679c16d23787c532a0107537c9c
	    cmd:   "\n\t\tbwa mem -M -t 16 {{arg[0]}}/g1k_v37.fa {{arg[1]}} {{arg[2]}} > {{arg[3]}}\n\t"
	      arg[0]:
	        g1k_v37.fa:     sha256:2f9cd9e853a9284c53884e6a551b1c7284795dd053f255d630aeeb114d1fa81f (2.9GiB)
	        g1k_v37.fa.amb: sha256:dd51a07041a470925c1ebba45c2f534af91d829f104ade8fc321095f65e7e206 (6.4KiB)
	        g1k_v37.fa.ann: sha256:68928e712ef48af64c5b6a443f2d2b8517e392ae58b6a4ab7191ef7da3f7930e (6.7KiB)
	        g1k_v37.fa.bwt: sha256:2aec938930b8a2681eb0dfbe4f865360b98b2b6212c1fb9f7991bc74f72d79d8 (2.9GiB)
	        g1k_v37.fa.pac: sha256:d62039666da85d859a29ea24af55b3c8ffc61ddf02287af4d51b0647f863b94c (739.5MiB)
	        g1k_v37.fa.sa:  sha256:99eb6ff6b54fba663c25e2642bb2a6c82921c931338a7144327c1e3ee99a4447 (1.4GiB)
	      arg[1]:
	        .: sha256:0c1f85aa9470b24d46d9fc67ba074ca9695d53a0dee580ec8de8ed46ef347a85 (1.8GiB)
	      arg[2]:
	        .: sha256:47f5e749123d8dda92b82d5df8e32de85273989516f8e575d9838adca271f630 (1.7GiB)
	      arg[3]: output 0
	    top:
	         /bin/bash -e -l -o pipefail -c ..bwa mem -M -t 16 /arg/0/0/g1k_v37.fa /arg/1/0 /arg/2/0 > /return/0 .
	         bwa mem -M -t 16 /arg/0/0/g1k_v37.fa /arg/1/0 /arg/2/0

Note that the read pairs are files. Files in Reflow do not have
names; they are just blobs of data. When Reflow runs a process that
requires input files, those anonymous files are materialized on disk,
but the filenames are not meaningful. In this case, we can see from
the "top" output (these are the actual running processes, as reported
by the OS), that the r1 ended up being called "/arg/1/0" and r2
"/arg/2/0". The output is a file named "/return/0".

Finally, alignment is complete. Aligning a single read pair took
around 19m, and produced 13.2 GiB of output. Upon completion, Reflow
prints runtime statistics and the result.

	reflow: total n=5 time=1h9m57s
	        ident           n   ncache transfer runtime(m) cpu            mem(GiB)    disk(GiB)      tmp(GiB)
	        align.align     1   0      0B       17/17/17   15.6/15.6/15.6 7.8/7.8/7.8 12.9/12.9/12.9 0.0/0.0/0.0
	        align.Main.r2   1   0      0B
	        align.Main.r1   1   0      0B
	        align.reference 1   0      0B       51/51/51   1.0/1.0/1.0    4.4/4.4/4.4 6.5/6.5/6.5    0.0/0.0/0.0
	        align.g1kv37    1   0      0B
	
	becb0485

Reflow represents file values by the SHA256 digest of the file's
content. In this case, that's not very useful: you want the file,
not its digest. Reflow provides mechanisms to export data. In this
case let's copy the resulting file to an S3 bucket.

We'll make use of the "files" system module to copy the aligned file
to an external S3 bucket. Modify align.rf's `Main` to the following
(but pick an S3 bucket you own), and then run it again. Commentary is
inline for clarity.

	@requires(cpu := 16, mem := 24*GiB, disk := 50*GiB)	
	val Main = {
		r1 := file("s3://1000genomes/phase3/data/HG00103/sequence_read/SRR062640_1.filt.fastq.gz")
		r2 := file("s3://1000genomes/phase3/data/HG00103/sequence_read/SRR062640_2.filt.fastq.gz")
		// Instantiate the system modules "files" (system modules begin
		// with $), assigning its instance to the "files" identifier. To
		// view the documentation for this module, run `reflow doc
		// $/files`.
		files := make("$/files")
		// As before.
		aligned := align(r1, r2)
		// Use the files module's Copy function to copy the aligned file to
		// the provided destination.
		files.Copy(aligned, "s3://marius-test-bucket/aligned.sam")
	}

And run it again:

	% reflow run align.rf
	reflow: run ID: 9f0f3596
	reflow: total n=2 time=1m9s
	        ident         n   ncache transfer runtime(m) cpu mem(GiB) disk(GiB) tmp(GiB)
	        align_2.align 1   1      0B
	        align_2.Main  1   0      13.2GiB
	
	val<.=becb0485 13.2GiB>


Here we see that Reflow did not need to recompute the aligned file;
it is instead retrieved from cache. The reference index generation is
skipped altogether.  Status lines that indicate "xfer" (instead of
"run") means that Reflow is performing a cache transfer in place of
running the computation. Reflow claims to have transferred a 13.2 GiB
file to `s3://marius-test-bucket/aligned.sam`. Indeed it did:

	% aws s3 ls s3://marius-test-bucket/aligned.sam
	2018-12-13 16:29:49 14196491221 aligned.sam.

## 1000align

This code was modularized and generalized in
[1000align](https://github.com/grailbio/reflow/tree/master/doc/1000align). Here,
fastq, bam, and alignment utilities are split into their own
parameterized modules. The toplevel module, 1000align, is
instantiated from the command line. Command line invocations (`reflow
run`) can pass module parameters through flags (strings, booleans,
and integers):

	% reflow run 1000align.rf -help
	usage of 1000align.rf:
	  -out string
	    	out is the target of the output merged BAM file (required)
	  -sample string
	    	sample is the name of the 1000genomes phase 3 sample (required)

For example, to align the full sample from above, we can invoke
1000align.rf with the following arguments:

	% reflow run 1000align.rf -sample HG00103 -out s3://marius-test-bucket/HG00103.bam

In this case, if your account limits allow it, Reflow will launch
additional EC2 instances in order to further parallelize the work to
be done. (Since we're aligning multiple pairs of FASTQ files).
In this run, we can see that Reflow is aligning 5 pairs in parallel
across 2 instances (four can fit on the initial m4.16xlarge instance).

	% reflow ps -l
	e74d4311 align.align.sam 11:45AM 0:00 running 10.9GiB 31.8 6.9GiB   bwa ec2-34-210-201-193.us-west-2.compute.amazonaws.com:9000/6a7ffa00d6b0d9e1/e74d4311708f1c9c8d3894a06b59029219e8a545c69aa79c3ecfedc1eeb898f6
	59c561be align.align.sam 11:45AM 0:00 running 10.9GiB 32.7 6.4GiB   bwa ec2-34-210-201-193.us-west-2.compute.amazonaws.com:9000/6a7ffa00d6b0d9e1/59c561be5f627143108ce592d640126b88c23ba3d00974ad0a3c801a32b50fbe
	ba688daf align.align.sam 11:47AM 0:00 running 8.7GiB  22.6 2.9GiB   bwa ec2-18-236-233-4.us-west-2.compute.amazonaws.com:9000/ae348d6c8a33f1c9/ba688daf5d50db514ee67972ec5f0a684f8a76faedeb9a25ce3d412e3c94c75c
	0caece7f align.align.sam 11:47AM 0:00 running 8.7GiB  25.9 3.4GiB   bwa ec2-18-236-233-4.us-west-2.compute.amazonaws.com:9000/ae348d6c8a33f1c9/0caece7f38dc3d451d2a7411b1fcb375afa6c86a7b0b27ba7dd1f9d43d94f2f9
	0b59e00c align.align.sam 11:47AM 0:00 running 10.4GiB 22.9 926.6MiB bwa ec2-18-236-233-4.us-west-2.compute.amazonaws.com:9000/ae348d6c8a33f1c9/0b59e00c848fa91e3b0871c30da3ed7e70fbc363bdc48fb09c3dfd61684c5fd9

When it completes, an approximately 17GiB BAM file is deposited to s3:

	% aws s3 ls s3://marius-test-bucket/HG00103.bam
	2018-12-14 15:27:33 18761607096 HG00103.bam.

## A note on Reflow's EC2 cluster manager

Reflow comes with a built-in cluster manager, which is responsible
for elastically increasing or decreasing required compute resources.
The AWS EC2 cluster manager keeps track of instance type availability
and account limits, and uses these to launch the most appropriate set
of instances for a given job. When instances become idle, they will
terminate themselves if they are idle for more than 10 minutes; idle
instances are reused when possible.

The cluster manager may be configured under the "ec2cluster" key in 
Reflow's configuration. Its parameters are documented by
[godoc](https://godoc.org/github.com/grailbio/reflow/ec2cluster#Config).
(Formal documentation is forthcoming.)

## Setting up a TaskDB
Setting up a TaskDB is entirely optional. The TaskDB is used to store a record of reflow runs,
their sub-tasks (mainly `exec`s), the EC2 instances that were instantiated, etc.
It provides the following benefits:

* Tools such as `reflow info` will work better, especially in a multi-user environment.

  That is, if you have a single AWS account and share it with other users to run `reflow`, then
a TaskDB allows you to monitor and query info about all runs within the account (using `reflow ps`, `reflow info`, etc)
* Determine cost of a particular run (included in the output of `reflow info`)
* Determine cost of the cluster (`reflow ps -p` - see documentation using `reflow ps --help`)

The following command can be used to setup a TaskDB (refer documentation:

	% reflow setup-taskdb -help

Note that the same dynamodb table and S3 bucket which were used to setup the cache (see above),
could optionally be used here.  But note that this (TaskDB) feature comes with a cost (DynamoDB),
and by keeping them separate, the costs can be managed independently.

Example:

	% reflow setup-taskdb <table_name> <s3_bucket_name>
	reflow: attempting to create DynamoDB table ...
	reflow: created DynamoDB table ...
	reflow: waiting for table to become active; current status: CREATING
	reflow: created secondary index Date-Keepalive-index
	reflow: waiting for table to become active; current status: UPDATING
	reflow: waiting for index Date-Keepalive-index to become active; current status: CREATING
	...
	reflow: created secondary index RunID-index
	reflow: waiting for table to become active; current status: UPDATING
	reflow: waiting for index RunID-index to become active; current status: CREATING
	...


## Documentation

- [Language summary](LANGUAGE.md)
- [Go package docs](https://godoc.org/github.com/grailbio/reflow)

## Developing and building Reflow

Reflow is implemented in Go, and its packages are go-gettable. 
Reflow is also a [Go module](https://github.com/golang/go/wiki/Modules)
and uses modules to fix its dependency graph.

After checking out the repository, 
the usual `go` commands should work, e.g.:

	% go test ./...

The package `github.com/grailbio/reflow/cmd/reflow`
(or subdirectory `cmd/reflow` in the repository)
defines the main command for Reflow.
Because Reflow relies on being able to
distribute its current build,
the binary must be built using the `buildreflow` tool
instead of the ordinary Go tooling.
Command `buildreflow` acts like `go build`,
but also cross compiles the binary 
for the remote target (Linux/amd64 currently supported),
and embeds the cross-compiled binary.

	% cd $CHECKOUT/cmd/reflow
	% go install github.com/grailbio/reflow/cmd/buildreflow
	% buildreflow

## Debugging Reflow runs

The `$HOME/.reflow/runs` directory contains logs, traces and other 
information for each Reflow run. If the run you're looking for is
no longer there, the `info` and `cat` tools can be used if you have 
the run ID:

	% reflow info 2fd5a9b6
	runid    user       start   end    RunLog   EvalGraph Trace
    2fd5a9b6 username   4:41PM  4:41PM 29a4b506 90f40bfc  4ec75aac
    
    % reflow cat 29a4b506 > /tmp/29a4b506.runlog

    # fetch the evalgraph data, pass to the dot tool to generate an svg image (viewable in your browser)
    % reflow cat 90f40bfc | dot -Tsvg > /tmp/90f40bfc.svg

For more information about tracing, see: [doc/tracing.md](doc/tracing.md).

## Support and community

Please join us on on [Gitter](https://gitter.im/grailbio/reflow) or 
on the [mailing list](https://groups.google.com/forum/#!forum/reflowlets)
to discuss Reflow.


