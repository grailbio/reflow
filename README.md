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

## Getting Reflow

You can get binaries (macOS/amd64, Linux/amd64) for the latest
release at the [GitHub release
page](https://github.com/grailbio/reflow/releases).

Reflow is implemented in Go, and its packages are go-gettable. You
can retrieve Reflow and its dependencies with

	% go get [-u] github.com/grailbio/reflow

and build the "reflow" binary using

	% go install github.com/grailbio/reflow/cmd/reflow
	
Note that Reflow makes use of its own agent binaries, called
reflowlets.  These are compiled for Linux/amd64, and are invoked by
the cluster manager during instance bootstrapping. Thus, when you
make changes to the code, you may also have to update the agent
docker image. This process is handled by command releasereflow, which
can be installed by

	% go install github.com/grailbio/reflow/cmd/releasereflow

Releasereflow builds target agent binaries and uploads them to a
Docker repository (the release builds use the grailbio/reflowlet
repository). Releasereflow then updates the file
[version.go](https://github.com/grailbio/reflow/blob/master/cmd/reflow/version.go)
which is compiled into the standard reflow binary.

## Quickstart - AWS

Reflow is distributed with an EC2 cluster manager, and a memoization
cache implementation based on S3. These must be configured before
use. Reflow maintains a configuration file in `$HOME/.reflow/config.yaml`
by default (this can be overridden with the `-config` option). Reflow's
setup commands modify this file directly. After each step, the current 
configuration can be examined by running `reflow config`.

Note Reflow must have access to AWS credentials in the environment
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) while running these
commands.

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
of which take one argument naming the S3 bucket and DynamoDB table
name to be used, respectively. The S3 bucket is used to store file
objects while the DynamoDB table is used to store associations
between logically named computations and their concrete output. Note
that S3 bucket names are global, so pick a name that's likely to be
unique.

	% reflow setup-s3-repository reflow-quickstart-cache
	2017/10/18 15:09:10 creating s3 bucket reflow-quickstart-cache
	2017/10/18 15:09:12 created s3 bucket reflow-quickstart-cache
	% reflow setup-dynamodb-assoc reflow-quickstart
	2017/10/18 15:09:40 creating DynamoDB table reflow-quickstart
	2017/10/18 15:09:40 created DynamoDB table reflow-quickstart
	% reflow config
	assoc: dynamodb,reflow-quickstart
	repository: s3,reflow-quickstart-cache

	<rest is same as before>

The setup commands created the S3 bucket and DynamoDB table as
needed, and modified the configuration accordingly.

We're now ready to run our first "hello world" program!

Create a file called "hello.rf" with the following contents:

	val Main = exec(image := "ubuntu", mem := GiB) (out file) {"
		echo hello world >>{{out}}
	"}

and run it:

	% reflow run hello.rf
	2017/10/18 15:11:05 run name: marius@localhost/e08374e8
	2017/10/18 15:11:08 ec2cluster: launched instance i-0bd7d189617e53767: t2.small: 1.9GiB 1 100.0GiB
	2017/10/18 15:11:54 -> hello.Main   3dca1cc0 run    exec ubuntu echo hello world >>{{out}}
	2017/10/18 15:12:02 <- hello.Main   3dca1cc0 ok     exec 0s 12B
	2017/10/18 15:12:02 total n=1 time=8s
		ident      n   ncache runtime(m) cpu mem(GiB) disk(GiB) tmp(GiB)
		hello.Main 1   0                                        
		
	file(sha256=sha256:a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447, size=12)

Here, Reflow started a new t2.small instance (Reflow matches the workload with 
available instance types), ran "echo hello world" inside of an Ubuntu container, 
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

	// g1kv37 is the a human reference FASTA sequence. (All
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
	// file g1kv37 (i..e,
	// s3://1000genomes/technical/reference/human_g1k_v37.fasta.gz)
	val reference = exec(image := "biocontainers/bwa", mem := GiB, cpu := 1) (out dir) {"
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
		exec(image := "biocontainers/bwa", mem := 20*GiB, cpu := 16) (out file) {"
			bwa mem -M -t 16 {{reference}}/g1k_v37.fa {{r1}} {{r2}} > {{out}}
		"}

	// We're ready to test our workflow now. We pick an arbitrary read
	// pair from the 1000genomes data set, and invoke align. There are a
	// few things of note here. First is the identifier "Main". This
	// names the expression that's evaluated by "reflow run" -- the
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
	    "reflow run" -- the entry point of the computation. Second, we've defined Main
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
	2017/10/18 15:47:51 run name: marius@localhost/fb77159e
	2017/10/18 15:47:54 ec2cluster: launched instance i-0a6a6901865001c4c: c4.4xlarge: 28.5GiB 16 100.0GiB

Reflow launched a new instance: the previously launched instance (a
t2.small) was not big enough to fit the requirements of align.rf.
Note also that Reflow assigns a run name for each "reflow run"
invocation. This can be used to look up run details with the "reflow
info" command. In this case:

	% reflow info marius@localhost/fb77159e
	marius@localhost/fb77159e
	    time:    Wed Oct 18 15:47:52 2017
	    program: /Users/marius/align.rf
	    phase:   Eval
	    alloc:   ec2-34-210-106-90.us-west-2.compute.amazonaws.com:9000/a79b63ddf0952cff
	    log:     /Users/marius/.reflow/runs/marius@localhost/fb77159efc639b14aaff55fcd5229484ec78694ada7973bd0af862fd4f06adbd.execlog

Here we see that the run is currently being performed on the alloc named
`ec2-34-210-106-90.us-west-2.compute.amazonaws.com:9000/a79b63ddf0952cff`.
An alloc is a resource reservation on a single machine. A run can
make use of multiple allocs to distribute work across multiple
machines. The alloc is a URI, and the first component is the real 
hostname. You can ssh into the host in order to inspect what's going on.
Reflow launched the instance with your public SSH key (as long as it was
setup by `reflow setup-ec2`, and `$HOME/.ssh/id_rsa.pub` existed at that time).

	% ssh core@ec2-34-210-106-90.us-west-2.compute.amazonaws.com
	...

As the run progresses, Reflow prints execution status on the console. Lines beginning
with "->" indicate that a task was started (e.g., download a file, start an exec), while
lines beginning with "<-" indicate that a task finished.

	...
	2017/10/18 16:03:07 -> example.Main.r2 a8788913 run  intern ..hase3/data/HG00103/sequence_read/SRR062640_2.filt.fastq.gz
	2017/10/18 16:03:07 -> example.Main.r1 882d8d39 run  intern ..hase3/data/HG00103/sequence_read/SRR062640_1.filt.fastq.gz
	2017/10/18 16:03:14 -> example.reference a4a82ba4 run    exec biocontainers/bwa # Ignore failures here. The f..bwa index -a bwtsw g1k_v37.fa

Here, Reflow started downloading r1 and r2 in parallel with creating the reference.
Creating the reference is an expensive operation. We can examine it while it's running
with "reflow ps":

	% reflow ps 
	marius@localhost/220ab625 example.reference 4:03PM 0:00 running 272.8MiB 8.0 4.4GiB bwa

This tells us that the only task that's currently running is bwa to compute the reference.
It's currently using 272MiB of memory, 8 cores, and 4.4 GiB of disk space. By passing the -l
option, reflow ps also prints the task's exec URI.

	% reflow ps -l
	marius@localhost/220ab625 example.reference 4:03PM 0:00 running 302.8MiB 8.0 4.4GiB bwa ec2-52-11-236-67.us-west-2.compute.amazonaws.com:9000/6b2879b9c282a12f/a4a82ba4b5c6d82985d31e79be5a8fe568d75a86db284d78f9972df525cf70d3

An exec URI is a handle to the actual task being executed. It
globally identifies all tasks, and can examined with "reflow info":

	% reflow info ec2-52-11-236-67.us-west-2.compute.amazonaws.com:9000/6b2879b9c282a12f/a4a82ba4b5c6d82985d31e79be5a8fe568d75a86db284d78f9972df525cf70d3
	ec2-52-11-236-67.us-west-2.compute.amazonaws.com:9000/6b2879b9c282a12f/a4a82ba4b5c6d82985d31e79be5a8fe568d75a86db284d78f9972df525cf70d3
	    state: running
	    type:  exec
	    ident: example.reference
	    image: biocontainers/bwa
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
this output can be examined with "reflow logs":

	% reflow logs ec2-52-11-236-67.us-west-2.compute.amazonaws.com:9000/6b2879b9c282a12f/a4a82ba4b5c6d82985d31e79be5a8fe568d75a86db284d78f9972df525cf70d3
	gzip: /arg/0/0: decompression OK, trailing garbage ignored
	[bwa_index] Pack FASTA... 22.36 sec
	[bwa_index] Construct BWT for the packed sequence...
	[BWTIncCreate] textLength=6203609478, availableWord=448508744
	[BWTIncConstructFromPacked] 10 iterations done. 99999990 characters processed.
	[BWTIncConstructFromPacked] 20 iterations done. 199999990 characters processed.
	[BWTIncConstructFromPacked] 30 iterations done. 299999990 characters processed.
	[BWTIncConstructFromPacked] 40 iterations done. 399999990 characters processed.
	[BWTIncConstructFromPacked] 50 iterations done. 499999990 characters processed.
	[BWTIncConstructFromPacked] 60 iterations done. 599999990 characters processed.
	%

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
dependencies have changed. This way, we always know what being
computed is correct (the result is the same as if we had computed the
result from scratch), but avoid paying the cost of redundant
computation.

After a little while, the reference will have finished generating,
and Reflow begins alignment. Here, Reflow reports that the reference
took 56 minutes to compute, and produced 8 GiB of output.

	2017/10/19 10:04:39 <- align.reference a4a82ba4 ok     exec 56m49s 8.0GiB
	2017/10/19 10:04:39 -> align.align  80b24fa0 run    exec biocontainers/bwa bwa mem -M -t 16 {{reference}..37.fa {{r1}} {{r2}} > {{out}}

If we query ("info") the reference exec again, Reflow reports precisely what
was produced:

	% reflow info ec2-34-215-254-146.us-west-2.compute.amazonaws.com:9000/e34ae0cf7c0482f6/a4a82ba4b5c6d82985d31e79be5a8fe568d75a86db284d78f9972df525cf70d3
	ec2-34-215-254-146.us-west-2.compute.amazonaws.com:9000/e34ae0cf7c0482f6/a4a82ba4b5c6d82985d31e79be5a8fe568d75a86db284d78f9972df525cf70d3
	    state: complete
	    type:  exec
	    ident: align.reference
	    image: biocontainers/bwa
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
	marius@localhost/e22cbf4c align.align 10:04AM 0:00 running 7.8GiB 127.9 5.8GiB bwa ec2-34-215-254-146.us-west-2.compute.amazonaws.com:9000/e34ae0cf7c0482f6/80b24fa01b988ca666d4c1eae0fa21d1dbd68e9cd18a82fc4cd6da20fec5abbd
	% reflow info ec2-34-215-254-146.us-west-2.compute.amazonaws.com:9000/e34ae0cf7c0482f6/80b24fa01b988ca666d4c1eae0fa21d1dbd68e9cd18a82fc4cd6da20fec5abbd
	ec2-34-215-254-146.us-west-2.compute.amazonaws.com:9000/e34ae0cf7c0482f6/80b24fa01b988ca666d4c1eae0fa21d1dbd68e9cd18a82fc4cd6da20fec5abbd
	    state: running
	    type:  exec
	    ident: align.align
	    image: biocontainers/bwa
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
	% 

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

	2017/10/19 10:25:13 <- align.align  80b24fa0 ok     exec 19m27s 13.2GiB
	2017/10/19 10:25:13 total n=2 time=1h18m24s
		ident           n   ncache runtime(m) cpu               mem(GiB)    disk(GiB)      tmp(GiB)
		align.align     1   0      19/19/19   123.9/123.9/123.9 7.8/7.8/7.8 12.9/12.9/12.9 0.0/0.0/0.0
		align.reference 1   0      57/57/57   7.8/7.8/7.8       4.4/4.4/4.4 8.0/8.0/8.0    0.0/0.0/0.0
		
	file(sha256=sha256:becb04856590893048e698b1c4bc26192105a44866a06ad8af4f7bda0104c43b, size=14196491221)
	% 

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
		// view the documentation for this module, run "reflow doc
		// $/files".
		files := make("$/files")
		// As before.
		aligned := align(r1, r2)
		// Use the files module's Copy function to copy the aligned file to
		// the provided destination.
		files.Copy(aligned, "s3://marius-test-bucket/aligned.sam")
	}

And run it again:

	% reflow run align.rf
	2017/10/19 10:46:01 run name: marius@localhost/e18f1312
	2017/10/19 10:46:02 -> align.align  80b24fa0 xfer   exec biocontainers/bwa bwa mem -M -t 16 {{reference}..37.fa {{r1}} {{r2}} > {{out}}
	2017/10/19 10:47:27 <- align.align  80b24fa0 ok     exec 0s 13.2GiB
	2017/10/19 10:47:28 -> align.Main   9e252400 run  extern s3://marius-test-bucket/aligned.sam 13.2GiB
	2017/10/19 10:49:42 <- align.Main   9e252400 ok   extern 2m6s 0B
	2017/10/19 10:49:42 total n=2 time=3m40s
		ident       n   ncache runtime(m) cpu mem(GiB) disk(GiB) tmp(GiB)
		align.Main  1   0                                        
		align.align 1   1                                        
		
	val<>

Here we see that Reflow did not need to recompute the aligned file,
it is instead retrieved from cache. The reference index generation is
skipped altogether.  Status lines that indicate "xfer" (instead of
"run") means that Reflow is performing a cache transfer in place of
running the computation. Reflow claims to have transferred a 13.2 GiB
file to `s3://marius-test-bucket/aligned.sam`. Indeed it did:

	% aws s3 ls s3://marius-test-bucket/aligned.sam
	2017-10-19 10:47:37 14196491221 aligned.sam

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

	% reflow run 1000align.rf  -sample HG00103 -out s3://marius-test-bucket/HG00103.bam

In this case, if your account limits allow it, Reflow will launch
additional EC2 instances in order to further parallelize the work to
be done. (Since we're aligning multiple pairs of FASTQ files).
In this run, we can see that Reflow is aligning 5 pairs in parallel
across 2 instances (four can fit on the initial m4.16xlarge instance).

	% reflow ps -l
	marius@localhost/66986b84 align.align.sam 1:43PM 0:00 running 7.8GiB  4.7  6.5GiB   bwa ec2-52-40-140-59.us-west-2.compute.amazonaws.com:9000/21d327bb4da2b7cf/0551e1353c385dc420c00d93ff5b645c5b6dd022986d42fabb4003d9c632f383
	marius@localhost/66986b84 align.align.sam 1:43PM 0:00 running 10.5GiB 58.1 4.2GiB   bwa ec2-52-40-140-59.us-west-2.compute.amazonaws.com:9000/21d327bb4da2b7cf/a39f5f6d8a8ed8b3200cc1ca6b6497dd6bc05ad501bd3f54587139e255972f21
	marius@localhost/66986b84 align.align.sam 1:44PM 0:00 running 8.6GiB  50.8 925.0MiB bwa ec2-52-40-140-59.us-west-2.compute.amazonaws.com:9000/5cbba84c0ca1cb4e/5a956304a2cd3ec37db6240c82a97b4660803f2ea30f2b69139a384cd0664f68
	marius@localhost/66986b84 align.align.sam 1:44PM 0:00 running 9.1GiB  3.3  0B       bwa ec2-52-40-140-59.us-west-2.compute.amazonaws.com:9000/5cbba84c0ca1cb4e/5ebd233dc5495c5c090c5177c7bfdecc1882972cf3504b532d22be1200e7e5f1
	marius@localhost/66986b84 align.align.sam 1:48PM 0:00 running 6.8GiB  31.5 0B       bwa ec2-34-212-44-28.us-west-2.compute.amazonaws.com:9000/f128adf14a7a3d5a/6c1266c7c3e8fc9f2a56e370b552a163e14bf6f4ae73b7b5b067d408eb38dbcf

When it completes, an approximately 17GiB BAM file is deposited to s3:

	% aws s3 ls s3://marius-test-bucket/HG00103.bam
	2017-10-24 20:25:48 18752460252 HG00103.bam

## A note on Reflow's cluster manager

Reflow comes with a built-in cluster manager, which is responsible
for elastically increasing or decreasing required compute resources.
The AWS EC2 cluster manager keeps track of instance type availability
and account limits, and uses these to launch the most appropriate set
of instances for a given job. When instances become idle, they will
terminate themselves if they are idle for more than 10 minutes; idle
instances are reused when possible.

## Documentation

- [Language summary](LANGUAGE.md)
- [Go package docs](https://godoc.org/github.com/grailbio/reflow)

## Support and community

Please join us on on [Gitter](https://gitter.im/grailbio/reflow) or 
on the [mailing list](https://groups.google.com/forum/#!forum/reflowlets)
to discuss Reflow.


