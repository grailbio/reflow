// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

/*
Package lang implements the reflow language.

The reflow language is a simple, type-safe, applicative domain
specific language used to construct Reflow flows.

A reflow expression is one of the following, where e1, e2, ..
themselves represent expressions; id represents an identifier.
Other literals are exemplars.

	(e1)                      // parenthesization
	param("name", "help")     // parameter definition
	let id = e1 in e2         // let-binding
	func(e1, e2)              // function application (arbitrary arity)
	image(e1)                 // import docker image e1
	intern(e1)                // internalize data from url e1 (string or list of strings), see below
	groupby(e1, e2)           // group the value e1 by the regular expression e2
	concat(e1, e2)            // concatenate the strings e1 and e2
	map(e1, e2)               // map the function e2 onto the list e1
	collect(e1, e2)           // filter out files in e1 that don't match the regexp in e2
	collect(e1, e2, e3)       // filter out files in e1 that don't match the regexp in e2;
	                          // then rewrite keys with replacement string e3
	pullup(vs...)			  // flatten values into one
	e1 { bash script {{e2}} } // evaluate a bash script inside the image e1;
	                          // materialize the value e2 into its namespace and
	                          // substitute {{e2}} for its path
	e1["attr1=val1",..]       // set attributes on images
	args                      // command line arguments (list of strings)
	"literal string"          // a literal string

A reflow program comprises a number of toplevel definitions,
each of which are one of:

	include("path")           // read definitions from the given file
	extern(e1, e2)            // externalize e1 to url e2
	id = e1                   // bind e1 to identifier id
	func(id) = e1             // define a function where e1 is evaluated with
	                          // the bound value of id upon application.

For example, the following program produces a flow that will
align a pair of FASTQ files.

	// A Docker image that contains the BWA aligner.
	bwa = image("619867110810.dkr.ecr.us-west-2.amazonaws.com/wgsv1:latest")

	// A read pair stored on S3.
	r1 = intern("s3f://grail-marius/demultiplex2/W044216555475mini/FC0/W044216555475mini_S2_L001_R1_001.fastq.gz")
	r2 = intern("s3f://grail-marius/demultiplex2/W044216555475mini/FC0/W044216555475mini_S2_L001_R2_001.fastq.gz")

	// The BWA reference we'll be using. This fetches the entire s3 prefix,
	// which contains both the FASTA files as well as a BWA index.
	decoyAndViral = intern("s3://grail-scna/reference/bwa_decoy_viral_index")

	// Align a pair of fastq files using BWA. Outputs a BAM file.
	// We reserve approximately 12GB of memory for this operation.
	align(r1, r2) = bwa["rss=12000000000"] {
		/usr/local/bin/bwa mem {{decoyAndViral}}/decoy_and_viral.fa {{r1}} {{r2}} | \
			/usr/local/bin/samtools view -Sb - > $out
	}

	// Upload the results of the expression "align(r1, r2)" to a file in S3.
	extern(align(r1, r2), "s3://grail-marius/aligned.bam")

Interns

If function intern is handed a comma-separated list of arguments, it
interns each separately and combines them into a single "virtual"
value. The resulting output contains the union of all of the URLs,
with the basename (directory name for directory interns, file names
for file interns) appended to the keys of each respective intern. In this
mode, directory interns must end in "/" so that the names are translated
correctly. In the following example, "input" is a value containing INDEX
and the contents of "s3://grail-marius/dir1" under the "dir1/" prefix.

	input = intern("s3://grail-marius/dir1/,s3f://grail-marius/INDEX")

In this mode, empty list entries are ignored, thus adding a "," after a
URL also hoists URLS into a directory. In the following example, reflow
presents a directory with one file named "INDEX".

	input = intern("s3f://grail-marius/INDEX,")

Type checking and evaluation

Reflow programs are type checked by inference: reflow computes the type of
each expressions and checks that it is subsequently used correctly.

Reflow types are one of:

	string             // the type of expressions producing strings
	num                // the type of expressions producing numeric values
	flow               // the type of expressions producing flows
	flowlist           // the type of expressions producint lists of flows
	func(n, r)         // the type of n-ary functions returning type r
	template           // the type of command literals
	image              // the type of expressions producing Docker image refs
	void               // the type of side-effecting expressions

Here are some examples of expressions and their types:

	"hello world"                                  // string
	let h = "hello world" in h                     // string
	image("ubuntu")                                // image
	image("ubuntu") {
		echo hello world
	}                                              // flow
	intern("s3://grail-marius/foobar")             // flow
	extern(out, "s3://...")                        // void
	let h(a, b, c) = string in h                   // func(3, string)

The program is then evaluated into a Flow, which may in turn be evaluated
on a computing cluster by the reflow evaluator.

Bugs and future work

The language has many flaws and short-cuts. In particular, it is
somewhat hamstrung by its static type checking discipline: for
example, we currently restrict the type of function arguments so
that they may be safely inferred without a more complicated type
inferencing scheme.

We can get rid of this restriction while also retaining safety by
more carefully staging reflow evaluation. Currently, a reflow
program is evaluated into a flow, but the semantics of map demand
that some evaluation is deferred (since we don't know its input
beforehand). However, we can sever this tie by representing maps
differently. Namely, they may evaluate to a flow where arguments
are "holes", named by a de-Brujin index (so that maps may be
nested safely). This evaluation scheme would permit the reflow
language to use runtime typing while at the same time exposing
errors before the (expensive) flow evaluation occurs.

The language also has several other problems and inconsistencies.
First, it has shift-reduce conflicts, which we should seek to
avoid. Second, it lacks some common features for which users
compensate. For example, retaining filename information across
groupby-map-merge operations is cumbersome. This can be addressed
in future refinements of the language.
*/
package lang
