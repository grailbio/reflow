
## Reflow language overview

Reflow defines a domain specific language (DSL) for expressing
workflow computation. The language allows the user to easily and
concisely express workflows that compose tools that exchange data via
files. The Reflow language is:

- functional: it emphasizes expressions and composition
- lazily evaluated: expressions are evaluated only as needed
- referentially transparent: expressions may be replaced with the value they produce
- modular: code is packaged in reusable, parameterized modules
- implicitly parallel: computations sequenced only when there are data dependencies
- type safe: programs are checked for type correctness, catching common errors before programs are run

This combination of traits means that users can write straightforward
programs that perform the smallest amount of computation needed for
the desired result while computation is fully parallellized.

Because Reflow is referentially transparent, the runtime can freely
memoize the results of sub expressions, reusing them within a program
or multiple programs.

Reflow takes many syntactic hints from Go, and the grammar is simple,
compact, and regular. It shares many of Go's identifiers, but also
simplifies value constructors to be more appropriate for an
applicative language like Reflow.

The full grammar is documented in [package syntax](https://godoc.org/github.com/grailbio/reflow/syntax).

## Basic expressions

Every Reflow file (files ending with ".rf") is a module. If a module
contains a toplevel identifier called `Main`, it may be run by the
`reflow run` command. A module is a set of declarations, binding
expressions to identifiers. The following module implements the
classic "hello, world!", in Reflow:

	val Main = "hello, world!"

When we invoke `reflow run`, Reflow evaluates the expression bound to 
`Main`, and prints the resulting value:

	% reflow run hello.rf
	"hello, world!"

Reflow is type safe: before evaluating the expression, the module was
checked by the type checker, and all expressions without an explicit type
declaration was assigned a type. We can examine these with `reflow doc`:
in this case, Main was inferred to be of type `string`, as expected.

	% reflow doc hello.rf
	Declarations
	
	val Main string
	
We can also explicitly assign types to identifiers. For example, if we modify
our program to be

	val Main int = "hello, world!"

We are (wrongly) declaring that Main should be of type `int`. This will cause 
Reflow's type checker to complain.

	% reflow run hello.rf
	hello.rf:1:31: cannot use value (type string) as type int

## Syntactic overview

The following is an overview of Reflow's syntactic features:

<dl>
  <dt>strings (type <code>string</code>)</dt>
  <dd>UTF-8-encoded Unicode strings; examples: <code>"hello, world!"</code>, <code>`raw strings"!@#$`</code></dd>
  <dt>integers (type <code>int</code>)</dt>
  <dd>Integers are arbitrary precision; examples: <code>1</code>, <code>2</code>, <code>31415926535897932384626433832795028841971693993751058209749445923078164062862089986280348253421170679</code>.</dd>
  <dt>booleans (type <code<>bool</code>)</dt>
  <dd><code>true</code>, <code>false</code></dt>
  <dt>files (type <code>file</code>)</dt>
  <dd>Files are blobs of bytes; can be imported from external URLs: <code>file("s3://grail-marius/test")</code>; or from a local file: <code>file("/path/to/file")</code>.</dd>
  <dt>directories (type <code>dir</code>)</dt>
  <dd>Directories are dictionaries mapping paths (string) to files; can be imported from external URLs: <code>dir("s3://grail-marius/testdir/")</code>; or from a local file: <code>dir("/path/to/dir/")</code>.</dd>
  <dt>tuples (type <code>(t1, t2, t3, ..)</code>)</dt>
  <dd>Tuples are an ordered, fixed-size list of heterogeneously typed elements; examples: <code>(1, "foo", "bar")</code> (type <code><(int, string, string)</code>), </code>("foo", (1,2,3), 3)</code> (type <code>(string, (int, int, int), int)</code>).</dd>
  <dt>lists (type <code>[t]</code>)</dt>
  <dd>Lists are variable length collections of homogenously typed elemets; examples: <code>[1, 2, 3, 4]</code> (type <code>[int]</code>), <code>["a", "b", "c"]</code> (type <code>[string]</code>).</dd>
  <dt>maps (type <code>[k:v]</code>)</dt>
  <dd>Maps are a mapping of keys to values; examples: <code>["one": 1, "two": 2]</code> (type <code>[string: int]</code>), <code>[1: 10, 2: 20]</code> (type <code>[int: int]</code>).</dd>
  <dt>records (type <code>{f1 t1, f2 t2, f3 t3}</code></dt>
  <dd>Records store a unordered collection of typed fields; examples: <code>{a: 123, b: "hello world"}</code> (type <code>{a int, b string}</code>).</dd>
  <dt>assignment</dt>
  <dd>Values can be assigned identifiers with <code>val</code>; example: <code>val ident = 123</code> (the identifier <code>ident</code> has type <code>int</code>). Assignments can specify a type annotation: <code>val ident int = 123</code> and can also perform pattern matching (see below), and a syntax shortcut is provided for simple assignments where neither is required: <code>ident := 123</code>.</dd>
  <dt>conditionals</dt>
  <dd>Conditionals compute a branch depending on a condition; example: <code>if x < 0 { -x } else { x }</code></dd>
  <dt>functions (type <code>func(a1, a2) r</code>)</dt>
  <dd>Functions abstract code over a list of parameters, they are lexically scoped; examples: <code>func(x int, y int) => x*y</code> (type <code>func(x, y int) int</code>. It is very common to declare a function and assign it to an identifier; Reflow provides syntax sugar for this:
<pre>
func abs(x int) = if x < 0 { -x } else { x }
</pre>
(type <code>func(int) int</code> &mdash; Reflow infers the returned type for us).
</dd>
  <dt>blocks</dt>
  <dd>Blocks are a list of declarations and an expression, example: 
<pre>
{
	a := 1*2
	b := "foo"
	c := "bar"
	(a, b, c)
}
</pre>(type <code>(int, string, string)</code>)
Blocks are useful to combine with functions, e.g.:
<pre>
func absMul(x, y int) = {
	x := abs(x)
	y := abs(y)
	x*y
}
</pre>
Blocks (and toplevel declarations) have a "strong update" property: 
identifier names may be re-bound; the latest lexical binding applies
at the usage site.
</dd>
  <dt>execs</dt>
  <dd>
  An exec runs a shell script inside of a Docker container. Execs can refer to
  data (e.g., files, directories, strings) interpolated from its environment (expressions
  delimited by <code>{{</code> and <code>}}</code>); they
  can return a number of files and directories.
  The following exec runs <code>echo hello world</code> inside of the 
  <code>ubuntu</code> image, placing its output in a file. It reserves 
  1 CPU and 100 MiB of memory.
  <pre>
exec(image := "ubuntu", cpu := 1, mem := 100*MiB) (out file) {"
	echo hello world >{{out}}
"}
  </pre>
  (This exec has type <code>file</code>.)
  <p/>
  Execs can return multiple files or directories. The following defines a function that
  produces a pair of files from two input files to <a href="http://bedtools.readthedocs.io/en/latest/">bedtools</a>:
  <pre>
val image = "..."

// Bins returns a BED resources for a given bin size and a given
// genome with the given bin size.
func Bins(alignmentGenome, genomeSizes file, binSize int) (bed, nuc file) = 
	exec(image, mem := GiB, cpu := 1) (bed, nuc file)	{"
		bedtools makewindows -g {{genomeSizes}} -w {{binSize}} > {{bed}}
		bedtools nuc -fi {{alignmentGenome}} -bed {{bed}} > {{nuc}}
	"}
</pre>
Execs provide a shortcut syntax: <code>exec(image, ..)</code> is syntax sugar for
<code>exec(image := image, ..)</code>.
  </dd>
<dt>pattern matching</dt>
<dd>
Reflow supports pattern matching in assignments and list comprehensions (see below).
Pattern matching syntax is complementary to the syntax for constructing literals, so
for example, the list <code>[1,2,3]</code> can be pattern matched as <code>val [a, b, c] = [1, 2, 3]</code>, 
resulting in <code>a := 1; b := 2; c := 3</code>. Elements can be ignored with <code>_</code> and
patterns can be nested. For example, 
<pre>
val tup = (1, {r: ("a", 1)}, [1, 2, 3])
val (_, {r: (a, one)}, [first, _, third]) = tup
</pre>
results in <code>a := "a"; one := 1; first := 1; third := 3</code>.
</dd>

<dt>comprehensions</dt>
  <dd>
  Comprehensions provide list and map processing functionality. A
  comprehension ranges over a list (or map) and applies an expression
  to each entry, returning a list of the results. Comprehensions also
  perform inline pattern matching. The following example will apply
  <code>absMul</code> to each pair of integers:
  <pre>
val integers = [(1, 2), (-4, 3), (1, 1)]
val multiplied = [absMul(x, y) | (x, y) <- integers]
</pre>
Each pair of integers are multiplied together, producing <code>[2, 12, 1]</code>.
  </dd>
</dl>

## Modules

Every Reflow (".rf") file is a _module_. Modules are reusable
components that can be parameterized and then instantiated by other
modules. Once instantiated, a module is a value like any other, and
may be passed around (even to instantiate yet other modules).

Toplevel value bindings within a module are exported if they begin
with a capital letter, otherwise they remain private to the module.

A Reflow module must declare any parameters before regular declarations
commence. Parameters may have default values, and, if they do, they 
may be omitted when instantiated.

	// samples specifies the sample name to be processed.
	param sample string
	// assay specifies the assay to apply to the sample.
	param assay string
	// mapq specifies the mapq filter threshold to use.
	param mapq = 60
	
	// Declarations begin here.


Params may be grouped together; the above is equivalent to:

	param (
		// samples specifies the sample name to be processed.
		sample string
		// assay specifies the assay to apply to the sample.
		assay string
		// mapq specifies the mapq filter threshold to use.
		mapq = 60
	)
	
	// Declarations begin here.

Parameters may then be used like any other value in the module.

We use `make` to instantiate a module. Make takes 
the module's path as its first argument, and then a set of declarations
to specify arguments. For example:

	val processor = make("./processor.rf", sample := "SAMPLE_123", assay := "assay1")

As with records and execs, `make` implements a syntactic shortcut when
the identifier name matches the parameter name, e.g.:

	val sample = ...
	val assay = ...
	val processor = make("./processor.rf", sample, assay)

Reflow provides a number of system modules; they begin with `$/`.
They are: `$/test`, `$/dirs`, `$/files`, `$/regexp`, `$/strings`, and `$/path`.
Reflow module documentation may be inspected with the command 
`reflow doc module`.

If a module defines the identifier `Main`, it can be invoked by `reflow run`.
`reflow run` can instantiate such modules using command line flags, and they 
can be queried by `reflow run module -help`, for example:

	% cat main.rf
	param (
		a = "ok"
		b string
		c int
	)
	
	val Main = (a, b, c)
	
	% reflow run main.rf -help
	usage of main.rf:
	  -a string
	    	 (default "ok")
	  -b string
	    	(required)
	  -c int
	    	(required)
	% reflow run main.rf -b hello -c 123
	("ok", "hello", 123)
	% reflow run main.rf -b hello -c 123 -a notok
	("notok", "hello", 123)
	% 

## Evaluation semantics

Reflow evaluates its programs similar to dataflow languages:
evaluation is sequenced only where there exists a data dependency.
This means that, for example, in a block like:

	val result = {
		x := exec(...) ...
		y := exec(...) ...
		z := exec(...) ...
		(x, y, z)
	}

`x`, `y`, and `z` are evaluated in parallel. Similarly, list
comprehensions are also evaluated in parallel. 

Reflow programs are also evaluated _by need_, that is to say, lazily.
This means that an expression is never evaluated unless it is required
by the results sought by the toplevel invocation. Lazy evaluation holds
everywhere in Reflow: for example, if you compute a list of expensive
results, but access only the first item, the remaning ones are never 
computed. See the [wikipedia page](https://en.wikipedia.org/wiki/Lazy_evaluation)
for more details on lazy evaluation.

The combination of these &mdash; dataflow parallelism and lazy
evaluation means that most Reflow programs can be written in a
"sequential" manner, and Reflow is able to parallelize the program to
the extent allowable by data dependencies, skipping needless
computation altogether.

Sometimes, it's useful to force ordering, for example because there 
exists a dependency that is invisible to Reflow. Reflow thus provides
an escape hatch, the `~>` operator, that forces sequencing of two 
computations that otherwise would not be. The following example
forces a copy to occur before invoking an exec:

	val files = make("$/files")
	val copy = files.Copy(file("s3://grail-marius/source"), "s3://grail-marius/destination")
	
	val Main = copy ~> exec(image := "ubuntu") (out file) {"
		# do something that assumes s3://grail-marius/destination exists
	"}


