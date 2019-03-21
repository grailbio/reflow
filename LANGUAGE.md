
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
the desired result while computation is fully parallelized.

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
declaration were assigned a type. We can examine these with `reflow doc`:
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
	<dt>floats (type <code>float</code>)</dt>
  <dd>Floats are arbitrary precision; examples: <code>1.0</code>, <code>2.23</code>,  <code>3e10</code>, <code>0.5e-8</code>,  <code>3.1415926535897932384626433832795028841971693993751058209749445923078164062862089986280348253421170679</code>.</dd>
  <dt>booleans (type <code>bool</code>)</dt>
  <dd><code>true</code>, <code>false</code></dt>
  <dt>files (type <code>file</code>)</dt>
  <dd>Files are blobs of bytes; can be imported from external URLs: <code>file("s3://grail-marius/test")</code>; or from a local file: <code>file("/path/to/file")</code>.</dd>
  <dt>directories (type <code>dir</code>)</dt>
  <dd>Directories are dictionaries mapping paths (string) to files; can be imported from external URLs: <code>dir("s3://grail-marius/testdir/")</code>; or from a local file: <code>dir("/path/to/dir/")</code>.</dd>
  <dt>tuples (type <code>(t1, t2, t3, ..)</code>)</dt>
  <dd>Tuples are an ordered, fixed-size list of heterogeneously typed elements; examples: <code>(1, "foo", "bar")</code> (type <code>(int, string, string)</code>), </code>("foo", (1,2,3), 3)</code> (type <code>(string, (int, int, int), int)</code>).</dd>
  <dt>lists (type <code>[t]</code>)</dt>
  <dd>Lists are variable length collections of homogenously typed elements; examples: <code>[1, 2, 3, 4]</code> (type <code>[int]</code>), <code>["a", "b", "c"]</code> (type <code>[string]</code>).</dd>
  <dt>maps (type <code>[k:v]</code>)</dt>
  <dd>Maps are a mapping of keys to values; examples: <code>["one": 1, "two": 2]</code> (type <code>[string: int]</code>), <code>[1: 10, 2: 20]</code> (type <code>[int: int]</code>).</dd>
  <dt>records (type <code>{f1 t1, f2 t2, f3 t3}</code></dt>
  <dd>Records store an unordered collection of typed fields; examples: <code>{a: 123, b: "hello world"}</code> (type <code>{a int, b string}</code>).</dd>
	<dt>sum types (type <code>#T1(t1) | #T2(t2) | #T3(t3)</code>)</dt>
	<dd>Sum types (a.k.a. algebraic data types, variant types, unions) express multiple disjoint possibilities for a value.  For example, you might want to express the idea of "nil or some integer value", which you could encode as <code>#Nil | #SomeInt(int)</code>.  Another use case might be expression of "file or directory", which you could encode as <code>#File(file) | #Dir(dir)</code>.  As you may have noticed from the <code>#Nil</code> variant above, variants do not require elements, so you can use sum types to encode "enumerations", e.g. <code>#Yes | #No | #Maybe</code>.
	Sum types are also polymorphic, which means that you can use variants anywhere they structurally fit:
<pre>
type YesNo #Yes | #No
type YesNoMaybe #Yes | #No | #Maybe
func parseDecision(s string) YesNo =
	if s == "yes" {
		#Yes
	} else {
		#No
	}
}
func printDecision(d YesNoMaybe) string =
	switch d {
	case #Yes:
		"yes"
	case #No:
		"no"
	case #Maybe:
		"maybe"
	}
Main := printDecision(parseDecision("nope")) // prints "no"
</pre>
	Here, we pass a <code>#Yes | #No</code> to a function expecting a <code>#Yes | #No | #Maybe</code>.  Because every variant of <code>#Yes | #No</code> is compatible with <code>#Yes | #No | #Maybe</code>, this is allowed.
	</dd>
  <dt>assignment</dt>
  <dd>Values can be assigned identifiers with <code>val</code>; example: <code>val ident = 123</code> (the identifier <code>ident</code> has type <code>int</code>). Assignments can specify a type annotation: <code>val ident int = 123</code> and can also perform pattern matching (see below), and a syntax shortcut is provided for simple assignments where neither is required: <code>ident := 123</code>.</dd>
  <dt>conditionals</dt>
  <dd>Conditionals compute a branch depending on a condition; example: <code>if x < 0 { -x } else if x >= 0 && x < 2 { x } else { x - 2 }</code></dd>
	<dt>switch expressions</dt>
	<dd>Switch expressions can be used to try to match multiple patterns.  They evaluate to the value of the matching case's expression.
<pre>
val someList [string] = ...
switch someList {
case []:
	"the list is empty"
case [s]:
	"the list has exactly one element: " + s
case [s, _, ..._]:
	"the list has more than one element, and the first one is: " + s
}
</pre>
	The cases of a switch expression must be exhaustive:
<pre>
val someList [string] = ...
switch someList {
case []:
	"the list is empty"
}
// ERROR: ...because any non-empty list will not match a case.
</pre>
	Switches are particularly useful when used with sum types:
<pre>
pi := 3.14159
type square {length float}
type rectangle {length float, width float}
type circle {radius float}
type shape #Point | #Custom(float) | #Square(square) | #Rectangle(rectangle) | #Circle(circle)
func computeArea(s shape) float =
	switch s {
	case #Point:
		0.0
	case #Custom(a):
		a
	case #Square(s):
		s.length * s.length
	case #Rectangle(r):
		r.length * r.width
	case #Circle(c):
		pi * c.radius * c.radius
	}
computeArea(#Circle({radius: 2.0}))                // 12.56636
computeArea(#Point)                                // 0.0
computeArea(#Rectangle({length: 3.0, width: 4.0})) // 12.0
computeArea(#Custom(3.0))                          // 3.0
</pre></dd>
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
	exec(image, mem := GiB, cpu := 1) (bed, nuc file) {"
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
When matching lists, your pattern must be the same length as the list.  For
example,
<pre>
val [a, b] = ["a", "b"] // ok
val [a, b] = ["a", "b", "c"] // will fail with an error
val [a, b] = ["a"] // will fail with an error
</pre>
Use <code>...x</code> to match the remainder of the list.  For example,
<pre>
val lst = ["a", "b", "c", "d", "e"]
val [a, b, ...cde] = lst
</pre>
results in <code>a := "a"; b := "b"; cde := ["c", "d", "e"]</code>.
If you want to ignore the tail of the list, use <code>...</code> (no
identifier),
<pre>
val lst = ["a", "b", "c", "d", "e"]
val [a, b, ...] = lst
</pre>
Use pattern matching to extract values from variants:
<pre>
type Message string
type Excuse string
type YesNo #Yes(Message) | #No(Excuse)
val decision YesNo = #No("just because")
val #No(reason) = decision // reason == "just because"
val #Yes(excuse) = decision // ERROR: cannot match tag #Yes with a variant with tag #No
</pre>
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
<p>
Comprehensions can range over multiple lists (or maps),
computing the cross product of these. For example
<pre>
val integers = [1,2,3,4]
val chars = ["a", "b", "c"]
val cross = [(i, c) | i <- integers, c <- chars]
</pre>
computes the cross product <code>[(1, "a"), (1, "b"), (1, "c"), (2, "a"), (2, "b"), (2, "c"), (3, "a"), (3, "b"), (3, "c"), (4, "a"), (4, "b"), (4, "c")]</code>.
<p>
Finally, comprehensions can apply filters.
As an example, the following computes the
cross product only for even integers:
<pre>
val evenCross = [(i, c) | i <- integers, if i%2 == 0, c <- chars]
</pre>
resulting in <code>[(2, "a"), (2, "b"), (2, "c"), (4, "a"), (4, "b"), (4, "c")]</code>.
  </dd>
</dl>

## Operators
### Sum `+`
The sum operator can be used with list, dir, map, int, float, string types.
When summing a map or a dir, if the same key exists in the LHS and RHS, the RHS is picked.  e.g.:
```
    a := ["a": 2, "b": 1]
    b := ["a": 3]
    Main := a + b
```
Main is `["a", 3, b: "1"]`.
### Other arithmetic operators `-, *, /`
Subtract, multiply and division operators can be used with int and float types.
### modulo `%`
Modulo operator is valid only for int types.
### Negation `-`
Negation operation is valid for int and float types.

### Comparison
- `==, !=` Equality/inequality operators can be used with list, map, tuple, record, dir, string, int and float types. 
When comparing files, the digests of the files are compared. When comparing dirs, the names and digests of the files in the directories are compared.
- `<, >, <=, >=` Comparison operators can be used with int, float and string types.

### Bitwise shift `<<, >>`
Bit shift operators can be used with int type.

### Logical `||, &&, !`
Logical operators can be used with boolean expressions.

## Builtins

Reflow provides a number of builtin functions. They provide special semantics 
or typing that cannot be expressed in core Reflow.

### Data structure length: `len`

Builtin `len` evaluates to the length of the underlying data structure:

- `len` of a file evaluates to the size of the file in bytes;
- `len` of a directory returns the number of entries in the directory;
- `len` of a list returns the number of entries in the list;
- `len` of a map returns the number of entries in the map.

### Numeric coercion: `int`, `float`

Builtins `int` and `float` coerces floating point numbers to integers
and vice versa.

### Tupling and untupling: `zip`, `unzip`

Builtin `zip` takes two lists and returns a new list of tuples 
consisting of elements from each.
Builtin `unzip` is `zip`'s inverse: 
given a list of tuples, it returns two lists of elements,
each from a different tuple position.

### Flattening lists: `flatten`

Builtin `flatten` concatenates all the elements in a list of lists.

### Map coercion: `map`

Builtin `map` converts its argument to a map.
Given a list of tuples, a map is constructed by 
taking the first element of the tuple as a key
and the second element as values.
Given a directory, a map is constructed using 
the directory's paths as keys with its files as values.

### List coercion: `list`

Builtin `list` converts its argument to a list.
Given a map, `list` returns a list of tuples,
where the first element of the tuple are 
the map's keys and the second its values.
Given a directory, `list` returns a list of tuples
of paths and files.

### Reduce/fold a list: `reduce`,`fold`

Builtin `reduce` reduces a list given a function by repeatedly calling the
function with one element of the list at a time, left to right.

    `reduce(func(type, type) type, [type]) type`
`reduce` panics if the list is empty.
```
a := reduce(func(i, j int) => i + j, [1, 2, 3, 4])
b := reduce(func(i, j {a int}) => if i.a > j.a { {a: i.a} } else { {a: j.a} }, [{a: 2}, {a:7}, {a: 1}])
```

Builtin `fold` left folds the list with an initial value and a supplied
function. It repeatedly calls the function with one element of the list
at a time and uses the result as the accumulated value of the next function
call.

    `fold(func(typeA, typeB) typeA, list [typeB], init typeA) typeA`

`typeA` and `typeB` can be the same. Fold returns the initial value if
the list is empty.

```
a := fold(func(i, j int) => i + j, [1, 2, 3], 0)
b := fold(func(i, j {b int}) => {b: i.b + j.b}, [{b:1}, {b:1}], {b:0})
c := fold(func(i {b int}, j int) => {b: i.b + j}, [1, 2, 3], {b:0})
d := fold(func(i, j int) => if i >= j { i } else { j }, [], 0)
```

### Runtime errors: `panic`

Builtin `panic` halts execution with a (string) message.
Panic is used to indicate a runtime error.

### Sequencing: `~>`

The expression

	e1 ~> e2

forces expression `e1` to be evaluated before `e2`.
Since Reflow has lazy evaluation semantics 
(see [Evaluation Semantics](#evaluation-semantics) below),
evaluation is usually sequenced only when there is a data dependency
between the two expressions `e1` and `e2`.
The `~>` operator allows the user to override this behavior by
introducing a new control dependency.
This can be useful when there are external dependencies between 
`e1` and `e2` that are not witnessed by their data dependencies.

### Trace debugging: `trace`

Builtin `trace` forces its argument, 
pretty-prints it to standard error along with the source-code position 
of the `trace` expression; the value is then returned.
Trace is useful for debugging:
for example, the following prints the size of a file as it is evaluated.

	val f = exec(image, cpu, mem) (out file) {"
		some_complicated_command >{{out}}
	"}

	val g = {
		val input = trace(f)
		exec(...) {" {{ input}} "}
	}

### Ranging: `range`

Builtin `range` produces a list of integers in the provided range (exclusive).
For example, the following expressions are equivalent:

	range(0, 5) == [0, 1, 2, 3, 4]
	range(0, 1) == [0]
	range(0, 0) == []

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

<a id='evaluation-semantics'></a>
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
results, but access only the first item, the remaining ones are never
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
