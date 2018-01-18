// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

/*
Package syntax implements the Reflow language.

It is a simple, type-safe, applicative domain specific language
that compiles to Reflow's Flow IR.

A Reflow type is one of the following (where t1, t2, etc. are
themselves types):

	int                                // the type of arbitrary precision integers
	float                              // the type of arbitrary precision floats
	string                             // the type of (UTF-8 encoded) strings
	bool                               // the type of booleans
	file                               // the type of files
	dir                                // the type of directories
	(t1, t2, .., tn)                   // the type of the tuple consisting of types
	                                   // t1, t2, t3...
	(id1, id2 t1, .., idn tn)          // the type of the tuple (t1, t1, ..., tn), but
	                                   // with optional labels (syntactic affordance)
	[t]                                // the type of list of type t
	[k:v]                              // the type of map with keys of type k
	                                   // and values of type v
	{id1 t1, id2 t2}                   // the type of a struct with fields id1, id2
	                                   // of types t1, t2 respectively
	{id1, id2 t1, id3 t3}              // the type of struct{id1 t1, id2 t1, id3 t3}
	                                   // (syntactic affordance)
	func(t1, t2, ..., tn) tr           // the type of a function with argument types
	                                   // t1, t2, ..., tn, and return type tr
	func(a1, a2 t1, ..., an tn) tr     // a function of type func(t1, t1, ..., tn) tr
	                                   // with labels (syntactic affordance)

A Reflow expression is one of the following (where e1, e2, .. are themselves
expressions, d1, d2, .. are declarations; t1, t2, .. are types):

	123                                // a literal (arbitrary precision) integer
	"abc"                              // a literal UTF-8 encoded string
	ident                              // an identifier
	[e1, e2, e3, ..]                   // a list of e1, e2, e3..
	[e1, e2, ...e3]                    // list e1, e2, concatenated with list e3
	[e1:e2, e3:e4, ..]                 // a map  key e1 to value e2, etc.
	[e1:e2, e3:e4, ...e5]              // as above, with map e5 appended
	(e1, e2, e3, ..)                   // a tuple of e1, e2, e3, ..
	{id1: e1, id2: e2, ..}             // a struct with fields id1 with value e1, id2 with value e2, ..
	{id1, id2, ..}                     // a shorthand for {id1: id1, id2: id2}
	{d1; d2; ...; dn; e1}              // a block of declarations usable by expression e1
	func(id1, id2 t1, id3 t3) t4 => e1 // a function literal with arguments and return type; evaluates e1
	func(id1, id2 t1, id3 t3) => e1    // a function literal with arguments, return type omitted
	exec(d1, d2, ..) t1 {{ template }} // an exec with declarations d1, d2, .., returning t1 with template
	                                   // identifiers are valid declarations in this context; they are
	                                   // deparsed as id := id.
	e1 <op> e2                         // a binary op (||, &&, <, >, <=, >=, !=, ==, +, /, %, &, <<, >>)
	<op> e1                            // unary expression (!)
	if e1 { d1; d2; ..; e2 }
	else { d3; d4; ..; e3 }            // conditional expression (with declaration blocks)
	(e1)                               // parenthesized expression to control precedence
	int(e1)                            // builtin float to int type conversion
	float(e1)                          // builtin int to float type conversion
	len(e1)                            // builtin length operator
	zip(e1, e2)                        // builtin zip operator
	unzip(e1)                          // builtin unzip operator
	map(e1)                            // convert e1 to a map
	list(e1)                           // convert e1 to a list
	make(strlit, d1, ..., dn)          // builtin make primitive. identifiers are valid declarations in
	                                   // this context; they are deparsed as id := id.
	panic(e1)                          // terminate the program with error e1
	[e1 | c1, c2,..., cn]              // list comprehension: evaluate e1 in the environment provided by
	                                   // the given clauses (see below)
	e1 ~> e2                           // force evaluation of e1, ignore its result, then evaluate e2.
	flatten(e1)                        // flatten a list of lists into a single list
	trace(e1)                          // trace expression e1: evaluate it, print it to console,
	                                   // and return it. Can be used for debugging.

A comprehension clause is one of the following:

	pat <- e1                          // bind a pattern to each of the list or map e1
	if e1                              // filter entries that do not meet the predicate e1

A Reflow declaration is one of the following:

	val id = e1 or id := e1            // assign e1 to id
	val id t1 = e1                     // assign e1 to id with type t1
	type id t1                         // declare id as a type alias to t1
	func id(a1, a2 t1) r1 = e1         // sugar for id := func(a1, a2 t1) r1 => e1
	func id(a1, a2 t1) = e1            // sugar for id := func(a1, a2 t1) => e1

Value declarations may be preceded by one of the following
annoations, each of which takes a list of declarations.

	@requires(...)                     // resource requirement annotation,
	                                   // takes declarations mem int,
	                                   // cpu int or cpu float, disk int,
	                                   // cpufeatures[string, and wide
	                                   // bool. They indicate resource
	                                   // requirements for computing the
	                                   // declaration; if wide is set to
	                                   // true, then the resource
	                                   // requirements have no
	                                   // theoretical upper bound. Wide
	                                   // is thus useful for
	                                   // declarations whose
	                                   // parallelization factor is not
	                                   // known statically, for example
	                                   // when processing sharded data.

Value declarations can take destructive pattern bindings, mimicing
value constructors. Currently tuples and lists are supported.
Patterns accept identifiers and "_" (ignore), but not yet literal
values. Patterns follow the grammar:

	_
	[p1, p2, ..., pn]
	(p1, p2, ..., pn)
	{id1: p1, ..., idn: pn}
	{id1, ..., id3}                    // sugar for {id1: id1, ..., idn: idn}

For example, the declaration

	val [(x1, y1), (x2, y2), _] = e1

pattern matches e1 to a list of length three, whose elements are 2-tuples.
The first two tuples are bound; the third is ignored.

A Reflow module consists of, in order: a optional keyspace, a set of
optional parameters, and a set of declarations.

	keyspace "com.grail.WGSv1"         // defines the keyspace to "com.grail.WGS1"

	param id1 = e1                     // defines parameter id1 with default e1
	param id2 string                   // defines parameter without default, of type t1

	param (                            // block version of parameters
		id3 int
		id4 stirng
	)

	declarations                       // declarations as above

The Reflow language infers types, except for in function arguments
and non-default params. Everywhere else, types can safely be omitted.
When a type is supplied, it is used as an ascription: the expression
is ascribed to the given type; the type checker fails if the
expression's type is incompatible with its ascription.

Following Go, semicolons are inserted if a newline appears after
the following tokens: identifier, string, integer, template, ')',
'}', or ']'

All bytes in a line following the characters "//" are commentary.
*/
package syntax

//go:generate goyacc reflow.y
