# Reflow User's Guide

*Note:* This user's guide is a work in progress.

There is a separate [language summary](../LANGUAGE.md)
describing Reflow's data processing DSL.
This will be merged into this user's guide over time.

## Overview 

Reflow is a language and runtime for distributed, 
incremental data processing in the cloud.

This user's guide documents the Reflow system:
the Reflow language;
command `reflow`;
and its runtime.

## Contents

  * [Overview](#overview)
  * [Contents](#contents)
  * [Distribution](#distribution)
    * [Bundles](#bundles)

## Distribution

Since Reflow modules may depend on other modules,
it isn't always sufficient to distribute 
a single Reflow source file.

Reflow includes a _bundle_ mechanism to
simplify distribution of modules.

### Bundles

A bundle is an archived version of a Reflow module,
packaged together with its full set of dependencies.
A bundled module may be used like any other Reflow module:
it can be passed to `reflow run`, `reflow doc`, etc.,
and may be instantiated by other modules.

A bundle is a single file with the extension ".rfx".
Bundles are stand-alone and hermetic,
and are designed to ease distribution of Reflow modules.

Command `reflow bundle` creates a new bundle.
It takes as its argument the Reflow module to be bundled
together with an optional set of command-line flags.
These flags provide default arguments to the module's parameters.

For example,
to bundle the module `test.rf`:

	param (
		x = "ok"
		y string
	)

	val Main = x+y

with a default argument for `y`, 
we invoke the command

	% reflow bundle test.rf -y="hello"

This creates the file `test.rfx`,
which we can treat like any other Reflow module:

	% reflow doc test.rfx
	Parameters
	
	val x string = "ok"
	val y string = "hello"
	
	Declarations
	
	val Main string

Note that the module's documentation 
now contains the parameter that we provided
in the `reflow bundle` invocation.

We provide further arguments or overrides
when using `reflow run`:

	% reflow run test.rfx -x="hello "
	"hello hello"

