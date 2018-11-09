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
  * [Porting Reflow](#porting-reflow)

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

## Porting Reflow

Reflow defines a small set of interfaces that are used to
interact with external cloud services. To add support for
new services (e.g., a storage or compute backends), 
implementations of the relevant interfaces must be provided.

The interfaces are:

- [runner.Cluster](https://github.com/grailbio/reflow/blob/db37d993f656f2569481a0aebc97f64fbde795b1/runner/cluster.go#L17) 
  for compute backends;
- [blob.Store](https://github.com/grailbio/reflow/blob/db37d993f656f2569481a0aebc97f64fbde795b1/blob/blob.go#L21) 
  for blob-storage backends (to access data within Reflow and to store cached objects);
- [assoc.Assoc](https://github.com/grailbio/reflow/blob/db37d993f656f2569481a0aebc97f64fbde795b1/assoc/assoc.go#L49) 
  for cache associations backends (roughly, a key to object mapping).

Existing implementations of these include:

- [ec2cluster.Cluster](https://github.com/grailbio/reflow/blob/db37d993f656f2569481a0aebc97f64fbde795b1/ec2cluster/ec2cluster.go#L61)
  implements `runner.Cluster` on top of AWS's EC2 computer service;
- [s3blob.Store](https://github.com/grailbio/reflow/blob/db37d993f656f2569481a0aebc97f64fbde795b1/blob/s3blob/s3blob.go#L48)
  `blob.Store` on top of AWS's S3 storage service.
- [dydbassoc.Assoc](https://github.com/grailbio/reflow/blob/db37d993f656f2569481a0aebc97f64fbde795b1/assoc/dydbassoc/dydbassoc.go#L43) 
  implements a `assoc.Assoc` on top of AWS's DynamoDB service.

Reflow's [configuration package](https://github.com/grailbio/reflow/tree/master/config)
is used to wire together different implementations of these underlying services.
New service implementations 
should also implement configuration providers 
for use with this package. 
For example,
blob stores based on `s3blob.Blob` 
are configured through the configuration provider package
[github.com/grailbio/reflow/blob/master/config/s3config](https://github.com/grailbio/reflow/blob/master/config/s3config/s3.go).
