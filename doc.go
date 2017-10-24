// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package reflow implements the core data structures and (abstract) runtime
// for Reflow.
//
// Reflow is a system for distributed program execution. The programs are described
// by Flows, which are an abstract specification of the program's execution. Each Flow
// node can take any number of other Flows as dependent inputs and perform some
// (local) execution over these inputs in order to compute some output value.
//
// Reflow supports a limited form of dynamic dependencies: a Flow may evaluate to
// a list of values, each of which may be executed independently. This mechanism
// also provides parallelism.
//
// The system orchestrates Flow execution by evaluating the flow in the manner of an
// abstract syntax tree; see Eval for more details.
package reflow
