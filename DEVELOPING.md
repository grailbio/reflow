# Development notes

This document contains various notes for working with Reflow's code base.

# Running a local Reflowlet

It's often useful to run a local standing Reflowlet, instead of using -local,
e.g., when debugging interactions between the Reflow evaluator and 
a Reflowlet, or debugging work stealing.

First, launch a local Reflowlet. (If you launch multiple instances, make sure
they have different working directories.)

	% reflowlet -addr :9000 -config /tmp/config -dir /tmp/reflowlet

And then use the "static" cluster provider to use them from a Reflow
invocation, e.g.,:

	% reflow -config /tmp/config -cluster static,localhost:9000 ...

Multiple instances are separated by commas.

