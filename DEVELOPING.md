# Development notes

This document contains various notes for working with Reflow's code base.

# Running a local Reflow server

It's often useful to run a local standing Reflow server, instead of using -local,
e.g., when debugging interactions between the Reflow evaluator and 
a Reflow server, or debugging work stealing.

First, launch a local Reflow server. (If you launch multiple instances, make sure
they have different working directories.)

	% reflow serve -addr :9000 -config /tmp/config -dir /tmp/reflowlet

And then use the "static" cluster provider to use them from a Reflow
invocation, e.g.,:

	% reflow -config /tmp/config -cluster static,localhost:9000 ...

Multiple instances are separated by commas.
