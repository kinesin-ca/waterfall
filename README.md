Waterfall
=========

Waterfall is a declarative task execution framework. 
Why Another Execution Framework
===============================

There are many, many execution frameworks out there that support defining
tasks with inter-task dependencies. Most of them only partially include
scheduling in their design.

Overview
--------

Resources
=========

Resources are at the heart of Waterfall. They are simple things: labels
with an associated set of time intervals. Tasks produce resources for
given intervals.

Tasks
=====

Tasks are commands that run on a set schedule. Each task produces one or
more `Resource`. The run schedule naturally breaks up the timeline into
intervals. When a task runs at time $T_n$, it will make make each resource
it provides available over the interval $(T_{n-1},T]$.
