# Waterfall

Waterfall is a declarative task execution framework. 

# Why Another Execution Framework

There are many, many execution frameworks out there that support defining
tasks with inter-task dependencies. Most of them only partially include
scheduling in their design.

# Building and Running

```bash
cargo build

# wf is a cli for running worlds directly

# A redis instance is required for storage

# Run using the local executor
cargo run --bin wf -- --config examples/config.json --world examples/world.json

# Starting an agent
# wfw is a (W)ater(F)low (W)orker
cargo run --bin wfw
cargo run --bin wf -- --config examples/config_wfw.json --world examples/world.json
```

# Overview

## Example

## Resources

Resources are at the heart of Waterfall. They are simple things: labels
with an associated set of time intervals. Tasks produce resources for
given intervals.

## Tasks

Tasks are commands that run on a set schedule. Each task produces one or
more `Resource`. The run schedule naturally breaks up the timeline into
intervals. When a task runs at time `T_n`, it will make make each resource
it provides available over the interval `(T_{n-1},T]`.

### Commands

A task has three commands defined:

- **check** - Command used to run an out-of-band verification of the data. Should have no side effects.
- **up** - Command run to create resources.
- **down** - Command run when removing resources.

### Dependencies

Tasks will run at their scheduled time (or immediately if their scheduled time
has passed already).

It's possible to define additional constraints on launching, though. Some tasks
may need resources produced by other tasks before it can start.
