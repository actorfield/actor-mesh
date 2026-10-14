# AGENTS.md

Guidance for agents working in this repository.

## What this is

Actor Mesh is a minimal distributed actor runtime: an NNG pub/sub proxy
(`proxy/`) and a fork/exec actor runtime (`runtime/`) with LMDB durability and
optional Linux confinement. A "handler" is any process that reads a payload from
stdin and writes a result to stdout.

## Build & test

- `make` — builds `bin/actor`, `bin/mesh-proxy`, `bin/actor-confine`
- `make test-mesh` — core mesh integration tests (needs `bin/actor` + proxy)
- `make test-concurrency` / `make test-isolation` — concurrency + Linux confinement
- Cross-compile / static builds: `./build.sh` (see its header)

The runtime is Linux-targeted for the confinement paths (`runtime/actor_isolation.c`).
On macOS, `make` builds `mesh-proxy` and the examples but not `actor` (Linux-only
`sys/prctl.h`, Landlock, seccomp).

## Layout

- `runtime/` — actor + bus + isolation
- `proxy/` — the shared forwarder
- `tests/` — integration tests
- `examples/ping-pong/` — minimal two-actor example
- `docs/` — Quarto book
- `SPEC.md` / `DESIGN.md` — spec + design

## Style

- C11, `-Wall -Wextra -O2 -std=c11`. No heap allocation on the per-tuple path.
- Comments explain *why*, not what.
- Keep changes minimal; do not add frameworks or dependencies.
