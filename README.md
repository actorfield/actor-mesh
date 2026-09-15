# Actor Mesh

A minimal distributed actor mesh built on Unix primitives.  
No frameworks. No sidecars. No brokers. Just processes.

**Runtime:** ~1,500 lines of C, plus ~1,300 of optional isolation &nbsp;|&nbsp; **Proxy:** ~130 lines of C &nbsp;|&nbsp; **Handler:** any process that speaks stdio

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│                  NNG Proxy                       │
│   sub0 ← collects published messages             │
│   pub0 → fans out to subscribers                 │
│   nng_device(sub, pub) — dumb forwarding         │
└───────────────────┬──────────────────────────────┘
                    │  tcp://
          ┌─────────┼─────────┐
          ↓         ↓         ↓
       Actor A   Actor B   Actor C
       (any)     (any)     (any)
```

Three concepts:
- **Proxy** — pub/sub bus. Actors publish to topics. Subscribers receive them.
- **Actor** — fork/exec loop. Receives message, runs handler, publishes result. LMDB for durability.
- **Handler** — any process. Reads payload from stdin, writes result to stdout, exits.

Optionally, an actor can confine itself and each tuple it handles — uid, resource
limits, cgroup, Landlock, seccomp, and per-handler namespaces — all opt-in through
the environment. See [Isolation](DESIGN.md#isolation).

## Quick Start

```sh
# 1. Clone
git clone https://github.com/muralidhar-challa/actor-mesh.git
cd actor-mesh

# 2. Install deps (Fedora)
sudo dnf install nng-devel lmdb-devel gcc make

# 3. Build
make                                    # actor + proxy
cd examples/employee-mesh && make       # handlers

# 4. Start a mesh
bash test-mcp-mesh.sh "how many employees?"
```

## What Gets Built

| Binary | Purpose |
|--------|---------|
| `bin/actor` | Runtime — forks handlers, manages LMDB, connects to mesh |
| `bin/mesh-proxy` | Bus — forwards pub/sub messages, ~130 LOC |
| `bin/llm-agent` | ReAct agent handler — calls LLM, uses tools |

## Adding Your Own Agent

```sh
# 1. Write a handler (any language, stdin → stdout)
cat > my-handler.sh << 'EOF'
#!/bin/sh
payload=$(cat)
echo "got: $payload"
EOF
chmod +x my-handler.sh

# 2. Start it as an actor
ACTOR_ID=my-agent \
ACTOR_TOPIC=my_topic \
ACTOR_RESULT_TOPIC=my_result \
ACTOR_HANDLER=./my-handler.sh \
ACTOR_LMDB_PATH=/tmp/my-agent \
ACTOR_BUS_SUB=tcp://127.0.0.1:5556 \
ACTOR_BUS_PUB=tcp://127.0.0.1:5557 \
./bin/actor &

# 3. Send a message
./bin/pub my_topic "hello"
```

## Tests

```sh
make test-mesh               # 10/10 — proxy, actor, handler contract, TTL, registry

make test-concurrency        # ACTOR_CONCURRENCY and child reaping
make test-isolation          # ACTOR_* confinement (Linux)
```

`test-isolation` skips cases the host cannot support — unprivileged namespaces,
cgroup v2 delegation — rather than failing them. To exercise everything in the
image actors actually run in:

```sh
podman build -f Dockerfile.test -t actor-test .
podman run --rm actor-test sh -c 'cd /src && ./bin/test-isolation'
```

## Documents

| File | What |
|------|------|
| [DESIGN.md](DESIGN.md) | Architecture, tuple header, handler contract, memory model |
| [SPEC.md](SPEC.md) | Formal specification (Z-notation schemas, procedures, predicates) |
| [AGENTS.md](AGENTS.md) | Multi-agent patterns, tool discovery, LLM integration |
| [START.md](START.md) | Getting started, troubleshooting, MCP tool auto-discovery |
| [DESIGN.md#isolation](DESIGN.md#isolation) | Optional process/tuple confinement — namespaces, Landlock, seccomp, cgroups |

## Lineage

This design independently converges on:

- **Erlang/OTP (1986)** — actor model, let it crash, message passing
- **Unix pipes (1969)** — stdio as universal process interface
- **Linda tuplespace (1986)** — decoupled coordination via shared space
- **Plan 9** — everything is a file, communicate via read/write

The difference: C native, zero framework, same binary edge to cloud.
