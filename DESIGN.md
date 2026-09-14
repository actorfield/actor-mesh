# Actor Mesh — Design Document

## Overview

A minimal distributed actor mesh built on Unix primitives.
No frameworks. No sidecars. No brokers. Just processes.

The runtime is ~506 lines of C. The proxy is ~57 lines of C.
A handler is any process that speaks stdio.

---

## Core Principle

Everything in the mesh is an Actor. The only difference between actor types
is what they subscribe to and what handler they run.

```
SQL Tool    →  actor + handlers/sqlite-tool   subscribes to: sql_query
LLM Agent   →  actor + handlers/llm-agent     subscribes to: user_message, sql_result
Observer    →  actor + <any handler>           subscribes to: heartbeat
```

One binary. Config and handler define behaviour.

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

Each actor:
- Connects to proxy via NNG pub0 (publish) and sub0 (subscribe)
- Forks a handler process per tuple
- Writes payload to handler stdin
- Reads result from handler stdout
- Publishes result back to bus
- Maintains local LMDB for durability

---

## Tuple Header

Fixed 256-byte binary header. No serialisation library. Cast and read — zero copy.
The header is followed directly by payload bytes in a single contiguous buffer.

```c
typedef struct __attribute__((packed)) {
    char     topic[32];           // NNG subscription prefix (at offset 0)
    uint8_t  id[16];              // uuidv7 binary
    uint8_t  correlation_id[16];  // trace chain end to end
    uint8_t  causation_id[16];    // direct parent tuple id
    char     origin[32];          // actor id who emitted
    int64_t  emitted_at;          // unix nanoseconds
    int64_t  ttl;                 // nanoseconds, 0 = no expiry
    int32_t  attempt;             // retry count, 0 = first
    uint32_t payload_len;         // bytes following this header
    uint8_t  _reserved[120];      // pad to 256 bytes
} actor_header_t;
```

Wire format:
```
[ actor_header_t 256 bytes ][ payload bytes ]
```

Payload is opaque bytes. The runtime never inspects it.

---

## Handler Contract

Handler is any process. The only contract:

```
read payload bytes from stdin
do work
write result bytes to stdout
exit
```

Runtime forks one handler per tuple. Fresh process every time.
No persistent state in handler. No memory leaks. No OOM.

Result topic routing: if stdout starts with a line of the form `topic_name\n`,
the runtime strips it and uses that as the publish topic, overriding
`ACTOR_RESULT_TOPIC`. This lets a single handler emit different tuple types.

```
sql_query\n{"type":"sql_query","sql":"SELECT ..."}
```

Header env vars available to every handler:

| Variable | Value |
|---|---|
| `ACTOR_TUPLE_ID` | hex id of incoming tuple |
| `ACTOR_CORRELATION_ID` | hex correlation id (same across chain) |
| `ACTOR_CAUSATION_ID` | hex id of direct parent tuple |
| `ACTOR_TUPLE_ORIGIN` | origin actor id |
| `ACTOR_ATTEMPT` | retries and replays so far, 0 = first run |
| `ACTOR_TUPLE_TOPIC` | topic that delivered the tuple |
| `ACTOR_TUPLE_DEADLINE` | `emitted_at + ttl` in unix ns; `0` = no deadline |

---

## Multi-topic Subscription

`ACTOR_TOPIC` accepts a comma-separated list:

```sh
ACTOR_TOPIC=user_message,sql_result ./actor
```

One actor instance receives tuples on all listed topics.

### Lanes

A topic can have its own handler, result topic or concurrency:

```sh
ACTOR_TOPIC=sql_query,cancel
ACTOR_HANDLER=./handlers/sqlite-tool   ACTOR_CONCURRENCY=4
ACTOR_HANDLER_cancel=./handlers/cancel ACTOR_CONCURRENCY_cancel=1
```

A topic with a setting of its own becomes a lane: its own socket and its own
workers, so it never waits behind another topic's work. Topics without one
share the default lane, which is the whole actor when nothing is set. The
lanes together may use at most 32 workers; asking for more fails startup.
Per-topic settings need identifier topics (`[A-Za-z0-9_]`), the same rule as
a handler's topic override.

---

## Handler Examples

**Shell (zero deps):**
```sh
#!/bin/sh
payload=$(cat)
# Example with any OpenAI-compatible API:
curl -s https://api.openai.com/v1/chat/completions \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -d "$(echo "$payload" | jq -c '{model:"claude-haiku-4-5-20251001",max_tokens:1000,messages:[{role:"user",content:.query}]}')" \
  | jq -c '{answer:.content[0].text}'
```

**C binary (zero alloc):**
```c
static char g_in[256*1024];
int main(void) {
    size_t n = fread(g_in, 1, sizeof(g_in)-1, stdin);
    g_in[n] = '\0';
    // ... process ...
    fputs(result, stdout);
}
```

---

## Runtime Lifecycle

```
1. Read config from environment
2. Apply isolation if configured — before sockets, LMDB, or any thread
3. Connect NNG pub0 + sub0 to proxy
4. Open LMDB
5. Enter poll loop:
   a. Emit heartbeat every ACTOR_HEARTBEAT_MS
   b. NNG recvmsg (100ms timeout)
   c. Receive header + payload as single buffer
   d. Cast to actor_header_t (zero copy)
   e. Check TTL — drop if expired
   f. Write frame to LMDB inbox (durability)
   g. Set header env vars, fork handler (+ per-tuple namespaces if configured)
   h. Write payload to handler stdin
   i. Read result into static buffer (capped at ACTOR_MAX_PAYLOAD)
   j. Build result header (carry correlation chain)
   k. Write result frame to LMDB outbox
   l. Publish result to NNG bus
   m. Clear LMDB inbox + outbox
6. On SIGTERM — stop receiving, let running handlers finish, exit.
   One that fails is not retried; it stays in the inbox for the next run.
```

---

## Durability

LMDB per actor pod. Three named databases:

```
inbox/   {uuidv7} → raw frame    written before fork, cleared after publish
outbox/  {uuidv7} → raw frame    written before publish, cleared after publish
state/   {key}    → bytes        handler-managed state (e.g. conversation history)
```

On restart, every tuple left in the inbox is processed again before new
messages, with `ACTOR_ATTEMPT` one higher so the handler can tell. The
inbox entry is cleared only after the result is published, so this also
covers a crash mid-publish: delivery is at-least-once.
No central coordinator needed.

Handlers can use the same LMDB (via `ACTOR_LMDB_PATH`) to persist state
keyed by `ACTOR_CORRELATION_ID`, enabling multi-turn conversations across
separate invocations.

---

## Memory Model

Zero heap allocation per tuple in the runtime.

```c
static uint8_t g_result_buf[ACTOR_MAX_PAYLOAD];
static uint8_t g_frame_buf[ACTOR_MAX_PAYLOAD + sizeof(actor_header_t)];
```

Memory footprint is fixed and known at compile time.
Payloads exceeding cap are dropped with a log message — never malloc.

Override at build time:
```bash
make ACTOR_MAX_PAYLOAD=4194304
```

Handlers follow the same discipline: static buffers only, no malloc/realloc.
Data flows as pointers from stdin buffer to stdout — no heap involvement.

---

## Isolation

Optional, opt-in, Linux only. An actor with none of these set behaves exactly
as it did before they existed — that property is asserted by the test suite,
not assumed.

Two scopes, because isolation here has two lifetimes:

**Actor-lifetime** — applied once at startup, before the poll loop and before
any thread is created. Privilege drop, resource limits, cgroup membership,
Landlock, seccomp. These define what this actor may ever do.

**Tuple-lifetime** — applied per message, in the forked child, before `exec`.
Namespaces, created and destroyed with one tuple.

```
1. Read config from environment
2. Isolation: uid/gid, rlimits, cgroup, Landlock, seccomp   ← actor scope
3. Connect NNG, open LMDB, start workers
4. Poll loop:
     fork → unshare namespaces → exec handler               ← tuple scope
```

### Why namespaces are per tuple

A tuple is a self-contained unit of work. The runtime already holds that for
memory (fixed buffers, no heap per tuple) and for process identity (a fresh
fork per handler). The process *tree* was where it leaked: a handler's own
children could outlive it, get reparented to the actor, and accumulate across
tuples meant to be independent.

A namespace created per tuple closes that. When the handler exits, the kernel
kills whatever remains inside it — teardown is not something anyone has to
remember. The tuple leaves no trace, which is what the model claimed in the
first place.

`unshare(CLONE_NEWPID)` does not move the caller into the new namespace; it
makes the caller's *next* fork PID 1 of it, and the namespace dies when that
PID 1 exits. So the runtime forks again after unsharing, and the grandchild —
which is PID 1 — becomes the handler. Placed at startup instead, the first
handler would be PID 1 and every later tuple would fork into a dead namespace.

### Fail closed

A variable that is set and cannot be honoured aborts startup. An actor that
runs less confined than it believes it is, is worse than one that never tried:
the deployment sees no error and assumes a boundary it does not have. A
malformed value is an error too — reading `ACTOR_UID=root` as uid 0 is the
worst available reading of a typo.

### Practical notes

- **`ACTOR_LMDB_PATH` must be inside `ACTOR_LANDLOCK_RW`.** The actor opens its
  database after the ruleset is enforced. This is checked at startup so the
  message names the real problem.
- **`ACTOR_LANDLOCK_RO` must cover the whole exec path**: `/bin/sh`, the handler,
  the dynamic loader and every library they need. On glibc that is `/usr` and
  `/lib64`; on musl `/usr` and `/lib`. A missing entry produces a handler that
  cannot start.
- **`ACTOR_RLIMIT_NPROC` counts every process and thread for the real uid,
  system-wide** — not the actor's descendants. Give an actor its own uid and
  size the limit against `ps -L -u <uid> | wc -l`, or a value that looks
  generous can already be below current usage and the actor dies on its first
  `pthread_create`. Root in the initial user namespace, and any process holding
  `CAP_SYS_ADMIN` or `CAP_SYS_RESOURCE`, is exempt from it, so setting it there
  without an `ACTOR_UID` to drop to fails startup.
- **Namespaces need privilege.** Unprivileged `unshare(CLONE_NEWPID)` is EPERM
  on a stock host; the runtime retries behind a user namespace, which works
  where unprivileged user namespaces are enabled. `ACTOR_TUPLE_USERNS=0` opts
  out.
- **`ACTOR_SECCOMP=1` and `ACTOR_TUPLE_UNSHARE` conflict** and are refused
  together: the filter is inherited by the child, where the namespaces need
  `unshare` and the mount family. Use `ACTOR_SECCOMP=permissive_mount`.

### Observing what handlers actually call

The seccomp denylist is a fixed set of calls no handler should need. It is not
derived from your handlers, and it cannot be: the handler contract is "any
process that speaks stdio", so the syscall set moves with the language, the
libc, and the work. The list shipped here was validated by tracing the runtime
plus shell handlers — 57 distinct calls — which is enough to justify a denylist
and nowhere near enough to justify an allowlist.

eBPF is the right tool for closing that gap. It belongs to the *node*, not to
the actor: a privileged agent shipped in the node image, observing the
unprivileged workloads around it. That is the usual shape and it keeps the
privilege boundary intact — the actor still cannot see or influence the tracer,
and the seccomp filter denies `bpf` so it cannot start one of its own. Same rule
as cgroups: the runtime is observed and joins what it is given; it does not
provision.

If you build the node image, this is close to free. `ACTOR_CGROUP_PATH` already
gives every actor a cgroup identity the kernel can filter on, so a single
node-level agent attributes syscalls per actor with no change to the runtime and
no per-actor configuration.

What it buys:

- **Keeping the denylist honest.** If a handler starts calling something the
  list denies, you see it before it becomes a failed tuple in production.
- **Narrowing toward an allowlist where it is safe.** For a *known, fixed* set
  of handlers — one org's tools, say — the observed set over weeks is real
  evidence, and an allowlist built from it is defensible in a way one built from
  a hand-trace is not.
- **Attributing a denial.** A tuple that failed under seccomp says only that the
  handler exited non-zero. A trace says which syscall, in which handler, for
  which correlation id.

Off-the-shelf tooling is enough; nothing custom is required:

```sh
# What is every handler under this actor calling?
sudo execsnoop-bpfcc                       # exec of each handler
sudo syscount-bpfcc -p $(pgrep -f bin/actor) -L    # per-syscall counts

# Or scope by cgroup, if actors join one (ACTOR_CGROUP_PATH)
sudo bpftrace -e 'tracepoint:raw_syscalls:sys_enter
                  /cgroup == cgroupid("/sys/fs/cgroup/actors/org-a")/
                  { @[args.id] = count(); }'
```

The cgroup form is the useful one at scale: `ACTOR_CGROUP_PATH` already gives
each actor a stable identity the kernel understands, so a trace can be scoped to
one actor, one org, or the whole fleet without touching the runtime.

This is deliberately not built into the runtime. Adding it would mean the actor
loading kernel programs — the privilege the rest of this design spends its
effort giving up, and the one thing a compromised handler would most want. As a
separate privileged component on a node image you control, it is exactly the
right place for it.

### Example

```sh
ACTOR_ID=sqlite-tool-1 \
ACTOR_TOPIC=sql_query ACTOR_RESULT_TOPIC=sql_result \
ACTOR_HANDLER=/handlers/sqlite-tool \
ACTOR_LMDB_PATH=/var/actor/db \
ACTOR_UID=61000 ACTOR_GID=61000 \
ACTOR_RLIMIT_AS=536870912 ACTOR_RLIMIT_NOFILE=256 \
ACTOR_LANDLOCK_RO=/usr:/lib64 \
ACTOR_LANDLOCK_RW=/var/actor/db \
ACTOR_TUPLE_UNSHARE=pid,ipc,uts \
ACTOR_SECCOMP=permissive_mount \
./actor &
```

---

## Heartbeat

Every actor emits a heartbeat tuple periodically, listing what it is running:

```json
{"id": "sqlite-tool-1", "inbox": 1, "outbox": 0,
 "running": [{"tuple": "0190…", "correlation": "0190…", "topic": "sql_query",
              "pid": 4312, "age_ms": 850, "terminating": false}],
 "services": [{"name": "web", "pid": 4100, "restarts": 0}]}
```

Topic: `heartbeat`
TTL: 3 × heartbeat interval

Any actor subscribed to `heartbeat` can observe the mesh state. `running` is
the actor's process table: `pid` leads the handler's process group, and
`correlation` is what a `_term` names to stop it. It is empty on Windows.

The thread that reaps handlers sends it, so a long-running handler never
silences it — at `ACTOR_CONCURRENCY=1` the only worker is busy for exactly as
long as the heartbeat matters most.

---

## Retry Policy

On handler failure (non-zero exit):
```
attempt 1  →  retry after 100ms
attempt 2  →  retry after 200ms
attempt 3  →  retry after 400ms
max retry  →  drop tuple, log error
```

Controlled by `ACTOR_RETRY_MAX` (default 3).
Payload cap exceeded → drop immediately, no retry.

TTL is a deadline for starting work, not for finishing it. It is checked
before the handler runs and again before each retry, so a tuple whose caller
has given up is rejected as `ttl_expired` rather than run again. A running
handler is not stopped at it; it can read the deadline from
`ACTOR_TUPLE_DEADLINE` and budget its own work.

---

## Remote control

A message starts a handler; `_term` is the message that stops one.

Every actor subscribes to `_term`. The header names the target, so the
runtime still never reads a payload:

- `correlation_id` — the chain to stop. Required; all-zero is ignored.
- `causation_id` — if non-zero, only the tuple with that id.

Each handler leads its own process group. On `_term` the actor sends
SIGTERM to the group of every matching handler, and SIGKILL to whatever is
left after `ACTOR_TERM_GRACE_MS`. The SIGKILL is what guarantees the stop:
the init of a PID namespace ignores SIGTERM from outside unless it handles
it. The tuple is reported as `tuple_rejected` with reason `terminated`, and
is neither retried nor replayed.

`_term` is read by the thread that reaps handlers, not by the workers, so it
arrives even when every worker is busy. It stops what is running; a tuple
still queued or waiting between retries is not affected. Anyone who can
publish to the bus can send one — the same trust as any other publish.

---

## Services

A handler lives for one tuple. A service lives as long as the actor:

```sh
ACTOR_INIT='./migrate'                  # once, to completion, before anything is served
ACTOR_SERVICE_web='./web --port 8080'   # started with the actor, restarted when it exits
```

`ACTOR_INIT` runs after isolation is applied and before the bus is joined; a
non-zero exit stops the actor, like any other failed startup step. Each
`ACTOR_SERVICE_<name>` starts before the first tuple is served, in its own
process group, and the thread that reaps handlers restarts it when it exits.
After five restarts within a minute the actor gives up and exits non-zero,
so whatever supervises the actor backs off instead of watching it spin. At
shutdown, services get SIGTERM once the handlers have drained, and SIGKILL
after `ACTOR_TERM_GRACE_MS`. The heartbeat lists them under `services`.

Both inherit the actor's isolation. Neither is available on Windows.

---

## Built-in bus

`mesh-proxy` is a forwarder with two sockets. An actor given the same two
variables runs that forwarder itself, on a thread of its own:

```sh
PROXY_SUB_BIND=tcp://127.0.0.1:5557 PROXY_PUB_BIND=tcp://127.0.0.1:5556 \
ACTOR_BUS_PUB=tcp://127.0.0.1:5557  ACTOR_BUS_SUB=tcp://127.0.0.1:5556 ... ./actor
```

The bus listens before the actor dials it and before any service starts, so
nothing waits on start-up order, and it closes last. Everything else on the
mesh dials it exactly as it would dial `mesh-proxy`. It sends no heartbeat of
its own; the actor's covers it. Not available on Windows.

---

## Configuration

| Variable | Required | Default | Description |
|---|---|---|---|
| `ACTOR_ID` | ✅ | — | Unique instance id |
| `ACTOR_TOPIC` | ✅ | — | Subscription topic (comma-sep for multi) |
| `ACTOR_RESULT_TOPIC` | ✅ | — | Default topic stamped on result tuples |
| `ACTOR_BUS_SUB` | ✅ | — | NNG pub endpoint e.g. `tcp://bus:5556` |
| `ACTOR_BUS_PUB` | ✅ | — | NNG sub endpoint e.g. `tcp://bus:5557` |
| `ACTOR_HANDLER` | ✅ | — | Handler binary path |
| `ACTOR_LMDB_PATH` | ✅ | — | LMDB directory path |
| `ACTOR_TTL_NS` | ☐ | 0 | Default tuple TTL nanoseconds |
| `ACTOR_HEARTBEAT_MS` | ☐ | 5000 | Heartbeat interval ms |
| `ACTOR_RETRY_MAX` | ☐ | 3 | Max handler retries |
| `ACTOR_CONCURRENCY` | ☐ | 1 | Messages in flight at once (max 32) |
| `ACTOR_TERM_GRACE_MS` | ☐ | 5000 | SIGTERM to SIGKILL grace for `_term` (Unix) |
| `ACTOR_HANDLER_<topic>`, `ACTOR_RESULT_TOPIC_<topic>`, `ACTOR_CONCURRENCY_<topic>` | ☐ | — | Give a topic its own lane (see [Lanes](#lanes)) |
| `ACTOR_INIT` | ☐ | — | Command run once before serving (see [Services](#services)) |
| `ACTOR_SERVICE_<name>` | ☐ | — | Command kept running for the actor's lifetime (Unix) |

Isolation (Linux, all optional — see [Isolation](#isolation)):

| Variable | Default | Description |
|---|---|---|
| `ACTOR_UID` / `ACTOR_GID` | — | Drop to this uid/gid, irreversibly |
| `ACTOR_RLIMIT_AS` / `_CPU` / `_NOFILE` / `_NPROC` | — | Resource limits |
| `ACTOR_CGROUP_PATH` | — | Existing cgroup v2 directory to join |
| `ACTOR_LANDLOCK_RO` | — | Colon-separated read-only paths |
| `ACTOR_LANDLOCK_RW` | — | Colon-separated read/write paths |
| `ACTOR_LANDLOCK_NET_CONNECT` | — | Colon-separated outbound TCP ports |
| `ACTOR_SECCOMP` | — | `1`, or `permissive_mount` with namespaces |
| `ACTOR_TUPLE_UNSHARE` | — | Per-handler namespaces: `pid,ipc,uts,net,mount` |
| `ACTOR_ROOTFS` | — | `pivot_root` target; needs `pid,mount` |
| `ACTOR_TUPLE_USERNS` | — | `0` disables the user-namespace fallback |

Proxy:

| Variable | Default | Description |
|---|---|---|
| `PROXY_SUB_BIND` | `tcp://*:5557` | Actors publish here |
| `PROXY_PUB_BIND` | `tcp://*:5556` | Actors subscribe here |

Set both on an actor and it hosts the bus itself; see [Built-in bus](#built-in-bus).

---

## File Layout

```
actor-mesh/
├── Makefile                    build actor + proxy
├── DESIGN.md
├── runtime/
│   ├── actor.h                 public API — actor_run()
│   ├── actor.c                 runtime (~506 lines, zero malloc)
│   ├── actor_tuple.h           256-byte header + helpers
│   ├── actor_uuid.h            uuidv7 single-header, no deps
│   ├── bus.c, bus.h            pub/sub forwarder, shared with the proxy
│   └── main.c                  12-line entrypoint
├── proxy/
│   └── proxy.c                 NNG pub/sub fanout (~57 lines)
├── examples/
│   └── employee-mesh/          HR Q&A demo (ReAct loop over SQLite)
│       ├── Makefile            build + run + query
│       ├── client.c            CLI client — sends query, prints answer
│       ├── handlers/
│       │   ├── llm-agent.c     ReAct agent — Ollama + LMDB state
│       │   └── sqlite-tool.c   SQL executor — reads EMPLOYEE_DB
│       └── db/
│           └── employee.db     sample employee database
└── vendor/
    ├── cjson/                  cJSON v1.7.18
    ├── mpack/                  MessagePack
    └── smhasher/               MurmurHash
```

---

## Build

```bash
# runtime
make

# employee-mesh demo
cd examples/employee-mesh
make
make run
make query Q="who manages the Development department?"
make stop
```

---

## Deployment

**Bare process:**
```sh
./mesh-proxy &
ACTOR_ID=sqlite-tool-1 \
ACTOR_TOPIC=sql_query \
ACTOR_RESULT_TOPIC=sql_result \
ACTOR_BUS_SUB=tcp://localhost:5556 \
ACTOR_BUS_PUB=tcp://localhost:5557 \
ACTOR_HANDLER=./handlers/sqlite-tool \
ACTOR_LMDB_PATH=/var/actor/sqlite-tool \
./actor &
```

**K8s (optional):**
```yaml
env:
  - name: ACTOR_ID
    valueFrom: { fieldRef: { fieldPath: metadata.name } }
  - name: ACTOR_TOPIC
    value: sql_query
  - name: ACTOR_RESULT_TOPIC
    value: sql_result
  - name: ACTOR_BUS_SUB
    value: tcp://proxy:5556
  - name: ACTOR_BUS_PUB
    value: tcp://proxy:5557
  - name: ACTOR_HANDLER
    value: /handlers/sqlite-tool
  - name: ACTOR_LMDB_PATH
    value: /var/actor/lmdb
```

K8s is optional. The runtime has no awareness of it.

---

## Lineage

This design independently converges on:

- **Erlang/OTP (1986)** — actor model, let it crash, message passing
- **Unix pipes (1969)** — stdio as universal process interface
- **Linda tuplespace (1986)** — decoupled coordination via shared space
- **Plan 9** — everything is a file, communicate via read/write

The difference: C native, zero framework, same binary edge to cloud.
