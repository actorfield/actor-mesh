Subject: Actor Mesh — getting started

## Quick Start

```sh
# 1. Clone the repo
fossil clone actor-mesh.fossil actor-mesh
cd actor-mesh
fossil open actor-mesh.fossil

# 2. Install deps (Fedora)
sudo dnf install nng-devel lmdb-devel gcc make

# 3. Build
make                                    # actor + proxy

# 4. Run the tests
make test-mesh
```

## What gets built

| Binary | Purpose |
|---|---|
| `bin/actor` | Runtime — forks handlers, manages LMDB, connects to mesh |
| `bin/mesh-proxy` | Bus — forwards pub/sub messages, ~130 LOC |

## Run the tests

```sh
make test-mesh          # 10/10 unit tests

make test-concurrency   # ACTOR_CONCURRENCY + child reaping
make test-isolation     # ACTOR_* confinement (Linux; skips what the host lacks)
```

## Architecture

Three concepts, zero frameworks:

- **Proxy** — pub/sub bus. Actors publish messages to topics. Subscribers receive them.
- **Actor** — fork/exec loop. Receives message, runs handler, publishes result. LMDB for durability.
- **Handler** — any process. Reads payload from stdin, writes result to stdout, exits.

```
Actor → pub topic:orders → Proxy → sub topic:orders → Actor
```

## Adding your own handler

```sh
# 1. Write a handler (any language, stdin → stdout)
cat > my-handler.sh << 'EOF'
#!/bin/sh
payload=$(cat)
echo "got: $payload"
EOF
chmod +x my-handler.sh

# 2. Start it as an actor
ACTOR_ID=my-actor \
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

## Key files

| File | What |
|---|---|
| `AGENTS.md` | Agent instructions for working in this repo |
| `DESIGN.md` | Architecture overview, tuple header, handler contract |
| `docs/` | Quarto docs (build with `quarto render docs/`) |
| `tests/` | C integration test suite |
| `vendor/` | Vendored deps (cjson, lmdb, mpack, smhasher) |

## Troubleshooting

- **Port in use**: `fuser -k 5556/tcp 5557/tcp`
- **LMDB errors**: `rm -rf /tmp/actor-mesh` or whatever `ACTOR_LMDB_PATH` is
- **Handler not found**: Use absolute paths for `ACTOR_HANDLER`
