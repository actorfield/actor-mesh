Subject: Actor Mesh — getting started

## Quick Start

```sh
# 1. Clone the repo
fossil clone actor-mesh.fossil actor-mesh
cd actor-mesh
fossil open actor-mesh.fossil

# 2. Install deps (Fedora)
sudo dnf install nng-devel lmdb-devel gcc make python3-msgpack

# 3. Build
make                                    # actor + proxy
cd examples/employee-mesh && make       # handlers
cd ../.. && make -C examples/ping-pong  # pub tool

# 4. Start a simple mesh
cd examples/employee-mesh
export BRIDGE_LIB="$PWD/handlers/lib"
bash test-mcp-mesh.sh "how many employees?"
```

## What gets built

| Binary | Purpose |
|---|---|
| `bin/actor` | Runtime — forks handlers, manages LMDB, connects to mesh |
| `bin/mesh-proxy` | Bus — forwards pub/sub messages, ~130 LOC |
| `bin/llm-agent` | ReAct agent handler — calls LLM, uses tools |
| `handlers/tools/shell-exec.sh` | Run shell commands as an actor |

## Run the tests

```sh
make test-mesh          # 10/10 unit tests

make test-concurrency   # ACTOR_CONCURRENCY + child reaping
make test-isolation     # ACTOR_* confinement (Linux; skips what the host lacks)

gcc -Wall -O2 -std=c11 tests/test-employee.c -lnng -o bin/test-employee
bin/test-employee        # 9/9 integration (needs Ollama or API key)
```

## Architecture

Three concepts, zero frameworks:

- **Proxy** — pub/sub bus. Actors publish messages to topics. Subscribers receive them.
- **Actor** — fork/exec loop. Receives message, runs handler, publishes result. LMDB for durability.
- **Handler** — any process. Reads payload from stdin, writes result to stdout, exits.

```
Actor → pub topic:orders → Proxy → sub topic:orders → Actor
```

## Adding your own agent

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

## Auto-discovering tools (MCP)

```sh
# Start the tool registry
ACTOR_ID=registry ACTOR_TOPIC=_tool_announce,_tool_discover \
ACTOR_RESULT_TOPIC=_tool_list \
ACTOR_HANDLER=./handlers/registry/tool-registry.py \
ACTOR_LMDB_PATH=/tmp/reg ./bin/actor &

# Announce an MCP server as a tool
# Agent discovers it automatically via _tool_list
```

## Key files

| File | What |
|---|---|
| `AGENTS.md` | Multi-agent patterns + Claude Platform comparison |
| `DESIGN.md` | Architecture overview, tuple header, handler contract |
| `docs/` | Quarto docs (build with `quarto render docs/`) |
| `tests/` | C integration test suite |
| `vendor/` | Vendored deps (clay, mpack, cjson, lmdb) |

## Troubleshooting

- **Port in use**: `fuser -k 5556/tcp 5557/tcp`
- **LMDB errors**: `rm -rf /tmp/actor-mesh` or whatever `ACTOR_LMDB_PATH` is
- **Agent not responding**: Check Ollama is running: `curl http://localhost:11434/api/tags`
- **Handler not found**: Use absolute paths for `ACTOR_HANDLER`
