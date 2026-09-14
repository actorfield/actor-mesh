# Actor Mesh — Formal Specification

A distributed actor mesh built on Unix primitives. Runtime: 624 lines of C.
Proxy: 100 lines of C. Handler: any process that speaks stdio.

---

## §1 Base Types and Constants

### 1.1 Primitive Types

```
UUID    ≙ uint8[16]                     — 16-byte binary uuidv7
HexID   ≙ char[33]                      — null-terminated 32-char lowercase hex
Path    ≙ char[*]                       — null-terminated file path
Topic   ≙ char[32]                      — null-terminated topic string
URL     ≙ char[*]                       — NNG URL e.g. "tcp://host:port"
Frame   ≙ uint8[256 + N]  where N ≤ M   — header + payload in contiguous buffer
                                              M = ACTOR_MAX_PAYLOAD
```

### 1.2 Named Constants

```
ACTOR_MAX_PAYLOAD  : ℕ   = 1 048 576   — 1 MiB default, override at compile time
ACTOR_MAX_FRAME    : ℕ   = ACTOR_MAX_PAYLOAD + 256
HEADER_SIZE        : ℕ   = 256          — sizeof(actor_header_t), enforced by _Static_assert
RECV_TIMEOUT_MS    : ℕ   = 100          — NNG recvmsg timeout (heartbeat granularity)
DIAL_RETRIES       : ℕ   = 30           — max connection attempts to proxy
DIAL_BACKOFF_S     : ℕ   = 1            — seconds between dial retries
MDB_MAP_SIZE       : ℕ   = 67 108 864   — 64 MiB LMDB environment cap
MDB_MAX_DBS        : ℕ   = 3            — inbox, outbox, state
```

### 1.3 Derived Functions

```
now_ns() : ℤ
  ts ← clock_gettime(CLOCK_REALTIME)
  return ts.tv_sec × 1 000 000 000 + ts.tv_nsec

now_ms() : ℤ
  return now_ns() ÷ 1 000 000

backoff(attempt : ℕ) : ℤ               where attempt ≥ 1
  return 100 000 000 × 2^(attempt − 1)   — nanoseconds
```

---

## §2 Tuple Model

### 2.1 Header Schema

```
┌─ actor_header_t ────────────────────────────────────────────────────┐
│ topic          : Topic             — subscription prefix (offset 0) │
│ id             : UUID              — uuidv7, unique per emission    │
│ correlation_id : UUID              — end-to-end trace identifier    │
│ causation_id   : UUID              — direct parent tuple id         │
│ origin         : char[32]          — ACTOR_ID of emitter            │
│ emitted_at     : ℤ                 — unix nanoseconds               │
│ ttl            : ℤ                 — nanoseconds, 0 ≡ ∞             │
│ attempt        : ℕ                 — retry count, 0 ≡ first         │
│ payload_len    : ℕ32               — bytes following header         │
│ _reserved      : uint8[120]        — zeroed pad                     │
├─────────────────────────────────────────────────────────────────────┤
│ sizeof = 256                                                        │
│ topic at offset 0 for NNG sub0 prefix matching                      │
│ payload at offset 256 = (uint8*)(hdr + 1)                           │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 Wire Format

```
wire(t : actor_header_t, p : uint8[t.payload_len]) : uint8[256 + t.payload_len]
  ≙  [ bytes_of(t) 256 bytes ][ bytes_of(p) t.payload_len bytes ]
```

### 2.3 TTL Expiry Predicate

```
predicate Expired(h : actor_header_t) ≙
  h.ttl ≠ 0  ∧  now_ns() > h.emitted_at + h.ttl
```

### 2.4 Tuple Initialisation

```
procedure Init(h : actor_header_t*, topic : Topic, origin : char[32],
               corr_id : UUID|null, caus_id : UUID|null, plen : ℕ32):
  { post: h._reserved = 0₁₂₀
          ∧ h.topic = trunc₃₂(topic)
          ∧ h.origin = trunc₃₂(origin)
          ∧ h.correlation_id = corr_id ∨ 0₁₆
          ∧ h.causation_id   = caus_id ∨ 0₁₆
          ∧ h.payload_len = plen
          ∧ h.emitted_at = now_ns()
          ∧ h.id = h.id₀   -- caller must set via uuidv7_gen()         }
```

### 2.5 Payload Access

```
predicate ValidFrame(buf : uint8*, len : ℕ) ≙
  len ≥ 256  ∧  ((actor_header_t*)buf).payload_len ≤ len − 256

payload(h : actor_header_t*) : uint8*  ≙  (uint8*)(h + 1)
```

---

## §3 Actor Runtime State

### 3.1 Configuration

```
┌─ ActorConfig ───────────────────────────────────────────────────────┐
│ id           : char[32]          — ACTOR_ID                         │
│ topic_list   : char[256]         — ACTOR_TOPIC, comma-separated     │
│ result_topic : Topic             — ACTOR_RESULT_TOPIC               │
│ bus_sub_url  : URL               — ACTOR_BUS_SUB, e.g. tcp://p:5556 │
│ bus_pub_url  : URL               — ACTOR_BUS_PUB, e.g. tcp://p:5557 │
│ handler_cmd  : char[*]           — ACTOR_HANDLER, sh -c argument    │
│ lmdb_path    : Path              — ACTOR_LMDB_PATH                  │
│ ttl_ns       : ℤ                 — ACTOR_TTL_NS,   default 0        │
│ heartbeat_ms : ℕ                 — ACTOR_HEARTBEAT_MS, default 5000 │
│ retry_max    : ℕ₀                — ACTOR_RETRY_MAX, default 3       │
├─────────────────────────────────────────────────────────────────────┤
│ { id, topic_list, result_topic, bus_sub_url, bus_pub_url,           │
│   handler_cmd, lmdb_path } all ≠ null                               │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.2 Runtime State

```
┌─ ActorState ────────────────────────────────────────────────────────┐
│ cfg           : ActorConfig                                         │
│ nng_pub       : nng_socket                                          │
│ nng_sub       : nng_socket                                          │
│ mdb_env       : MDB_env*                                            │
│ dbi_inbox     : MDB_dbi        — key=t.id, val=wire(t,p)            │
│ dbi_outbox    : MDB_dbi        — key=t.id, val=wire(t,p)            │
│ dbi_state     : MDB_dbi        — key=bytes, val=bytes (handler)     │
│ g_stop        : 𝔹              — true → exit poll loop              │
│ g_result_buf  : uint8[M]       — handler stdout buffer, M = CAP     │
│ g_frame_buf   : uint8[M+256]   — assembly buffer for wire format    │
│ last_hb_ms    : ℤ              — last heartbeat emission time       │
├─────────────────────────────────────────────────────────────────────┤
│ CAP = ACTOR_MAX_PAYLOAD                                             │
│ g_result_buf ∩ g_frame_buf = {}   — independent static buffers      │
│ No heap allocation per tuple                                        │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.3 Connection State

```
┌─ Connection ────────────────────────────────────────────────────────┐
│ nng_pub dialled   : 𝔹                                               │
│ nng_sub dialled   : 𝔹                                               │
│ subscriptions     : ℙ Topic         — derived from topic_list       │
├─────────────────────────────────────────────────────────────────────┤
│ subscribers = { t ∈ Topic | ∃tok ∈ split(cfg.topic_list, ',')       │
│                             ∧ tok = trim(tok) ∧ t = tok }           │
│ Each subscription uses strlen(t)+1 bytes for exact match            │
└─────────────────────────────────────────────────────────────────────┘

Lane ≙ { topics ⊆ subscribers, handler, result_topic, concurrency, sub }
  — a topic with any of ACTOR_{HANDLER,RESULT_TOPIC,CONCURRENCY}_<topic>
    set gets a lane of its own; the others share one, which is every topic
    when none is set. Σ lane.concurrency ≤ ACTOR_MAX_CONCURRENCY (32).

Service ≙ { name, cmd, pid, restarts }        — ACTOR_SERVICE_<name>=<cmd>
  — started before the first tuple, in its own process group; restarted by
    the reaper when it exits; a 6th exit within 60s stops the actor, which
    then exits non-zero. ACTOR_INIT runs once, to completion, before the
    bus is joined. Unix only.
```

---

## §4 Proxy State

```
┌─ ProxyState ────────────────────────────────────────────────────────┐
│ sub_sock       : nng_socket    — binds PROXY_SUB_BIND (sub0)        │
│ pub_sock       : nng_socket    — binds PROXY_PUB_BIND (pub0)        │
│ g_stop         : 𝔹             — true → exit                       │
│ proxy_id       : char[32]      — PROXY_ID, default "proxy"          │
│ hb_ms          : ℕ             — PROXY_HEARTBEAT_MS, default 5000   │
├─────────────────────────────────────────────────────────────────────┤
│ sub_sock subscribes ""  — wildcard: receives ALL messages           │
│ pub_sock binds all addresses in comma-separated URL list            │
└─────────────────────────────────────────────────────────────────────┘
```

The forwarder is runtime/bus.c. An actor with both PROXY_SUB_BIND and
PROXY_PUB_BIND set runs the same forwarder on a thread of its own (Unix).

---

## §5 LMDB Durability

### 5.1 Database Invariants

```
┌─ Durability ────────────────────────────────────────────────────────┐
│ inbox  : UUID ⇸ Frame    — one entry per processing tuple           │
│ outbox : UUID ⇸ Frame    — transient, deleted immediately after send│
│ state  : bytes ⇸ bytes   — handler-managed, never cleared by runtime│
├─────────────────────────────────────────────────────────────────────┤
│ dom(inbox) ∩ dom(outbox) = ∅                                        │
│ ∀ k ∈ dom(inbox) : k is the id of a tuple currently being processed │
│ |inbox| ≤ ACTOR_MAX_CONCURRENCY  (one per busy worker)              │
│ Environment size ≤ 64 MiB                                            │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.2 Crash Recovery Model

```
Before processing  →  lmdb_put(inbox,  t.id, wire(t, p))
After success       →  lmdb_del(inbox,  t.id)
                      [outbox was put and deleted within publish_result]
After max retries   →  lmdb_del(inbox,  t.id)
While stopping      →  no retry; a failed tuple stays in inbox

On restart:
  — outbox is cleared; replaying the inbox supersedes it
  — each inbox entry is processed again before new messages,
    with attempt + 1, by the configured workers
  — an entry past its TTL is rejected (ttl_expired) and deleted
  — delivery is therefore at-least-once
```

---

## §6 Handler Contract

### 6.1 Execution Schema

```
┌─ HandlerInvocation ─────────────────────────────────────────────────┐
│ Δ(inbox)                                                            │
│ cmd     : char[*]       — cfg.handler_cmd                           │
│ stdin   : uint8[plen]   — input payload bytes                       │
│ env     : Environment   — parent env + header vars                  │
├─────────────────────────────────────────────────────────────────────┤
│ Handler is spawned as:                                              │
│   Unix:    fork() + exec("/bin/sh", "sh", "-c", cmd, NULL)           │
│   Windows: CreateProcess("cmd.exe", "/c", cmd, ...)                 │
│                                                                     │
│ Child process:                                                      │
│   reads  payload from stdin (pipe)                                  │
│   writes result  to stdout (pipe)                                   │
│   stderr inherits parent's stderr                                   │
│   exits  with status code                                           │
└─────────────────────────────────────────────────────────────────────┘
```

### 6.2 Injected Environment

```
setenv("ACTOR_TUPLE_ID",       hex(t.id),             1)
setenv("ACTOR_CORRELATION_ID", hex(t.correlation_id), 1)
setenv("ACTOR_CAUSATION_ID",   hex(t.causation_id),   1)
setenv("ACTOR_TUPLE_ORIGIN",   t.origin,              1)
setenv("ACTOR_ATTEMPT",        str(attempt),          1)  — retries and replays
setenv("ACTOR_TUPLE_TOPIC",    t.topic,               1)
setenv("ACTOR_TUPLE_DEADLINE", str(t.ttl ≠ 0 ? t.emitted_at + t.ttl : 0), 1)
```

### 6.3 Topic Routing by Handler

```
predicate ValidTopicByte(c : uint8) ≙
  (c ∈ ['a','z'] ∪ ['A','Z'] ∪ ['0','9'] ∪ {'_'})

ParseTopicOverride(buf : uint8*, len : ℕ) : (Topic | null, ℕ)
  ≡  if len = 0: return (null, 0)
     i ← 0
     while i < min(len, 31):
       if buf[i] = '\n' ∧ i > 0:        — valid override
         return (buf[0..i−1], i+1)
       if ¬ValidTopicByte(buf[i]):      — invalid char, no override
         return (null, 0)
       i ← i + 1
     return (null, 0)                    — no newline found
```

### 6.4 Exit Code Semantics

```
RunStatus ::= RUN_OK | RUN_FAILED | RUN_TOO_LARGE | RUN_TERMINATED

RUN_OK         ≙ exited 0; result_len bytes are in g_result_buf
                 (result_len = 0: nothing to publish, treated as done)
RUN_FAILED     ≙ spawn failed, exited non-zero, or killed by a signal
RUN_TOO_LARGE  ≙ output reached ACTOR_MAX_PAYLOAD
RUN_TERMINATED ≙ stopped by _term: SIGTERM to its process group, then
                 SIGKILL after ACTOR_TERM_GRACE_MS

RejectReason ::= payload_cap_exceeded | result_cap_exceeded
               | max_retries_exceeded | ttl_expired | terminated
               — the wire values of a tuple_rejected "reason"
```

---

## §7 Procedures

### 7.1 Actor Main

```
procedure actor_run() : {0, −1}:
  { pre:  ACTOR_ID, ACTOR_TOPIC, ACTOR_RESULT_TOPIC,
          ACTOR_BUS_SUB, ACTOR_BUS_PUB, ACTOR_HANDLER,
          ACTOR_LMDB_PATH are set in environment          }

  cfg ← LoadConfig()
  if cfg = ⊥: return −1

  signal(SIGTERM, → g_stop ← 1)
  signal(SIGINT,  → g_stop ← 1)

  if NngSetup(cfg) < 0: return −1
  if LmdbSetup(cfg) < 0: return −1

  — heartbeats come from the reaper thread every cfg.heartbeat_ms, so a
    busy worker never delays them; on Windows, which has none, this loop
    sends them
  while ¬g_stop:
    msg ← ⊥
    rc ← nng_recvmsg(nng_sub, &msg, 0)    — 100ms timeout
    if rc = NNG_ETIMEDOUT:  continue
    if rc ≠ 0:  break if g_stop else continue

    body     ← nng_msg_body(msg)
    body_len ← nng_msg_len(msg)

    if body_len < 256:
      drop, nng_msg_free(msg), continue

    hdr     ← (actor_header_t*)body
    payload ← body + 256
    plen    ← body_len − 256

    if Expired(hdr):
      PublishRejection(hdr, "ttl_expired")
      nng_msg_free(msg), continue

    ProcessTuple(hdr, payload, plen)
    nng_msg_free(msg)

  nng_close(nng_pub)
  nng_close(nng_sub)
  mdb_env_close(mdb_env)
  return 0
```

### 7.2 NngSetup

```
procedure NngSetup(cfg : ActorConfig) : {0, −1}:
  if nng_pub0_open(&nng_pub) ≠ 0: return −1
  for i ∈ [0, 30):
    if nng_dial(nng_pub, cfg.bus_pub_url, ...) = 0: break
    sleep(1)
  if not connected: return −1

  for each lane ∈ lanes:                  — one SUB socket per lane (§3.3)
    if nng_sub0_open(&lane.sub) ≠ 0: return −1
    for i ∈ [0, 30):
      if nng_dial(lane.sub, cfg.bus_sub_url, ...) = 0: break
      sleep(1)
    if not connected: return −1
    for each tok ∈ split(lane.topics, ','):
      nng_socket_set(lane.sub, NNG_OPT_SUB_SUBSCRIBE, tok, strlen(tok) + 1)
      — +1 includes null byte for exact (non-prefix) match
    nng_socket_set_ms(lane.sub, NNG_OPT_RECVTIMEO, 100)
  return 0
```

### 7.3 ProcessTuple

```
procedure ProcessTuple(hdr : actor_header_t*, payload : uint8*,
                       plen : ℕ):
  { pre:  plen ≤ ACTOR_MAX_PAYLOAD                               }

  if plen > ACTOR_MAX_PAYLOAD:
    PublishRejection(hdr, "payload_cap_exceeded")
    return

  — durability checkpoint
  wire ← [ bytes_of(hdr) 256 bytes ][ payload plen bytes ]
  LmdbPut(dbi_inbox, hdr.id, 16, wire, 256 + plen)

  attempt ← 0
  while attempt ≤ cfg.retry_max:
    (run, result_len) ← InvokeHandler(hdr, payload, plen, hdr.attempt + attempt)

    if run = RUN_OK:
      if result_len > 0: PublishResult(hdr, result_len)
      break

    if run = RUN_TOO_LARGE:
      PublishRejection(hdr, "result_cap_exceeded")
      break  — no retry on overflow

    if run = RUN_TERMINATED:
      PublishRejection(hdr, "terminated")
      break  — deliberate: neither retried nor replayed

    — RUN_FAILED
    if g_stop: return  — stays in the inbox for the next run (§5.2)
    attempt ← attempt + 1
    if attempt > cfg.retry_max:
      PublishRejection(hdr, "max_retries_exceeded")
      break

    nanosleep(backoff(attempt))
    if Expired(hdr):
      PublishRejection(hdr, "ttl_expired")
      break  — the caller gave up while we waited
    log("retry %d/%d", attempt, cfg.retry_max)

  LmdbDel(dbi_inbox, hdr.id, 16)
```

### 7.4 InvokeHandler

```
procedure InvokeHandler(hdr : actor_header_t*, payload : uint8*,
                        plen : ℕ, attempt : ℕ) : (RunStatus, ℕ):
  { post: result_len ≤ ACTOR_MAX_PAYLOAD                          }

  SetHeaderEnvVars(hdr, attempt)    — §6.2

  return PlatformSpawn(payload, plen, g_result_buf, ACTOR_MAX_PAYLOAD)
```

### 7.5 PublishResult

```
procedure PublishResult(in_hdr : actor_header_t*, result_len : ℕ):
  { pre:  result_len > 0                                            }

  (override, offset) ← ParseTopicOverride(g_result_buf, result_len)

  out_topic   ← override ≠ null  ?  override    : cfg.result_topic
  out_payload ← g_result_buf + offset
  out_plen    ← result_len − offset

  — build result header
  Init(&out_hdr, out_topic, cfg.id,
       in_hdr.correlation_id,    — carry end-to-end trace
       in_hdr.id,                — direct parent
       out_plen)
  Uuidv7Gen(out_hdr.id)
  out_hdr.ttl ← cfg.ttl_ns

  — assemble frame
  wire ← [ bytes_of(out_hdr) 256 bytes ][ out_payload out_plen bytes ]
  LmdbPut(dbi_outbox, out_hdr.id, 16, wire, 256 + out_plen)

  nng_send(nng_pub, wire, 256 + out_plen, 0)

  LmdbDel(dbi_outbox, out_hdr.id, 16)
```

### 7.6 PublishRejection

```
procedure PublishRejection(in_hdr : actor_header_t*, reason : char[*]):
  payload ← FormatJson({ tuple_id      : hex(in_hdr.id),
                         correlation_id: hex(in_hdr.correlation_id),
                         origin        : in_hdr.origin,
                         topic         : in_hdr.topic,
                         reason        : reason })
  plen ← strlen(payload)

  Init(&hdr, "tuple_rejected", cfg.id,
       in_hdr.correlation_id,
       in_hdr.id,
       plen)
  Uuidv7Gen(hdr.id)

  wire ← [ bytes_of(hdr) 256 bytes ][ payload plen bytes ]
  nng_send(nng_pub, wire, 256 + plen, 0)
```

### 7.7 EmitHeartbeat

```
procedure EmitHeartbeat(id : char[32]):
  inbox_sz  ← LmdbCount(dbi_inbox)
  outbox_sz ← LmdbCount(dbi_outbox)
  running   ← [ { tuple, correlation, topic, pid, age_ms, terminating }
                | slot ∈ children, slot busy ]      — [] on Windows
  payload   ← FormatJson({ id: id, inbox: inbox_sz, outbox: outbox_sz,
                           running: running,
                           services: [ { name, pid, restarts } ] })
  plen      ← strlen(payload)

  Init(&hdr, "heartbeat", id, null, null, plen)
  Uuidv7Gen(hdr.id)
  hdr.ttl ← cfg.heartbeat_ms × 3 × 1_000_000  — 3× interval

  wire ← [ bytes_of(hdr) 256 bytes ][ payload plen bytes ]
  nng_send(nng_pub, wire, 256 + plen, 0)
```

### 7.8 Proxy Main

```
procedure proxy_main():
  signal(SIGTERM, → g_stop ← 1)
  signal(SIGINT,  → g_stop ← 1)

  sub ← Sub0Open()
  pub ← Pub0Open()

  ListenAll(sub, PROXY_SUB_BIND)     — "tcp://*:5557" default
  SubscribeAll(sub, "")              — wildcard: receive everything
  ListenAll(pub, PROXY_PUB_BIND)     — "tcp://*:5556" default
  SetRecvTimeout(sub, 100ms)

  last_hb ← 0
  while ¬g_stop:
    if now_ms() − last_hb ≥ hb_ms:
      EmitHeartbeat(proxy_id)
      last_hb ← now_ms()

    — Mesh forwarding
    msg ← ⊥
    rc ← nng_recvmsg(sub, &msg, 0)
    if rc = NNG_ETIMEDOUT: continue
    if rc ≠ 0: break if g_stop else continue
    nng_sendmsg(pub, msg, 0)         — forward to all subscribers
    nng_msg_free(msg)

  nng_close(pub)
  nng_close(sub)
```

### 7.9 PlatformSpawn

```
procedure PlatformSpawn(stdin_data : uint8*, in_len : ℕ,
                        stdout_buf : uint8*, out_cap : ℕ) : (RunStatus, ℕ):
  { pre:  out_cap = ACTOR_MAX_PAYLOAD                                 }

  — Unix path
  pipe(to_child)     — parent writes, child reads
  pipe(from_child)   — child writes, parent reads

  pid ← fork()
  if pid = 0:
    dup2(to_child[0],   STDIN_FILENO)
    dup2(from_child[1], STDOUT_FILENO)
    close all pipe ends
    exec("/bin/sh", "sh", "-c", cfg.handler_cmd, NULL)
    _exit(1)           — exec failed

  close(to_child[0])
  close(from_child[1])

  write(to_child[1], stdin_data, in_len)
  close(to_child[1])          — send EOF to child

  len ← 0
  while (n ← read(from_child[0], stdout_buf + len, out_cap − len)) > 0:
    len ← len + n
    if len = out_cap:
      drain remaining bytes   — read and discard until EOF
      close(from_child[0])
      waitpid(pid, ...)
      return (RUN_TOO_LARGE, 0)

  close(from_child[0])
  waitpid(pid, &status, 0)

  if ¬WIFEXITED(status) ∨ WEXITSTATUS(status) ≠ 0:
    return (RUN_FAILED, 0)     — handler error or signal
  return (RUN_OK, len)
```

---

## §8 Predicates

### 8.1 Configuration Validity

```
predicate ValidConfig(cfg : ActorConfig) ≙
  cfg.id ≠ null  ∧  strlen(cfg.id) ≤ 31
  ∧ cfg.topic_list ≠ null  ∧  strlen(cfg.topic_list) ≤ 255
  ∧ cfg.result_topic ≠ null  ∧  strlen(cfg.result_topic) ≤ 31
  ∧ cfg.bus_sub_url ≠ null
  ∧ cfg.bus_pub_url ≠ null
  ∧ cfg.handler_cmd ≠ null
  ∧ cfg.lmdb_path ≠ null
  ∧ cfg.retry_max ≥ 0
  ∧ cfg.heartbeat_ms ≥ 0
```

### 8.2 Correlation Chain

```
predicate ChainIntact(result : actor_header_t, cause : actor_header_t) ≙
  result.correlation_id = cause.correlation_id
  ∧ result.causation_id = cause.id
```

### 8.3 Payload Valid

```
predicate PayloadAcceptable(plen : ℕ) ≙
  plen ≤ ACTOR_MAX_PAYLOAD
```

### 8.4 Result Valid

```
predicate ResultAcceptable(run : RunStatus, result_len : ℕ) ≙
  result_len ≤ ACTOR_MAX_PAYLOAD
  ∧ (run = RUN_OK        ⇒  handler exited 0; result_len = 0 means nothing to publish)
  ∧ (run = RUN_FAILED    ⇒  handler exited non-zero, was signalled, or spawn failed)
  ∧ (run = RUN_TOO_LARGE ⇒  handler output reached ACTOR_MAX_PAYLOAD)
```

### 8.5 Retry Deserves

```
predicate RetryWarranted(run : RunStatus, attempt : ℕ, retry_max : ℕ₀) ≙
  run = RUN_FAILED                   — handler failure
  ∧ ¬g_stop                          — never while stopping
  ∧ attempt ≤ retry_max             — retries remaining
```

### 8.6 Heartbeat Deserves

```
predicate HeartbeatDue(now_ms : ℤ, last_hb_ms : ℤ, interval_ms : ℕ) ≙
  interval_ms > 0  ∧  now_ms − last_hb_ms ≥ interval_ms
```

### 8.7 Topic Filter Match

```
predicate TopicMatch(msg_topic : Topic, sub_topic : Topic) ≙
  msg_topic starts with sub_topic
  — NNG sub0 performs prefix matching on message body bytes
  — null byte in subscription ensures exact match for simple topics
```

---

## §9 Cross-Cutting Invariants

### 9.1 Memory Model

```
┌─ Memory Invariants ─────────────────────────────────────────────────┐
│ MI1: No heap allocation per tuple                                   │
│      g_result_buf[ACTOR_MAX_PAYLOAD]        — static                │
│      g_frame_buf[ACTOR_MAX_PAYLOAD + 256]   — static                │
│      All string/ID buffers on stack (33, 37, 64, 128, 256, 512)    │
│                                                                     │
│ MI2: Payloads exceeding ACTOR_MAX_PAYLOAD are dropped               │
│      — never malloc to accommodate                                  │
│      — incoming: rejected with "payload_cap_exceeded"               │
│      — outgoing: drained to /dev/null, rejected "result_cap_exceeded"│
│                                                                     │
│ MI3: CAP is a compile-time constant                                 │
│      settable via -DACTOR_MAX_PAYLOAD=N or make ACTOR_MAX_PAYLOAD=N │
└─────────────────────────────────────────────────────────────────────┘
```

### 9.2 Correlation Invariants

```
┌─ Correlation Invariants ────────────────────────────────────────────┐
│ CI1: correlation_id is carried forward without mutation              │
│      ∀ result tuple r published for input i:                        │
│        r.correlation_id = i.correlation_id                          │
│                                                                     │
│ CI2: causation_id references direct parent                          │
│      ∀ result tuple r published for input i:                        │
│        r.causation_id = i.id                                        │
│                                                                     │
│ CI3: Rejection preserves correlation chain                          │
│      ∀ rejection r for input i:                                     │
│        r.correlation_id = i.correlation_id                          │
│        ∧ r.causation_id = i.id                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 9.3 Durability Invariants

```
┌─ Durability Invariants ─────────────────────────────────────────────┐
│ DI1: At most one tuple in processing at any time                    │
│      |inbox| ≤ ACTOR_MAX_CONCURRENCY  (one per busy worker)         │
│                                                                     │
│ DI2: inbox and outbox are disjoint                                  │
│      dom(inbox) ∩ dom(outbox) = ∅                                   │
│                                                                     │
│ DI3: inbox entry exists iff tuple is being processed                │
│      t.id ∈ dom(inbox)  ⇔  ProcessTuple(t, ...) has started         │
│                            ∧ has not yet deleted the entry          │
│                                                                     │
│ DI4: outbox entry is transient                                      │
│      outbox entry created and deleted within PublishResult()        │
│      — serves as synchronous durability point, not recovery queue   │
│                                                                     │
│ DI5: state database is handler territory                            │
│      Runtime never reads or writes dbi_state                        │
│      Handlers access it via ACTOR_LMDB_PATH env var                 │
└─────────────────────────────────────────────────────────────────────┘
```

### 9.4 Resource Invariants

```
┌─ Resource Invariants ───────────────────────────────────────────────┐
│ RI1: Proxy is optional but assumed                                  │
│      Actor will dial retry for up to 30 seconds                     │
│      If proxy never appears, actor_run returns −1                   │
│                                                                     │
│ RI2: SIGTERM/SIGINT cause clean exit                                │
│      g_stop ← 1, loop breaks at next iteration boundary             │
│      No in-flight tuple draining                                    │
│      LMDB inbox persists across restart                             │
│                                                                     │
│ RI3: LMDB environment size is bounded                               │
│      mapsize = 64 MiB                                               │
│      max databases = 3                                              │
│                                                                     │
│ RI4: One handler process per tuple                                  │
│      fork/exec per message, no persistent handler process           │
│      No resource leak accumulation across tuples                    │
└─────────────────────────────────────────────────────────────────────┘
```

### 9.5 Protocol Invariants

```
┌─ Protocol Invariants ───────────────────────────────────────────────┐
│ PI1: topic field at offset 0                                        │
│      Required by NNG sub0 prefix matching on message body           │
│      Enforced by struct layout and _Static_assert(sizeof == 256)    │
│                                                                     │
│ PI2: _reserved bytes are zeroed                                     │
│      actor_tuple_init() zeroes the full header via memset           │
│      Consumers MUST NOT interpret _reserved bytes                   │
│                                                                     │
│ PI3: payload_len reflects actual trailing bytes                     │
│      Payload_len ≤ received_body_len − 256 (clamped by receiver)    │
│      Sender writes exactly payload_len bytes after header           │
│                                                                     │
│ PI4: UUIDv7 monotonic ordering                                      │
│      Within the same process: id_a.ms ≤ id_b.ms for a emitted first │
│      Across processes: no guarantee (rand() fallback on non-Linux)  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## §10 WASM Actor Model

### 10.1 State

```
┌─ WasmActorState ────────────────────────────────────────────────────┐
│ ws          : EMSCRIPTEN_WEBSOCKET_T  — WebSocket to proxy          │
│ origin      : "browser"               — fixed origin string         │
│ callbacks   : Topic ⇸ (JSON → void)   — JS callback registry        │
├─────────────────────────────────────────────────────────────────────┤
│ Transport: WebSocket binary frames                                  │
│ Frame format: identical to actor_header_t + payload                 │
│ UUID source: crypto.getRandomValues() via EM_ASM                    │
│ Timestamp:   emscripten_get_now() × 10⁶ (micro → nano)             │
└─────────────────────────────────────────────────────────────────────┘
```

### 10.2 Exports

```
procedure actor_init(url : char[*]) : {0, −1}:
  connect WebSocket to url
  register onopen, onmessage, onerror callbacks
  return 0 if connected, −1 otherwise

procedure actor_request(topic : char[*], payload : char[*]):
  BuildFrame(topic, origin, uuid4(), payload)
  ws.send_binary(frame)

procedure actor_poll():
  — no-op, WebSocket events are async
```

### 10.3 WASM Frame Builder (standalone)

```
procedure BuildFrame(topic : char[tl], origin : char[ol],
                     id : UUID, payload : char[pl],
                     output : uint8[256+pl]) : ℕ:
  { post: return = 256 + pl
          ∧ output[0..31]   = topic padded with nulls
          ∧ output[32..47]  = id
          ∧ output[80..111] = origin padded with nulls
          ∧ output[138..141]= pl (uint32 LE)
          ∧ output[256..]   = payload                              }

procedure ParseTopic(frame : uint8*, flen : ℕ, out : char[33]):
  if flen < 256: out ← ""; return
  out[0..31] ← frame[0..31]; out[32] ← '\0'

procedure ParsePayload(frame : uint8*, flen : ℕ,
                       out : uint8*) : ℕ:
  if flen < 256: return 0
  plen ← clamp(*(uint32*)(frame + 132), flen − 256)
  memcpy(out, frame + 256, plen)
  return plen

procedure ParseId(frame : uint8*, flen : ℕ, out : UUID):
  if flen < 256: out ← 0₁₆; return
  memcpy(out, frame + 32, 16)

procedure ParseCausationId(frame : uint8*, flen : ℕ, out : UUID):
  if flen < 256: out ← 0₁₆; return
  memcpy(out, frame + 64, 16)
```

---

## §11 Configuration Schema

### 11.1 Actor Environment Variables

```
┌─ ActorEnv ──────────────────────────────────────────────────────────┐
│ ACTOR_ID            : char[32]    — required, unique instance id    │
│ ACTOR_TOPIC          : char[256]   — required, comma-separated list  │
│ ACTOR_RESULT_TOPIC   : Topic       — required, default publish topic │
│ ACTOR_BUS_SUB        : URL         — required, e.g. tcp://p:5556    │
│ ACTOR_BUS_PUB        : URL         — required, e.g. tcp://p:5557    │
│ ACTOR_HANDLER        : char[*]     — required, sh -c argument       │
│ ACTOR_LMDB_PATH      : Path        — required, LMDB directory       │
│ ACTOR_TTL_NS         : ℤ           — optional, default 0 (∞)        │
│ ACTOR_HEARTBEAT_MS   : ℕ           — optional, default 5000          │
│ ACTOR_RETRY_MAX      : ℕ₀          — optional, default 3            │
│ ACTOR_MAX_PAYLOAD    : ℕ           — compile-time only              │
├─────────────────────────────────────────────────────────────────────┤
│ All required vars must be set or cfg_load returns −1                │
│ No defaults for required vars — explicit configuration required     │
└─────────────────────────────────────────────────────────────────────┘
```

### 11.2 Proxy Environment Variables

```
┌─ ProxyEnv ──────────────────────────────────────────────────────────┐
│ PROXY_SUB_BIND       : URL      — optional, default tcp://*:5557    │
│ PROXY_PUB_BIND       : URL      — optional, default tcp://*:5556    │
│ PROXY_ID             : char[32] — optional, default "proxy"         │
│ PROXY_HEARTBEAT_MS   : ℕ        — optional, default 5000            │
├─────────────────────────────────────────────────────────────────────┤
│ Bind URLs support comma-separated lists for multi-homed hosts       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## §12 Build Model

### 12.1 Compilation Units

```
┌─ BuildGraph ────────────────────────────────────────────────────────┐
│ actor       : main.c + actor.c + actor.h + actor_tuple.h            │
│               + actor_uuid.h  →  bin/actor                          │
│                                                                     │
│ mesh-proxy  : proxy.c + actor_tuple.h + actor_uuid.h                │
│               →  bin/mesh-proxy                                     │
│                                                                     │
│ Dependencies: libnng (pubsub0), liblmdb, libpthread                 │
│ Compiler:     gcc or zig cc (clang)                                 │
│ Standard:     C11 (_POSIX_C_SOURCE 200809L)                         │
│ Flags:        -Wall -Wextra -O2 -std=c11                            │
│                                                                     │
│ Cross-compile: build.sh --target <triple>                           │
│   Supported: x86_64-linux-musl, aarch64-linux-musl, native          │
│   Uses zig to cross-compile nng + lmdb from source                  │
└─────────────────────────────────────────────────────────────────────┘
```

### 12.2 Vendored Dependencies

```
vendor/
  cjson/     — cJSON v1.7.18         (JSON parsing, handler use)
  lmdb/      — LMDB (liblmdb)        (durability engine)
  mpack/     — MessagePack           (binary serialisation, handler use)
  smhasher/  — MurmurHash            (hashing, handler use)
  clay.h     — Clay UI library       (desktop UI rendering)
  nng/       — fetched at build time (NNG transport)

Runtime source:        0 vendored deps (only nng + lmdb, linked dynamically)
Handler examples:      cjson, mpack, smhasher, libcurl, libsqlite3, onnxruntime
UI:                    clay.h + SDL2
```

### 12.3 Target Invariants

```
┌─ Build Invariants ──────────────────────────────────────────────────┐
│ BI1: actor binary has no framework dependencies                     │
│      Links only: libnng, liblmdb, libpthread, libc                  │
│                                                                     │
│ BI2: Single-file compilation per binary                             │
│      actor:    gcc main.c actor.c -lnng -llmdb -o actor             │
│      proxy:    gcc proxy.c -lnng -llmdb -o mesh-proxy               │
│                                                                     │
│ BI3: All headers are self-contained                                 │
│      No #include ordering requirements beyond standard practice      │
│      actor_tuple.h and actor_uuid.h are single-header libraries     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## §13 Test Model

### 13.1 Test Suite

```
test-mesh.c  — 10 integration tests
  1. proxy: forward           — pub message reaches sub via proxy
  2. proxy: topic filter       — wrong topic not received
  3. actor: respond            — actor processes and publishes result
  4. actor: retry              — handler retries on failure
  5. actor: multi-topic        — comma-separated subscriptions work
  6. actor: TTL expiry         — expired tuples are dropped
  7. handler: env vars         — injected environment arrives
  8. handler: topic route      — handler can override result topic
  9. registry: store tool      — tool registry persists announcements
  10. heartbeat: actors emit   — heartbeat messages are published

test-employee.c — 9 integration tests (requires LLM API key or Ollama)
```

### 13.2 Test Invariants

```
┌─ Test Invariants ───────────────────────────────────────────────────┐
│ TI1: Each test is self-contained                                    │
│      Starts proxy + actor, runs test, kills all processes           │
│      Uses unique ports (55656/55657) and unique LMDB paths          │
│                                                                     │
│ TI2: No test ordering dependency                                    │
│      Tests use free_ports() + cleanup() for isolation               │
│                                                                     │
│ TI3: Result is boolean                                              │
│      PASS or FAIL with diagnostic message                           │
│      Aggregate: FAILURES count, non-zero → exit failure             │
└─────────────────────────────────────────────────────────────────────┘
```
