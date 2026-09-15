#define _POSIX_C_SOURCE 200809L
/* actor.c — generic actor runtime
 *
 * Zero malloc. Fixed static buffers. Capped payload.
 * Loop: poll NNG → check TTL → write inbox LMDB → fork handler
 *       → write payload stdin → read result stdout → build result header
 *       → write outbox LMDB → publish → clear LMDB
 *
 * Handler sees: payload bytes on stdin
 * Handler emits: result bytes on stdout
 * Handler knows: nothing else
 */

#include "actor.h"
#include "actor_tuple.h"
#include "actor_uuid.h"
#include "bus.h"
#ifndef _WIN32
#  include "actor_isolation.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <errno.h>
#include <time.h>

#ifndef _WIN32
#  include <pthread.h>
#  include <sys/wait.h>
#endif

#ifdef _WIN32
#  include <windows.h>
#endif

#include <nng/nng.h>
#include <nng/protocol/pubsub0/pub.h>
#include <nng/protocol/pubsub0/sub.h>

#include <lmdb.h>

/* ── Payload cap ─────────────────────────────────────────────────────────── */

/* ACTOR_MAX_PAYLOAD — hard ceiling on payload size.
 * Override at build time: -DACTOR_MAX_PAYLOAD=4194304 for 4MB.
 * Payloads exceeding this are dropped — never malloc to accommodate. */
#ifndef ACTOR_MAX_PAYLOAD
#  define ACTOR_MAX_PAYLOAD (1024 * 1024)   /* 1MB default */
#endif

#define ACTOR_MAX_FRAME (ACTOR_MAX_PAYLOAD + sizeof(actor_header_t))

/* ── Concurrency ─────────────────────────────────────────────────────────── */

/* ACTOR_CONCURRENCY (env) — how many messages may be in flight at once.
 * 1 (the default) is exactly the historical behaviour: one thread, one
 * message, one handler. Above 1, that many worker threads each run the same
 * receive→handle→publish cycle.
 *
 * Raising this is worth it because the per-message cost is dominated by
 * fork/exec of the handler binary (~100MB statically linked, ~200ms), which is
 * mostly waiting rather than CPU. It is capped because each worker costs a
 * thread plus its own ACTOR_MAX_PAYLOAD-sized buffers. */
#define ACTOR_MAX_CONCURRENCY 32

/* Thread-local storage. Windows keeps a single worker (see cfg_load), so the
 * portable spelling is only needed on the POSIX path. */
#ifdef _WIN32
#  define ACTOR_TLS
#else
#  define ACTOR_TLS __thread
#endif

/* ── Static buffers — zero heap ──────────────────────────────────────────── */

/* Thread-local, not merely static: with ACTOR_CONCURRENCY > 1 several worker
 * threads run a message each, and a shared buffer would have one thread's
 * handler output overwritten by another's mid-publish. Still no heap — each
 * thread gets its own fixed allocation, so the cap is per worker rather than
 * per process. */

/* g_result_buf: handler stdout is read into here — fixed, never grows */
static ACTOR_TLS uint8_t g_result_buf[ACTOR_MAX_PAYLOAD];

/* g_frame_buf: header + payload assembled here for LMDB writes and NNG sends */
static ACTOR_TLS uint8_t g_frame_buf[ACTOR_MAX_FRAME];

/* ── Config ──────────────────────────────────────────────────────────────── */

typedef struct {
    const char* id;
    const char* topic;
    const char* result_topic;
    const char* bus_sub;
    const char* bus_pub;
    const char* handler;
    const char* lmdb_path;
    int64_t     ttl_ns;
    int         heartbeat_ms;
    int         retry_max;
    int         concurrency;
    int         term_grace_ms;
} actor_cfg_t;

static actor_cfg_t cfg;

static int cfg_load(void) {
    cfg.id           = getenv("ACTOR_ID");
    cfg.topic        = getenv("ACTOR_TOPIC");
    cfg.result_topic = getenv("ACTOR_RESULT_TOPIC");
    cfg.bus_sub      = getenv("ACTOR_BUS_SUB");
    cfg.bus_pub      = getenv("ACTOR_BUS_PUB");
    cfg.handler      = getenv("ACTOR_HANDLER");
    cfg.lmdb_path    = getenv("ACTOR_LMDB_PATH");

    if (!cfg.id || !cfg.topic || !cfg.result_topic ||
        !cfg.bus_sub || !cfg.bus_pub ||
        !cfg.handler || !cfg.lmdb_path) {
        fprintf(stderr, "[actor] missing required env vars\n");
        fprintf(stderr, "  required: ACTOR_ID ACTOR_TOPIC ACTOR_RESULT_TOPIC"
                        " ACTOR_BUS_SUB ACTOR_BUS_PUB ACTOR_HANDLER"
                        " ACTOR_LMDB_PATH\n");
        return -1;
    }

    const char* ttl = getenv("ACTOR_TTL_NS");
    cfg.ttl_ns = ttl ? atoll(ttl) : 0;

    const char* hb = getenv("ACTOR_HEARTBEAT_MS");
    cfg.heartbeat_ms = hb ? atoi(hb) : 5000;

    const char* rm = getenv("ACTOR_RETRY_MAX");
    cfg.retry_max = rm ? atoi(rm) : 3;

    const char* tg = getenv("ACTOR_TERM_GRACE_MS");
    cfg.term_grace_ms = tg ? atoi(tg) : 5000;
    if (cfg.term_grace_ms < 0) cfg.term_grace_ms = 0;

    /* Default 1 = the historical single-message-at-a-time behaviour, so an
       existing deployment gets exactly what it had until it opts in. */
    const char* cc = getenv("ACTOR_CONCURRENCY");
    cfg.concurrency = cc ? atoi(cc) : 1;
    if (cfg.concurrency < 1) cfg.concurrency = 1;
    if (cfg.concurrency > ACTOR_MAX_CONCURRENCY) cfg.concurrency = ACTOR_MAX_CONCURRENCY;
#ifdef _WIN32
    /* The Windows spawn path stamps the process environment before
       CreateProcess, which is only safe with one worker. */
    cfg.concurrency = 1;
#endif

    return 0;
}

/* ── Lanes ───────────────────────────────────────────────────────────────── */

/* A lane is a set of topics served by one handler, with its own socket and
   workers. Topics without an ACTOR_{HANDLER,RESULT_TOPIC,CONCURRENCY}_<topic>
   setting share the default lane; a topic with one gets a lane of its own, so
   it never waits behind the others. With no such settings there is one lane:
   the actor as it always was. */
#define ACTOR_MAX_LANES 16

typedef struct {
    char        topics[256];   /* comma-separated, as subscribed */
    const char* handler;
    const char* result_topic;
    int         concurrency;
    nng_socket  sub;
} lane_t;

static lane_t g_lanes[ACTOR_MAX_LANES];
static int    g_nlanes;

static const char* topic_env(const char* name, const char* topic) {
    char key[64];
    snprintf(key, sizeof(key), "%s_%s", name, topic);
    return getenv(key);
}

static int lanes_load(void) {
    char list[256];
    snprintf(list, sizeof(list), "%s", cfg.topic);
    lane_t* shared = NULL;
    int     total  = 0;
    char*   save   = NULL;
    for (char* t = strtok_r(list, ",", &save); t; t = strtok_r(NULL, ",", &save)) {
        while (*t == ' ') t++;
        if (!*t) continue;
        const char* h  = topic_env("ACTOR_HANDLER", t);
        const char* rt = topic_env("ACTOR_RESULT_TOPIC", t);
        const char* cc = topic_env("ACTOR_CONCURRENCY", t);
        bool        own = h || rt || cc;
        lane_t*     l   = own ? NULL : shared;
        if (!l) {
            if (g_nlanes == ACTOR_MAX_LANES) {
                fprintf(stderr, "[actor] ACTOR_TOPIC needs more than %d lanes\n", ACTOR_MAX_LANES);
                return -1;
            }
            l = &g_lanes[g_nlanes++];
            l->handler      = h  ? h  : cfg.handler;
            l->result_topic = rt ? rt : cfg.result_topic;
            l->concurrency  = cfg.concurrency;
            if (cc) {
                l->concurrency = atoi(cc);
                if (l->concurrency < 1) l->concurrency = 1;
            }
            total += l->concurrency;
            if (!own) shared = l;
        }
        size_t used = strlen(l->topics);
        int    n    = snprintf(l->topics + used, sizeof(l->topics) - used, "%s%s", used ? "," : "", t);
        if (n < 0 || (size_t)n >= sizeof(l->topics) - used) {
            fprintf(stderr, "[actor] a lane's topics do not fit in %d bytes\n", (int)sizeof(l->topics) - 1);
            return -1;
        }
    }
    if (g_nlanes == 0) {
        fprintf(stderr, "[actor] ACTOR_TOPIC names no topic\n");
        return -1;
    }
    if (total > ACTOR_MAX_CONCURRENCY) {
        fprintf(stderr, "[actor] lanes ask for %d workers; the most is %d\n",
                total, ACTOR_MAX_CONCURRENCY);
        return -1;
    }
#ifdef _WIN32
    if (g_nlanes > 1) {
        fprintf(stderr, "[actor] per-topic lanes need more than one worker, which Windows lacks\n");
        return -1;
    }
#endif
    return 0;
}

/* The lane serving a header's topic; lane 0 if none does any more. */
static int lane_of(const char topic[32]) {
    char t[33];
    snprintf(t, sizeof(t), "%.*s", 32, topic);
    size_t n = strlen(t);
    for (int i = 0; i < g_nlanes; i++) {
        for (const char* p = g_lanes[i].topics; *p; ) {
            const char* e   = strchr(p, ',');
            size_t      len = e ? (size_t)(e - p) : strlen(p);
            if (len == n && memcmp(p, t, n) == 0) return i;
            if (!e) break;
            p = e + 1;
        }
    }
    return 0;
}

/* ── State ───────────────────────────────────────────────────────────────── */

static nng_socket nng_pub;
#ifndef _WIN32
static nng_socket nng_ctl;   /* control topics, read by the reaper */
#endif
static MDB_env*   mdb_env;
static MDB_dbi    dbi_inbox;
static MDB_dbi    dbi_outbox;
static MDB_dbi    dbi_state;

static volatile sig_atomic_t g_stop = 0;

/* Inbox entries left by the previous run, replayed before new messages, each
   by the lane its topic belongs to. */
static uint8_t    g_replay[ACTOR_MAX_CONCURRENCY][16];
static int        g_replay_lane[ACTOR_MAX_CONCURRENCY];
static atomic_int g_replay_taken[ACTOR_MAX_CONCURRENCY];
static int        g_replay_n;
static atomic_int g_replay_left;

/* Where a worker's frame came from. A replayed one lives in g_frame_buf and
   is already in the inbox. */
typedef enum { TUPLE_RECEIVED, TUPLE_REPLAYED } tuple_source_t;

static int64_t mono_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

/* ── Signal ──────────────────────────────────────────────────────────────── */

static void on_signal(int s) { (void)s; g_stop = 1; }

/* ── NNG ─────────────────────────────────────────────────────────────────── */

/* The proxy may not be listening yet. */
static int dial_retry(nng_socket s, const char* url, const char* what) {
    for (int i = 0; i < 30; i++) {
        int rc = nng_dial(s, url, NULL, 0);
        if (rc == 0) return 0;
        fprintf(stderr, "[actor] %s dial %s: %s (retry %d)\n", what, url, nng_strerror(rc), i);
        sleep(1);
    }
    return -1;
}

static int nng_setup(void) {
    int rc;

    if ((rc = nng_pub0_open(&nng_pub)) != 0) {
        fprintf(stderr, "[actor] nng_pub0_open: %s\n", nng_strerror(rc));
        return -1;
    }
    if (dial_retry(nng_pub, cfg.bus_pub, "pub") < 0) return -1;

    /* One SUB socket per lane. NNG sub0 prefix-matches on the message body and
     * the topic sits at offset 0 of actor_header_t, so each topic is subscribed
     * with its null terminator for an exact match. The 100ms receive timeout is
     * how a worker notices g_stop. */
    for (int i = 0; i < g_nlanes; i++) {
        lane_t* l = &g_lanes[i];
        if ((rc = nng_sub0_open(&l->sub)) != 0) {
            fprintf(stderr, "[actor] nng_sub0_open: %s\n", nng_strerror(rc));
            return -1;
        }
        if (dial_retry(l->sub, cfg.bus_sub, "sub") < 0) return -1;
        char  list[sizeof(l->topics)];
        char* save = NULL;
        memcpy(list, l->topics, sizeof(list));
        for (char* t = strtok_r(list, ",", &save); t; t = strtok_r(NULL, ",", &save)) {
            if ((rc = nng_socket_set(l->sub, NNG_OPT_SUB_SUBSCRIBE, t, strlen(t) + 1)) != 0) {
                fprintf(stderr, "[actor] sub subscribe %s: %s\n", t, nng_strerror(rc));
                return -1;
            }
        }
        nng_socket_set_ms(l->sub, NNG_OPT_RECVTIMEO, 100);
    }

#ifndef _WIN32
    /* Its own socket, so a _term still arrives when every worker is busy. */
    if ((rc = nng_sub0_open(&nng_ctl)) != 0 ||
        (rc = nng_dial(nng_ctl, cfg.bus_sub, NULL, 0)) != 0 ||
        (rc = nng_socket_set(nng_ctl, NNG_OPT_SUB_SUBSCRIBE, "_term", sizeof("_term"))) != 0) {
        fprintf(stderr, "[actor] control socket: %s\n", nng_strerror(rc));
        return -1;
    }
#endif

    return 0;
}

/* ── LMDB ────────────────────────────────────────────────────────────────── */

static int lmdb_setup(void) {
    mdb_env_create(&mdb_env);
    mdb_env_set_mapsize(mdb_env, 64UL * 1024 * 1024);
    mdb_env_set_maxdbs(mdb_env, 3);

    int rc = mdb_env_open(mdb_env, cfg.lmdb_path, 0, 0664);
    if (rc) {
        fprintf(stderr, "[actor] lmdb open failed: %s\n", mdb_strerror(rc));
        return -1;
    }

    MDB_txn* txn;
    mdb_txn_begin(mdb_env, NULL, 0, &txn);
    mdb_dbi_open(txn, "inbox",  MDB_CREATE, &dbi_inbox);
    mdb_dbi_open(txn, "outbox", MDB_CREATE, &dbi_outbox);
    mdb_dbi_open(txn, "state",  MDB_CREATE, &dbi_state);

    /* The inbox holds what the last run had in flight -- at most one tuple
       per worker. Replaying it covers any outbox copy, so drop those. */
    mdb_drop(txn, dbi_outbox, 0);
    MDB_cursor*   cur;
    MDB_val       k, v;
    MDB_cursor_op op = MDB_FIRST;
    if (mdb_cursor_open(txn, dbi_inbox, &cur) == 0) {
        while (g_replay_n < ACTOR_MAX_CONCURRENCY && mdb_cursor_get(cur, &k, &v, op) == 0) {
            op = MDB_NEXT;
            if (k.mv_size != 16) continue;
            memcpy(g_replay[g_replay_n], k.mv_data, 16);
            g_replay_lane[g_replay_n++] = v.mv_size >= sizeof(actor_header_t)
                ? lane_of(((const actor_header_t*)v.mv_data)->topic) : 0;
        }
        mdb_cursor_close(cur);
    }
    atomic_store(&g_replay_left, g_replay_n);
    mdb_txn_commit(txn);
    return 0;
}

static void lmdb_put(MDB_dbi dbi,
                     const void* key, size_t klen,
                     const void* val, size_t vlen) {
    MDB_txn* txn;
    if (mdb_txn_begin(mdb_env, NULL, 0, &txn)) return;
    MDB_val k = { klen, (void*)key };
    MDB_val v = { vlen, (void*)val };
    mdb_put(txn, dbi, &k, &v, 0);
    mdb_txn_commit(txn);
}

static void lmdb_del(MDB_dbi dbi, const void* key, size_t klen) {
    MDB_txn* txn;
    if (mdb_txn_begin(mdb_env, NULL, 0, &txn)) return;
    MDB_val k = { klen, (void*)key };
    mdb_del(txn, dbi, &k, NULL);
    mdb_txn_commit(txn);
}

static size_t lmdb_count(MDB_dbi dbi) {
    MDB_txn*  txn;
    MDB_stat  stat;
    if (mdb_txn_begin(mdb_env, NULL, MDB_RDONLY, &txn)) return 0;
    mdb_stat(txn, dbi, &stat);
    mdb_txn_abort(txn);
    return stat.ms_entries;
}

/* ── Handler spawn — cross-platform ──────────────────────────────────────── */

/* The per-message values handed to the handler through its environment.
 * Carried as a value rather than set on the parent before forking, because
 * with several workers running the parent's environment is shared mutable
 * state -- see the setenv note in the Unix platform_spawn. */
typedef struct {
    char id_hex[33];
    char corr_hex[33];
    char caus_hex[33];
    char origin[32];
    char attempt[12];
    /* 33, not 32: the header field is a null-PADDED 32-byte array, not a
       null-terminated string, so a topic occupying all 32 bytes needs one
       more byte to terminate. */
    char topic[33];
    char deadline[21];   /* emitted_at + ttl, unix ns; "0" = none */
} child_env_t;

/* How one handler run ended. */
typedef enum {
    RUN_OK,         /* exited 0; *out_len bytes of output are in out */
    RUN_FAILED,     /* could not start, exited non-zero, or was killed */
    RUN_TOO_LARGE,  /* output exceeded ACTOR_MAX_PAYLOAD */
    RUN_TERMINATED, /* stopped by _term */
} run_status_t;

/* platform_spawn launches the handler, pipes payload to its stdin,
 * reads its stdout into out, and says how the run ended.
 * Unix: fork/exec   Windows: CreateProcess */
#ifdef _WIN32

static void running_json(char* out, size_t cap) { snprintf(out, cap, "[]"); }
static void services_json(char* out, size_t cap) { snprintf(out, cap, "[]"); }

static bool services_requested(void) {
    char* env   = GetEnvironmentStringsA();
    bool  found = false;
    for (char* p = env; p && *p; p += strlen(p) + 1)
        if (strncmp(p, "ACTOR_SERVICE_", 14) == 0) { found = true; break; }
    if (env) FreeEnvironmentStringsA(env);
    return found;
}

static run_status_t platform_spawn(const uint8_t* in,  size_t in_len,
                                   uint8_t*       out, size_t out_cap,
                                   const child_env_t* env, const actor_header_t* hdr,
                                   const char* handler, size_t* out_len) {
    (void)hdr;   /* _term is Unix-only: it needs process groups */
    /* Windows runs a single worker (cfg_load clamps concurrency to 1 there),
       so mutating the process environment before CreateProcess is safe. */
    SetEnvironmentVariableA("ACTOR_TUPLE_ID",       env->id_hex);
    SetEnvironmentVariableA("ACTOR_CORRELATION_ID", env->corr_hex);
    SetEnvironmentVariableA("ACTOR_CAUSATION_ID",   env->caus_hex);
    SetEnvironmentVariableA("ACTOR_TUPLE_ORIGIN",   env->origin);
    SetEnvironmentVariableA("ACTOR_ATTEMPT",        env->attempt);
    SetEnvironmentVariableA("ACTOR_TUPLE_TOPIC",    env->topic);
    SetEnvironmentVariableA("ACTOR_TUPLE_DEADLINE", env->deadline);
    HANDLE stdin_rd  = NULL, stdin_wr  = NULL;
    HANDLE stdout_rd = NULL, stdout_wr = NULL;
    SECURITY_ATTRIBUTES sa = { sizeof(sa), NULL, TRUE };
    run_status_t result = RUN_FAILED;

    if (!CreatePipe(&stdin_rd,  &stdin_wr,  &sa, 0)) return RUN_FAILED;
    if (!CreatePipe(&stdout_rd, &stdout_wr, &sa, 0)) goto fail_stdin;
    SetHandleInformation(stdin_wr,  HANDLE_FLAG_INHERIT, 0);
    SetHandleInformation(stdout_rd, HANDLE_FLAG_INHERIT, 0);

    STARTUPINFOA si = {0};
    si.cb         = sizeof(si);
    si.hStdInput  = stdin_rd;
    si.hStdOutput = stdout_wr;
    si.hStdError  = GetStdHandle(STD_ERROR_HANDLE);
    si.dwFlags    = STARTF_USESTDHANDLES;

    char cmdline[1024];
    snprintf(cmdline, sizeof(cmdline), "cmd.exe /c %s", handler);

    PROCESS_INFORMATION pi = {0};
    if (!CreateProcessA(NULL, cmdline, NULL, NULL, TRUE,
                        0, NULL, NULL, &si, &pi))
        goto fail_stdout;

    CloseHandle(stdin_rd);   stdin_rd  = NULL;
    CloseHandle(stdout_wr);  stdout_wr = NULL;

    DWORD written = 0;
    if (!WriteFile(stdin_wr, in, (DWORD)in_len, &written, NULL) || written != in_len)
        goto fail_proc;
    CloseHandle(stdin_wr); stdin_wr = NULL;

    size_t len = 0;
    for (;;) {
        DWORD n = 0;
        if (!ReadFile(stdout_rd, out + len, (DWORD)(out_cap - len), &n, NULL) || n == 0)
            break;
        len += n;
        if (len == out_cap) {
            uint8_t drain[256]; DWORD d;
            while (ReadFile(stdout_rd, drain, sizeof(drain), &d, NULL) && d > 0) {}
            result = RUN_TOO_LARGE;
            goto fail_proc;
        }
    }
    *out_len = len;
    result = RUN_OK;

    WaitForSingleObject(pi.hProcess, INFINITE);
    DWORD ec;
    if (GetExitCodeProcess(pi.hProcess, &ec) && ec != 0) result = RUN_FAILED;

fail_proc:
    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);
fail_stdout:
    if (stdout_rd) CloseHandle(stdout_rd);
    if (stdout_wr) CloseHandle(stdout_wr);
fail_stdin:
    if (stdin_rd)  CloseHandle(stdin_rd);
    if (stdin_wr)  CloseHandle(stdin_wr);
    return result;
}

#else /* Unix — fork/exec */

/* ── Child status dispatcher ─────────────────────────────────────────────────
 *
 * Exactly one thread calls waitpid, and it hands each status to whichever
 * worker owns that pid.
 *
 * This replaces the old arrangement, where every spawn did its own blocking
 * waitpid and a between-messages `waitpid(-1, WNOHANG)` sweep collected
 * orphans. That sweep was safe only because the loop was serial: with more
 * than one handler running it would harvest another worker's child, that
 * worker's waitpid would fail with ECHILD leaving `status` never written, and
 * WIFEXITED would read uninitialised memory -- silently reporting a FAILED
 * handler run as successful, which is the one outcome this runtime must never
 * produce. (The old code's own comment called this out as the reason not to
 * use SIGCHLD or SIG_IGN.)
 *
 * Centralising the wait removes the race rather than working around it, and
 * orphan reaping falls out for free: this process is PID 1 in its container,
 * so it inherits orphaned grandchildren, and any reaped pid that matches no
 * slot is simply one of those and is discarded.
 *
 * The registration ordering is the subtle part. A worker holds `child_mu`
 * across fork() and the store of the new pid, so the reaper -- which must take
 * the same mutex to dispatch -- cannot observe a not-yet-registered child.
 * Therefore "reaped a pid with no slot" unambiguously means "orphan", never
 * "a child whose parent has not finished registering it".
 */

typedef struct {
    pid_t   pid;                 /* 0 = free slot; also its process group */
    int     status;              /* raw waitpid status, valid when done   */
    int     done;                /* set by the reaper                     */
    uint8_t tuple_id[16];        /* what it is running, for _term         */
    uint8_t correlation_id[16];
    char    topic[33];
    int64_t started_ms;          /* monotonic, for the heartbeat          */
    bool    terminated;          /* _term has sent SIGTERM                */
    int64_t kill_at_ms;          /* SIGKILL deadline, 0 = none            */
} child_slot_t;

static child_slot_t   g_children[ACTOR_MAX_CONCURRENCY];
static pthread_mutex_t child_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  child_cv = PTHREAD_COND_INITIALIZER;
static atomic_int      g_reaper_stop;   /* set once every worker has returned */

static void heartbeat_tick(void);

static bool id_is_zero(const uint8_t id[16]) {
    for (int i = 0; i < 16; i++) if (id[i]) return false;
    return true;
}

/* _term: SIGTERM the process group of every running handler in the named
   correlation chain -- or only the named tuple, when causation_id is set --
   and leave escalate_kills to SIGKILL what remains after the grace. */
static void term_matching(const actor_header_t* req) {
    if (id_is_zero(req->correlation_id)) return;
    bool one = !id_is_zero(req->causation_id);
    pthread_mutex_lock(&child_mu);
    for (int i = 0; i < ACTOR_MAX_CONCURRENCY; i++) {
        child_slot_t* c = &g_children[i];
        if (!c->pid || c->done || c->terminated) continue;
        if (memcmp(c->correlation_id, req->correlation_id, 16) != 0) continue;
        if (one && memcmp(c->tuple_id, req->causation_id, 16) != 0) continue;
        kill(-c->pid, SIGTERM);
        c->terminated = true;
        c->kill_at_ms = mono_ms() + cfg.term_grace_ms;
    }
    pthread_mutex_unlock(&child_mu);
}

/* Only the header matters, so a fixed buffer is enough: nng_recv copies what
   fits and reports the full length. */
static void control_poll(void) {
    uint8_t buf[sizeof(actor_header_t)];
    size_t  sz = sizeof(buf);
    while (nng_recv(nng_ctl, buf, &sz, NNG_FLAG_NONBLOCK) == 0) {
        if (sz >= sizeof(actor_header_t)) term_matching((const actor_header_t*)buf);
        sz = sizeof(buf);
    }
}

static void escalate_kills(void) {
    int64_t now = mono_ms();
    pthread_mutex_lock(&child_mu);
    for (int i = 0; i < ACTOR_MAX_CONCURRENCY; i++) {
        child_slot_t* c = &g_children[i];
        if (c->pid && !c->done && c->kill_at_ms && now >= c->kill_at_ms) {
            kill(-c->pid, SIGKILL);
            c->kill_at_ms = 0;
        }
    }
    pthread_mutex_unlock(&child_mu);
}

/* The running handlers, as the heartbeat's JSON array. Stops at a whole
   entry if cap is reached, so the array is always valid. */
static void running_json(char* out, size_t cap) {
    size_t  n   = 0;
    int64_t now = mono_ms();
    out[n++] = '[';
    pthread_mutex_lock(&child_mu);
    for (int i = 0; i < ACTOR_MAX_CONCURRENCY; i++) {
        const child_slot_t* c = &g_children[i];
        if (!c->pid || c->done) continue;
        char tid[33], cid[33];
        actor_uuid_hex(c->tuple_id,       tid);
        actor_uuid_hex(c->correlation_id, cid);
        int w = snprintf(out + n, cap - n,
                         "%s{\"tuple\":\"%s\",\"correlation\":\"%s\",\"topic\":\"%s\","
                         "\"pid\":%d,\"age_ms\":%lld,\"terminating\":%s}",
                         n > 1 ? "," : "", tid, cid, c->topic, (int)c->pid,
                         (long long)(now - c->started_ms), c->terminated ? "true" : "false");
        if (w < 0 || (size_t)w >= cap - n - 1) break;   /* keep room for ']' */
        n += (size_t)w;
    }
    pthread_mutex_unlock(&child_mu);
    out[n++] = ']';
    out[n]   = '\0';
}

/* ── Services ──────────────────────────────────────────────────────────────
 *
 * A handler lives for one tuple; a service lives as long as the actor.
 * ACTOR_SERVICE_<name>=<command> is started before any tuple is served and
 * restarted by the reaper when it exits. More than SERVICE_RESTARTS restarts
 * inside SERVICE_WINDOW_MS and the actor gives up and exits non-zero, so
 * whatever supervises it backs off rather than watching it spin. */
#define ACTOR_MAX_SERVICES 8
#define SERVICE_RESTARTS   5
#define SERVICE_WINDOW_MS  60000

typedef struct {
    char        name[32];
    const char* cmd;
    pid_t       pid;                        /* 0 = not running; also its group */
    int         restarts;
    int64_t     recent[SERVICE_RESTARTS];   /* when the last restarts happened */
} service_t;

static service_t  g_services[ACTOR_MAX_SERVICES];
static int        g_nservices;
static atomic_int g_services_failed;

extern char** environ;

static int services_load(void) {
    static const char prefix[] = "ACTOR_SERVICE_";
    const size_t      plen     = sizeof(prefix) - 1;
    for (char** e = environ; *e; e++) {
        const char* eq = strchr(*e, '=');
        if (strncmp(*e, prefix, plen) != 0 || !eq || !eq[1]) continue;
        if (g_nservices == ACTOR_MAX_SERVICES) {
            fprintf(stderr, "[actor] more than %d ACTOR_SERVICE_* entries\n", ACTOR_MAX_SERVICES);
            return -1;
        }
        service_t* s = &g_services[g_nservices++];
        snprintf(s->name, sizeof(s->name), "%.*s", (int)(eq - *e - plen), *e + plen);
        s->cmd = eq + 1;
    }
    return 0;
}

static void service_spawn(service_t* s) {
    pid_t pid = fork();
    if (pid == 0) {
        setpgid(0, 0);
        execl("/bin/sh", "sh", "-c", s->cmd, NULL);
        _exit(127);
    }
    if (pid < 0) fprintf(stderr, "[actor] service %s: fork: %s\n", s->name, strerror(errno));
    else         setpgid(pid, pid);
    s->pid = pid > 0 ? pid : 0;
}

/* Reaper only: restart a service that exited, within its budget. */
static void service_reaped(pid_t pid, int status) {
    for (int i = 0; i < g_nservices; i++) {
        service_t* s = &g_services[i];
        if (s->pid != pid) continue;
        s->pid = 0;
        if (g_stop) return;
        int64_t now    = mono_ms();
        int     recent = 0;
        for (int k = 0; k < SERVICE_RESTARTS; k++)
            if (s->recent[k] && now - s->recent[k] < SERVICE_WINDOW_MS) recent++;
        if (recent == SERVICE_RESTARTS) {
            fprintf(stderr, "[actor] service %s restarted %d times in %ds; giving up\n",
                    s->name, SERVICE_RESTARTS, SERVICE_WINDOW_MS / 1000);
            atomic_store(&g_services_failed, 1);
            g_stop = 1;
            return;
        }
        s->recent[s->restarts++ % SERVICE_RESTARTS] = now;
        fprintf(stderr, "[actor] service %s (pid %d) exited with status %d; restarting\n",
                s->name, (int)pid,
                WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status));
        service_spawn(s);
        return;
    }
}

/* Reaper only, at shutdown: SIGTERM each service, SIGKILL after the grace. */
static void services_stop(void) {
    int64_t kill_at = mono_ms() + cfg.term_grace_ms;
    bool    killed  = false;
    for (int i = 0; i < g_nservices; i++)
        if (g_services[i].pid) kill(-g_services[i].pid, SIGTERM);
    for (;;) {
        bool left = false;
        for (int i = 0; i < g_nservices; i++) left |= g_services[i].pid != 0;
        if (!left) return;
        if (!killed && mono_ms() >= kill_at) {
            for (int i = 0; i < g_nservices; i++)
                if (g_services[i].pid) kill(-g_services[i].pid, SIGKILL);
            killed = true;
        }
        int   st;
        pid_t pid = waitpid(-1, &st, WNOHANG);
        if (pid < 0 && errno == ECHILD) return;
        if (pid > 0) {
            for (int i = 0; i < g_nservices; i++)
                if (g_services[i].pid == pid) g_services[i].pid = 0;
            continue;
        }
        struct timespec ts = { 0, 5 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
}

static void services_json(char* out, size_t cap) {
    size_t n = 0;
    out[n++] = '[';
    for (int i = 0; i < g_nservices; i++) {
        const service_t* s = &g_services[i];
        int w = snprintf(out + n, cap - n, "%s{\"name\":\"%s\",\"pid\":%d,\"restarts\":%d}",
                         n > 1 ? "," : "", s->name, (int)s->pid, s->restarts);
        if (w < 0 || (size_t)w >= cap - n - 1) break;
        n += (size_t)w;
    }
    out[n++] = ']';
    out[n]   = '\0';
}

/* ACTOR_INIT runs once, to completion, before anything is served. A failure
   stops the actor like any other startup step. */
static int run_init(void) {
    const char* cmd = getenv("ACTOR_INIT");
    if (!cmd || !*cmd) return 0;
    pid_t pid = fork();
    if (pid == 0) {
        execl("/bin/sh", "sh", "-c", cmd, NULL);
        _exit(127);
    }
    int st = 0;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st) || WEXITSTATUS(st) != 0) {
        fprintf(stderr, "[actor] ACTOR_INIT failed\n");
        return -1;
    }
    return 0;
}

/* ── Built-in bus ──────────────────────────────────────────────────────────
 *
 * An actor given PROXY_SUB_BIND and PROXY_PUB_BIND hosts the bus itself, on a
 * thread of its own, listening before anything here dials it and closing
 * after everything else. */
static bus_t      g_bus;
static pthread_t  g_bus_thread;
static bool       g_bus_hosted;
static atomic_int g_bus_stop;

static void* bus_main(void* arg) {
    (void)arg;
    while (!atomic_load(&g_bus_stop)) bus_forward_once(&g_bus);
    return NULL;
}

static int bus_host(void) {
    const char* sub = getenv("PROXY_SUB_BIND");
    const char* pub = getenv("PROXY_PUB_BIND");
    if (!sub && !pub) return 0;
    if (!sub || !pub) {
        fprintf(stderr, "[actor] hosting the bus needs both PROXY_SUB_BIND and PROXY_PUB_BIND\n");
        return -1;
    }
    int rc = bus_open(&g_bus, sub, pub);
    if (rc != 0) {
        fprintf(stderr, "[actor] bus: %s\n", nng_strerror(rc));
        return -1;
    }
    if (pthread_create(&g_bus_thread, NULL, bus_main, NULL) != 0) {
        fprintf(stderr, "[actor] could not start the bus thread\n");
        bus_close(&g_bus);
        return -1;
    }
    g_bus_hosted = true;
    return 0;
}

static void bus_unhost(void) {
    if (!g_bus_hosted) return;
    atomic_store(&g_bus_stop, 1);
    pthread_join(g_bus_thread, NULL);
    bus_close(&g_bus);
}

/* Reaper thread: the only caller of waitpid in the process. */
static void* reaper_main(void* arg) {
    (void)arg;
    while (!atomic_load(&g_reaper_stop)) {
        control_poll();
        escalate_kills();
        heartbeat_tick();
        int   status;
        pid_t pid = waitpid(-1, &status, WNOHANG);
        if (pid <= 0) {
            /* Nothing exited (0) or no children at all (-1/ECHILD). Sleep
               briefly rather than spin; the handler runs for far longer than
               this, so the added latency is not measurable. */
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 5 * 1000 * 1000 };
            nanosleep(&ts, NULL);
            continue;
        }
        pthread_mutex_lock(&child_mu);
        for (int i = 0; i < ACTOR_MAX_CONCURRENCY; i++) {
            if (g_children[i].pid == pid) {
                g_children[i].status = status;
                g_children[i].done   = 1;
                break;
            }
        }
        /* No match: an orphaned grandchild we inherited as PID 1. Reaping it
           was the whole job -- nothing to dispatch. */
        pthread_cond_broadcast(&child_cv);
        pthread_mutex_unlock(&child_mu);
        service_reaped(pid, status);
    }
    services_stop();
    return NULL;
}

typedef struct {
    int  status;       /* raw waitpid status */
    bool terminated;   /* stopped by _term   */
} child_exit_t;

/* Block until the reaper reports `slot`'s child, then free the slot. */
static child_exit_t await_child(int slot) {
    pthread_mutex_lock(&child_mu);
    /* The reaper outlives every worker, so this always gets an answer. */
    while (!g_children[slot].done)
        pthread_cond_wait(&child_cv, &child_mu);
    child_slot_t* c = &g_children[slot];
    child_exit_t  e = { c->status, c->terminated };
    /* A terminated handler's leftovers go with it. */
    if (c->terminated) kill(-c->pid, SIGKILL);
    c->pid        = 0;
    c->done       = 0;
    c->terminated = false;
    c->kill_at_ms = 0;
    pthread_mutex_unlock(&child_mu);
    return e;
}

static run_status_t platform_spawn(const uint8_t* in,  size_t in_len,
                                   uint8_t*       out, size_t out_cap,
                                   const child_env_t* env, const actor_header_t* hdr,
                                   const char* handler, size_t* out_len) {
    int to_child[2], from_child[2];

    if (pipe(to_child)   < 0) return RUN_FAILED;
    if (pipe(from_child) < 0) { close(to_child[0]); close(to_child[1]); return RUN_FAILED; }

    /* Claim a slot, then fork and register the pid without releasing the
       mutex -- see the dispatcher note above for why the two must be atomic
       with respect to the reaper. */
    pthread_mutex_lock(&child_mu);
    int slot = -1;
    for (int i = 0; i < ACTOR_MAX_CONCURRENCY; i++) {
        if (g_children[i].pid == 0 && !g_children[i].done) { slot = i; break; }
    }
    if (slot < 0) {
        pthread_mutex_unlock(&child_mu);
        close(to_child[0]); close(to_child[1]);
        close(from_child[0]); close(from_child[1]);
        fprintf(stderr, "[actor] no free child slot\n");
        return RUN_FAILED;
    }

    pid_t pid = fork();
    if (pid < 0) {
        pthread_mutex_unlock(&child_mu);
        close(to_child[0]); close(to_child[1]);
        close(from_child[0]); close(from_child[1]);
        return RUN_FAILED;
    }

    if (pid == 0) {
        /* Its own process group, so _term can signal the whole tree. */
        setpgid(0, 0);

        /* Child. setenv happens HERE, after the fork, not in the parent before
           it: the parent may be several worker threads deep, and setenv mutates
           process-wide state, so stamping the environment in the parent would
           race -- one worker's tuple id could reach another worker's handler.
           The child is single-threaded, so doing it here is race-free and each
           handler sees exactly its own message. */
        setenv("ACTOR_TUPLE_ID",       env->id_hex,   1);
        setenv("ACTOR_CORRELATION_ID", env->corr_hex, 1);
        setenv("ACTOR_CAUSATION_ID",   env->caus_hex, 1);
        setenv("ACTOR_TUPLE_ORIGIN",   env->origin,   1);
        setenv("ACTOR_ATTEMPT",        env->attempt,  1);
        /* Which subscription delivered this tuple. ACTOR_TOPIC may name
           several, and without this a handler serving more than one cannot
           tell them apart -- so a single actor could subscribe widely but not
           dispatch, and callers ran one actor per topic instead. */
        setenv("ACTOR_TUPLE_TOPIC",    env->topic,    1);
        setenv("ACTOR_TUPLE_DEADLINE", env->deadline, 1);

        /* Per-tuple namespaces, before exec so they live and die with this one
           tuple. A failure here must not become a handler that runs anyway:
           _exit non-zero is already how this path reports a failed run, and
           the retry/rejection logic upstream handles it unchanged. */
        if (actor_isolation_tuple() != 0) _exit(1);

        dup2(to_child[0],   STDIN_FILENO);
        dup2(from_child[1], STDOUT_FILENO);
        close(to_child[0]); close(to_child[1]);
        close(from_child[0]); close(from_child[1]);
        execl("/bin/sh", "sh", "-c", handler, NULL);
        _exit(1);
    }

    setpgid(pid, pid);   /* the child does the same; whichever runs first wins */
    child_slot_t* c = &g_children[slot];
    c->pid    = pid;
    c->done   = 0;
    c->status = 0;
    memcpy(c->tuple_id,       hdr->id,             16);
    memcpy(c->correlation_id, hdr->correlation_id, 16);
    snprintf(c->topic, sizeof(c->topic), "%.*s", 32, hdr->topic);
    c->started_ms = mono_ms();
    pthread_mutex_unlock(&child_mu);

    close(to_child[0]);
    close(from_child[1]);

    if (write(to_child[1], in, in_len) < 0) {
        close(to_child[1]);
        close(from_child[0]);
        await_child(slot);
        return RUN_FAILED;
    }
    close(to_child[1]);

    size_t  len = 0;
    ssize_t n;
    while ((n = read(from_child[0], out + len, out_cap - len)) > 0) {
        len += (size_t)n;
        if (len == out_cap) {
            uint8_t drain[256];
            while (read(from_child[0], drain, sizeof(drain)) > 0) {}
            close(from_child[0]);
            await_child(slot);
            fprintf(stderr, "[actor] result exceeded ACTOR_MAX_PAYLOAD=%d, dropping\n",
                    ACTOR_MAX_PAYLOAD);
            return RUN_TOO_LARGE;
        }
    }
    close(from_child[0]);

    child_exit_t e = await_child(slot);
    if (e.terminated) return RUN_TERMINATED;
    if (!WIFEXITED(e.status) || WEXITSTATUS(e.status) != 0) return RUN_FAILED;

    *out_len = len;
    return RUN_OK;
}

#endif /* _WIN32 */

/* invoke_handler: collect the per-message env and spawn the handler. The
 * values travel as a value into platform_spawn, which applies them
 * where it is safe to do so for the platform (in the forked child on Unix, on
 * the parent before CreateProcess on Windows). */
static run_status_t invoke_handler(const actor_header_t* hdr,
                                   const uint8_t*        payload,
                                   size_t                payload_len,
                                   int                   attempt,
                                   const char*           handler,
                                   size_t*               result_len) {
    child_env_t env;
    actor_uuid_hex(hdr->id,             env.id_hex);
    actor_uuid_hex(hdr->correlation_id, env.corr_hex);
    actor_uuid_hex(hdr->causation_id,   env.caus_hex);
    snprintf(env.origin,  sizeof(env.origin),  "%.*s", 31, hdr->origin);
    snprintf(env.attempt, sizeof(env.attempt), "%d", attempt);
    snprintf(env.topic,   sizeof(env.topic),   "%.*s", 32, hdr->topic);
    int64_t deadline = hdr->ttl ? hdr->emitted_at + hdr->ttl : 0;
    snprintf(env.deadline, sizeof(env.deadline), "%lld", (long long)deadline);

    return platform_spawn(payload, payload_len, g_result_buf, ACTOR_MAX_PAYLOAD, &env,
                          hdr, handler, result_len);
}

/* ── Publish ─────────────────────────────────────────────────────────────── */

/* If handler output begins with a bare topic name on its own line
 * (e.g. "sql_query\n{...}"), use that topic and skip the prefix line.
 * Otherwise fall back to the lane's result topic and use the full buffer.
 * A valid topic prefix: only [a-zA-Z0-9_] chars followed immediately by '\n'. */
static const char* result_topic(size_t result_len, size_t* payload_off, const char* dflt) {
    *payload_off = 0;
    const char* p = (const char*)g_result_buf;
    size_t i = 0;
    while (i < result_len && i < 31) {
        char c = p[i];
        if (c == '\n') {
            if (i == 0) break;           /* empty first line — no override   */
            *payload_off = i + 1;
            return p;                    /* caller uses p[0..i-1] as topic   */
        }
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') ||  c == '_')) {
            break;                       /* non-identifier char — no override */
        }
        i++;
    }
    return dflt;
}

static void publish_result(const actor_header_t* in_hdr, size_t result_len,
                           const char* dflt_topic) {
    size_t        payload_off  = 0;
    const char*   raw_topic    = result_topic(result_len, &payload_off, dflt_topic);

    /* if the handler wrote a topic prefix, null-terminate it in the buffer */
    char topic_buf[32] = {0};
    if (payload_off > 0) {
        size_t tlen = payload_off - 1;   /* exclude the '\n' */
        if (tlen >= sizeof(topic_buf)) tlen = sizeof(topic_buf) - 1;
        memcpy(topic_buf, raw_topic, tlen);
    } else {
        strncpy(topic_buf, raw_topic, sizeof(topic_buf) - 1);
    }

    const uint8_t* payload     = g_result_buf + payload_off;
    size_t         payload_len = result_len   - payload_off;

    actor_header_t out_hdr;
    actor_tuple_init(&out_hdr,
                     topic_buf,
                     cfg.id,
                     in_hdr->correlation_id,
                     in_hdr->id,
                     (uint32_t)payload_len);
    actor_uuid_gen(out_hdr.id);
    out_hdr.ttl = cfg.ttl_ns;

    /* assemble header + payload into frame buffer, write to LMDB outbox */
    size_t frame_len = sizeof(actor_header_t) + payload_len;
    memcpy(g_frame_buf, &out_hdr, sizeof(actor_header_t));
    memcpy(g_frame_buf + sizeof(actor_header_t), payload, payload_len);
    lmdb_put(dbi_outbox, out_hdr.id, 16, g_frame_buf, frame_len);

    /* publish as single contiguous message — NNG sub0 prefix-matches on topic */
    nng_send(nng_pub, g_frame_buf, frame_len, 0);

    lmdb_del(dbi_outbox, out_hdr.id, 16);
}

/* ── Rejection ───────────────────────────────────────────────────────────── */

/* Why a tuple was rejected; the names are the wire values of "reason". */
typedef enum {
    REJECT_PAYLOAD_CAP_EXCEEDED,
    REJECT_RESULT_CAP_EXCEEDED,
    REJECT_MAX_RETRIES_EXCEEDED,
    REJECT_TTL_EXPIRED,
    REJECT_TERMINATED,
} reject_reason_t;

static const char* const reject_names[] = {
    [REJECT_PAYLOAD_CAP_EXCEEDED] = "payload_cap_exceeded",
    [REJECT_RESULT_CAP_EXCEEDED]  = "result_cap_exceeded",
    [REJECT_MAX_RETRIES_EXCEEDED] = "max_retries_exceeded",
    [REJECT_TTL_EXPIRED]          = "ttl_expired",
    [REJECT_TERMINATED]           = "terminated",
};

static void publish_rejection(const actor_header_t* in_hdr, reject_reason_t why) {
    const char* reason = reject_names[why];
    char   id_hex[33], corr_hex[33];
    actor_uuid_hex(in_hdr->id,             id_hex);
    actor_uuid_hex(in_hdr->correlation_id, corr_hex);

    /* Header strings are null-padded, not terminated, and origin is the
       sender's to choose: bound both, and keep origin from breaking the JSON. */
    char origin[33];
    snprintf(origin, sizeof(origin), "%.*s", 32, in_hdr->origin);
    for (char* c = origin; *c; c++)
        if (*c == '"' || *c == '\\' || (unsigned char)*c < 0x20) *c = '?';

    char payload[512];
    size_t plen = (size_t)snprintf(payload, sizeof(payload),
        "{\"tuple_id\":\"%s\",\"correlation_id\":\"%s\","
        "\"origin\":\"%s\",\"topic\":\"%.*s\",\"reason\":\"%s\"}",
        id_hex, corr_hex,
        origin, 32, in_hdr->topic, reason);
    if (plen >= sizeof(payload)) plen = sizeof(payload) - 1;

    actor_header_t hdr;
    actor_tuple_init(&hdr, "tuple_rejected", cfg.id,
                     in_hdr->correlation_id, in_hdr->id, (uint32_t)plen);
    actor_uuid_gen(hdr.id);

    uint8_t frame[sizeof(actor_header_t) + sizeof(payload)];
    memcpy(frame, &hdr, sizeof(actor_header_t));
    memcpy(frame + sizeof(actor_header_t), payload, plen);
    nng_send(nng_pub, frame, sizeof(actor_header_t) + plen, 0);

    fprintf(stderr, "[actor] rejected tuple %s reason=%s\n", id_hex, reason);
}

/* ── Heartbeat ───────────────────────────────────────────────────────────── */

static void emit_heartbeat(void) {
    char running[ACTOR_MAX_CONCURRENCY * 224];
    char services[1024];
    running_json(running, sizeof(running));
    services_json(services, sizeof(services));

    char   payload[sizeof(running) + sizeof(services) + 256];
    size_t plen = (size_t)snprintf(payload, sizeof(payload),
                                   "{\"id\":\"%s\",\"inbox\":%zu,\"outbox\":%zu,"
                                   "\"running\":%s,\"services\":%s}",
                                   cfg.id, lmdb_count(dbi_inbox), lmdb_count(dbi_outbox),
                                   running, services);
    if (plen >= sizeof(payload)) plen = sizeof(payload) - 1;

    actor_header_t hdr;
    actor_tuple_init(&hdr, "heartbeat", cfg.id, NULL, NULL, (uint32_t)plen);
    actor_uuid_gen(hdr.id);
    hdr.ttl = (int64_t)cfg.heartbeat_ms * 3 * 1000000LL;

    uint8_t frame[sizeof(actor_header_t) + sizeof(payload)];
    memcpy(frame, &hdr, sizeof(actor_header_t));
    memcpy(frame + sizeof(actor_header_t), payload, plen);
    nng_send(nng_pub, frame, sizeof(actor_header_t) + plen, 0);
}

/* Send a heartbeat if one is due. One thread only: the reaper on Unix, so a
   busy worker never delays it; the receive loop on Windows, which has none. */
static void heartbeat_tick(void) {
    static int64_t last_ms;
    if (cfg.heartbeat_ms <= 0) return;
    int64_t now = mono_ms();
    if (last_ms && now - last_ms < cfg.heartbeat_ms) return;
    emit_heartbeat();
    last_ms = now;
}

/* ── Process one tuple ───────────────────────────────────────────────────── */

static void process_tuple(const actor_header_t* hdr,
                          const uint8_t*        payload,
                          size_t                payload_len,
                          tuple_source_t        source,
                          const lane_t*         lane) {
    /* hard cap on incoming payload */
    if (payload_len > ACTOR_MAX_PAYLOAD) {
        publish_rejection(hdr, REJECT_PAYLOAD_CAP_EXCEEDED);
        return;
    }

    /* publish_result reuses g_frame_buf, which may hold this very frame */
    uint8_t id[16];
    memcpy(id, hdr->id, sizeof(id));

    /* write inbox LMDB — a replay is already in g_frame_buf */
    size_t frame_len = sizeof(actor_header_t) + payload_len;
    if (source == TUPLE_RECEIVED) {
        memcpy(g_frame_buf, hdr, sizeof(actor_header_t));
        memcpy(g_frame_buf + sizeof(actor_header_t), payload, payload_len);
    }
    lmdb_put(dbi_inbox, id, sizeof(id), g_frame_buf, frame_len);

    /* exponential backoff retry loop */
    int attempt = 0;
    while (attempt <= cfg.retry_max) {
        size_t       result_len = 0;
        run_status_t run = invoke_handler(hdr, payload, payload_len,
                                          hdr->attempt + attempt, lane->handler, &result_len);
        if (run == RUN_OK) {
            /* no output means nothing to publish */
            if (result_len > 0) publish_result(hdr, result_len, lane->result_topic);
            break;
        }
        if (run == RUN_TOO_LARGE) {
            /* no point retrying */
            publish_rejection(hdr, REJECT_RESULT_CAP_EXCEEDED);
            break;
        }
        if (run == RUN_TERMINATED) {
            /* deliberate: neither retried nor replayed */
            publish_rejection(hdr, REJECT_TERMINATED);
            break;
        }

        /* Stopping: leave it in the inbox for the next run to replay. */
        if (g_stop) return;

        attempt++;
        if (attempt > cfg.retry_max) {
            publish_rejection(hdr, REJECT_MAX_RETRIES_EXCEEDED);
            break;
        }

        int64_t ns = 100000000LL << (attempt < 20 ? attempt - 1 : 19);
        struct timespec backoff = { ns / 1000000000LL, ns % 1000000000LL };
        nanosleep(&backoff, NULL);
        if (actor_tuple_expired(hdr)) {   /* the caller gave up while we waited */
            publish_rejection(hdr, REJECT_TTL_EXPIRED);
            break;
        }
        fprintf(stderr, "[actor] retry %d/%d\n", attempt, cfg.retry_max);
    }

    lmdb_del(dbi_inbox, id, sizeof(id));
}

/* ── Main loop ───────────────────────────────────────────────────────────── */

/* Orphaned descendants are reaped by the dispatcher's reaper thread -- see the
 * child status dispatcher above. This comment is kept because the problem it
 * documents is still the reason that machinery exists.
 *
 * platform_spawn waits for the shell it forks, so its own child never lingers.
 * The leak is one generation further down: the handler's shell spawns its own
 * children, and any that outlive it are reparented to this process. In a
 * container that is PID 1, whose kernel-assigned duty is to reap them -- and
 * nothing here did, so they accumulated as zombies for the lifetime of the
 * pod. Observed live: 21 of them (gpg, gpgconf, sh) aged up to four hours, in
 * a container whose only live process was this one.
 *
 * Zombies hold a PID table slot each. Left long enough the cgroup's pids.max
 * is reached, and the next fork in ANY process in that container fails with
 * EAGAIN -- which surfaces as an unrelated handler dying for no visible
 * reason, far from the cause.
 *
 * This used to be a `waitpid(-1, WNOHANG)` sweep run between messages, safe
 * only because the loop was serial: a sweep cannot tell an orphan from another
 * worker's child, and harvesting the latter loses the exit status that decides
 * success or retry -- silently turning a failed run into a successful one. The
 * reaper thread resolves both jobs at once by being the single waiter and
 * dispatching each status to the slot that owns it; a pid matching no slot is
 * by construction an orphan.
 *
 */

/* Copy this lane's next unclaimed replay into g_frame_buf, as attempt + 1. */
static bool replay_next(int lane, size_t* frame_len) {
    if (atomic_load(&g_replay_left) == 0) return false;
    for (int i = 0; i < g_replay_n; i++) {
        if (g_replay_lane[i] != lane || atomic_exchange(&g_replay_taken[i], 1)) continue;
        atomic_fetch_sub(&g_replay_left, 1);

        MDB_txn* txn;
        MDB_val  k = { 16, g_replay[i] }, v;
        if (mdb_txn_begin(mdb_env, NULL, MDB_RDONLY, &txn) != 0) return false;
        bool found = mdb_get(txn, dbi_inbox, &k, &v) == 0;
        bool fits  = found && v.mv_size >= sizeof(actor_header_t) && v.mv_size <= ACTOR_MAX_FRAME;
        if (fits) {
            memcpy(g_frame_buf, v.mv_data, v.mv_size);
            ((actor_header_t*)g_frame_buf)->attempt++;
            *frame_len = v.mv_size;
        }
        mdb_txn_abort(txn);
        if (found && !fits) {
            fprintf(stderr, "[actor] inbox entry of %zu bytes cannot be replayed, dropping\n",
                    v.mv_size);
            lmdb_del(dbi_inbox, g_replay[i], 16);
        }
        return fits;
    }
    return false;
}

/* One turn of a lane's receive loop, shared by its workers; false means stop. */
static bool serve_once(int lane) {
    const lane_t*  l   = &g_lanes[lane];
    nng_msg*       msg = NULL;
    const uint8_t* frame;
    size_t         frame_len;
    tuple_source_t source;

    if (replay_next(lane, &frame_len)) {
        source = TUPLE_REPLAYED;
        frame  = g_frame_buf;
    } else {
        int rc = nng_recvmsg(l->sub, &msg, 0);
        if (rc == NNG_ETIMEDOUT) return true;
        if (rc != 0) {
            if (!g_stop) fprintf(stderr, "[actor] recv error: %s\n", nng_strerror(rc));
            return false;
        }
        source    = TUPLE_RECEIVED;
        frame     = nng_msg_body(msg);
        frame_len = nng_msg_len(msg);
    }

    if (frame_len < sizeof(actor_header_t)) {
        fprintf(stderr, "[actor] short message %zu bytes, dropping\n", frame_len);
        nng_msg_free(msg);
        return true;
    }

    /* A producer whose clock the runtime can't trust leaves emitted_at at 0,
       and the first actor to receive the tuple stamps it from its own clock.
       TTL is judged as now > emitted_at + ttl, so a stamp from a skewed clock
       would expire tuples early or never. Stamped before the inbox write, so
       a replay keeps it. */
    if (source == TUPLE_RECEIVED) {
        actor_header_t* in = (actor_header_t*)nng_msg_body(msg);
        if (in->emitted_at == 0) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            in->emitted_at = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
        }
    }

    const actor_header_t* hdr         = (const actor_header_t*)frame;
    const uint8_t*        payload     = frame + sizeof(actor_header_t);
    size_t                payload_len = frame_len - sizeof(actor_header_t);

    /* TTL check — before process_tuple, so an abandoned request costs the
       check and nothing else. This is what keeps a backed-up queue from
       feeding on itself: work whose caller has already given up is dropped
       rather than forked. */
    if (actor_tuple_expired(hdr)) {
        publish_rejection(hdr, REJECT_TTL_EXPIRED);
        if (source == TUPLE_REPLAYED) lmdb_del(dbi_inbox, hdr->id, 16);
    } else {
        process_tuple(hdr, payload, payload_len, source, l);
    }
    if (msg) nng_msg_free(msg);
    return true;
}

#ifndef _WIN32
/* Extra worker, bound to one lane. nng sockets are safe to use from several
   threads, so each worker simply blocks in its own recv; the lane's SUB socket
   hands each message to exactly one of them. */
static void* worker_main(void* arg) {
    int lane = (int)(intptr_t)arg;
    while (!g_stop) {
        if (!serve_once(lane)) break;
    }
    return NULL;
}
#endif

/* nng sizes its pools by core count -- two task threads and one expire thread
   per core, plus resolvers and a poller -- and every thread carries this
   process's TLS, which is the 2 MiB of per-worker buffers. On a many-core node
   the pools, not the work, set the footprint. The actor drives a few local
   sockets, and the minimums serve that on any machine. Must precede the first
   nng call. */
static void fix_nng_threads(void) {
#if NNG_MAJOR_VERSION == 1 && NNG_MINOR_VERSION >= 11
    nng_init_set_parameter(NNG_INIT_NUM_TASK_THREADS,     2);
    nng_init_set_parameter(NNG_INIT_NUM_EXPIRE_THREADS,   1);
    nng_init_set_parameter(NNG_INIT_NUM_POLLER_THREADS,   1);
    nng_init_set_parameter(NNG_INIT_NUM_RESOLVER_THREADS, 1);
#endif
}

int actor_run(void) {
    fix_nng_threads();
    if (cfg_load() < 0 || lanes_load() < 0) return -1;
#ifdef _WIN32
    if (getenv("ACTOR_INIT") || services_requested() ||
        getenv("PROXY_SUB_BIND") || getenv("PROXY_PUB_BIND")) {
        fprintf(stderr, "[actor] ACTOR_INIT, ACTOR_SERVICE_* and hosting the bus "
                        "are not available on Windows\n");
        return -1;
    }
#else
    if (services_load() < 0) return -1;
#endif

#ifndef _WIN32
    /* Confinement goes here and nowhere else: after the config read, before
       the sockets, the LMDB open and -- critically -- before any thread is
       created. See actor_isolation.h for why that ordering is load bearing.
       An actor that requested isolation it cannot have does not proceed. */
    if (actor_isolation_apply() < 0) {
        fprintf(stderr, "[actor] FATAL: isolation requested but not applied\n");
        return -1;
    }
    if (run_init() < 0 || bus_host() < 0) return -1;
#endif

    signal(SIGTERM, on_signal);
    signal(SIGINT,  on_signal);

    if (nng_setup()  < 0) return -1;
    if (lmdb_setup() < 0) return -1;

    fprintf(stderr, "[actor] id=%s topic(s)=%s handler=%s max_payload=%d concurrency=%d\n",
            cfg.id, cfg.topic, cfg.handler, ACTOR_MAX_PAYLOAD, cfg.concurrency);
    for (int i = 0; g_nlanes > 1 && i < g_nlanes; i++)
        fprintf(stderr, "[actor] lane %d: topic(s)=%s handler=%s result=%s concurrency=%d\n",
                i, g_lanes[i].topics, g_lanes[i].handler, g_lanes[i].result_topic,
                g_lanes[i].concurrency);
    if (g_replay_n) fprintf(stderr, "[actor] replaying %d tuple(s) from the inbox\n", g_replay_n);

#ifndef _WIN32
    /* The reaper runs even at concurrency 1: it is what collects the orphaned
       grandchildren this process inherits as PID 1, a job the old between-
       messages sweep used to do. */
    for (int i = 0; i < g_nservices; i++) service_spawn(&g_services[i]);

    pthread_t reaper;
    int reaper_started = (pthread_create(&reaper, NULL, reaper_main, NULL) == 0);
    if (!reaper_started) {
        fprintf(stderr, "[actor] FATAL: could not start reaper thread\n");
        return -1;
    }

    /* The main thread is lane 0's first worker. */
    pthread_t workers[ACTOR_MAX_CONCURRENCY];
    int nworkers = 0;
    for (int li = 0; li < g_nlanes; li++) {
        for (int k = (li == 0); k < g_lanes[li].concurrency; k++) {
            if (pthread_create(&workers[nworkers], NULL, worker_main, (void*)(intptr_t)li) != 0) {
                fprintf(stderr, "[actor] could not start a worker for lane %d\n", li);
                break;
            }
            nworkers++;
        }
    }
#endif

    while (!g_stop) {
#ifdef _WIN32
        heartbeat_tick();
#endif
        if (!serve_once(0)) break;
    }

    fprintf(stderr, "[actor] shutting down\n");
#ifndef _WIN32
    g_stop = 1;
    for (int i = 0; i < nworkers; i++) pthread_join(workers[i], NULL);
    atomic_store(&g_reaper_stop, 1);
    pthread_join(reaper, NULL);
#endif
    nng_close(nng_pub);
    for (int i = 0; i < g_nlanes; i++) nng_close(g_lanes[i].sub);
#ifndef _WIN32
    nng_close(nng_ctl);
    bus_unhost();
#endif
    mdb_env_close(mdb_env);
#ifndef _WIN32
    if (atomic_load(&g_services_failed)) return -1;
#endif
    return 0;
}
