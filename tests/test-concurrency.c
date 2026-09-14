#define _POSIX_C_SOURCE 200809L
/* test-concurrency.c — ACTOR_CONCURRENCY and the child status dispatcher.
 *
 * The dangerous change here is not "run several handlers at once", it is that
 * child reaping had to move. It used to be a between-messages
 * `waitpid(-1, WNOHANG)` sweep, safe only because the loop was serial: with
 * more than one handler in flight a sweep cannot tell an orphan from another
 * worker's child, and harvesting the latter loses the exit status that decides
 * success or retry — silently reporting a FAILED handler run as a successful
 * one.
 *
 * So the tests that matter most are not the throughput ones. They are the ones
 * asserting that a failing handler still fails, and a signalled one still
 * fails, while several handlers run concurrently — i.e. that the reaper hands
 * each status to the right worker and never lets one go missing.
 *
 * Build:  make test-concurrency && ./bin/test-concurrency
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <spawn.h>
#include <signal.h>
#include <unistd.h>
#include <sys/wait.h>
#include <time.h>
#include <nng/nng.h>
#include <nng/protocol/pubsub0/pub.h>
#include <nng/protocol/pubsub0/sub.h>

static int failures = 0;
extern char **environ;
static void ms(int m) { struct timespec t = { m / 1000, (m % 1000) * 1000000L }; nanosleep(&t, NULL); }

#define SP "tcp://127.0.0.1:55666"
#define PP "tcp://127.0.0.1:55667"
#define TEST(n) printf("=== %s ===\n", n)
#define PASS()  printf("  PASS\n")
#define FAIL(m) do { printf("  FAIL: %s\n", m); failures++; } while (0)
#define CHECK(c,m) do { if (c) PASS(); else FAIL(m); } while (0)

static void cleanup(void) {
    system("pkill -9 -f 'bin/mesh-proxy' 2>/dev/null");
    system("pkill -9 -f 'bin/actor' 2>/dev/null");
    ms(300);
}

static pid_t sp_(char **a, char **e) { pid_t p; if (posix_spawn(&p, a[0], NULL, NULL, a, e)) return -1; return p; }

static int64_t now_ms(void) {
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

/* Build and publish a frame. ttl_ns 0 = no expiry. */
static void sendm(const char *topic, const char *payload, int64_t ttl_ns, int64_t emitted_override) {
    nng_socket s; nng_pub0_open(&s); nng_dial(s, PP, NULL, 0); ms(60);
    size_t pl = strlen(payload);
    uint8_t f[1024] = {0};
    size_t tl = strlen(topic); if (tl > 31) tl = 31;
    memcpy(f, topic, tl);
    memcpy(f + 80, "test", 4);                       /* origin */
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    int64_t ns = emitted_override ? emitted_override
                                  : ts.tv_sec * 1000000000LL + ts.tv_nsec;
    memcpy(f + 112, &ns, 8);                         /* emitted_at */
    memcpy(f + 120, &ttl_ns, 8);                     /* ttl        */
    uint32_t p32 = (uint32_t)pl;
    memcpy(f + 132, &p32, 4);                        /* payload_len */
    memcpy(f + 256, payload, pl);
    nng_send(s, f, 256 + pl, 0);
    nng_close(s);
}

/* Subscribers must exist BEFORE anything is published — this is pub/sub, so a
 * frame sent while nobody is listening is simply gone. Every test therefore
 * opens its collectors first, then sends, then drains. Getting this backwards
 * makes a perfectly good actor look like it dropped everything. */
static nng_socket sub_open(const char *topic) {
    nng_socket s; nng_sub0_open(&s); nng_dial(s, SP, NULL, 0);
    nng_socket_set(s, NNG_OPT_SUB_SUBSCRIBE, topic, strlen(topic));
    nng_socket_set_ms(s, NNG_OPT_RECVTIMEO, 1200);
    ms(120);   /* let the dial settle before the caller publishes */
    return s;
}

/* Drain an already-open subscriber for up to `budget` ms, returning early once
 * `expect` frames have arrived (0 = always use the whole budget). Timing tests
 * MUST pass `expect`: otherwise the elapsed time they measure is dominated by
 * the drain budget rather than by how long the handlers actually took. */
static int drain_n(nng_socket s, int budget, int expect, char *first, size_t cap) {
    int n = 0;
    int64_t deadline = now_ms() + budget;
    while (now_ms() < deadline) {
        if (expect > 0 && n >= expect) break;
        nng_msg *m = NULL;
        if (nng_recvmsg(s, &m, 0) != 0) continue;   /* recv timeout — keep waiting */
        if (n == 0 && first && nng_msg_len(m) > 256) {
            size_t l = nng_msg_len(m) - 256; if (l >= cap) l = cap - 1;
            memcpy(first, (uint8_t *)nng_msg_body(m) + 256, l); first[l] = 0;
        }
        n++; nng_msg_free(m);
    }
    return n;
}

static int drain(nng_socket s, int budget, char *first, size_t cap) {
    return drain_n(s, budget, 0, first, cap);
}

static pid_t start_proxy(void) {
    char *a[] = { "./bin/mesh-proxy", NULL };
    char *e[] = { "PROXY_SUB_BIND=" PP, "PROXY_PUB_BIND=" SP, NULL };
    pid_t p = sp_(a, e); ms(600); return p;
}

/* NOTE: handler output starting with a bare identifier line is consumed by the
 * runtime as a TOPIC OVERRIDE (see result_topic), so `echo ok` publishes to
 * topic "ok" and never to ACTOR_RESULT_TOPIC. Handlers here emit ":ok" so the
 * leading ':' disables that and results land where the test subscribes. */
static pid_t start_actor(const char *handler, const char *conc, const char *retry, const char *dir) {
    static char h[512], c[64], r[64], d[256];
    snprintf(h, sizeof h, "ACTOR_HANDLER=%s", handler);
    snprintf(c, sizeof c, "ACTOR_CONCURRENCY=%s", conc);
    snprintf(r, sizeof r, "ACTOR_RETRY_MAX=%s", retry);
    snprintf(d, sizeof d, "ACTOR_LMDB_PATH=%s", dir);
    char cmd[512]; snprintf(cmd, sizeof cmd, "rm -rf %s; mkdir -p %s", dir, dir);
    system(cmd);
    char *a[] = { "./bin/actor", NULL };
    char *e[] = { "ACTOR_BUS_SUB=" SP, "ACTOR_BUS_PUB=" PP, "ACTOR_HEARTBEAT_MS=0",
                  "ACTOR_ID=tc", "ACTOR_TOPIC=work", "ACTOR_RESULT_TOPIC=done",
                  h, c, r, d, NULL };
    pid_t p = sp_(a, e); ms(700); return p;
}

static void stop(pid_t proxy, pid_t actor) {
    if (actor > 0) { kill(actor, SIGKILL); waitpid(actor, NULL, 0); }
    if (proxy > 0) { kill(proxy, SIGKILL); waitpid(proxy, NULL, 0); }
    ms(200);
}

/* ── 1. Concurrency actually overlaps handler runs ────────────────────────── */

static void t_parallel(void) {
    TEST("concurrency=4 overlaps handlers (vs serial at 1)");
    cleanup();
    pid_t pp = start_proxy();
    pid_t ap = start_actor("sh -c 'sleep 0.5; echo :ok'", "4", "0", "/tmp/tc_par");
    nng_socket done = sub_open("done");
    int64_t t0 = now_ms();
    for (int i = 0; i < 4; i++) sendm("work", "x", 0, 0);
    int n = drain_n(done, 4000, 4, NULL, 0);
    int64_t elapsed = now_ms() - t0;
    nng_close(done);
    stop(pp, ap);
    printf("  4 x 500ms handlers took %lldms, got %d results\n", (long long)elapsed, n);
    /* Serial would be >=2000ms. Allow generous headroom for fork/exec. */
    CHECK(n == 4 && elapsed < 1800, "did not overlap (or lost results)");
}

static void t_serial_default(void) {
    TEST("default concurrency is 1 (unchanged behaviour)");
    cleanup();
    pid_t pp = start_proxy();
    /* No ACTOR_CONCURRENCY -> default 1 */
    char *a[] = { "./bin/actor", NULL };
    system("rm -rf /tmp/tc_ser; mkdir -p /tmp/tc_ser");
    char *e[] = { "ACTOR_BUS_SUB=" SP, "ACTOR_BUS_PUB=" PP, "ACTOR_HEARTBEAT_MS=0",
                  "ACTOR_ID=tc", "ACTOR_TOPIC=work", "ACTOR_RESULT_TOPIC=done",
                  "ACTOR_HANDLER=sh -c 'sleep 0.4; echo :ok'", "ACTOR_RETRY_MAX=0",
                  "ACTOR_LMDB_PATH=/tmp/tc_ser", NULL };
    pid_t ap = sp_(a, e); ms(700);
    nng_socket done = sub_open("done");
    int64_t t0 = now_ms();
    for (int i = 0; i < 3; i++) sendm("work", "x", 0, 0);
    int n = drain_n(done, 5000, 3, NULL, 0);
    int64_t elapsed = now_ms() - t0;
    nng_close(done);
    stop(pp, ap);
    printf("  3 x 400ms handlers took %lldms, got %d results\n", (long long)elapsed, n);
    CHECK(n == 3 && elapsed >= 1100, "default should still be serial");
}

/* ── 2. THE ONE THAT MATTERS: failures stay failures under concurrency ────── */

static void t_failure_not_silent_success(void) {
    TEST("a failing handler is NOT reported as success (concurrency=4)");
    cleanup();
    pid_t pp = start_proxy();
    /* Always exits non-zero after a beat, so several are in flight at once and
       the reaper must route each status to the right worker. With retries
       disabled, every message must end as a rejection and never as a result. */
    pid_t ap = start_actor("sh -c 'sleep 0.2; exit 3'", "4", "0", "/tmp/tc_fail");
    nng_socket rej = sub_open("tuple_rejected");
    nng_socket done = sub_open("done");
    for (int i = 0; i < 6; i++) sendm("work", "x", 0, 0);
    char reason[512] = {0};
    int rejected = drain(rej, 5000, reason, sizeof reason);
    int succeeded = drain(done, 600, NULL, 0);
    nng_close(rej); nng_close(done);
    stop(pp, ap);
    printf("  rejected=%d succeeded=%d  first=%.120s\n", rejected, succeeded, reason);
    CHECK(succeeded == 0, "a failed handler run was published as a SUCCESS");
    CHECK(rejected == 6, "not every failed run was rejected");
    CHECK(strstr(reason, "max_retries_exceeded") != NULL, "unexpected rejection reason");
}

static void t_signalled_handler_is_failure(void) {
    TEST("a killed handler is a failure, not a success (concurrency=4)");
    cleanup();
    pid_t pp = start_proxy();
    /* Dies by signal rather than exit code: WIFEXITED is false, which must not
       be read as success. */
    pid_t ap = start_actor("sh -c 'sleep 0.2; kill -9 $$'", "4", "0", "/tmp/tc_sig");
    nng_socket rej = sub_open("tuple_rejected");
    nng_socket done = sub_open("done");
    for (int i = 0; i < 4; i++) sendm("work", "x", 0, 0);
    int rejected = drain(rej, 5000, NULL, 0);
    int succeeded = drain(done, 600, NULL, 0);
    nng_close(rej); nng_close(done);
    stop(pp, ap);
    printf("  rejected=%d succeeded=%d\n", rejected, succeeded);
    CHECK(succeeded == 0, "a signalled handler was published as a SUCCESS");
    CHECK(rejected == 4, "not every signalled run was rejected");
}

static void t_mixed_success_and_failure(void) {
    TEST("interleaved good/bad handlers each get their OWN status");
    cleanup();
    pid_t pp = start_proxy();
    /* Payload decides the exit code, so successes and failures run
       concurrently. If the reaper ever crossed statuses between slots, the
       counts would not come out 3/3. */
    pid_t ap = start_actor(
        "sh -c 'read x; sleep 0.2; if [ \"$x\" = good ]; then echo :ok; else exit 7; fi'",
        "4", "0", "/tmp/tc_mix");
    nng_socket done = sub_open("done");
    nng_socket rej  = sub_open("tuple_rejected");
    for (int i = 0; i < 3; i++) { sendm("work", "good", 0, 0); sendm("work", "bad", 0, 0); }
    int ok  = drain(done, 5000, NULL, 0);
    int bad = drain(rej, 1500, NULL, 0);
    nng_close(done); nng_close(rej);
    stop(pp, ap);
    printf("  succeeded=%d rejected=%d (expect 3/3)\n", ok, bad);
    CHECK(ok == 3 && bad == 3, "statuses were crossed between concurrent handlers");
}

/* ── 3. Orphan reaping still happens (the reason reaping existed) ─────────── */

static void t_orphans_reaped(void) {
    TEST("orphaned grandchildren are still reaped (no zombies)");
    cleanup();
    pid_t pp = start_proxy();
    /* The handler backgrounds a child that outlives it; that child is
       reparented to the actor, which must reap it. */
    pid_t ap = start_actor("sh -c '(sleep 0.3; exit 0) & echo :ok'", "4", "0", "/tmp/tc_orph");
    nng_socket done = sub_open("done");
    for (int i = 0; i < 5; i++) sendm("work", "x", 0, 0);
    int n = drain(done, 4000, NULL, 0);
    nng_close(done);
    ms(1200);  /* let the grandchildren exit and be reaped */
    char cmd[256];
    snprintf(cmd, sizeof cmd,
             "ps -o stat=,ppid= -ax 2>/dev/null | awk '$1 ~ /^Z/ && $2 == %d' | wc -l", ap);
    FILE *f = popen(cmd, "r");
    int zombies = -1;
    if (f) { if (fscanf(f, "%d", &zombies) != 1) zombies = -1; pclose(f); }
    stop(pp, ap);
    printf("  results=%d zombies=%d\n", n, zombies);
    CHECK(n == 5, "lost results");
    CHECK(zombies == 0, "orphaned grandchildren were left as zombies");
}

/* ── 4. TTL is enforced before the handler is spawned ─────────────────────── */

static void t_ttl_expired_not_run(void) {
    TEST("an expired tuple is rejected without running the handler");
    cleanup();
    pid_t pp = start_proxy();
    system("rm -f /tmp/tc_ttl_ran");
    pid_t ap = start_actor("sh -c 'touch /tmp/tc_ttl_ran; echo :ok'", "4", "0", "/tmp/tc_ttl");
    /* emitted 10s ago with a 1s ttl -> already expired on arrival */
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    int64_t old = (ts.tv_sec - 10) * 1000000000LL + ts.tv_nsec;
    nng_socket rej = sub_open("tuple_rejected");
    sendm("work", "x", 1000000000LL, old);
    char reason[512] = {0};
    int rejected = drain(rej, 3000, reason, sizeof reason);
    nng_close(rej);
    int ran = access("/tmp/tc_ttl_ran", 0) == 0;
    stop(pp, ap);
    printf("  rejected=%d handler_ran=%d first=%.120s\n", rejected, ran, reason);
    CHECK(rejected == 1, "expired tuple was not rejected");
    CHECK(!ran, "handler ran for an already-expired tuple");
    CHECK(strstr(reason, "ttl_expired") != NULL, "wrong rejection reason");
}

static void t_live_ttl_still_runs(void) {
    TEST("a tuple within its ttl still runs (ttl is not a blanket drop)");
    cleanup();
    pid_t pp = start_proxy();
    pid_t ap = start_actor("sh -c 'echo :ok'", "4", "0", "/tmp/tc_ttl2");
    nng_socket done = sub_open("done");
    sendm("work", "x", 60000000000LL, 0);   /* 60s ttl, emitted now */
    int n = drain(done, 3000, NULL, 0);
    nng_close(done);
    stop(pp, ap);
    printf("  results=%d\n", n);
    CHECK(n == 1, "a live tuple was dropped");
}

/* ── 5. Retries ───────────────────────────────────────────────────────────── */

static void t_retry_attempts_and_backoff(void) {
    TEST("retries count up in ACTOR_ATTEMPT and keep backing off past 1s");
    cleanup();
    pid_t pp = start_proxy();
    system("rm -f /tmp/tc_retry_log");
    pid_t ap = start_actor("sh -c 'echo $ACTOR_ATTEMPT >> /tmp/tc_retry_log; exit 1'",
                           "1", "5", "/tmp/tc_retry");
    nng_socket rej = sub_open("tuple_rejected");
    int64_t t0 = now_ms();
    sendm("work", "x", 0, 0);
    int rejected = drain_n(rej, 8000, 1, NULL, 0);
    int64_t elapsed = now_ms() - t0;
    nng_close(rej);
    stop(pp, ap);
    char log[64] = {0};
    FILE *f = fopen("/tmp/tc_retry_log", "r");
    if (f) { log[fread(log, 1, sizeof log - 1, f)] = 0; fclose(f); }
    int ok = strcmp(log, "0\n1\n2\n3\n4\n5\n") == 0;
    for (char *c = log; *c; c++) if (*c == '\n') *c = ' ';
    printf("  rejected=%d elapsed=%lldms attempts=[%s]\n", rejected, (long long)elapsed, log);
    CHECK(rejected == 1, "exhausted retries were not rejected");
    CHECK(ok, "ACTOR_ATTEMPT did not count the retries");
    /* 100+200+400+800+1600ms: the fifth backoff is the one that used to vanish */
    CHECK(elapsed >= 3100, "backoff was skipped");
}

int main(void) {
    printf("actor concurrency tests\n\n");
    t_parallel();
    t_serial_default();
    t_failure_not_silent_success();
    t_signalled_handler_is_failure();
    t_mixed_success_and_failure();
    t_orphans_reaped();
    t_ttl_expired_not_run();
    t_live_ttl_still_runs();
    t_retry_attempts_and_backoff();
    cleanup();
    printf("\n%s (%d failure%s)\n", failures ? "FAILED" : "ALL PASSED",
           failures, failures == 1 ? "" : "s");
    return failures ? 1 : 0;
}
