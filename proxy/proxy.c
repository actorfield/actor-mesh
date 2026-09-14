// proxy.c — NNG mesh bus
//
// Actors publish to PROXY_SUB_BIND and subscribe to PROXY_PUB_BIND.
// Every message is forwarded unchanged.

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <nng/nng.h>
#include <nng/protocol/pubsub0/pub.h>
#include <nng/protocol/pubsub0/sub.h>
#include "actor_tuple.h"
#include "actor_uuid.h"

static volatile sig_atomic_t g_stop = 0;
static void on_signal(int s) { (void)s; g_stop = 1; }
static nng_socket g_pub;

static void emit_heartbeat(const char* id) {
    char payload[128];
    size_t plen = snprintf(payload, sizeof(payload), "{\"id\":\"%s\"}", id);
    actor_header_t hdr;
    actor_tuple_init(&hdr, "heartbeat", id, NULL, NULL, (uint32_t)plen);
    actor_uuid_gen(hdr.id);
    uint8_t frame[sizeof(actor_header_t) + 128];
    memcpy(frame, &hdr, sizeof(actor_header_t));
    memcpy(frame + sizeof(actor_header_t), payload, plen);
    nng_send(g_pub, frame, sizeof(actor_header_t) + plen, 0);
}

static int listen_all(nng_socket sock, const char *urls) {
    char buf[256]; const char *start = urls, *p;
    for (p = urls; ; p++) {
        if (*p == ',' || *p == '\0') {
            size_t len = (size_t)(p - start);
            while (len > 0 && start[len-1] == ' ') len--;
            if (len >= sizeof(buf)) len = sizeof(buf)-1;
            memcpy(buf, start, len); buf[len] = '\0';
            int rc = nng_listen(sock, buf, NULL, 0);
            if (rc != 0) {
                fprintf(stderr, "[proxy] listen %s: %s\n", buf, nng_strerror(rc));
                if (*p == '\0') return rc;
            } else fprintf(stderr, "[proxy] listen %s: ok\n", buf);
            if (*p == '\0') return 0;
            start = p + 1; while (*start == ' ') start++; p = start - 1;
        }
    }
}

int main(void) {
    const char* sub_bind = getenv("PROXY_SUB_BIND") ?: "tcp://*:5557";
    const char* pub_bind = getenv("PROXY_PUB_BIND") ?: "tcp://*:5556";
    const char* proxy_id = getenv("PROXY_ID") ?: "proxy";
    int hb_ms = getenv("PROXY_HEARTBEAT_MS") ? atoi(getenv("PROXY_HEARTBEAT_MS")) : 5000;

    signal(SIGTERM, on_signal);
    signal(SIGINT,  on_signal);

    nng_socket sub; int rc;
    if ((rc = nng_sub0_open(&sub)) != 0) {
        fprintf(stderr, "[proxy] sub open: %s\n", nng_strerror(rc)); return 1;
    }
    if ((rc = nng_pub0_open(&g_pub)) != 0) {
        fprintf(stderr, "[proxy] pub open: %s\n", nng_strerror(rc)); nng_close(sub); return 1;
    }
    listen_all(sub, sub_bind);
    nng_socket_set(sub, NNG_OPT_SUB_SUBSCRIBE, "", 0);
    listen_all(g_pub, pub_bind);
    nng_socket_set_ms(sub, NNG_OPT_RECVTIMEO, 100);

    fprintf(stderr, "[proxy] id=%s sub=%s pub=%s hb=%d\n", proxy_id, sub_bind, pub_bind, hb_ms);

    int64_t last_hb = 0;
    while (!g_stop) {
        if (hb_ms > 0) {
            struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
            int64_t now_ms = ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
            if (now_ms - last_hb >= hb_ms) { emit_heartbeat(proxy_id); last_hb = now_ms; }
        }
        nng_msg* msg = NULL;
        rc = nng_recvmsg(sub, &msg, 0);
        if (rc == NNG_ETIMEDOUT) continue;
        if (rc != 0) { if (g_stop) break; continue; }
        rc = nng_sendmsg(g_pub, msg, 0);
        /* On success nng_sendmsg takes ownership and frees msg itself; freeing
           it here too is a double-free (heap corruption -> SIGSEGV under load).
           Only the caller-retains-ownership failure path must free. */
        if (rc != 0) {
            fprintf(stderr, "[proxy] send err: %s\n", nng_strerror(rc));
            nng_msg_free(msg);
        }
    }

    fprintf(stderr, "[proxy] shutting down\n");
    nng_close(g_pub); nng_close(sub);
    return 0;
}
