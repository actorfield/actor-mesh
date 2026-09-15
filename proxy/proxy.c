// proxy.c — NNG mesh bus
//
// Actors publish to PROXY_SUB_BIND and subscribe to PROXY_PUB_BIND. The
// forwarder is runtime/bus.c; this adds a heartbeat.

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <nng/nng.h>
#include "actor_tuple.h"
#include "actor_uuid.h"
#include "bus.h"

static volatile sig_atomic_t g_stop = 0;
static void on_signal(int s) { (void)s; g_stop = 1; }

static void emit_heartbeat(nng_socket pub, const char* id) {
    char payload[128];
    size_t plen = snprintf(payload, sizeof(payload), "{\"id\":\"%s\"}", id);
    if (plen >= sizeof(payload)) plen = sizeof(payload) - 1;
    actor_header_t hdr;
    actor_tuple_init(&hdr, "heartbeat", id, NULL, NULL, (uint32_t)plen);
    actor_uuid_gen(hdr.id);
    uint8_t frame[sizeof(actor_header_t) + sizeof(payload)];
    memcpy(frame, &hdr, sizeof(actor_header_t));
    memcpy(frame + sizeof(actor_header_t), payload, plen);
    nng_send(pub, frame, sizeof(actor_header_t) + plen, 0);
}

int main(void) {
    const char* sub_bind = getenv("PROXY_SUB_BIND") ?: "tcp://*:5557";
    const char* pub_bind = getenv("PROXY_PUB_BIND") ?: "tcp://*:5556";
    const char* proxy_id = getenv("PROXY_ID") ?: "proxy";
    int hb_ms = getenv("PROXY_HEARTBEAT_MS") ? atoi(getenv("PROXY_HEARTBEAT_MS")) : 5000;

    signal(SIGTERM, on_signal);
    signal(SIGINT,  on_signal);

    bus_t bus;
    int rc = bus_open(&bus, sub_bind, pub_bind);
    if (rc != 0) {
        fprintf(stderr, "[proxy] bus: %s\n", nng_strerror(rc));
        return 1;
    }
    fprintf(stderr, "[proxy] id=%s sub=%s pub=%s hb=%d\n", proxy_id, sub_bind, pub_bind, hb_ms);

    int64_t last_hb = 0;
    while (!g_stop) {
        if (hb_ms > 0) {
            struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
            int64_t now_ms = ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
            if (now_ms - last_hb >= hb_ms) { emit_heartbeat(bus.pub, proxy_id); last_hb = now_ms; }
        }
        rc = bus_forward_once(&bus);
        if (rc != 0 && rc != NNG_ETIMEDOUT && !g_stop)
            fprintf(stderr, "[proxy] forward: %s\n", nng_strerror(rc));
    }

    fprintf(stderr, "[proxy] shutting down\n");
    bus_close(&bus);
    return 0;
}
