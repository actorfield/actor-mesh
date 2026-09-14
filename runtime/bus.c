/* bus.c — the mesh's pub/sub forwarder; see bus.h */
#include "bus.h"

#include <stdio.h>
#include <string.h>
#include <nng/protocol/pubsub0/pub.h>
#include <nng/protocol/pubsub0/sub.h>

static int listen_all(nng_socket sock, const char* urls) {
    char buf[256];
    for (const char* p = urls; *p; ) {
        while (*p == ' ') p++;
        if (!*p) break;
        const char* e   = strchr(p, ',');
        size_t      len = e ? (size_t)(e - p) : strlen(p);
        while (len && p[len - 1] == ' ') len--;
        if (len >= sizeof(buf)) len = sizeof(buf) - 1;
        memcpy(buf, p, len);
        buf[len] = '\0';
        int rc = nng_listen(sock, buf, NULL, 0);
        if (rc != 0) {
            fprintf(stderr, "[bus] listen %s: %s\n", buf, nng_strerror(rc));
            return rc;
        }
        fprintf(stderr, "[bus] listen %s: ok\n", buf);
        if (!e) break;
        p = e + 1;
    }
    return 0;
}

int bus_open(bus_t* b, const char* sub_binds, const char* pub_binds) {
    int rc;
    if ((rc = nng_sub0_open(&b->sub)) != 0) return rc;
    if ((rc = nng_pub0_open(&b->pub)) != 0) {
        nng_close(b->sub);
        return rc;
    }
    if ((rc = nng_socket_set(b->sub, NNG_OPT_SUB_SUBSCRIBE, "", 0)) != 0 ||
        (rc = nng_socket_set_ms(b->sub, NNG_OPT_RECVTIMEO, 100)) != 0 ||
        (rc = listen_all(b->sub, sub_binds)) != 0 ||
        (rc = listen_all(b->pub, pub_binds)) != 0) {
        bus_close(b);
        return rc;
    }
    return 0;
}

int bus_forward_once(bus_t* b) {
    nng_msg* msg = NULL;
    int rc = nng_recvmsg(b->sub, &msg, 0);
    if (rc != 0) return rc;
    /* A successful nng_sendmsg takes ownership of msg; only a failed one
       leaves it ours to free. */
    if ((rc = nng_sendmsg(b->pub, msg, 0)) != 0) nng_msg_free(msg);
    return rc;
}

void bus_close(bus_t* b) {
    nng_close(b->pub);
    nng_close(b->sub);
}
