/* bus.h — the mesh's pub/sub forwarder
 *
 * Every frame published to the sub side goes, unchanged, to every subscriber
 * of the pub side. mesh-proxy is this plus a heartbeat; an actor given
 * PROXY_SUB_BIND and PROXY_PUB_BIND runs it on a thread of its own.
 */
#ifndef ACTOR_BUS_H
#define ACTOR_BUS_H

#include <nng/nng.h>

typedef struct {
    nng_socket sub;   /* publishers dial this:  PROXY_SUB_BIND */
    nng_socket pub;   /* subscribers dial this: PROXY_PUB_BIND */
} bus_t;

/* Listen on every address of each comma-separated list. 0, or an nng error. */
int bus_open(bus_t* b, const char* sub_binds, const char* pub_binds);

/* Forward one frame, waiting at most 100ms for it. 0, NNG_ETIMEDOUT, or
   another nng error. */
int bus_forward_once(bus_t* b);

void bus_close(bus_t* b);

#endif /* ACTOR_BUS_H */
