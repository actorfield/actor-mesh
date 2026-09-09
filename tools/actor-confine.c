/* actor-confine — narrow this process's Landlock domain, then exec.
 *
 *   actor-confine <program> [args...]
 *
 * The problem it solves has one shape: a handler needs a capability that what
 * it spawns must not have. Landlock is inherited and can only narrow, so a
 * ruleset applied by the runtime at actor startup cannot express "the handler
 * may reach the bus, the program it runs may not" -- both are the same domain.
 *
 * The moment matters more than the rules, which is why this is a separate
 * program rather than another ACTOR_* variable the runtime reads before exec.
 * The handler opens what it needs FIRST, then execs this, then the confined
 * program runs. Landlock governs opening and connecting, not descriptors
 * already held, so the ordering is the mechanism:
 *
 *   handler: connect to the bus              <- allowed, domain is still wide
 *   handler: spawn a broker holding that fd
 *   handler: exec actor-confine -- ./program <- domain narrows here
 *   program: connect to the bus              <- denied
 *   program: talk to the broker instead      <- the only route left
 *
 * Configured by ACTOR_RESTRICT_RO / ACTOR_RESTRICT_RW /
 * ACTOR_RESTRICT_NET_CONNECT, in the same formats as the ACTOR_LANDLOCK_*
 * variables they narrow. Setting none of them is not an error: it execs
 * unchanged, so a deployment can add the wrapper and the rules separately.
 *
 * Fails closed. If a restriction was asked for and could not be applied, this
 * exits without exec'ing rather than running the program unconfined -- the
 * whole point is that the program never runs with more than it should.
 */
#include <stdio.h>
#include <unistd.h>

#include "../runtime/actor_isolation.h"

int main(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: actor-confine <program> [args...]\n");
        return 2;
    }
    if (actor_isolation_restrict() != 0) {
        fprintf(stderr, "[actor-confine] refusing to exec %s unconfined\n", argv[1]);
        return 1;
    }
    execvp(argv[1], &argv[1]);
    /* Only reachable if exec failed; the domain is already narrowed, so
       there is nothing to undo. */
    perror("[actor-confine] execvp");
    return 127;
}
