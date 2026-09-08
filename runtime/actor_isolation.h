/* actor_isolation.h — optional process confinement, applied at startup
 *
 * Phase 1: privilege drop and resource limits.
 *
 * Every knob is opt-in through the environment. An actor with none of these
 * set behaves exactly as it did before this file existed -- that is the
 * property the tests in tests/test-isolation.c defend, and it is what lets an
 * existing deployment upgrade without noticing.
 *
 * The rule for everything here is fail closed: a variable that is set and
 * cannot be honoured aborts startup. An actor must never run less confined
 * than it believes it is, because a deployment that configures confinement,
 * sees no error, and gets none is worse off than one that never tried.
 *
 *   ACTOR_UID / ACTOR_GID      drop to this uid/gid, irreversibly
 *   ACTOR_RLIMIT_AS            address space, bytes
 *   ACTOR_RLIMIT_CPU           cpu time, seconds
 *   ACTOR_RLIMIT_NOFILE        open file descriptors
 *   ACTOR_RLIMIT_NPROC         processes/threads for the real uid
 *
 * Returns 0 if everything requested was applied (including the case where
 * nothing was requested), -1 if anything requested could not be.
 */
#ifndef ACTOR_ISOLATION_H
#define ACTOR_ISOLATION_H

/* Apply the actor-lifetime isolation named in the environment.
 *
 * MUST be called before any pthread_create. Two reasons, and the function
 * enforces the first by refusing to run if it can tell threads already exist:
 *
 *   - setresuid/setresgid are per-thread at the syscall level; glibc's
 *     wrappers broadcast to the other threads by signal. Running before any
 *     thread exists avoids depending on that machinery at all.
 *   - the namespace work in later phases has a hard kernel restriction of the
 *     same shape (unshare(CLONE_NEWUSER) fails EINVAL in a multithreaded
 *     process), so the constraint is worth holding from the start rather than
 *     discovering later.
 */
int actor_isolation_apply(void);

/* Stack a narrower Landlock domain on the calling process, from
 * ACTOR_RESTRICT_RO / ACTOR_RESTRICT_RW / ACTOR_RESTRICT_NET_CONNECT.
 *
 * For a handler that needs to keep a capability while denying it to whatever
 * it spawns: open the connection, call this, then spawn. Landlock governs
 * opening and connecting, not descriptors already held, so the parent keeps
 * what it has and the child cannot obtain it. Domains nest, so this can only
 * narrow -- never widen, whatever the environment says.
 *
 * Returns 0 if everything requested was applied (including nothing requested),
 * -1 if anything requested could not be. */
int actor_isolation_restrict(void);

/* Apply the tuple-lifetime isolation named in ACTOR_TUPLE_UNSHARE.
 *
 * Call in the forked child, before exec. The namespaces are created per tuple
 * and destroyed when the handler exits -- see the comment in the .c for why
 * the PID namespace in particular cannot be created at startup.
 *
 * Returns 0 if everything requested was applied (including nothing requested),
 * -1 otherwise. The caller must _exit non-zero on -1: a tuple must never run
 * less isolated than it was configured to be.
 */
int actor_isolation_tuple(void);

#endif /* ACTOR_ISOLATION_H */
