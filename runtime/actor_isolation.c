#define _GNU_SOURCE
/* actor_isolation.c — Phase 1: privilege drop and resource limits.
 *
 * See actor_isolation.h for the contract. The short version: every knob is
 * opt-in, an unset variable is never an error, and a set variable that cannot
 * be applied always is.
 */

#include "actor_isolation.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <grp.h>
#include <fcntl.h>
/* linux/landlock.h comes from linux-headers, which is not installed in every
   base image the runtime is built in -- notably alpine, which Dockerfile.actor
   uses, where its absence is a build failure rather than a missing feature.
   The Landlock UAPI is a stable kernel ABI, so declare what is needed when the
   header is not present. Availability is decided at RUNTIME by asking the
   kernel for its ABI version, never at compile time. */
#if defined(__has_include)
#  if __has_include(<linux/landlock.h>)
#    include <linux/landlock.h>
#    define ACTOR_HAVE_LANDLOCK_H 1
#  endif
#endif

#ifndef ACTOR_HAVE_LANDLOCK_H
struct landlock_ruleset_attr {
    uint64_t handled_access_fs;
    uint64_t handled_access_net;
};
struct landlock_path_beneath_attr {
    uint64_t allowed_access;
    int32_t  parent_fd;
} __attribute__((packed));
struct landlock_net_port_attr {
    uint64_t allowed_access;
    uint64_t port;
};
enum landlock_rule_type {
    LANDLOCK_RULE_PATH_BENEATH = 1,
    LANDLOCK_RULE_NET_PORT     = 2,
};
#define LANDLOCK_CREATE_RULESET_VERSION   (1U << 0)
#define LANDLOCK_ACCESS_FS_EXECUTE        (1ULL << 0)
#define LANDLOCK_ACCESS_FS_WRITE_FILE     (1ULL << 1)
#define LANDLOCK_ACCESS_FS_READ_FILE      (1ULL << 2)
#define LANDLOCK_ACCESS_FS_READ_DIR       (1ULL << 3)
#define LANDLOCK_ACCESS_FS_REMOVE_DIR     (1ULL << 4)
#define LANDLOCK_ACCESS_FS_REMOVE_FILE    (1ULL << 5)
#define LANDLOCK_ACCESS_FS_MAKE_CHAR      (1ULL << 6)
#define LANDLOCK_ACCESS_FS_MAKE_DIR       (1ULL << 7)
#define LANDLOCK_ACCESS_FS_MAKE_REG       (1ULL << 8)
#define LANDLOCK_ACCESS_FS_MAKE_SOCK      (1ULL << 9)
#define LANDLOCK_ACCESS_FS_MAKE_FIFO      (1ULL << 10)
#define LANDLOCK_ACCESS_FS_MAKE_BLOCK     (1ULL << 11)
#define LANDLOCK_ACCESS_FS_MAKE_SYM       (1ULL << 12)
#define LANDLOCK_ACCESS_NET_BIND_TCP      (1ULL << 0)
#define LANDLOCK_ACCESS_NET_CONNECT_TCP   (1ULL << 1)
#endif /* !ACTOR_HAVE_LANDLOCK_H */
#include <sys/syscall.h>
#include <sys/prctl.h>
#include <sched.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/wait.h>

/* ── Environment parsing ─────────────────────────────────────────────────── */

/* Parse a non-negative integer. Returns 0 on success, -1 if the value is not a
 * clean number -- a typo in a security-relevant variable must not silently
 * become 0, which is uid 0. */
static int parse_ull(const char* s, unsigned long long* out) {
    if (!s || !*s) return -1;
    errno = 0;
    char* end = NULL;
    unsigned long long v = strtoull(s, &end, 10);
    if (errno || !end || *end != '\0') return -1;
    *out = v;
    return 0;
}

/* ── Resource limits ─────────────────────────────────────────────────────── */

static int apply_rlimit(const char* env_name, int resource, const char* label) {
    const char* v = getenv(env_name);
    if (!v) return 0;                      /* unset is not a failure */

    unsigned long long n;
    if (parse_ull(v, &n)) {
        fprintf(stderr, "[actor] isolation: %s=\"%s\" is not a number\n", env_name, v);
        return -1;
    }

    struct rlimit rl = { (rlim_t)n, (rlim_t)n };
    if (setrlimit(resource, &rl) != 0) {
        fprintf(stderr, "[actor] isolation: setrlimit(%s, %llu): %s\n",
                label, n, strerror(errno));
        return -1;
    }

    /* Read back. setrlimit can succeed while clamping to a lower hard limit
       the process cannot raise, and a limit quietly weaker than the one asked
       for is exactly the silent degradation this module exists to prevent. */
    struct rlimit got;
    if (getrlimit(resource, &got) != 0) {
        fprintf(stderr, "[actor] isolation: getrlimit(%s): %s\n", label, strerror(errno));
        return -1;
    }
    if (got.rlim_cur != (rlim_t)n) {
        fprintf(stderr, "[actor] isolation: %s applied as %llu, not the requested %llu\n",
                label, (unsigned long long)got.rlim_cur, n);
        return -1;
    }
    return 0;
}

/* ── Privilege drop ──────────────────────────────────────────────────────── */

/* Drop to ACTOR_GID then ACTOR_UID.
 *
 * Order matters: after the uid drop the process is generally no longer
 * privileged enough to change its gid, so the gid must go first.
 *
 * setresgid/setresuid rather than POSIX setgid/setuid deliberately. They set
 * real, effective and saved-set ids explicitly, so an incomplete drop is an
 * error. POSIX setuid drops all three only when the caller is privileged --
 * called without privilege it moves the effective uid alone, leaves the
 * saved-set id intact so privilege remains regainable, and still reports
 * success. That silent partial drop is the failure this module exists to
 * prevent. Both calls are outside POSIX but present on Linux, which is the
 * platform this file is compiled for.
 */
static int drop_privilege(void) {
    const char* uid_s = getenv("ACTOR_UID");
    const char* gid_s = getenv("ACTOR_GID");
    if (!uid_s && !gid_s) return 0;

    unsigned long long uid_v = 0, gid_v = 0;
    if (uid_s && parse_ull(uid_s, &uid_v)) {
        fprintf(stderr, "[actor] isolation: ACTOR_UID=\"%s\" is not a number\n", uid_s);
        return -1;
    }
    if (gid_s && parse_ull(gid_s, &gid_v)) {
        fprintf(stderr, "[actor] isolation: ACTOR_GID=\"%s\" is not a number\n", gid_s);
        return -1;
    }

    if (gid_s) {
        /* Drop supplementary groups first. They survive setresgid, and a
           process still carrying the launching user's groups has not really
           dropped its group privilege. Failure here is only tolerated when
           there is nothing to drop -- an unprivileged actor with no
           supplementary groups beyond its own is already where it needs to
           be. */
        if (setgroups(0, NULL) != 0 && errno != EPERM) {
            fprintf(stderr, "[actor] isolation: setgroups(0): %s\n", strerror(errno));
            return -1;
        }
        if (setresgid((gid_t)gid_v, (gid_t)gid_v, (gid_t)gid_v) != 0) {
            fprintf(stderr, "[actor] isolation: setresgid(%llu): %s\n",
                    gid_v, strerror(errno));
            return -1;
        }
    }

    if (uid_s) {
        if (setresuid((uid_t)uid_v, (uid_t)uid_v, (uid_t)uid_v) != 0) {
            fprintf(stderr, "[actor] isolation: setresuid(%llu): %s\n",
                    uid_v, strerror(errno));
            return -1;
        }
    }

    /* Verify the postcondition rather than trust the return. What matters is
       the state the process is actually in, and a drop that half-happened must
       not reach the poll loop. getresuid/getresgid would read the saved-set id
       directly but are themselves non-standard; comparing real and effective
       ids catches every partial drop these two calls can produce. */
    if (gid_s) {
        gid_t rg = getgid(), eg = getegid();
        if (rg != (gid_t)gid_v || eg != (gid_t)gid_v) {
            fprintf(stderr, "[actor] isolation: gid drop incomplete (real=%u effective=%u "
                            "wanted=%llu)\n", (unsigned)rg, (unsigned)eg, gid_v);
            return -1;
        }
    }
    if (uid_s) {
        uid_t ru = getuid(), eu = geteuid();
        if (ru != (uid_t)uid_v || eu != (uid_t)uid_v) {
            fprintf(stderr, "[actor] isolation: uid drop incomplete (real=%u effective=%u "
                            "wanted=%llu)\n", (unsigned)ru, (unsigned)eu, uid_v);
            return -1;
        }
    }
    return 0;
}





/* pivot_root into ACTOR_ROOTFS.
 *
 * Only meaningful with a mount namespace, and only reached from inside one.
 * After this the handler's filesystem is the rootfs and nothing above it is
 * reachable -- the old root is unmounted rather than merely moved aside, so
 * there is no directory left to walk back out through.
 *
 * pivot_root requires the new root to be a mount point, which an ordinary
 * directory is not; the bind of the directory onto itself is what makes it
 * one. That is the standard sequence and the reason for the apparently
 * redundant first mount.
 *
 * Called before /proc is remounted, so the fresh /proc lands inside the new
 * root rather than in a filesystem about to disappear.
 */
static int do_pivot_root(const char* rootfs) {
    /* The new root must be a mount point. */
    if (mount(rootfs, rootfs, NULL, MS_BIND | MS_REC, NULL) != 0) {
        fprintf(stderr, "[actor] isolation: bind ACTOR_ROOTFS=\"%s\": %s\n",
                rootfs, strerror(errno));
        return -1;
    }

    /* A place to park the old root while pivoting. Inside the new root, so it
       exists after the pivot and can be unmounted from there. */
    char oldroot[PATH_MAX];
    int n = snprintf(oldroot, sizeof(oldroot), "%s/.actor_oldroot", rootfs);
    if (n < 0 || (size_t)n >= sizeof(oldroot)) {
        fprintf(stderr, "[actor] isolation: ACTOR_ROOTFS path too long\n");
        return -1;
    }
    if (mkdir(oldroot, 0700) != 0 && errno != EEXIST) {
        fprintf(stderr, "[actor] isolation: mkdir \"%s\": %s "
                        "(ACTOR_ROOTFS must be writable to stage the pivot)\n",
                oldroot, strerror(errno));
        return -1;
    }

    if (syscall(SYS_pivot_root, rootfs, oldroot) != 0) {
        fprintf(stderr, "[actor] isolation: pivot_root(\"%s\"): %s\n",
                rootfs, strerror(errno));
        return -1;
    }
    if (chdir("/") != 0) {
        fprintf(stderr, "[actor] isolation: chdir(/) after pivot_root: %s\n", strerror(errno));
        return -1;
    }

    /* Detach the old root entirely. Until this succeeds the handler can still
       reach the host filesystem through /.actor_oldroot, so a failure here is
       a failure of the whole phase, not a cosmetic leftover. */
    if (umount2("/.actor_oldroot", MNT_DETACH) != 0) {
        fprintf(stderr, "[actor] isolation: umount2(old root): %s\n", strerror(errno));
        return -1;
    }
    if (rmdir("/.actor_oldroot") != 0 && errno != ENOENT && errno != EBUSY) {
        /* Cosmetic only: the mount is already detached, so this directory is
           empty and leads nowhere. Not worth failing the tuple over. */
    }
    return 0;
}

/* ── Per-tuple namespaces ────────────────────────────────────────────────────
 *
 * Applied in the forked child, before exec, so the namespaces live and die
 * with one tuple.
 *
 * Why per tuple rather than once at startup. A tuple is a self-contained unit
 * of work -- that is the tuple space's premise, and the runtime already holds
 * it for memory (fixed buffers, no heap per tuple) and for process identity (a
 * fresh fork per handler). The process TREE was the place it leaked: a
 * handler's own children could outlive it, get reparented to the actor, and
 * accumulate across tuples that were supposed to be independent. A namespace
 * created here closes that: when the handler exits, the kernel kills whatever
 * remains inside it. The tuple leaves no trace, which is what the model
 * claimed in the first place.
 *
 * The PID namespace needs care, and is the reason this function forks.
 * unshare(CLONE_NEWPID) does NOT move the caller into the new namespace: it
 * arranges for the caller's NEXT fork to produce a child that is PID 1 of it,
 * and the namespace is destroyed when that PID 1 exits. So unsharing and then
 * exec'ing in the same process leaves the handler outside the namespace it
 * just made -- the namespace exists, is empty, and confines nothing.
 *
 * Hence the second fork below. The caller unshares, forks, and the grandchild
 * -- which IS pid 1 of the new namespace -- becomes the handler. The
 * intermediate process waits and relays the exit status, so the runtime's
 * existing view of "the child I forked exited with status N" is unchanged and
 * the reaper, retry and rejection paths need no adjustment.
 *
 * Placing this per tuple rather than at startup matters for the same reason:
 * called once at startup the first handler would be pid 1, the namespace would
 * die with it, and every later tuple would fork into a dead namespace.
 *
 * Cost is tens of microseconds against a fork/exec measured in the hundreds of
 * milliseconds for a statically linked handler. Not measurable. The mount
 * namespace with pivot_root is the exception and is not part of this phase.
 *
 * Returns 0 on success. On failure the caller _exits non-zero, which the
 * existing retry path already treats as a failed handler run -- no new error
 * channel, and a tuple never runs less isolated than configured.
 */
int actor_isolation_tuple(void) {
    const char* spec = getenv("ACTOR_TUPLE_UNSHARE");
    if (!spec || !*spec) return 0;

    char buf[256];
    if (strlen(spec) >= sizeof(buf)) return -1;
    strcpy(buf, spec);

    int flags = 0;
    char* save = NULL;
    for (char* t = strtok_r(buf, ",", &save); t; t = strtok_r(NULL, ",", &save)) {
        while (*t == ' ') t++;
        if      (!strcmp(t, "pid"))   flags |= CLONE_NEWPID;
        else if (!strcmp(t, "ipc"))   flags |= CLONE_NEWIPC;
        else if (!strcmp(t, "uts"))   flags |= CLONE_NEWUTS;
        else if (!strcmp(t, "net"))   flags |= CLONE_NEWNET;
        else if (!strcmp(t, "mount")) flags |= CLONE_NEWNS;
        else if (!*t) continue;
        else {
            fprintf(stderr, "[actor] isolation: unknown namespace \"%s\" in "
                            "ACTOR_TUPLE_UNSHARE\n", t);
            return -1;
        }
    }
    if (!flags) return 0;

    /* ACTOR_ROOTFS only means anything with a mount namespace to apply it in.
       Set without one it would be silently ignored, leaving the handler on the
       host root while the operator believes otherwise. */
    const char* rootfs_early = getenv("ACTOR_ROOTFS");
    if (rootfs_early && *rootfs_early && !(flags & CLONE_NEWNS)) {
        fprintf(stderr, "[actor] isolation: ACTOR_ROOTFS is set but \"mount\" is not in "
                        "ACTOR_TUPLE_UNSHARE -- it would have no effect\n");
        return -1;
    }
    /* pivot_root also needs a pid namespace here: the /proc remount that makes
       the new root usable belongs to the pid-1 child, and without CLONE_NEWPID
       that child does not exist. */
    if (rootfs_early && *rootfs_early && !(flags & CLONE_NEWPID)) {
        fprintf(stderr, "[actor] isolation: ACTOR_ROOTFS needs \"pid\" in "
                        "ACTOR_TUPLE_UNSHARE alongside \"mount\"\n");
        return -1;
    }

    /* Every namespace here except user needs CAP_SYS_ADMIN. An unprivileged
       actor gets that capability by first entering a user namespace, where it
       is root -- so try the direct unshare, and on EPERM retry behind a user
       namespace rather than failing an actor that could have been isolated.
       Doing it in this order means a privileged actor never creates a user
       namespace it does not need.
       ACTOR_TUPLE_USERNS=0 opts out, for hosts where unprivileged user
       namespaces are disabled and the operator would rather see the EPERM. */
    if (unshare(flags) != 0) {
        int first_errno = errno;
        const char* allow_userns = getenv("ACTOR_TUPLE_USERNS");
        int may_userns = !(allow_userns && !strcmp(allow_userns, "0"));

        if (first_errno == EPERM && may_userns && !(flags & CLONE_NEWUSER)) {
            /* Capture the ids BEFORE entering the user namespace. Inside it,
               and before any mapping exists, getuid() reports the overflow uid
               (65534) -- writing that into uid_map asks to map a uid this
               process does not own in the parent namespace, which is EPERM.
               The mapping has to name the uid we had on the way in. */
            uid_t outer_uid = getuid();
            gid_t outer_gid = getgid();
            if (unshare(CLONE_NEWUSER) == 0 && unshare(flags) == 0) {
                /* Map this uid/gid into the new namespace so the handler runs
                   as itself rather than as nobody.
                   The mapping must be the IDENTITY one. A process may only map
                   uids it already owns in the parent namespace, so the
                   tempting "0 <uid> 1" -- map me to root inside -- is EPERM
                   whenever the actor is not privileged, including inside a
                   rootless container where it is already in a user namespace.
                   setgroups must be denied before gid_map may be written.
                   Failure is not fatal: the namespaces are already in place and
                   confining, and a handler that does not care about its own uid
                   runs correctly as nobody. It IS reported, because an
                   unmapped handler cannot mount /proc and that is otherwise
                   diagnosed far from here. */
                uid_t u = outer_uid; gid_t g = outer_gid;
                char m[64]; int fd; int mapped = 1;
                if ((fd = open("/proc/self/setgroups", O_WRONLY | O_CLOEXEC)) >= 0) {
                    if (write(fd, "deny", 4) < 0) mapped = 0;
                    close(fd);
                }
                int n = snprintf(m, sizeof(m), "%u %u 1", (unsigned)u, (unsigned)u);
                if ((fd = open("/proc/self/uid_map", O_WRONLY | O_CLOEXEC)) >= 0) {
                    if (write(fd, m, (size_t)n) < 0) mapped = 0;
                    close(fd);
                } else mapped = 0;
                n = snprintf(m, sizeof(m), "%u %u 1", (unsigned)g, (unsigned)g);
                if ((fd = open("/proc/self/gid_map", O_WRONLY | O_CLOEXEC)) >= 0) {
                    if (write(fd, m, (size_t)n) < 0) mapped = 0;
                    close(fd);
                } else mapped = 0;
                if (!mapped || geteuid() != u) {
                    fprintf(stderr, "[actor] isolation: handler runs as uid %u in its user "
                                    "namespace (identity map unavailable); namespaces are "
                                    "active but privileged operations inside them, such as "
                                    "mounting /proc, will fail\n", (unsigned)geteuid());
                }
                goto unshared;
            }
        }
        fprintf(stderr, "[actor] isolation: unshare(ACTOR_TUPLE_UNSHARE=%s): %s%s\n",
                spec, strerror(first_errno),
                first_errno == EPERM
                    ? " (needs CAP_SYS_ADMIN, or unprivileged user namespaces enabled)"
                    : "");
        return -1;
    }
unshared:;

    /* Without CLONE_NEWPID nothing more is needed: the remaining namespaces
       take effect on the calling process immediately, and the caller execs. */
    if (!(flags & CLONE_NEWPID)) return 0;

    /* CLONE_NEWPID needs the extra fork described above: this process is not
       in the namespace it just created, but its next child is pid 1 of it. */
    pid_t inner = fork();
    if (inner < 0) {
        fprintf(stderr, "[actor] isolation: fork into pid namespace: %s\n", strerror(errno));
        return -1;
    }

    if (inner == 0) {
        /* pid 1 of the new namespace. /proc still shows the host's process
           list until it is remounted, which needs a mount namespace -- so only
           when one was also asked for. Without it the process tree is still
           confined; only the /proc view is stale. */
        if (flags & CLONE_NEWNS) {
            /* Detach this namespace's mounts from the host's before touching
               anything, or the changes below propagate back out. */
            if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0) {
                fprintf(stderr, "[actor] isolation: mount(/, MS_PRIVATE): %s\n",
                        strerror(errno));
                _exit(1);
            }
            /* pivot_root first, so the /proc mounted below lands inside the
               new root rather than in a filesystem about to be detached. */
            const char* rootfs = getenv("ACTOR_ROOTFS");
            if (rootfs && *rootfs && do_pivot_root(rootfs) != 0) _exit(1);

            /* A fresh /proc so it reflects the new pid namespace. This is a
               convenience, not the boundary: pivot_root above is what confines
               the handler, and the pid namespace confines the process tree
               whether or not /proc shows it. Mounting proc needs privilege
               that many environments withhold even from root -- container
               runtimes commonly block mount(2) outright -- so a failure here
               leaves a stale /proc view and is reported, not fatal.
               Failing the tuple instead would make phase 6 unusable anywhere
               mount(2) is filtered, for no gain in confinement. */
            if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
                fprintf(stderr, "[actor] isolation: /proc not remounted (%s); the pid "
                                "namespace still confines the handler, but /proc shows "
                                "the outer namespace's processes\n", strerror(errno));
            }
        }
        return 0;   /* caller execs the handler here, as pid 1 */
    }

    /* Intermediate process: wait for pid 1 and exit with the same status, so
       the runtime sees exactly the exit it would have seen without any of
       this. When pid 1 exits the kernel kills everything else in the
       namespace -- which is the whole point: the tuple leaves no trace. */
    int st = 0;
    while (waitpid(inner, &st, 0) < 0 && errno == EINTR) { }
    if (WIFEXITED(st))   _exit(WEXITSTATUS(st));
    if (WIFSIGNALED(st)) _exit(128 + WTERMSIG(st));
    _exit(1);
}

/* ── cgroup v2 ───────────────────────────────────────────────────────────────
 *
 * Join an existing cgroup by writing this pid into its cgroup.procs.
 *
 * The runtime joins and never creates. Creating a hierarchy means deciding
 * where it lives, who owns it, what controllers it enables and who cleans it
 * up -- all of which belong to whatever launches actors, which already knows
 * those answers. An actor that had to create its own would also need write
 * access to the parent cgroup directory, which is a larger privilege than
 * anything else this module asks for.
 *
 * Note this joins the ACTOR, and every handler it forks inherits the
 * membership. Per-tuple accounting is not possible this way and is not
 * attempted.
 */
static int join_cgroup(void) {
    const char* path = getenv("ACTOR_CGROUP_PATH");
    if (!path) return 0;

    char procs[PATH_MAX];
    int n = snprintf(procs, sizeof(procs), "%s/cgroup.procs", path);
    if (n < 0 || (size_t)n >= sizeof(procs)) {
        fprintf(stderr, "[actor] isolation: ACTOR_CGROUP_PATH too long\n");
        return -1;
    }

    FILE* f = fopen(procs, "w");
    if (!f) {
        fprintf(stderr, "[actor] isolation: cgroup \"%s\": %s "
                        "(the cgroup must already exist and be writable; "
                        "the runtime never creates one)\n", procs, strerror(errno));
        return -1;
    }
    if (fprintf(f, "%d\n", (int)getpid()) < 0 || fclose(f) != 0) {
        fprintf(stderr, "[actor] isolation: writing pid to \"%s\": %s\n",
                procs, strerror(errno));
        return -1;
    }

    /* Verify membership rather than trust the write. A cgroup.procs write can
       be accepted and still not place the process where it was asked -- for
       instance when the target has domain-controller constraints -- and an
       actor that believes it is accounted for when it is not is exactly the
       silent degradation this module refuses. */
    FILE* sf = fopen("/proc/self/cgroup", "r");
    if (!sf) {
        fprintf(stderr, "[actor] isolation: cannot read /proc/self/cgroup to verify\n");
        return -1;
    }
    char line[PATH_MAX + 64];
    int joined = 0;
    /* Unified hierarchy lines look like "0::/path/relative/to/mount". */
    while (fgets(line, sizeof(line), sf)) {
        char* rel = strstr(line, "0::");
        if (!rel) continue;
        rel += 3;
        line[strcspn(line, "\n")] = '\0';
        size_t rl = strlen(rel);
        if (rl && rl <= strlen(path) && strcmp(path + strlen(path) - rl, rel) == 0) joined = 1;
        break;
    }
    fclose(sf);
    if (!joined) {
        fprintf(stderr, "[actor] isolation: cgroup write to \"%s\" was accepted but "
                        "the process is not a member\n", path);
        return -1;
    }

    fprintf(stderr, "[actor] isolation: cgroup=%s\n", path);
    return 0;
}

/* ── Landlock ────────────────────────────────────────────────────────────────
 *
 * Path-based confinement: after landlock_restrict_self the process may only
 * touch what was explicitly granted. Everything else fails as though it were
 * not there.
 *
 * Two things about the read set are easy to get wrong and expensive to debug,
 * so they are stated here rather than left to the operator:
 *
 *   - ACTOR_LMDB_PATH must be inside ACTOR_LANDLOCK_RW. The actor opens its
 *     LMDB after this runs, so a path outside the ruleset turns into an
 *     LMDB error at startup rather than an obvious configuration one. This
 *     module checks for it explicitly and says so.
 *
 *   - The exec path needs more than the handler binary. The runtime runs
 *     handlers via execl("/bin/sh", "sh", "-c", ...), so /bin/sh, the handler
 *     itself, the dynamic loader and every shared library they pull in must be
 *     readable and executable. On a distro build of this runtime that is
 *     /lib64 and /usr/lib64 (libnng, liblmdb, libc, libtinfo, the mbedtls
 *     trio, ld-linux). A statically linked handler needs far less.
 *
 * There is no ambient "allow everything not mentioned": a ruleset with no
 * rules denies everything. So an ACTOR_LANDLOCK_RO that forgets the loader
 * produces a handler that cannot start, which is why phase 2 does not ship
 * without the enumeration above having been done for a real handler.
 */

#ifndef LANDLOCK_ACCESS_FS_REFER
#  define LANDLOCK_ACCESS_FS_REFER (1ULL << 13)
#endif
#ifndef LANDLOCK_ACCESS_FS_TRUNCATE
#  define LANDLOCK_ACCESS_FS_TRUNCATE (1ULL << 14)
#endif
#ifndef LANDLOCK_ACCESS_FS_IOCTL_DEV
#  define LANDLOCK_ACCESS_FS_IOCTL_DEV (1ULL << 15)
#endif

/* Syscall numbers, in case the libc headers predate them. These are fixed
   per architecture and identical across every arch this runtime targets. */
#ifndef __NR_landlock_create_ruleset
#  define __NR_landlock_create_ruleset 444
#endif
#ifndef __NR_landlock_add_rule
#  define __NR_landlock_add_rule 445
#endif
#ifndef __NR_landlock_restrict_self
#  define __NR_landlock_restrict_self 446
#endif

static long ll_create_ruleset(const struct landlock_ruleset_attr* attr,
                              size_t size, uint32_t flags) {
    return syscall(__NR_landlock_create_ruleset, attr, size, flags);
}
static long ll_add_rule(int fd, enum landlock_rule_type t,
                        const void* attr, uint32_t flags) {
    return syscall(__NR_landlock_add_rule, fd, t, attr, flags);
}
static long ll_restrict_self(int fd, uint32_t flags) {
    return syscall(__NR_landlock_restrict_self, fd, flags);
}

/* Read-only and read-write access sets. Both include EXECUTE: the handler and
   its libraries are read through the RO set, and a handler staged into a
   writable working directory still has to be runnable. */
#define LL_RO (LANDLOCK_ACCESS_FS_READ_FILE | \
               LANDLOCK_ACCESS_FS_READ_DIR  | \
               LANDLOCK_ACCESS_FS_EXECUTE)

#define LL_RW (LL_RO                             | \
               LANDLOCK_ACCESS_FS_WRITE_FILE     | \
               LANDLOCK_ACCESS_FS_REMOVE_FILE    | \
               LANDLOCK_ACCESS_FS_REMOVE_DIR     | \
               LANDLOCK_ACCESS_FS_MAKE_REG       | \
               LANDLOCK_ACCESS_FS_MAKE_DIR       | \
               LANDLOCK_ACCESS_FS_MAKE_SOCK      | \
               LANDLOCK_ACCESS_FS_MAKE_FIFO      | \
               LANDLOCK_ACCESS_FS_MAKE_SYM       | \
               LANDLOCK_ACCESS_FS_TRUNCATE)

/* Add one path to the ruleset. A path that does not exist is a configuration
   error: silently skipping it would grant less than asked for, which is the
   silent-degradation failure this module exists to prevent. */
static int ll_add_path(int rs, const char* path, uint64_t allowed) {
    int fd = open(path, O_PATH | O_CLOEXEC);
    if (fd < 0) {
        fprintf(stderr, "[actor] isolation: landlock path \"%s\": %s\n",
                path, strerror(errno));
        return -1;
    }
    struct landlock_path_beneath_attr pb = { .allowed_access = allowed,
                                             .parent_fd = fd };
    int rc = (int)ll_add_rule(rs, LANDLOCK_RULE_PATH_BENEATH, &pb, 0);
    if (rc) fprintf(stderr, "[actor] isolation: landlock_add_rule(\"%s\"): %s\n",
                    path, strerror(errno));
    close(fd);
    return rc;
}

/* Walk a colon-separated list, applying fn to each non-empty element. */
static int for_each_path(const char* list, int rs, uint64_t allowed,
                         int (*fn)(int, const char*, uint64_t)) {
    if (!list || !*list) return 0;
    char buf[4096];
    if (strlen(list) >= sizeof(buf)) {
        fprintf(stderr, "[actor] isolation: landlock path list too long\n");
        return -1;
    }
    strcpy(buf, list);
    char* save = NULL;
    for (char* t = strtok_r(buf, ":", &save); t; t = strtok_r(NULL, ":", &save)) {
        while (*t == ' ') t++;
        if (!*t) continue;
        if (fn(rs, t, allowed) != 0) return -1;
    }
    return 0;
}

/* Is `path` at or beneath any element of the colon-separated `list`? Used to
   check ACTOR_LMDB_PATH against the rw set before the ruleset is enforced, so
   the operator gets a clear message instead of an LMDB failure later. */
static int path_covered(const char* path, const char* list) {
    if (!list || !*list || !path) return 0;
    char rp[PATH_MAX];
    /* The LMDB directory may not exist yet; compare textually against the
       configured value in that case rather than failing the check. */
    const char* target = realpath(path, rp) ? rp : path;
    size_t tlen = strlen(target);

    char buf[4096];
    if (strlen(list) >= sizeof(buf)) return 0;
    strcpy(buf, list);
    char* save = NULL;
    for (char* t = strtok_r(buf, ":", &save); t; t = strtok_r(NULL, ":", &save)) {
        while (*t == ' ') t++;
        if (!*t) continue;
        char er[PATH_MAX];
        const char* entry = realpath(t, er) ? er : t;
        size_t elen = strlen(entry);
        if (elen == 1 && entry[0] == '/') return 1;              /* "/" covers all */
        if (tlen >= elen && strncmp(target, entry, elen) == 0 &&
            (target[elen] == '\0' || target[elen] == '/')) return 1;
    }
    return 0;
}

/* Build one Landlock ruleset from three lists and enforce it on this process.
 *
 * Split out of apply_landlock so the same code can enforce a SECOND, narrower
 * domain later in a descendant -- see actor_isolation_restrict(). Landlock
 * domains nest: a process already inside one can create another, and the
 * effective rights are the intersection. So this can only ever remove access,
 * which is what makes stacking safe to expose.
 *
 * `var_ro`/`var_rw`/`var_net` name the variables the values came from, purely
 * so an error message points at the knob the operator actually set.
 * `check_lmdb` belongs to the startup call only: a descendant is not the
 * process that opens the database.
 */
static int build_and_restrict(const char* ro, const char* rw, const char* net,
                              const char* var_ro, const char* var_rw,
                              const char* var_net, int check_lmdb) {
    if (!ro && !rw && !net) return 0;

    /* Negotiate the ABI. The kernel reports the highest version it supports;
       anything we ask for beyond that is silently absent, which is the one
       outcome not acceptable here -- so the unsupported case is an error, not
       a downgrade. */
    long abi = ll_create_ruleset(NULL, 0, LANDLOCK_CREATE_RULESET_VERSION);
    if (abi < 0) {
        fprintf(stderr, "[actor] isolation: landlock unavailable (%s). "
                        "Needs kernel 5.13+ with landlock enabled and in the "
                        "active LSM list.\n", strerror(errno));
        return -1;
    }
    if (net && abi < 4) {
        fprintf(stderr, "[actor] isolation: %s needs "
                        "landlock ABI 4 (kernel 6.7+); this kernel reports ABI %ld\n",
                var_net, abi);
        return -1;
    }

    /* Handled access must not name rights the running kernel does not know,
       or landlock_create_ruleset rejects the whole attr. Build it up by ABI. */
    uint64_t fs_handled = LL_RW;
    if (abi < 3) fs_handled &= ~(uint64_t)LANDLOCK_ACCESS_FS_TRUNCATE;

    /* Handling filesystem access while adding no path rule denies the whole
       filesystem -- that is the "no ambient allow" property this file relies
       on everywhere else. Harmless when ro/rw are set, fatal when they are
       not: a caller restricting only outbound TCP would silently lose every
       path as well. So handle the filesystem only when a path list says to. */
    if (!ro && !rw) fs_handled = 0;
    /* REFER (ABI 2+) is deliberately not handled: it is always denied without
       an explicit rule, and handling it would break rename/link inside a
       single rw subtree, which handlers legitimately do. */

    struct landlock_ruleset_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.handled_access_fs = fs_handled;
    size_t attr_size = sizeof(attr);
    if (abi >= 4 && net) {
        attr.handled_access_net = LANDLOCK_ACCESS_NET_CONNECT_TCP;
    } else if (abi < 4) {
        /* Older kernels have no net field; submit the smaller struct. */
        attr_size = offsetof(struct landlock_ruleset_attr, handled_access_fs)
                  + sizeof(attr.handled_access_fs);
    }

    int rs = (int)ll_create_ruleset(&attr, attr_size, 0);
    if (rs < 0) {
        fprintf(stderr, "[actor] isolation: landlock_create_ruleset: %s\n", strerror(errno));
        return -1;
    }

    /* The actor opens its own LMDB after this ruleset is enforced. A path
       outside the rw set turns into an opaque LMDB failure at startup, so
       catch it here where the message can name the actual problem. */
    const char* lmdb = check_lmdb ? getenv("ACTOR_LMDB_PATH") : NULL;
    if (lmdb && !path_covered(lmdb, rw)) {
        fprintf(stderr, "[actor] isolation: ACTOR_LMDB_PATH=\"%s\" is not inside "
                        "%s=\"%s\" -- the actor could not open its "
                        "own database\n", lmdb, var_rw, rw ? rw : "");
        close(rs);
        return -1;
    }
    (void)var_ro;

    if (for_each_path(ro, rs, LL_RO, ll_add_path) < 0) { close(rs); return -1; }
    if (for_each_path(rw, rs, fs_handled, ll_add_path) < 0) { close(rs); return -1; }

    /* Outbound TCP ports, when asked for. */
    if (net && abi >= 4) {
        char buf[1024];
        if (strlen(net) >= sizeof(buf)) {
            fprintf(stderr, "[actor] isolation: %s too long\n", var_net);
            close(rs); return -1;
        }
        strcpy(buf, net);
        char* save = NULL;
        for (char* t = strtok_r(buf, ":", &save); t; t = strtok_r(NULL, ":", &save)) {
            while (*t == ' ') t++;
            if (!*t) continue;
            unsigned long long port;
            if (parse_ull(t, &port) || port > 65535) {
                fprintf(stderr, "[actor] isolation: bad port \"%s\" in %s\n",
                        t, var_net);
                close(rs); return -1;
            }
            struct landlock_net_port_attr np = {
                .allowed_access = LANDLOCK_ACCESS_NET_CONNECT_TCP,
                .port = port
            };
            if (ll_add_rule(rs, LANDLOCK_RULE_NET_PORT, &np, 0)) {
                fprintf(stderr, "[actor] isolation: landlock_add_rule(port %llu): %s\n",
                        port, strerror(errno));
                close(rs); return -1;
            }
        }
    }

    /* no_new_privs is required before restrict_self, and is wanted anyway:
       it stops a setuid binary anywhere below this process regaining what was
       just dropped. */
    if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)) {
        fprintf(stderr, "[actor] isolation: prctl(NO_NEW_PRIVS): %s\n", strerror(errno));
        close(rs); return -1;
    }
    if (ll_restrict_self(rs, 0)) {
        fprintf(stderr, "[actor] isolation: landlock_restrict_self: %s\n", strerror(errno));
        close(rs); return -1;
    }
    close(rs);
    fprintf(stderr, "[actor] isolation: landlock abi=%ld ro=%s rw=%s net=%s\n",
            abi, ro ? ro : "-", rw ? rw : "-", net ? net : "-");
    return 0;
}

static int apply_landlock(void) {
    return build_and_restrict(getenv("ACTOR_LANDLOCK_RO"),
                              getenv("ACTOR_LANDLOCK_RW"),
                              getenv("ACTOR_LANDLOCK_NET_CONNECT"),
                              "ACTOR_LANDLOCK_RO", "ACTOR_LANDLOCK_RW",
                              "ACTOR_LANDLOCK_NET_CONNECT", 1);
}

/* Stack a narrower Landlock domain on the CALLING process.
 *
 * Unlike actor_isolation_apply(), this is meant to be called by a handler --
 * or by a helper the handler execs -- rather than by the runtime, because the
 * moment it runs is the point of the whole thing. A handler that must keep one
 * capability for itself while denying it to what it spawns opens that
 * connection first, then calls this, then spawns: the descendant inherits the
 * narrower domain and cannot reopen what the parent already holds.
 *
 * An already-open file descriptor survives -- Landlock governs the act of
 * opening or connecting, not a connection that exists. That asymmetry is not
 * an oversight to work around; it is the mechanism.
 *
 * Nothing here can widen anything. If the caller is already confined, the
 * result is the intersection.
 */
int actor_isolation_restrict(void) {
    return build_and_restrict(getenv("ACTOR_RESTRICT_RO"),
                              getenv("ACTOR_RESTRICT_RW"),
                              getenv("ACTOR_RESTRICT_NET_CONNECT"),
                              "ACTOR_RESTRICT_RO", "ACTOR_RESTRICT_RW",
                              "ACTOR_RESTRICT_NET_CONNECT", 0);
}


/* ── seccomp ─────────────────────────────────────────────────────────────────
 *
 * A denylist, not an allowlist, and that is a deliberate reversal of the usual
 * advice.
 *
 * An allowlist is stronger in principle. It is also unmaintainable here: the
 * handler contract is "any process that speaks stdio", so the syscall set is
 * whatever an arbitrary program in an arbitrary language needs. Tracing the
 * runtime plus two shell handlers on musl already gives 57 distinct calls, and
 * that set shifts with libc, with the language, and with what the handler
 * does. An allowlist tuned to it would fail closed on the first handler that
 * did something ordinary and unanticipated -- a handler dying with SIGSYS for
 * reasons that look unrelated to anything the operator changed.
 *
 * What CAN be enumerated exactly is the set of calls no handler has a
 * legitimate reason to make, and which are the ones that matter for escaping
 * confinement or attacking a neighbour:
 *
 *   ptrace, process_vm_readv/writev   read or write another process's memory
 *   kcmp                              compare another process's resources
 *   perf_event_open                   trace the system
 *   bpf                               load kernel programs; also how an actor
 *                                     could try to lift its own restrictions
 *   userfaultfd                       a classic exploit primitive
 *   kexec_load, kexec_file_load       replace the running kernel
 *   init_module, finit_module,
 *   delete_module                     load kernel code
 *   pivot_root, chroot                change the root -- the runtime does this
 *                                     itself in phase 6, BEFORE this filter is
 *                                     installed, so denying it here does not
 *                                     conflict (see the note below)
 *   mount, umount2, move_mount,
 *   open_tree, fsopen, fsconfig,
 *   fsmount                           remount the filesystem out from under
 *                                     the Landlock ruleset
 *   setns, unshare                    join or create namespaces -- an actor
 *                                     that could unshare after this filter is
 *                                     installed could undo phase 4's confinement
 *   add_key, keyctl, request_key      the kernel keyring
 *   swapon, swapoff, reboot           system-level operations
 *
 * Ordering with phase 6 is the subtle part, and the plan called it out as a
 * coupling to resolve. It resolves cleanly because the two run at different
 * scopes: this filter is installed once at actor STARTUP, while the mount and
 * pivot_root calls happen per TUPLE, in the forked child. seccomp filters are
 * inherited across fork, so a naive reading says the child's pivot_root would
 * be blocked by the parent's filter -- and it would. Hence
 * ACTOR_SECCOMP=permissive_mount, which keeps the mount family allowed for
 * deployments that use ACTOR_ROOTFS, and a startup check that refuses the
 * contradictory combination outright rather than letting phase 6 break in a
 * way that surfaces as a failed handler.
 *
 * SCMP_ACT_ERRNO(EPERM) rather than SIGSYS: a handler that gets EPERM from
 * ptrace can report a sensible error, while one killed by SIGSYS looks like a
 * crash. The runtime treats both as a failed tuple, but the former is far
 * easier to diagnose.
 */

/* Minimal seccomp-bpf. No libseccomp dependency: the filter is a fixed
   sequence of "if nr == X return EPERM", which is a few lines of BPF and
   avoids adding a library to a runtime whose whole premise is having none. */
#ifndef SECCOMP_MODE_FILTER
#  define SECCOMP_MODE_FILTER 2
#endif
#ifndef SECCOMP_SET_MODE_FILTER
#  define SECCOMP_SET_MODE_FILTER 1
#endif
#ifndef AUDIT_ARCH_X86_64
#  define AUDIT_ARCH_X86_64 0xc000003e
#endif
#ifndef AUDIT_ARCH_AARCH64
#  define AUDIT_ARCH_AARCH64 0xc00000b7
#endif

#if defined(__x86_64__)
#  define ACTOR_AUDIT_ARCH AUDIT_ARCH_X86_64
#elif defined(__aarch64__)
#  define ACTOR_AUDIT_ARCH AUDIT_ARCH_AARCH64
#else
#  define ACTOR_AUDIT_ARCH 0
#endif

struct actor_sock_filter { uint16_t code; uint8_t jt; uint8_t jf; uint32_t k; };
struct actor_sock_fprog  { unsigned short len; struct actor_sock_filter* filter; };

/* Classic BPF opcodes, spelled out rather than pulled from linux/filter.h,
   which is another header alpine does not ship without linux-headers. */
#define BPF_LD_W_ABS   0x20
#define BPF_JMP_JEQ_K  0x15
#define BPF_JMP_JA     0x05
#define BPF_RET_K      0x06
#define SECCOMP_RET_ALLOW 0x7fff0000U
#define SECCOMP_RET_ERRNO 0x00050000U
#define SECCOMP_RET_KILL_PROCESS 0x80000000U

/* Offsets into struct seccomp_data. */
#define SD_NR   0
#define SD_ARCH 4

/* Syscalls denied to every actor and every handler. Named by __NR_*, and
   skipped when a number is not defined for this architecture. */
static const int g_denied[] = {
#ifdef __NR_ptrace
    __NR_ptrace,
#endif
#ifdef __NR_process_vm_readv
    __NR_process_vm_readv,
#endif
#ifdef __NR_process_vm_writev
    __NR_process_vm_writev,
#endif
#ifdef __NR_kcmp
    __NR_kcmp,
#endif
#ifdef __NR_perf_event_open
    __NR_perf_event_open,
#endif
#ifdef __NR_bpf
    __NR_bpf,
#endif
#ifdef __NR_userfaultfd
    __NR_userfaultfd,
#endif
#ifdef __NR_kexec_load
    __NR_kexec_load,
#endif
#ifdef __NR_kexec_file_load
    __NR_kexec_file_load,
#endif
#ifdef __NR_init_module
    __NR_init_module,
#endif
#ifdef __NR_finit_module
    __NR_finit_module,
#endif
#ifdef __NR_delete_module
    __NR_delete_module,
#endif
#ifdef __NR_add_key
    __NR_add_key,
#endif
#ifdef __NR_keyctl
    __NR_keyctl,
#endif
#ifdef __NR_request_key
    __NR_request_key,
#endif
#ifdef __NR_swapon
    __NR_swapon,
#endif
#ifdef __NR_swapoff
    __NR_swapoff,
#endif
#ifdef __NR_reboot
    __NR_reboot,
#endif
#ifdef __NR_setns
    __NR_setns,
#endif
};

/* Denied unless the deployment also uses ACTOR_ROOTFS / per-tuple namespaces,
   which need these in the forked child. See the ordering note above. */
static const int g_denied_mount[] = {
#ifdef __NR_mount
    __NR_mount,
#endif
#ifdef __NR_umount2
    __NR_umount2,
#endif
#ifdef __NR_pivot_root
    __NR_pivot_root,
#endif
#ifdef __NR_chroot
    __NR_chroot,
#endif
#ifdef __NR_move_mount
    __NR_move_mount,
#endif
#ifdef __NR_open_tree
    __NR_open_tree,
#endif
#ifdef __NR_fsopen
    __NR_fsopen,
#endif
#ifdef __NR_fsconfig
    __NR_fsconfig,
#endif
#ifdef __NR_fsmount
    __NR_fsmount,
#endif
#ifdef __NR_unshare
    __NR_unshare,
#endif
};

static int apply_seccomp(void) {
    const char* mode = getenv("ACTOR_SECCOMP");
    if (!mode || !*mode || !strcmp(mode, "0")) return 0;

    int permissive_mount = !strcmp(mode, "permissive_mount");
    if (strcmp(mode, "1") && !permissive_mount) {
        fprintf(stderr, "[actor] isolation: ACTOR_SECCOMP=\"%s\" is not recognised "
                        "(use 1, or permissive_mount when ACTOR_ROOTFS or "
                        "ACTOR_TUPLE_UNSHARE is in use)\n", mode);
        return -1;
    }

    if (ACTOR_AUDIT_ARCH == 0) {
        fprintf(stderr, "[actor] isolation: ACTOR_SECCOMP is not supported on this "
                        "architecture\n");
        return -1;
    }

    /* The contradiction the plan flagged: a filter that blocks the mount family
       is inherited by the forked child, where phase 6 needs it. Refuse rather
       than let phase 6 break as a mysterious handler failure. */
    const char* tuple_ns = getenv("ACTOR_TUPLE_UNSHARE");
    if (!permissive_mount && tuple_ns && *tuple_ns) {
        fprintf(stderr, "[actor] isolation: ACTOR_SECCOMP=1 blocks unshare and the mount "
                        "family, which ACTOR_TUPLE_UNSHARE=%s needs in the forked child. "
                        "Use ACTOR_SECCOMP=permissive_mount, or drop one of the two.\n",
                tuple_ns);
        return -1;
    }

    size_t n_deny = sizeof(g_denied) / sizeof(g_denied[0]);
    size_t n_mount = permissive_mount ? 0 : sizeof(g_denied_mount) / sizeof(g_denied_mount[0]);
    size_t total   = n_deny + n_mount;

    /* 2 instructions of arch check + 1 load of nr + 2 per denied call + 1
       trailing allow. */
    size_t max_insns = 4 + total * 2 + 2;
    struct actor_sock_filter* f = calloc(max_insns, sizeof(*f));
    if (!f) {
        fprintf(stderr, "[actor] isolation: seccomp filter allocation failed\n");
        return -1;
    }

    size_t i = 0;
    /* Reject any architecture but the one this binary was built for: syscall
       numbers differ per arch, so a filter written for one is meaningless --
       and silently wrong -- under another. */
    f[i++] = (struct actor_sock_filter){ BPF_LD_W_ABS, 0, 0, SD_ARCH };
    f[i++] = (struct actor_sock_filter){ BPF_JMP_JEQ_K, 1, 0, (uint32_t)ACTOR_AUDIT_ARCH };
    f[i++] = (struct actor_sock_filter){ BPF_RET_K, 0, 0, SECCOMP_RET_KILL_PROCESS };
    f[i++] = (struct actor_sock_filter){ BPF_LD_W_ABS, 0, 0, SD_NR };

    for (size_t d = 0; d < n_deny; d++) {
        f[i++] = (struct actor_sock_filter){ BPF_JMP_JEQ_K, 0, 1, (uint32_t)g_denied[d] };
        f[i++] = (struct actor_sock_filter){ BPF_RET_K, 0, 0,
                                             SECCOMP_RET_ERRNO | (EPERM & 0xffff) };
    }
    for (size_t d = 0; d < n_mount; d++) {
        f[i++] = (struct actor_sock_filter){ BPF_JMP_JEQ_K, 0, 1, (uint32_t)g_denied_mount[d] };
        f[i++] = (struct actor_sock_filter){ BPF_RET_K, 0, 0,
                                             SECCOMP_RET_ERRNO | (EPERM & 0xffff) };
    }
    f[i++] = (struct actor_sock_filter){ BPF_RET_K, 0, 0, SECCOMP_RET_ALLOW };

    struct actor_sock_fprog prog = { (unsigned short)i, f };

    /* no_new_privs is a precondition for an unprivileged seccomp filter, and
       is set by the Landlock step when that runs. Set it unconditionally here
       so seccomp works on its own. */
    if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0) {
        fprintf(stderr, "[actor] isolation: prctl(NO_NEW_PRIVS): %s\n", strerror(errno));
        free(f);
        return -1;
    }
    if (syscall(__NR_seccomp, SECCOMP_SET_MODE_FILTER, 0, &prog) != 0) {
        /* Older kernels have no seccomp(2); fall back to the prctl interface. */
        if (prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &prog, 0, 0) != 0) {
            fprintf(stderr, "[actor] isolation: seccomp filter: %s\n", strerror(errno));
            free(f);
            return -1;
        }
    }
    free(f);

    fprintf(stderr, "[actor] isolation: seccomp mode=%s denied=%zu\n",
            permissive_mount ? "permissive_mount" : "1", total);
    return 0;
}

/* ── Entry point ─────────────────────────────────────────────────────────── */

/* True if this process appears to have more than one thread.
 *
 * /proc/self/status carries a labelled "Threads:" line, which avoids parsing
 * /proc/self/stat positionally past a comm field that may itself contain
 * spaces and parentheses. If /proc is not mounted we cannot tell, and this
 * returns 0: the check guards against a future reordering of actor_run, not
 * against an attacker, so being unable to perform it must not block an actor
 * that is in fact single-threaded. */
static int looks_multithreaded(void) {
    FILE* f = fopen("/proc/self/status", "r");
    if (!f) return 0;

    char line[256];
    int threads = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "Threads:", 8) == 0) {
            threads = atoi(line + 8);
            break;
        }
    }
    fclose(f);
    return threads > 1;
}

int actor_isolation_apply(void) {
    /* Nothing requested: return before doing anything at all, so an actor that
       does not opt in pays not even the /proc read. */
    int requested =
        getenv("ACTOR_UID")           || getenv("ACTOR_GID")         ||
        getenv("ACTOR_RLIMIT_AS")     || getenv("ACTOR_RLIMIT_CPU")  ||
        getenv("ACTOR_RLIMIT_NOFILE") || getenv("ACTOR_RLIMIT_NPROC")  ||
        getenv("ACTOR_LANDLOCK_RO")   || getenv("ACTOR_LANDLOCK_RW")   ||
        getenv("ACTOR_LANDLOCK_NET_CONNECT") || getenv("ACTOR_CGROUP_PATH") ||
        getenv("ACTOR_SECCOMP");
    if (!requested) return 0;

    if (looks_multithreaded()) {
        fprintf(stderr, "[actor] isolation: refusing to apply after threads have started. "
                        "actor_isolation_apply() must run before any pthread_create.\n");
        return -1;
    }

    /* Limits before the privilege drop: setrlimit can lower a hard limit but
       an unprivileged process cannot raise one back, so anything that needs
       privilege to set must happen while the process still has it.
       RLIMIT_NPROC is the exception that makes the ordering visible -- the
       kernel does not enforce it for real uid 0 or for a process holding
       CAP_SYS_ADMIN/CAP_SYS_RESOURCE, so it only starts to bite once the uid
       drop below has happened. Setting it here and dropping after is what
       makes it mean anything. */
    /* RLIMIT_NPROC deserves a warning the others do not. It counts every
       process AND thread already running under the REAL uid, system wide --
       not this actor's descendants. An actor sharing a uid with anything else
       (a login session, another actor, a busy service account) inherits that
       count, so a limit that looks generous can be below the current usage
       before this process creates a single thread. The failure then lands on
       the next pthread_create as EAGAIN, which surfaces from whatever library
       happened to ask first -- observed as "nng_pub0_open: Out of memory",
       which points nowhere near the cause.
       Give an actor its own uid before setting this, and size it against
       `ps -L -u <uid> | wc -l` rather than against the actor's own thread
       count. */
    if (apply_rlimit("ACTOR_RLIMIT_AS",     RLIMIT_AS,     "RLIMIT_AS")     < 0) return -1;
    if (apply_rlimit("ACTOR_RLIMIT_CPU",    RLIMIT_CPU,    "RLIMIT_CPU")    < 0) return -1;
    if (apply_rlimit("ACTOR_RLIMIT_NOFILE", RLIMIT_NOFILE, "RLIMIT_NOFILE") < 0) return -1;
    if (apply_rlimit("ACTOR_RLIMIT_NPROC",  RLIMIT_NPROC,  "RLIMIT_NPROC")  < 0) return -1;

    if (drop_privilege() < 0) return -1;

    /* cgroup before Landlock: joining needs to open a path under
       /sys/fs/cgroup, which an operator has no reason to put in the ruleset. */
    if (join_cgroup() < 0) return -1;

    /* Landlock before seccomp: building the ruleset opens paths, and a
       seccomp filter is one more thing that could interfere with that. Both
       are irreversible, and everything above still needs an unrestricted view
       of the filesystem to do its job. */
    if (apply_landlock() < 0) return -1;

    /* seccomp last: nothing after it needs a syscall the filter might deny. */
    if (apply_seccomp() < 0) return -1;

    /* One line naming what is actually in force, so an operator can tell
       isolation happened without reading the launcher's configuration. */
    fprintf(stderr, "[actor] isolation: uid=%u gid=%u", (unsigned)getuid(), (unsigned)getgid());
    const char* v;
    if ((v = getenv("ACTOR_RLIMIT_AS")))     fprintf(stderr, " as=%s", v);
    if ((v = getenv("ACTOR_RLIMIT_CPU")))    fprintf(stderr, " cpu=%s", v);
    if ((v = getenv("ACTOR_RLIMIT_NOFILE"))) fprintf(stderr, " nofile=%s", v);
    if ((v = getenv("ACTOR_RLIMIT_NPROC")))  fprintf(stderr, " nproc=%s", v);
    fprintf(stderr, "\n");
    return 0;
}
