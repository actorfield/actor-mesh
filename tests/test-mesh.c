#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <spawn.h>
#include <sys/wait.h>
#include <time.h>
#include <nng/nng.h>
#include <nng/protocol/pubsub0/pub.h>
#include <nng/protocol/pubsub0/sub.h>

static int failures = 0;
extern char **environ;
static void ms(int ms) { struct timespec t={ms/1000,(ms%1000)*1000000}; nanosleep(&t,NULL); }
#define SP "tcp://127.0.0.1:55656"
#define PP "tcp://127.0.0.1:55657"
#define TEST(n) printf("=== %s ===\n", n)
#define PASS() printf("  PASS\n")
#define FAIL(m) do { printf("  FAIL: %s\n", m); failures++; } while(0)
#define CHECK(c,m) do { if(c)PASS(); else FAIL(m); } while(0)

static void free_ports(void) { system("fuser -k 55656/tcp 55657/tcp 2>/dev/null"); ms(400); }
static void cleanup(void) {
    system("pkill -9 -f 'bin/mesh-proxy' 2>/dev/null");
    system("pkill -9 -f 'bin/actor' 2>/dev/null");
    ms(300);
}

static pid_t sp(char **a, char **e) { pid_t p; posix_spawn(&p, a[0], NULL, NULL, a, e); return p; }

/* Frame buffer is header + payload. It was f[261] -- 256 bytes of header and
   room for only 5 payload bytes -- so any test sending more (t9's 52-byte
   msgpack announce) wrote past the end of the stack buffer and smashed the
   return address. Sized to the header plus a payload cap the tests stay well
   under, and asserted rather than silently truncated. */
#define TEST_MAX_PAYLOAD 1024

static void sendm(const char *topic, const uint8_t *p, size_t pl) {
    if (pl > TEST_MAX_PAYLOAD) { FAIL("sendm: payload exceeds TEST_MAX_PAYLOAD"); return; }
    nng_socket s; nng_pub0_open(&s); nng_dial(s, PP, NULL, 0); ms(50);
    uint8_t f[256+TEST_MAX_PAYLOAD]={0}; int tl=strlen(topic); if(tl>31)tl=31;
    memcpy(f, topic, tl); memcpy(f+80, "test", 4);
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    int64_t ns=ts.tv_sec*1000000000LL+ts.tv_nsec;
    memcpy(f+112, &ns, 8); uint32_t pl2=pl; memcpy(f+132, &pl2, 4);
    memcpy(f+256, p, pl); nng_send(s, f, 256+pl, 0); nng_close(s);
}

static ssize_t recvm(const char *topic, uint8_t *b, size_t cap, int to) {
    nng_socket s; nng_sub0_open(&s); nng_dial(s, SP, NULL, 0);
    nng_socket_set(s, NNG_OPT_SUB_SUBSCRIBE, topic, strlen(topic)+1);
    nng_socket_set_ms(s, "recv-timeout", to);
    nng_msg *m=NULL;
    if(nng_recvmsg(s,&m,0)){nng_close(s);return -1;}
    size_t len=nng_msg_len(m);
    ssize_t r=len>256?(ssize_t)(len-256):0;
    if(r>0&&b&&(size_t)r<=cap)memcpy(b,(uint8_t*)nng_msg_body(m)+256,r);
    nng_msg_free(m); nng_close(s); return r;
}

/* ── Tests ── */

static void t1(void){ TEST("proxy: forward");
    free_ports(); cleanup();
    char *a[]={"./bin/mesh-proxy",NULL},*e[]={"PROXY_SUB_BIND=tcp://127.0.0.1:55657","PROXY_PUB_BIND=tcp://127.0.0.1:55656",NULL};
    pid_t pp=sp(a,e); ms(600);
    /* Create subscriber FIRST, then send */
    nng_socket sub; nng_sub0_open(&sub); nng_dial(sub, SP, NULL, 0);
    nng_socket_set(sub, NNG_OPT_SUB_SUBSCRIBE, "t1", 2);
    nng_socket_set_ms(sub, "recv-timeout", 3000);
    ms(100);
    uint8_t p[]="hello"; sendm("t1",p,5);
    nng_msg *m=NULL; CHECK(nng_recvmsg(sub,&m,0)==0,"not forwarded");
    if(m)nng_msg_free(m);
    nng_close(sub);
    kill(pp,SIGKILL); waitpid(pp,NULL,0);
}

static void t2(void){ TEST("proxy: topic filter");
    free_ports(); cleanup();
    char *a[]={"./bin/mesh-proxy",NULL},*e[]={"PROXY_SUB_BIND=tcp://127.0.0.1:55657","PROXY_PUB_BIND=tcp://127.0.0.1:55656",NULL};
    pid_t pp=sp(a,e); ms(600);
    sendm("topic_A",(uint8_t*)"x",1);
    CHECK(recvm("topic_B",NULL,0,1000)<0,"wrong topic"); kill(pp,SIGKILL); waitpid(pp,NULL,0);
}

static void t3(void){ TEST("actor: respond");
    free_ports(); cleanup();
    char *a[]={"./bin/mesh-proxy",NULL},*e[]={"PROXY_SUB_BIND=tcp://127.0.0.1:55657","PROXY_PUB_BIND=tcp://127.0.0.1:55656",NULL};
    pid_t pp=sp(a,e); ms(600);
    system("mkdir -p /tmp/tm3");
    char *aa[]={"./bin/actor",NULL},*ae[]={
        "ACTOR_BUS_SUB=tcp://127.0.0.1:55656","ACTOR_BUS_PUB=tcp://127.0.0.1:55657",
        "ACTOR_HEARTBEAT_MS=0","ACTOR_ID=a3","ACTOR_TOPIC=ping","ACTOR_RESULT_TOPIC=pong",
        "ACTOR_HANDLER=sh -c 'echo pong; echo ok'","ACTOR_LMDB_PATH=/tmp/tm3",NULL};
    pid_t ap=sp(aa,ae); ms(600);
    sendm("ping",(uint8_t*)"d",1);
    CHECK(recvm("pong",NULL,0,3000)>0,"no response");
    kill(pp,SIGKILL); kill(ap,SIGKILL); waitpid(pp,NULL,0); waitpid(ap,NULL,0);
}

static void t4(void){ TEST("actor: retry");
    free_ports(); cleanup();
    char *a[]={"./bin/mesh-proxy",NULL},*e[]={"PROXY_SUB_BIND=tcp://127.0.0.1:55657","PROXY_PUB_BIND=tcp://127.0.0.1:55656",NULL};
    pid_t pp=sp(a,e); ms(600);
    system("rm -rf /tmp/tm4; mkdir -p /tmp/tm4");
    char *aa[]={"./bin/actor",NULL},*ae[]={
        "ACTOR_BUS_SUB=tcp://127.0.0.1:55656","ACTOR_BUS_PUB=tcp://127.0.0.1:55657",
        "ACTOR_HEARTBEAT_MS=0","ACTOR_RETRY_MAX=3","ACTOR_ID=a4","ACTOR_TOPIC=ri","ACTOR_RESULT_TOPIC=ro",
        "ACTOR_HANDLER=sh -c 'A=/tmp/tm4/c; C=$(cat $A 2>/dev/null||echo 0); C=$((C+1)); echo $C > $A; [ $C -ge 2 ] && echo ro && echo ok || exit 1'",
        "ACTOR_LMDB_PATH=/tmp/tm4",NULL};
    pid_t ap=sp(aa,ae); ms(600);
    sendm("ri",(uint8_t*)"x",1);
    CHECK(recvm("ro",NULL,0,5000)>0,"retry failed");
    kill(pp,SIGKILL); kill(ap,SIGKILL); waitpid(pp,NULL,0); waitpid(ap,NULL,0);
}

static void t5(void){ TEST("actor: multi-topic");
    free_ports(); cleanup();
    char *a[]={"./bin/mesh-proxy",NULL},*e[]={"PROXY_SUB_BIND=tcp://127.0.0.1:55657","PROXY_PUB_BIND=tcp://127.0.0.1:55656",NULL};
    pid_t pp=sp(a,e); ms(600);
    system("mkdir -p /tmp/tm5");
    char *aa[]={"./bin/actor",NULL},*ae[]={
        "ACTOR_BUS_SUB=tcp://127.0.0.1:55656","ACTOR_BUS_PUB=tcp://127.0.0.1:55657",
        "ACTOR_HEARTBEAT_MS=0","ACTOR_ID=a5","ACTOR_TOPIC=ta,tb","ACTOR_RESULT_TOPIC=out",
        "ACTOR_HANDLER=sh -c 'echo out; echo got'","ACTOR_LMDB_PATH=/tmp/tm5",NULL};
    pid_t ap=sp(aa,ae); ms(600);
    sendm("tb",(uint8_t*)"x",1);
    CHECK(recvm("out",NULL,0,3000)>0,"2nd topic fail");
    kill(pp,SIGKILL); kill(ap,SIGKILL); waitpid(pp,NULL,0); waitpid(ap,NULL,0);
}

static void t6(void){ TEST("actor: TTL expiry");
    free_ports(); cleanup();
    char *a[]={"./bin/mesh-proxy",NULL},*e[]={"PROXY_SUB_BIND=tcp://127.0.0.1:55657","PROXY_PUB_BIND=tcp://127.0.0.1:55656",NULL};
    pid_t pp=sp(a,e); ms(600);
    system("mkdir -p /tmp/tm6");
    char *aa[]={"./bin/actor",NULL},*ae[]={
        "ACTOR_BUS_SUB=tcp://127.0.0.1:55656","ACTOR_BUS_PUB=tcp://127.0.0.1:55657",
        "ACTOR_HEARTBEAT_MS=0","ACTOR_ID=a6","ACTOR_TOPIC=ttl_in","ACTOR_RESULT_TOPIC=ttl_out",
        "ACTOR_HANDLER=sh -c 'echo ttl_out; echo bad'","ACTOR_LMDB_PATH=/tmp/tm6",
        "ACTOR_TTL_NS=1000000",NULL};
    pid_t ap=sp(aa,ae); ms(600);
    nng_socket s; nng_pub0_open(&s); nng_dial(s,PP,NULL,0); ms(50);
    /* emitted_at set, long past: a 0 would be stamped on arrival (t11). */
    uint8_t f[257]={0}; memcpy(f,"ttl_in",6); int64_t at=1,ttl=1; memcpy(f+112,&at,8); memcpy(f+120,&ttl,8); f[256]='x';
    nng_send(s,f,257,0); nng_close(s);
    CHECK(recvm("ttl_out",NULL,0,2000)<0,"expired processed");
    kill(pp,SIGKILL); kill(ap,SIGKILL); waitpid(pp,NULL,0); waitpid(ap,NULL,0);
}

static void t7(void){ TEST("handler: env vars");
    free_ports(); cleanup();
    char *a[]={"./bin/mesh-proxy",NULL},*e[]={"PROXY_SUB_BIND=tcp://127.0.0.1:55657","PROXY_PUB_BIND=tcp://127.0.0.1:55656",NULL};
    pid_t pp=sp(a,e); ms(600);
    system("mkdir -p /tmp/tm7");
    char *aa[]={"./bin/actor",NULL},*ae[]={
        "ACTOR_BUS_SUB=tcp://127.0.0.1:55656","ACTOR_BUS_PUB=tcp://127.0.0.1:55657",
        "ACTOR_HEARTBEAT_MS=0","ACTOR_ID=a7","ACTOR_TOPIC=ei","ACTOR_RESULT_TOPIC=eo",
        "ACTOR_HANDLER=sh -c 'echo eo; echo T=\\$ACTOR_TUPLE_ID C=\\$ACTOR_CORRELATION_ID O=\\$ACTOR_TUPLE_ORIGIN A=\\$ACTOR_ATTEMPT'",
        "ACTOR_LMDB_PATH=/tmp/tm7",NULL};
    pid_t ap=sp(aa,ae); ms(600);
    sendm("ei",(uint8_t*)"x",1);
    uint8_t b[512]; ssize_t n=recvm("eo",b,511,3000);
    CHECK(n>0,"no response"); if(n>0){b[n]=0;
        CHECK(strstr((char*)b,"T=")!=NULL,"TUPLE_ID"); CHECK(strstr((char*)b,"C=")!=NULL,"CORR_ID");
        CHECK(strstr((char*)b,"O=")!=NULL,"ORIGIN"); CHECK(strstr((char*)b,"A=")!=NULL,"ATTEMPT");}
    kill(pp,SIGKILL); kill(ap,SIGKILL); waitpid(pp,NULL,0); waitpid(ap,NULL,0);
}

static void t8(void){ TEST("handler: topic route");
    free_ports(); cleanup();
    char *a[]={"./bin/mesh-proxy",NULL},*e[]={"PROXY_SUB_BIND=tcp://127.0.0.1:55657","PROXY_PUB_BIND=tcp://127.0.0.1:55656",NULL};
    pid_t pp=sp(a,e); ms(600);
    system("mkdir -p /tmp/tm8");
    char *aa[]={"./bin/actor",NULL},*ae[]={
        "ACTOR_BUS_SUB=tcp://127.0.0.1:55656","ACTOR_BUS_PUB=tcp://127.0.0.1:55657",
        "ACTOR_HEARTBEAT_MS=0","ACTOR_ID=a8","ACTOR_TOPIC=di","ACTOR_RESULT_TOPIC=def",
        "ACTOR_HANDLER=sh -c 'echo custom; echo ok'","ACTOR_LMDB_PATH=/tmp/tm8",NULL};
    pid_t ap=sp(aa,ae); ms(600);
    sendm("di",(uint8_t*)"x",1);
    CHECK(recvm("custom",NULL,0,2000)>0,"route fail");
    kill(pp,SIGKILL); kill(ap,SIGKILL); waitpid(pp,NULL,0); waitpid(ap,NULL,0);
}

static void t9(void){ TEST("registry: store tool");
    free_ports(); cleanup();
    char *a[]={"./bin/mesh-proxy",NULL},*e[]={"PROXY_SUB_BIND=tcp://127.0.0.1:55657","PROXY_PUB_BIND=tcp://127.0.0.1:55656",NULL};
    pid_t pp=sp(a,e); ms(600);
    system("rm -rf /tmp/tm9; mkdir -p /tmp/tm9");
    char *aa[]={"./bin/actor",NULL},*ae[]={
        "ACTOR_BUS_SUB=tcp://127.0.0.1:55656","ACTOR_BUS_PUB=tcp://127.0.0.1:55657",
        "ACTOR_HEARTBEAT_MS=0","ACTOR_ID=a9","ACTOR_TOPIC=_tool_announce,_tool_discover","ACTOR_RESULT_TOPIC=_tool_list",
        "ACTOR_HANDLER=./examples/employee-mesh/handlers/registry/tool-registry.sh",
        "ACTOR_LMDB_PATH=/tmp/tm9",
        "BRIDGE_LIB=./examples/employee-mesh/handlers/lib",NULL};
    pid_t ap=sp(aa,ae); ms(600);
    uint8_t mp[]={0x83,0xa4,'t','y','p','e',0xae,'_','t','o','o','l','_','a','n','n','o','u','n','c','e',
        0xa5,'a','c','t','o','r',0xa2,'t','9',0xac,'c','a','p','a','b','i','l','i','t','i','e','s',
        0x91,0x81,0xa4,'n','a','m','e',0xa1,'q'};
    sendm("_tool_announce",mp,sizeof(mp)); ms(500);
    FILE *f=fopen("/tmp/tm9/tools/t9.json","r");
    CHECK(f!=NULL,"not stored"); if(f)fclose(f);
    kill(pp,SIGKILL); kill(ap,SIGKILL); waitpid(pp,NULL,0); waitpid(ap,NULL,0);
}

static void t10(void){ TEST("heartbeat: actors emit");
    free_ports(); cleanup();
    char *a[]={"./bin/mesh-proxy",NULL},*e[]={"PROXY_SUB_BIND=tcp://127.0.0.1:55657","PROXY_PUB_BIND=tcp://127.0.0.1:55656",NULL};
    pid_t pp=sp(a,e); ms(600);
    system("mkdir -p /tmp/tm10");
    char *aa[]={"./bin/actor",NULL},*ae[]={
        "ACTOR_BUS_SUB=tcp://127.0.0.1:55656","ACTOR_BUS_PUB=tcp://127.0.0.1:55657",
        "ACTOR_HEARTBEAT_MS=500","ACTOR_ID=hb","ACTOR_TOPIC=none","ACTOR_RESULT_TOPIC=ignored",
        "ACTOR_HANDLER=sh -c 'echo ignored; echo ok'","ACTOR_LMDB_PATH=/tmp/tm10",NULL};
    pid_t ap=sp(aa,ae); ms(1200); /* wait for at least 2 heartbeats */
    /* Subscribe to heartbeat and check we got at least one */
    nng_socket s; nng_sub0_open(&s); nng_dial(s, SP, NULL, 0);
    nng_socket_set(s, NNG_OPT_SUB_SUBSCRIBE, "heartbeat", 9);
    nng_socket_set_ms(s, "recv-timeout", 2000);
    nng_msg *m=NULL; int got=0;
    /* Reset m after every free: nng_recvmsg leaves it untouched on timeout,
       so a stale pointer here means the next iteration frees it twice. */
    for(int i=0;i<3;i++){
        m=NULL;
        if(nng_recvmsg(s,&m,0)==0){ got++; if(m){ nng_msg_free(m); m=NULL; } }
    }
    nng_close(s);
    CHECK(got>0,"no heartbeat received");
    if(got>0) printf("  received %d heartbeat(s)\n", got);
    kill(pp,SIGKILL); kill(ap,SIGKILL); waitpid(pp,NULL,0); waitpid(ap,NULL,0);
}

static void t11(void){ TEST("actor: an unset emitted_at is stamped on arrival");
    free_ports(); cleanup();
    char *a[]={"./bin/mesh-proxy",NULL},*e[]={"PROXY_SUB_BIND=tcp://127.0.0.1:55657","PROXY_PUB_BIND=tcp://127.0.0.1:55656",NULL};
    pid_t pp=sp(a,e); ms(600);
    system("mkdir -p /tmp/tm11");
    char *aa[]={"./bin/actor",NULL},*ae[]={
        "ACTOR_BUS_SUB=tcp://127.0.0.1:55656","ACTOR_BUS_PUB=tcp://127.0.0.1:55657",
        "ACTOR_HEARTBEAT_MS=0","ACTOR_ID=a11","ACTOR_TOPIC=st_in","ACTOR_RESULT_TOPIC=st_out",
        "ACTOR_HANDLER=sh -c 'echo st_out; echo ok'","ACTOR_LMDB_PATH=/tmp/tm11",NULL};
    pid_t ap=sp(aa,ae); ms(600);
    /* A ttl and no emitted_at: judged against 0, it would be long expired. */
    nng_socket s; nng_pub0_open(&s); nng_dial(s,PP,NULL,0); ms(50);
    uint8_t f[257]={0}; memcpy(f,"st_in",5); int64_t ttl=10000000000LL; memcpy(f+120,&ttl,8);
    uint32_t pl=1; memcpy(f+132,&pl,4); f[256]='x';
    nng_send(s,f,257,0); nng_close(s);
    CHECK(recvm("st_out",NULL,0,3000)>0,"unset emitted_at treated as expired");
    kill(pp,SIGKILL); kill(ap,SIGKILL); waitpid(pp,NULL,0); waitpid(ap,NULL,0);
}

int main(void){
    printf("Actor Mesh Test Suite\n\n");
    t1(); t2(); t3(); t4(); t5(); t6(); t7(); t8(); t9(); t10(); t11();
    printf("\n%s (%d/%d failures)\n",failures?"FAIL":"ALL PASSED",failures,11);
    cleanup(); return failures;
}
