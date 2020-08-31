#include "../include/raft.h"

#include <string.h>

#include "assert.h"
#include "byte.h"
#include "configuration.h"
#include "convert.h"
#include "election.h"
#include "err.h"
#include "heap.h"
#include "log.h"
#include "membership.h"
#include "tracing.h"

/* 1000 = 1 sec */
#define DEFAULT_ELECTION_TIMEOUT 20000 
#define DEFAULT_HEARTBEAT_TIMEOUT 5000
#define DEFAULT_SNAPSHOT_THRESHOLD 1024
#define DEFAULT_SNAPSHOT_TRAILING 2048

/* Set to 1 to enable tracing. */
#if 1
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

int raft_init(struct raft *r,
              struct raft_io *io,
              struct raft_fsm *fsm,
              const raft_id id,
              const char *address)
{
    int rv;
    assert(r != NULL);
    r->io = io;
    r->io->data = r;
    r->fsm = fsm;
    r->tracer = &NoopTracer;
    r->id = id;
    r->local_mc = readMC(id);
    printf("init: local mc is %lu", r->local_mc);
    /* Make a copy of the address */
    r->address = HeapMalloc(strlen(address) + 1);
    if (r->address == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    strcpy(r->address, address);
    r->current_term = 0;
    r->voted_for = 0;
    logInit(&r->log);
    raft_configuration_init(&r->configuration);
    r->configuration_index = 0;
    r->configuration_uncommitted_index = 0;
    r->election_timeout = DEFAULT_ELECTION_TIMEOUT;
    r->heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
    r->commit_index = 0;
    r->last_applied = 0;
    r->last_stored = 0;
    r->state = RAFT_UNAVAILABLE;
    r->transfer = NULL;
    r->snapshot.pending.term = 0;
    r->snapshot.threshold = DEFAULT_SNAPSHOT_THRESHOLD;
    r->snapshot.trailing = DEFAULT_SNAPSHOT_TRAILING;
    r->snapshot.put.data = NULL;
    r->close_cb = NULL;
    memset(r->errmsg, 0, sizeof r->errmsg);
    r->pre_vote = false;
    rv = r->io->init(r->io, r->id, r->address);
    if (rv != 0) {
        ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
        goto err_after_address_alloc;
    }
    return 0;

err_after_address_alloc:
    HeapFree(r->address);
err:
    assert(rv != 0);
    return rv;
}

static void ioCloseCb(struct raft_io *io)
{
    struct raft *r = io->data;
    raft_free(r->address);
    logClose(&r->log);
    raft_configuration_close(&r->configuration);
    if (r->close_cb != NULL) {
        r->close_cb(r);
    }
}

void raft_close(struct raft *r, void (*cb)(struct raft *r))
{
    assert(r->close_cb == NULL);
    if (r->state != RAFT_UNAVAILABLE) {
        convertToUnavailable(r);
    }
    r->close_cb = cb;
    r->io->close(r->io, ioCloseCb);
}

void raft_set_election_timeout(struct raft *r, const unsigned msecs)
{
    r->election_timeout = msecs;
}

void raft_set_heartbeat_timeout(struct raft *r, const unsigned msecs)
{
    r->heartbeat_timeout = msecs;
}

void raft_set_snapshot_threshold(struct raft *r, unsigned n)
{
    r->snapshot.threshold = n;
}

void raft_set_snapshot_trailing(struct raft *r, unsigned n)
{
    r->snapshot.trailing = n;
}

void raft_set_pre_vote(struct raft *r, bool enabled)
{
    r->pre_vote = enabled;
}

const char *raft_errmsg(struct raft *r)
{
    return r->errmsg;
}

int raft_bootstrap(struct raft *r, const struct raft_configuration *conf)
{
    int rv;

    if (r->state != RAFT_UNAVAILABLE) {
        return RAFT_BUSY;
    }

    rv = r->io->bootstrap(r->io, conf);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int raft_recover(struct raft *r, const struct raft_configuration *conf)
{
    int rv;

    if (r->state != RAFT_UNAVAILABLE) {
        return RAFT_BUSY;
    }

    rv = r->io->recover(r->io, conf);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

const char *raft_strerror(int errnum)
{
    return errCodeToString(errnum);
}

void raft_configuration_init(struct raft_configuration *c)
{
    configurationInit(c);
}

void raft_configuration_close(struct raft_configuration *c)
{
    configurationClose(c);
}

int raft_configuration_add(struct raft_configuration *c,
                           const raft_id id,
                           const char *address,
                           const int role)
{
    return configurationAdd(c, id, address, role);
}

int raft_configuration_encode(const struct raft_configuration *c,
                              struct raft_buffer *buf)
{
    return configurationEncode(c, buf);
}

unsigned long long raft_digest(const char *text, unsigned long long n)
{
    struct byteSha1 sha1;
    uint8_t value[20];
    uint64_t n64 = byteFlip64((uint64_t)n);
    uint64_t digest;

    byteSha1Init(&sha1);
    byteSha1Update(&sha1, (const uint8_t *)text, (uint32_t)strlen(text));
    byteSha1Update(&sha1, (const uint8_t *)&n64, (uint32_t)(sizeof n64));
    byteSha1Digest(&sha1, value);

    memcpy(&digest, value + (sizeof value - sizeof digest), sizeof digest);

    return byteFlip64(digest);
}

raft_mc inc_local_MC(struct raft* r) {
    r->local_mc = r->local_mc + 1;
    return r->local_mc;
}

raft_mc writeMC(raft_id r_id) {
    FILE *fptr;
    raft_mc cur_mc = readMC(r_id);

    char buf[20];
    snprintf(buf, sizeof buf, "./mc-xs%d", (int) r_id);

    fptr = fopen(buf,"w");
    fprintf(fptr,"%lu",cur_mc + 1);
    fclose(fptr);

    tracef("Write MC to (HARDCODED) %s: %d", buf, cur_mc + 1);

    return cur_mc + 1;
}

raft_mc readMC(raft_id r_id) {
    FILE *fptr;
    raft_mc num;
    char buf[20];
    snprintf(buf, sizeof buf, "./mc-xs%d", (int) r_id);

    fptr = fopen(buf,"r");
    fscanf(fptr,"%lu", &num);

    fclose(fptr); 

    return num;
}

raft_mc writeContent(raft_id r_id, char *str) {
    FILE *fptr;
    
    char buf[20];
    snprintf(buf, sizeof buf, "./cc-xs%d", (int) r_id);

    fptr = fopen(buf,"w");
    fprintf(fptr,"%s", str);
    fclose(fptr);

    tracef("Entry %s written to %s", str, buf);

    return writeMC(r_id);
}

void cleanStopPoint(raft_id r_id) {
    return writeStopPoint(r_id, 0, 0, 0, 0);
}

char* readContent(raft_id r_id) {
    FILE *fptr;
    char str[32];
    char buf[20];

    snprintf(buf, sizeof buf, "./cc-xs%d", (int) r_id);

    fptr = fopen(buf,"r");
    fscanf(fptr,"%s", str);

    tracef("Read content from (HARDCODED) %s: %s", buf, str);
    fclose(fptr); 
    return strdup(str);
}

raft_mc writeStopPoint(raft_id r_id, raft_mc prev_mc, 
                    raft_mc st_del_mc, raft_mc ed_del_mc,
                    raft_mc next_mc) {
    
    char buf[32];
    snprintf(buf, sizeof buf, "%lu|%lu-%lu|%lu", prev_mc, st_del_mc, ed_del_mc, next_mc);
    return writeContent(r_id, buf);
}

void readStopPoint(raft_id r_id, raft_mc prev_mc, 
                    raft_mc st_del_mc, raft_mc ed_del_mc,
                    raft_mc next_mc) {
    FILE *fptr;
    char buf[20];
    snprintf(buf, sizeof buf, "./cc-xs%d", (int) r_id);

    fptr = fopen(buf,"r");
    fscanf(fptr,"%lu|%lu-%lu|%lu", prev_mc, st_del_mc, ed_del_mc, next_mc);
    fclose(fptr); 
}