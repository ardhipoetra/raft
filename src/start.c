#include "../include/raft.h"
#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "entry.h"
#include "err.h"
#include "log.h"
#include "recv.h"
#include "snapshot.h"
#include "tick.h"
#include "tracing.h"

/* Set to 1 to enable tracing. */
#if 1
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

/* Restore the most recent configuration. */
static int restoreMostRecentConfiguration(struct raft *r,
                                          struct raft_entry *entry,
                                          raft_index index)
{
    struct raft_configuration configuration;
    int rv;
    raft_configuration_init(&configuration);
    rv = configurationDecode(&entry->buf, &configuration);
    if (rv != 0) {
        raft_configuration_close(&configuration);
        return rv;
    }
    raft_configuration_close(&r->configuration);
    r->configuration = configuration;
    r->configuration_index = index;
    return 0;
}

/* Restore the entries that were loaded from persistent storage. The most recent
 * configuration entry will be restored as well, if any.
 *
 * Note that we don't care whether the most recent configuration entry was
 * actually committed or not. We don't allow more than one pending uncommitted
 * configuration change at a time, plus
 *
 *   when adding or removing just asingle server, it is safe to switch directly
 *   to the new configuration.
 *
 * and
 *
 *   The new configuration takes effect on each server as soon as it is added to
 *   that server's log: the C_new entry is replicated to the C_new servers, and
 *   a majority of the new configuration is used to determine the C_new entry's
 *   commitment. This means that servers do notwait for configuration entries to
 *   be committed, and each server always uses the latest configurationfound in
 *   its log.
 *
 * as explained in section 4.1.
 *
 * TODO: we should probably set configuration_uncomitted_index as well, since we
 * can't be sure a configuration change has been comitted and we need to be
 * ready to roll back to the last committed configuration.
 */
static int restoreEntries(struct raft *r,
                          raft_index snapshot_index,
                          raft_term snapshot_term,
                          raft_index start_index,
                          struct raft_entry *entries,
                          size_t n)
{
    struct raft_entry *conf = NULL;
    raft_index conf_index;
    size_t i;
    int rv;
    logStart(&r->log, snapshot_index, snapshot_term, start_index);
    r->last_stored = start_index - 1;
    int j = 0;
    for (i = 0; i < n; i++) {
struct raft_entry *entry = &entries[i];

char msg[100];
sprintf(msg, "%llu-%d-%llu-%llu", entry->term, entry->type, entry->mc, entry->prev_mc);

uint8_t* hmac = malloc(sizeof(uint8_t) * SHA1_DIGEST_SIZE);
hmac_sha1(HMAC_KEY, strlen(HMAC_KEY), msg, strlen(msg), hmac, SHA1_DIGEST_SIZE);

for (int xx = 0; xx < SHA1_DIGEST_SIZE; xx++) {
    if (hmac[xx] != entry->hash[xx]) {
        char hex[100], hex2[100];
        tracef("ERROR: HMAC is not match at %d: %d vs %d", xx, hmac[xx], entry->hash[xx]);
        for (xx = 0, j = 0; xx < SHA1_DIGEST_SIZE; ++xx, j += 2) {
            sprintf(hex + j, "%02x", entry->hash[xx] & 0xff);
            sprintf(hex2 + j, "%02x", hmac[xx] & 0xff);
        }
        tracef("ERROR: stored \t%s", hex);
        tracef("ERROR: computed \t%s", hex2);
        break;
    }
}
// if j != 0, then it means we got error
if (j != 0) {
    goto abort;
} else {
    char hex[100];
    for (int xx = 0, jj = 0; xx < SHA1_DIGEST_SIZE; ++xx, jj += 2) {
        sprintf(hex + jj, "%02x", entry->hash[xx] & 0xff);
    }
    tracef("check HMAC %s done, no problem", hex);
}

rv = logAppend(&r->log, entry->term, entry->type, &entry->buf,
                entry->batch, entry->mc, false);
        if (rv != 0) {
            goto err;
        }
        r->last_stored++;
        if (entry->type == RAFT_CHANGE) {
            conf = entry;
            conf_index = r->last_stored;
        }
    }
    if (conf != NULL) {
        rv = restoreMostRecentConfiguration(r, conf, conf_index);
        if (rv != 0) {
            goto err;
        }
    }
    raft_free(entries);
    return 0;

abort:
    return RAFT_SHUTDOWN;
err:
    if (logNumEntries(&r->log) > 0) {
        logDiscard(&r->log, r->log.offset + 1);
    }
    return rv;
}

/* If we're the only voting server in the configuration, automatically
 * self-elect ourselves and convert to leader without waiting for the election
 * timeout. */
static int maybeSelfElect(struct raft *r)
{
    const struct raft_server *server;
    int rv;
    server = configurationGet(&r->configuration, r->id);
    if (server == NULL || server->role != RAFT_VOTER ||
        configurationVoterCount(&r->configuration) > 1) {
        return 0;
    }
    /* Converting to candidate will notice that we're the only voter and
     * automatically convert to leader. */
    rv = convertToCandidate(r, false /* disrupt leader */);
    if (rv != 0) {
        return rv;
    }
    assert(r->state == RAFT_LEADER);
    return 0;
}

int raft_start(struct raft *r)
{
    struct raft_snapshot *snapshot;
    raft_index snapshot_index = 0;
    raft_term snapshot_term = 0;
    raft_index start_index;
    struct raft_entry *entries;
    size_t n_entries;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_UNAVAILABLE);
    assert(r->heartbeat_timeout != 0);
    assert(r->heartbeat_timeout < r->election_timeout);
    assert(logNumEntries(&r->log) == 0);
    assert(logSnapshotIndex(&r->log) == 0);
    assert(r->last_stored == 0);

    r->local_mc = readMC(r->id);
    tracef("starting - refresh local mc %lu", r->local_mc);
    rv = r->io->load(r->io, &r->current_term, &r->voted_for, &snapshot,
                     &start_index, &entries, &n_entries);
    if (rv != 0) {
        ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
        return rv;
    }
    assert(start_index >= 1);

    /* If we have a snapshot, let's restore it. */
    if (snapshot != NULL) {
        tracef("restore snapshot with last index %llu and last term %llu",
               snapshot->index, snapshot->term);
        rv = snapshotRestore(r, snapshot);
        if (rv != 0) {
            snapshotDestroy(snapshot);
            entryBatchesDestroy(entries, n_entries);
            return rv;
        }
        snapshot_index = snapshot->index;
        snapshot_term = snapshot->term;
        raft_free(snapshot);
    } else if (n_entries > 0) {
        /* If we don't have a snapshot and the on-disk log is not empty, then
         * the first entry must be a configuration entry. */
        assert(start_index == 1);
        assert(entries[0].type == RAFT_CHANGE);

        /* As a small optimization, bump the commit index to 1 since we require
         * the first entry to be the same on all servers. */
        r->commit_index = 1;
        r->last_applied = 1;
    }

    /* Append the entries to the log, possibly restoring the last
     * configuration. */
    tracef("restore %lu entries starting at %llu", n_entries, start_index);
    rv = restoreEntries(r, snapshot_index, snapshot_term, start_index, entries,
                        n_entries);
    if (rv != 0) {
        entryBatchesDestroy(entries, n_entries);
        return rv;
    }

    /* 
    * check log head and MC
    * 
    */
    struct raft_log* lg = &r->log;
    size_t lognum = logNumEntries(lg);
    if (lognum > 1) {
        raft_mc current_mc = readMC(r->id);
        raft_mc last_mc = lg->entries[lg->back-1].mc;
        tracef("MC on latest entry is %lu - emmc MC is %lu", last_mc, current_mc);
        if (last_mc != current_mc) {
            tracef("ERROR: MC is not equal. Rollback suspected!");
            
            raft_mc prev_mc = 0; raft_mc first_delmc = 0; 
            raft_mc sto_mc = 0;  raft_mc end_delmc = 0;
            readStopPoint(r->id, &prev_mc, &first_delmc, &end_delmc, &sto_mc);

            if(current_mc != sto_mc) {
                tracef("stored MC is not equal to current MC %lu vs %lu. SHUTDOWN", sto_mc, current_mc);
                return RAFT_SHUTDOWN;
            }

            if (last_mc < prev_mc) {
                tracef("last MC is falling behind of what we stored. Rollback detected? %lu vs %lu. SHUTDOWN", last_mc, prev_mc);
                return RAFT_SHUTDOWN;
            }

            if (last_mc >= first_delmc && last_mc <= end_delmc) {
                tracef("WARN: truncation on previous session was not finished yet");
                //TODO: can join network but do not become a leader/candidate
            }

            tracef("False alarm, everything is okay! Possible cause was truncation. Setting last act to TRUNCATE");
            lg->last_act = RAFT_LOG_TRUNCATE;
        }
    }
    
    
    /* Start the I/O backend. The tickCb function is expected to fire every
     * r->heartbeat_timeout milliseconds and recvCb whenever an RPC is
     * received. */
    rv = r->io->start(r->io, r->heartbeat_timeout, tickCb, recvCb);
    if (rv != 0) {
        return rv;
    }

    /* By default we start as followers. */
    convertToFollower(r);

    /* If there's only one voting server, and that is us, it's safe to convert
     * to leader right away. If that is not us, we're either joining the cluster
     * or we're simply configured as non-voter, and we'll stay follower. */
    rv = maybeSelfElect(r);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

#undef tracef
