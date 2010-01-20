/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *
 */
#include "memcached.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <assert.h>

#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <assert.h>
#include <limits.h>
#include <stddef.h>

static conn *rep_serv = NULL;

static uint64_t replication_recv_id = 0;
static uint64_t replication_sent_id = 0;

static bool replication_in_sync = false;
static bool replication_cache_cleaned = false;

static bool replication_sync_once = false;
static bool replication_server_reconnect = false;

static struct event conn_event;

struct rep_index {
    uint64_t index1;
    uint64_t index2;
    bool is_new;
};

static void retry_handler(const int fd, const short which, void *arg) 
{
    evtimer_del(&conn_event);
    replication_client_init();
}

uint64_t replication_get_recv_id() 
{
    return replication_recv_id;
}

void replication_set_recv_id(uint64_t id)
{
    if (replication_in_sync) replication_recv_id = id;
}

uint64_t replication_get_sent_id() 
{
    return replication_sent_id;
}

/*
 * Callback for hash iteration. 
 */
static bool item_rep_clean(item *it, void *data)
{
    it->it_flags &= ~ITEM_REPDATA;
    return true;
}

void replication_set_sync(int phase)
{
    if (settings.verbose) {
        fprintf(stderr, "rep_sync %d\n", phase);
    }

    if (phase == 0) {
        // We get sync0 when the peer is restarted.
        if (!replication_cache_cleaned) {
            assoc_hash_iterate(item_rep_clean, NULL);
            replication_cache_cleaned = true;
        }

    } else if (phase == 1) {
        // We get sync1 if this is a reconnect
        // no need to clear REPDATA flag
        replication_cache_cleaned = true;

    } else {
        replication_in_sync = true;
        replication_sync_once = true;
    }
}

bool replication_is_reconnect()
{
    return replication_sync_once;
}

/*
 * Initialize replication
 */
int replication_init()
{
    if (settings.verbose) {
        fprintf(stderr, "replication: initializing\n");
    }

    replication_thread_init();

    bool start_listening = true;

    if (settings.rep_addr.s_addr != htonl(INADDR_ANY)) {
        if (replication_client_init() == 0) {
            start_listening = false;
        }
    }

    if (!settings.rep_listen || replication_server_init() == 0) {
        if (start_listening) create_listening_sockets();
        // jsh: TODO remove the call below
        // and do it when we get rep_sync 
        else
            create_listening_sockets();
        return 0;
    }

    return -1;
}

/*
 * Stop/start accepting replication connection
 */
void replication_accept(const bool do_accept) 
{
    conn *next;

    for (next = rep_serv; next; next = next->next) {
        if (do_accept) {
            update_event(next, EV_READ | EV_PERSIST);
            if (listen(next->sfd, settings.backlog) != 0) {
                perror("listen");
            }
        } else {
            update_event(next, 0);
            if (listen(next->sfd, 0) != 0) {
                perror("listen");
            }
        }
    }
}

/*
 * Replication server init - create listening socket
 */
int replication_server_init()
{   
    if (settings.verbose > 0)
        fprintf(stderr, "Creating listening sockets\n");

    if(server_socket(settings.rep_port, tcp_transport, 
                     NULL, &rep_serv, conn_rep_listen)) {
        fprintf(stderr, "replication: failed to initialize replication server socket\n");
        return(-1);
    }

    return(0);
}

/*
 * Initialize replication client 
 */
bool replication_client_start(conn *c)
{

    replication_in_sync = false;

    return true;
}

void replication_server_cleanup()
{
    replication_cache_cleaned = false;
}

void replication_client_cleanup()
{
    replication_in_sync = false;
    replication_cache_cleaned = false;
}

/*
 * Callback for hash iteration. 
 */
static bool item_init_replication(item *it, void *data)
{
    struct rep_index *ri = (struct rep_index*) data;

    uint64_t cas = ITEM_get_cas(it);

    if (!replication_cache_cleaned && ri->is_new) {
        it->it_flags &= ~ITEM_REPDATA;
    }

    // If expired or flushed, skip
    if ((settings.oldest_live != 0 && settings.oldest_live <= current_time &&
        it->time <= settings.oldest_live) || 
        (it->exptime != 0 && it->exptime <= current_time)) {
        return true;
    }

    if (settings.verbose > 2) {
        fprintf(stderr, "key: %s, cas: %llu, flags: %x, st: %llu, hv: %llu\n",
            ITEM_key(it), 
            (unsigned long long) cas, 
            it->it_flags,
            (unsigned long long) ri->index1,
            (unsigned long long) ri->index2);
    }

    if ((it->it_flags & ITEM_REPDIRTY) ||
        ((it->it_flags & ITEM_REPDATA) && (cas > ri->index2)) ||
        ((it->it_flags & ITEM_REPDATA) == 0 && (cas >= ri->index1))) {

        it->it_flags |= ITEM_REPDIRTY;

        return (replication_call_repitem(it) == 0);
    }

    return true;
}

/*
 * Start replication - transfer objects with cas > start_index
 */
bool replication_init_transfer(uint64_t start_index, uint64_t have_index, bool is_fresh)
{
    if (settings.verbose > 2)
        fprintf(stderr, 
            "replication: starting initial queuing %llu, %llu\n",
            (unsigned long long) start_index,
            (unsigned long long) have_index);
    
    struct rep_index i = { .index1 = start_index, 
        .index2 = have_index, .is_new = is_fresh};

    replication_sent_id = 0;

    replication_cache_cleaned = replication_cache_cleaned || !is_fresh;

    if (replication_server_reconnect) {
        replication_call_sync_continue();
    } else {
        replication_call_sync_start();
    }

    if (!assoc_hash_iterate(item_init_replication, &i)) {
        if (settings.verbose > 1)
            fprintf(stderr, "replication: initial queuing failed\n");

        return false;
    }

    replication_cache_cleaned = true;
    replication_server_reconnect = true;

    if (settings.verbose > 2)
        fprintf(stderr, "replication: initial queuing done\n");

    return true;
}

static int network_connect(int *status)
{
    int s;
    struct sockaddr_in server;
    int flags;

    if ((s = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        fprintf(stderr, "replication: failed to replication client socket\n");
        return(-1);
    }

    if ((flags = fcntl(s, F_GETFL, 0)) < 0 ||
        fcntl(s, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("setting O_NONBLOCK");
        close(s);
        return -1;
    }

    /* connect */
    memset((char *)&server, 0, sizeof(server));
    server.sin_family = AF_INET;
    server.sin_addr   = settings.rep_addr;
    server.sin_port   = htons(settings.rep_port);

    if (settings.verbose > 0) {
        fprintf(stderr, "replication: connect (peer=%s:%d)\n", 
                        inet_ntoa(settings.rep_addr), 
                        settings.rep_port);
    }

    *status = -1;
    if (connect(s, (struct sockaddr *)&server, sizeof(server)) < 0) {
        if (errno == EINPROGRESS) {
            *status = 1;
        } else if (errno  == EISCONN) {
            *status = 0;
        }
    } else {
       *status = 0;
    }

    return s;
}

void replication_conn_retry()
{
    struct timeval t = {.tv_sec = REPLICATION_CONNECT_RETRY_INTERVAL, 
            .tv_usec = 0 };

    if (settings.verbose > 1) {
        fprintf(stderr, "replication: starting timer: %d secs\n",
                        REPLICATION_CONNECT_RETRY_INTERVAL);
    }


    evtimer_set(&conn_event, retry_handler, 0);
    event_base_set(get_replication_thread()->base, &conn_event);
    evtimer_add(&conn_event, &t);
}

/*
 * Replication client init - connect to master
 */
int replication_client_init()
{
    if (settings.verbose) {
        fprintf(stderr, "replication: starting client_init\n");
    }

    int status = -1;
    int fd = network_connect(&status);

    if (settings.verbose> 2) {
        fprintf(stderr, "replication: connection to master: %s\n",
                fd < 0? "FAILED": (status == 0? "SUCCESS": "CONNECTING"));
    }

    if (fd >= 0 && status >= 0) {
        replication_conn_client(fd, (status > 0));
    } else {
        replication_conn_retry();
        return (-1);
    }

    return(0);
}

int replication_call_repitem(item *it)
{
    R_CMD r;
    r.key = NULL;
    r.item = it;

    if (replication_sent_id < ITEM_get_cas(it)) {
        replication_sent_id = ITEM_get_cas(it);
    }

    return (queue_rep_command(REPLICATION_REP, &r));
}

int replication_call_del(char *key, size_t keylen)
{
    R_CMD r;
    r.key    = key;
    r.keylen = keylen;
    return(queue_rep_command(REPLICATION_DEL, &r));
}

int replication_call_flush_all()
{
    R_CMD r;
    r.key = NULL;
    return(queue_rep_command(REPLICATION_FLUSH_ALL, &r));
}

int replication_call_defer_flush_all(const rel_time_t time)
{
    R_CMD r;
    r.key  = NULL;
    r.time = time;
    return(queue_rep_command(REPLICATION_DEFER_FLUSH_ALL, &r));
}

int replication_call_sync_start()
{
    R_CMD r;
    r.key = NULL;
    return(queue_rep_command(REPLICATION_SYNC0, &r));
}

int replication_call_sync()
{
    R_CMD r;
    r.key = NULL;
    return(queue_rep_command(REPLICATION_SYNC, &r));
}

int replication_call_sync_continue()
{
    R_CMD r;
    r.key = NULL;
    return(queue_rep_command(REPLICATION_SYNC1, &r));
}

/*
 * Called on the replication thread on libevent notification
 */
int process_replication(Q_ITEM *q, conn *c)
{
    assert(q != NULL);
    assert(c != NULL);

    switch (q->type) {
        case REPLICATION_REP:
            return(replication_rep(c, q->item));

        case REPLICATION_DEL:
            return(replication_del(c, q->key, q->keylen));

        case REPLICATION_FLUSH_ALL:
            return(replication_flush_all(c, 0));

        case REPLICATION_DEFER_FLUSH_ALL:
            return(replication_flush_all(c, q->time));

        case REPLICATION_SYNC0:
            return(replication_sync(c, 0));

        case REPLICATION_SYNC1:
            return(replication_sync(c, 1));

        case REPLICATION_SYNC:
            return(replication_sync(c, 2));

        default:
            fprintf(stderr, 
                "replication: got unknown command: %d\n", q->type);
    }

    return(0);
}
