/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Thread management for replication.
 */
#include "memcached.h"
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>

#include <sys/syscall.h>
#include <sys/types.h>


#define RQ_ITEMS_PER_ALLOC 256

static conn *rep_server_conn = NULL;
static conn *rep_client_conn = NULL;

static pthread_mutex_t rep_lock;
static pthread_cond_t rep_cond;

static LIBEVENT_THREAD rep_thread;

static void rep_libevent_process(int fd, short which, void *arg);
static void rqi_free(Q_ITEM *item);

/* Free list of Q_ITEM structs */
static Q_ITEM *rqi_freelist;
static pthread_mutex_t rqi_freelist_lock;

typedef struct {
    Q_ITEM *head;
    Q_ITEM *tail;
    uint64_t recycle;
    uint32_t length;
    pthread_mutex_t lock;
} RQ;

static RQ rep_queue;

/*
 * Initializes a replication queue.
 */
static void rq_init() 
{
    pthread_mutex_init(&rep_queue.lock, NULL);
    rep_queue.head = NULL;
    rep_queue.tail = NULL;
    rep_queue.length = 0;
    rep_queue.recycle = 0;
}

/*
 * Looks for an item on a replication queue
 * Returns the item, or NULL if no item is available
 */
static Q_ITEM *rq_pop() 
{
    Q_ITEM *item;

    pthread_mutex_lock(&rep_queue.lock);
    item = rep_queue.head;
    if (NULL != item) {
        rep_queue.head = item->next;
        if (NULL == rep_queue.head)
            rep_queue.tail = NULL;

        if (item->item != NULL) {
            item->item->it_flags &= ~ITEM_REPQUEUED;
        }
        rep_queue.length--;
    }
    pthread_mutex_unlock(&rep_queue.lock);

    return item;
}

/*
 * Flushes the replication queue
 */
static void rq_flush(bool all) 
{
    Q_ITEM *list, *item;

    pthread_mutex_lock(&rep_queue.lock);
    list = rep_queue.head;
    rep_queue.tail = rep_queue.head = NULL;
    rep_queue.length = 0;
    pthread_mutex_unlock(&rep_queue.lock);

    while (list != NULL) {
        item = list;
        list = item->next;

        if (all || 
            item->type == REPLICATION_SYNC ||
            item->type == REPLICATION_SYNC0 ||
            item->type == REPLICATION_SYNC1 || item->item != NULL) {
            if (item->item != NULL) {
                item->item->it_flags &= ~ITEM_REPQUEUED;
                item_remove(item->item);
            }

            rqi_free(item);

        } else {
            pthread_mutex_lock(&rep_queue.lock);
            if (NULL == rep_queue.tail)
                rep_queue.head = item;
            else
                rep_queue.tail->next = item;

            rep_queue.tail = item;
            rep_queue.length++;
            pthread_mutex_unlock(&rep_queue.lock);
        }
    } 
}

/*
 * Inserts an item back at the replication queue head
 */
static void rq_pushback(Q_ITEM *it) 
{
    pthread_mutex_lock(&rep_queue.lock);
    
    if (it->item == NULL ||
        !(it->item->it_flags & ITEM_REPQUEUED)) {

        if (it->item) {
            it->item->it_flags |= ITEM_REPQUEUED;
        }

        it->next = rep_queue.head;
        rep_queue.head = it;

        if (NULL == rep_queue.tail) {
            rep_queue.tail = it;
        }
        rep_queue.length++;
    }

    pthread_mutex_unlock(&rep_queue.lock);
}

/*
 * Adds an item to a replication queue.
 */
static void rq_push(Q_ITEM *it) 
{
    it->next = NULL;

    pthread_mutex_lock(&rep_queue.lock);

    if (it->item == NULL ||
        !(it->item->it_flags & ITEM_REPQUEUED)) {
        
        if (rep_queue.length >= settings.rep_qmax) {
            rep_queue.recycle++;
            rep_queue.length--;

            it->next = rep_queue.head->next;
            rqi_free(rep_queue.head);
            rep_queue.head = it->next;
            it->next = NULL;
        }

        if (it->item) {
            it->item->it_flags |= ITEM_REPQUEUED;
            it->item->refcount++;
        }

        if (NULL == rep_queue.tail)
            rep_queue.head = it;
        else
            rep_queue.tail->next = it;

        rep_queue.tail = it;

        rep_queue.length++;
    }

    pthread_mutex_unlock(&rep_queue.lock);
}

/*
 * Allocates a replication queue item.
 */
static Q_ITEM *rqi_alloc(void) 
{
    Q_ITEM *item = NULL;
    pthread_mutex_lock(&rqi_freelist_lock);
    if (rqi_freelist) {
        item = rqi_freelist;
        rqi_freelist = item->next;
    }
    pthread_mutex_unlock(&rqi_freelist_lock);

    if (NULL == item) {
        int i;

        /* Allocate a bunch of items at once to reduce fragmentation */
        item = calloc(sizeof(Q_ITEM), RQ_ITEMS_PER_ALLOC);
        if (NULL == item)
            return NULL;

        /*
         * Link together all the new items except the first one
         * (which we'll return to the caller) for placement on
         * the freelist.
         */
        for (i = 2; i < RQ_ITEMS_PER_ALLOC; i++)
            item[i - 1].next = &item[i];

        pthread_mutex_lock(&rqi_freelist_lock);
        item[RQ_ITEMS_PER_ALLOC - 1].next = rqi_freelist;
        rqi_freelist = &item[1];
        pthread_mutex_unlock(&rqi_freelist_lock);
    }

    item->item = NULL;
    item->next = NULL;
    return item;
}

/*
 * Frees a replication queue item (adds it to the freelist.)
 */
static void rqi_free(Q_ITEM *item) 
{
    pthread_mutex_lock(&rqi_freelist_lock);
    item->next = rqi_freelist;
    rqi_freelist = item;
    pthread_mutex_unlock(&rqi_freelist_lock);
}

/*
 * Creates and returns a new replication queue item
 * from the given type and command 
 */
static Q_ITEM *rqi_new(enum CMD_TYPE type, R_CMD *cmd)
{
    char       *key    = NULL;
    uint32_t    keylen = 0;
    rel_time_t  time   = 0;
    Q_ITEM     *q      = rqi_alloc();

    if(NULL == q) {
        if (settings.verbose > 2) {
            fprintf(stderr,"rqi_new: out of memory\n");
            return(NULL);
        }
    }

    q->type = type;
    q->time = current_time;
    q->next = NULL;

    switch (type) {
    case REPLICATION_REP:
        q->item = cmd->item;
        break;

    case REPLICATION_DEL:
        key    = cmd->key;
        keylen = cmd->keylen;
        break;

    case REPLICATION_FLUSH_ALL:
        break;

    case REPLICATION_DEFER_FLUSH_ALL:
        time   = cmd->time;
        break;

    case REPLICATION_SYNC:
    case REPLICATION_SYNC0:
    case REPLICATION_SYNC1:
        break;

    default:
        fprintf(stderr,"rqi_new: got unknown command: %d\n", type);
        rqi_free(q);
        return(NULL);
    }

    // jsh: TODO - get memory for key from a cache
    if (keylen) {
        if (q->keylen < keylen) {
            if (q->key) free(q->key);
            q->key = malloc(keylen + 1);
            if (q->key == NULL) {
                q->keylen = 0;
                rqi_free(q);
                q = NULL;
            } else {
                q->keylen = keylen;
            }
        }

        if (q != NULL) {
            memcpy(q->key, key, keylen);
            *(q->key + keylen) = 0;
        } 
    } else if (q->key != NULL) {
        *(q->key) = 0;
    }

    return(q);
}

/*
 * Initialize replication thread and data structures.
 */
static void setup_rep_thread()
{
    int fds[2];

    rep_thread.base = event_init();
    if (!rep_thread.base) {
        fprintf(stderr, "Can't allocate event base\n");
        exit(1);
    }

    if (pipe(fds)) {
        perror("Can't create notify pipe");
        exit(1);
    }

    rep_thread.notify_receive_fd = fds[0];
    rep_thread.notify_send_fd = fds[1];

    /* Listen for notifications from other threads */
    event_set(&rep_thread.notify_event, rep_thread.notify_receive_fd,
              EV_READ | EV_PERSIST, rep_libevent_process, &rep_thread);
    event_base_set(rep_thread.base, &rep_thread.notify_event);

    if (event_add(&rep_thread.notify_event, 0) == -1) {
        fprintf(stderr, "Can't monitor libevent notify pipe\n");
        exit(1);
    }

    if (pthread_mutex_init(&rep_thread.stats.mutex, NULL) != 0) {
        perror("Failed to initialize mutex");
        exit(EXIT_FAILURE);
    }

    rep_thread.suffix_cache = cache_create("suffix", REP_SUFFIX_SIZE, 
                                           sizeof(char*), NULL, NULL);
    if (rep_thread.suffix_cache == NULL) {
        fprintf(stderr, "Failed to create suffix cache\n");
        exit(EXIT_FAILURE);
    }
}


/*
 * Replication thread: main event loop
 */
static void *rep_thread_main(void *arg) 
{
    LIBEVENT_THREAD *me = arg;

    /* Any per-thread setup can happen here
     */
    rep_thread.thread_id = pthread_self();

    pthread_mutex_lock(&rep_lock);
    pthread_cond_signal(&rep_cond);
    pthread_mutex_unlock(&rep_lock);

    event_base_loop(me->base, 0);
    return NULL;
}

/*
 * Processes a replication item. This is called when
 * input arrives on the libevent wakeup pipe.
 */
static void rep_libevent_process(int fd, short which, void *arg) 
{
    assert(arg == &rep_thread);

    char buf[1];
    if (read(fd, buf, 1) != 1)
        if (settings.verbose > 0)
            fprintf(stderr, "Can't read from libevent pipe\n");

    if (rep_server_conn != NULL && rep_server_conn->state == conn_rep_send) {
        replication_trigger_send(rep_server_conn);
    }
}

/*
 * Processes the replication queue. 
 */
int rep_process_queue(conn *conn) 
{
    assert(conn == rep_server_conn);
    assert(conn != NULL);
    assert(is_replication_thread());

    Q_ITEM *qi;
    int ret, numitems = 0;

    qi = rq_pop();
    if (qi == NULL) {
        if (settings.verbose > 2)
            fprintf(stderr, "replication_event: no more items\n");
        return 0;
    } 

    ret = process_replication(qi, conn);

    if (settings.verbose > 2) {
        fprintf(stderr, "replication:  process_replication returned %d\n", ret);
    }

    if (ret <= -2) {
        rq_pushback(qi);
        return ret;

    } else if (ret <= -1) {
        rq_pushback(qi);
        return ret;

    } else {
        numitems++;
    }

    rqi_free(qi);

    return numitems;
}

/* 
 * Queues a replication command
 */
int queue_rep_command(enum CMD_TYPE type, R_CMD *cmd)
{
    if (rep_server_conn == NULL && 
        (type == REPLICATION_REP || type == REPLICATION_SYNC)) return 0;

    Q_ITEM *qi = rqi_new(type, cmd);

    // assert we have cache lock
    assert( !(qi != NULL && qi->item != NULL) || 
            (cache_lock.__data.__lock > 0 && 
             cache_lock.__data.__owner == syscall(SYS_gettid)));

    if (qi != NULL) {
        if (type == REPLICATION_SYNC0 || type == REPLICATION_SYNC1) {
            rq_pushback(qi);
        } else {
            rq_push(qi);
        }

        if (!is_replication_thread()) {
            if (write(rep_thread.notify_send_fd, "", 1) != 1) {
                perror("replication: writing to thread notify pipe");
            }
        }

    } else {
        fprintf(stderr, "replication: can't create Q_ITEM\n");
        return (-1);
    }

    return (0);
}

/*
 * Close a replication conneciton
 */
int replication_close(conn *c)
{
    if (c == rep_server_conn) {
        pthread_mutex_lock(&rep_lock);
        rep_server_conn = NULL;
        pthread_mutex_unlock(&rep_lock);

        if (settings.verbose)
            fprintf(stderr, "replication: server connection closed\n");

        rq_flush(false);

        replication_server_cleanup();
        replication_accept(true);

    } else if (c == rep_client_conn) {
        pthread_mutex_lock(&rep_lock);
        rep_client_conn = NULL;
        pthread_mutex_unlock(&rep_lock);

        if (settings.verbose)
            fprintf(stderr, "replication: connection to master closed\n");

        replication_client_cleanup();
        replication_conn_retry();
    }

    return(0);
}

/*
 * Handle a new replication server conneciton
 */
void replication_conn_server(int fd)
{
    pthread_mutex_lock(&rep_lock);

    if(rep_server_conn) {
        close(fd);
        if (settings.verbose)
            fprintf(stderr, "replication: ignoring duplicate connection\n");
    } else {
        // jsh:TODO move below stuff to the replication thread
        replication_accept(false);

        struct timeval t = {
            .tv_sec = REPLICATION_INIT_TIMEOUT, 
            .tv_usec = 0 };

        rep_server_conn = conn_new_timeout(fd, conn_rep_start, 
            EV_READ | EV_PERSIST, 
            DATA_BUFFER_SIZE, 
            tcp_transport, rep_thread.base, &t);

        rep_server_conn->thread = &rep_thread;
        fprintf(stderr, "replication: new connection\n");

        // hack to get timeout working:
        // trigger an event on the notify pipe so that the replication 
        // thread restarts the libevent loop with the timeout
        if (write(rep_thread.notify_send_fd, "", 1) != 1) {
            perror("replication: failed to trigger rep thread");
            close(fd);
            replication_accept(false);
        }

       // replication_marugoto(1);
       // replication_marugoto(0);
    }

    pthread_mutex_unlock(&rep_lock);
}

/*
 * Handle a new replication client conneciton
 */
void replication_conn_client(int fd, bool inprogress)
{
    pthread_mutex_lock(&rep_lock);

    if(rep_client_conn) {
        close(fd);
        if (settings.verbose)
            fprintf(stderr, "replication: ignoring duplicate client connection\n");
    } else {
        // jsh:TODO move below stuff to the replication thread
        struct timeval t = {
            .tv_sec = REPLICATION_CONNECTION_TIMEOUT, 
            .tv_usec = 0 };

        rep_client_conn = conn_new_timeout(fd, 
            conn_repconnect, 
            EV_WRITE, 
            DATA_BUFFER_SIZE, 
            tcp_transport, 
            rep_thread.base, inprogress? &t: NULL);

        rep_client_conn->thread = &rep_thread;

        // hack to get timeout working:
        // trigger an event on the notify pipe so that the replication 
        // thread restarts the libevent loop with the timeout
        if (write(rep_thread.notify_send_fd, "", 1) != 1) {
            perror("replication: failed to trigger rep thread");
            close(fd);
        }
    }

    pthread_mutex_unlock(&rep_lock);
}

/*
 * Returns true if this is the replication
 */
int is_replication_thread() 
{
    return pthread_self() == rep_thread.thread_id;
}

/*
 * Returns the replication thread
 */
LIBEVENT_THREAD *get_replication_thread()
{
    return &rep_thread;
}

/*
 * Checks whether the given connection is the replication server connection
 */
bool is_rep_server_conn(conn *c)
{
    assert(c != NULL);
    return (c == rep_server_conn);
}

/*
 * Checks whether the given connection is the replication connection
 */
bool is_rep_conn(conn *c)
{
    assert(c != NULL);
    return (c == rep_server_conn || c == rep_client_conn);
}

/*
 * Initializes the replication thread
 */
void replication_thread_init() 
{
    pthread_t       thread;
    pthread_attr_t  attr;
    int             ret;

    rep_server_conn = NULL;

    pthread_mutex_init(&rep_lock, NULL);
    pthread_cond_init(&rep_cond, NULL);

    rq_init();

    pthread_mutex_init(&rqi_freelist_lock, NULL);
    rqi_freelist = NULL;

    setup_rep_thread();

    pthread_mutex_lock(&rep_lock);

    pthread_attr_init(&attr);
    ret = pthread_create(&thread, &attr, rep_thread_main, &rep_thread);
    if (ret != 0) {
        fprintf(stderr, "Can't create replication thread: %s\n",
                strerror(ret));
        exit(1);
    }

    pthread_cond_wait(&rep_cond, &rep_lock);
    pthread_mutex_unlock(&rep_lock);
}

void repl_queue_stats(ADD_STAT add_stats, void *c)
{
    const char *fmt = "%s:%s";
    unsigned int old = 0, young = 0;
    char key_str[STAT_KEY_LEN];
    char val_str[STAT_VAL_LEN];
    int klen = 0, vlen = 0;

    pthread_mutex_lock(&rep_queue.lock);
    Q_ITEM *qi = rep_queue.head;
    while (qi != NULL && qi->type == REPLICATION_DEFER_FLUSH_ALL) {
        qi = qi->next;
    }
    if (qi != NULL) old = current_time - qi->time;

    qi = rep_queue.tail;
    if (qi != NULL && qi->type != REPLICATION_DEFER_FLUSH_ALL) {
        young = current_time - rep_queue.tail->time;
    }
    pthread_mutex_unlock(&rep_queue.lock);

    APPEND_NUM_FMT_STAT(fmt, "replication", "length", "%u", rep_queue.length);
    APPEND_NUM_FMT_STAT(fmt, "replication", "entry_max", "%u", settings.rep_qmax);
    APPEND_NUM_FMT_STAT(fmt, "replication", "young", "%u", young);
    APPEND_NUM_FMT_STAT(fmt, "replication", "old", "%u", old);
    APPEND_NUM_FMT_STAT(fmt, "replication", "recycled", "%llu", ((unsigned long long)rep_queue.recycle));

    add_stats(NULL, 0, NULL, 0, c);
}

// jsh: TODO
// link dirty items together always
