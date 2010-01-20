#ifndef MEMCACHED_REPLICATION_H
#define MEMCACHED_REPLICATION_H
#include <netdb.h>

#define REP_SUFFIX_SIZE 46

#define REP_VERSION "0.1"

#define REP_VERSION_TOKEN 1
#define REP_INDEX_TOKEN1 2
#define REP_INDEX_TOKEN2 3

#define REPLICATION_INIT_TIMEOUT 15
#define REPLICATION_CONNECTION_TIMEOUT 15
#define REPLICATION_CONNECT_RETRY_INTERVAL 15

enum CMD_TYPE {
  REPLICATION_REP,
  REPLICATION_DEL,
  REPLICATION_FLUSH_ALL,
  REPLICATION_DEFER_FLUSH_ALL,
  REPLICATION_SYNC0,
  REPLICATION_SYNC1,
  REPLICATION_SYNC,
};

typedef struct queue_item_t Q_ITEM;
struct queue_item_t {
  enum CMD_TYPE  type;
  char          *key;
  int            keylen;
  item          *item;
  rel_time_t     time;
  Q_ITEM        *next;
};

typedef struct replication_cmd_t R_CMD;
struct replication_cmd_t {
  char       *key;
  int         keylen;
  item          *item;
  rel_time_t  time;
};


void repl_queue_stats(ADD_STAT add_stats, void *c);

/* Thread functions */
void replication_thread_init(void);
LIBEVENT_THREAD *get_replication_thread(void);
int is_replication_thread(void);
int queue_rep_command(enum CMD_TYPE type, R_CMD *cmd);
int rep_process_queue(conn *conn);
int replication_close(conn *c);

bool replication_init_transfer(uint64_t start_index, uint64_t have_index, bool isfresh);

bool is_rep_conn(conn *);
bool is_rep_server_conn(conn *c);

int     rqi_free_list(void);

int     replication_cmd(conn *, Q_ITEM *);

int replication_rep(conn *c, item *it);
int replication_del(conn *c, char *k, int keylen);
int replication_flush_all(conn *c, rel_time_t exp);
int replication_sync(conn *c, int phase);

int replication_init(void);
void replication_accept(bool accept);
void replication_conn_server(int fd);
void replication_conn_client(int fd, bool inprogress);

int replication_call_repitem(item *it);

int replication_call_del(char *key, size_t keylen);
int replication_call_flush_all(void);
int replication_call_defer_flush_all(const rel_time_t time);

int replication_call_sync(void);
int replication_call_sync_start(void);
int replication_call_sync_continue(void);

int  replication_server_init(void);
int  replication_client_init(void);
void replication_conn_retry(void);

void replication_client_connect(conn *c);

int process_replication(Q_ITEM *qi, conn *conn);

bool replication_client_start(conn *c);

uint64_t replication_get_recv_id(void);
uint64_t replication_get_sent_id(void);
void replication_set_recv_id(uint64_t id);
void replication_set_sent_id(uint64_t id);
void replication_set_sync(int phase);

void replication_trigger_send(conn *c);
bool replication_is_reconnect(void);

void replication_client_cleanup(void);
void replication_server_cleanup(void);
#endif
