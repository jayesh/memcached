/* associative array */
void assoc_init(void);
item *assoc_find(const char *key, const size_t nkey);
int assoc_insert(item *item);
void assoc_delete(const char *key, const size_t nkey);
void do_assoc_move_next_bucket(void);
int start_assoc_maintenance_thread(void);
void stop_assoc_maintenance_thread(void);
char *assoc_key_snap(int *n);

typedef bool (*HASH_ITER_FN) (item *it, void *data);
bool assoc_hash_iterate(HASH_ITER_FN iter_fn, void *data);
