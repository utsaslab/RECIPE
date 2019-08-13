/*
 * Interface for the skip list data stucture.
 *
 * Author: Ian Dick, 2013
 *
 */
#ifndef SKIPLIST_H_
#define SKIPLIST_H_

#include "./atomic_ops/atomic_ops.h"

#include "common.h"
#include "ptst.h"
#include "garbagecoll.h"
#include <cstdint>
#include "../indexkey.h"

#define MAX_LEVELS 30

#define NUM_SIZES 1
#define NODE_SIZE 0

#define IDX(_i, _z) ((_z) + (_i)) % MAX_LEVELS
extern unsigned long sl_zero;

#ifndef USE_GENERIC_KEY
using sl_key_type = uint64_t;
#else 
using sl_key_type = GenericKey<31>;
#endif
// Always need a void * value type
using sl_value_type = void *;

/* bottom-level nodes */
typedef VOLATILE struct sl_node node_t;
struct sl_node {
        unsigned long   level;
        struct sl_node  *prev;
        struct sl_node  *next;
        sl_key_type     key;
        sl_value_type   val;
        struct sl_node  *succs[MAX_LEVELS];
        unsigned long   marker;
        unsigned long   raise_or_remove;
};

/* the skip list set */
typedef struct sl_set set_t;
struct sl_set {
        struct sl_node  *head;
};

node_t* node_new(sl_key_type key, sl_value_type val, node_t *prev, node_t *next,
                 unsigned int level, ptst_t *ptst);

node_t* marker_new(node_t *prev, node_t *next, ptst_t *ptst);

void node_delete(node_t *node, ptst_t *ptst);
void marker_delete(node_t *node, ptst_t *ptst);

set_t* set_new(int start);
void set_delete(set_t *set);
void set_print(set_t *set, int flag);
int set_size(set_t *set, int flag);

void set_subsystem_init(void);
void set_print_nodenums(set_t *set, int flag);

#endif /* SKIPLIST_H_ */
