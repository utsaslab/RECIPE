
#ifndef _BTREE_H
#define _BTREE_H

#include <stdint.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// The following are options that control the algorithmic / data layout properties
#define BTREE_NOPTR // Enable this to remove sibling pointer
#define BTREE_BINSEARCH // Enable this to perform binary search for node size > 8

#define SYSEXPECT(cond) do { if(!(cond)) { perror("ERROR: "); exit(1); } } while(0)

#define BTNODE_CAPACITY 16 // Number of elements per node; cannot be changed
#define BTNODE_MERGE_THRESHOLD (BTNODE_CAPACITY / 4)
#define BTNODE_KEY   0
#define BTNODE_VALUE 1
#define BTNODE_LEAF  0x1UL
#define BTNODE_INNER 0x2UL
#define BTNODE_ROOT  0x4UL

typedef int (*bt_cmp_t)(uint64_t, uint64_t);  // Comparator call back. Return negative if <, 0 if ==, positive if >

typedef struct btnode_t {
  uint64_t permute;    // Maps logical location to physical location in the node; 4 bits
  int16_t  size;       // Number of elements in the node
  uint16_t level;      // 0 means bottomost
  uint32_t property;   // Bit mask indicating the property of the node
#ifndef BTREE_NOPTR
  struct   btnode_t *next, *prev;
#endif
  uint64_t data[BTNODE_CAPACITY * 2];
} btnode_t;

typedef struct {
  btnode_t *root;
  bt_cmp_t cmp;
} btree_t;

inline uint64_t *btnode_at(btnode_t *node, int index, int isvalue) {
  assert(index < node->size); assert(node->size <= BTNODE_CAPACITY); assert(index >= 0);
  return &node->data[(((node->permute >> (index << 2)) & 0xFUL) << 1) + isvalue];
}

#ifdef BTREE_BINSEARCH
// First check middle and decide whether to use the middle
inline int benode_startindex(const btree_t *tree, btnode_t *node, uint64_t key, int otherwise) {
  int index = BTNODE_CAPACITY / 2;
  return (node->size > index && tree->cmp(key, *btnode_at(node, index, BTNODE_KEY)) >= 0) ? index : otherwise;
}
#else
inline int benode_startindex(const btree_t *tree, btnode_t *node, uint64_t key, int otherwise) {
  (void)tree; (void)node; (void)key; return otherwise;
}
#endif

// Virtualize next and prev pointer
#ifndef BTREE_NOPTR
inline void btnode_setnext(btnode_t *node, btnode_t *next) { node->next = next; }
inline btnode_t *btnode_getnext(btnode_t *node) { return node->next; }
inline void btnode_setprev(btnode_t *node, btnode_t *prev) { node->prev = prev; }
inline btnode_t *btnode_getprev(btnode_t *node) { return node->prev; }
#else
inline void btnode_setnext(btnode_t *node, btnode_t *prev) { (void)node; (void)prev; }
inline btnode_t *btnode_getnext(btnode_t *node) { (void)node; return NULL; }
inline void btnode_setprev(btnode_t *node, btnode_t *prev) { (void)node; (void)prev; }
inline btnode_t *btnode_getprev(btnode_t *node) { (void)node; return NULL; }
#endif

int bt_intcmp(uint64_t a, uint64_t b);
int bt_strcmp(uint64_t a, uint64_t b);
btnode_t *btnode_init(uint64_t property);
void btnode_free(btnode_t *node);
void btnode_freeall(btnode_t *node, int level);
void btnode_print(btnode_t *node);
btree_t *bt_init(bt_cmp_t cmp);
void bt_free(btree_t *tree);
int btnode_lb(const btree_t *tree, btnode_t *node, uint64_t key, int *exact);
int btnode_ub(const btree_t *tree, btnode_t *node, uint64_t key);
int btnode_insert(btree_t *tree, btnode_t *node, uint64_t key, uint64_t value);
void btnode_removeat(btnode_t *node, int index);
int btnode_remove(btree_t *tree, btnode_t *node, uint64_t key);
btnode_t *btnode_split(btnode_t *node);
btnode_t *btnode_merge(btnode_t *left, btnode_t *right);
btnode_t *btnode_smo(btree_t *tree, btnode_t *node, uint64_t key, btnode_t *parent, int parent_index);
btnode_t *bt_findleaf(btree_t *tree, uint64_t key);
int bt_insert(btree_t *tree, uint64_t key, uint64_t value);
int bt_remove(btree_t *tree, uint64_t key);
uint64_t bt_find(btree_t *tree, uint64_t key, int *success);
int bt_upsert(btree_t *tree, uint64_t key, uint64_t value);

#endif