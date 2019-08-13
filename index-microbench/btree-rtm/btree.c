
#include "btree.h"

// Note: Do not use (a - b) because it will be converted to int and lose precision
int bt_intcmp(uint64_t a, uint64_t b) { return a < b ? -1 : (a == b ? 0 : 1); }
int bt_strcmp(uint64_t a, uint64_t b) { return strcmp((char *)a, (char *)b); }

btnode_t *btnode_init(uint64_t property) {
  btnode_t *node = (btnode_t *)malloc(sizeof(btnode_t));
  SYSEXPECT(node != NULL);
  memset(node, 0x00, sizeof(btnode_t));
  node->property = property;
  return node;
}
void btnode_free(btnode_t *node) { free(node); }
// Recursively free all nodes
void btnode_freeall(btnode_t *node, int level) {
  if(node->property & BTNODE_INNER) for(int i = 0;i < node->size;i++) {
    btnode_t *child = *(btnode_t **)btnode_at(node, i, BTNODE_VALUE);
    btnode_freeall(child, level + 1);
  }
  btnode_free(node);
}

// Print a btnode object. Used for debugging or erorr message
void btnode_print(btnode_t *node) {
  printf("btnode_t * @ %p\n", node);
  printf("size = %d; level = %u; property = 0x%X", node->size, node->level, node->property);
  if(node->property & BTNODE_INNER) printf(" Inner");
  if(node->property & BTNODE_LEAF) printf(" Leaf");
  if(node->property & BTNODE_ROOT) printf(" Root");
  printf("\nLogical Layout [");
  for(int i = 0;i < node->size;i++)
    printf("%lu %lu (%p), ", *btnode_at(node, i, BTNODE_KEY), 
           *btnode_at(node, i, BTNODE_VALUE), (void *)*btnode_at(node, i, BTNODE_VALUE));
  printf("]");
  printf("\nPhysical Layout [");
  for(int i = 0;i < node->size;i++)
    printf("%lu %lu (%p), ", node->data[i << 1], node->data[(i << 1) + 1], (void *)node->data[(i << 1) + 1]);
  printf("]\n");
}

btree_t *bt_init(bt_cmp_t cmp) {
  btree_t *tree = (btree_t *)malloc(sizeof(btree_t));
  SYSEXPECT(tree != NULL);
  tree->cmp = cmp;
  tree->root = btnode_init(BTNODE_LEAF | BTNODE_ROOT);
  return tree;
}
void bt_free(btree_t *tree) {
  btnode_freeall(tree->root, 0);
  free(tree);
}

// Given a key, return the slot index with a key equal to or greater than the key
// Could be end of any active slot, which means the key is the biggest
// For inner nodes, do not search the first separator key because it can be -Inf
int btnode_lb(const btree_t *tree, btnode_t *node, uint64_t key, int *exact) {
  int start = benode_startindex(tree, node, key, (node->property & BTNODE_INNER) ? 1 : 0);
  for(int i = start;i < node->size;i++) {
    int cmpret = tree->cmp(*btnode_at(node, i, BTNODE_KEY), key);
    if(cmpret >= 0) { *exact = cmpret == 0; return i; }
  }
  *exact = 0;
  return node->size;
}

// Stop at the first location greater than the key. This function does not search the first element of inner node
int btnode_ub(const btree_t *tree, btnode_t *node, uint64_t key) {
  assert(node->property & BTNODE_INNER);
  int start = benode_startindex(tree, node, key, 1);
  for(int i = start;i < node->size;i++) if(tree->cmp(*btnode_at(node, i, BTNODE_KEY), key) > 0) return i;
  return node->size;
}

// Insert into a node which does not make the node full; Returns if insert is successful
int btnode_insert(btree_t *tree, btnode_t *node, uint64_t key, uint64_t value) {
  assert(node->size >= 0 && node->size < BTNODE_CAPACITY);
  node->data[node->size << 1] = key;
  node->data[(node->size << 1) + 1] = value;
  int exact;
  int bit_lower = btnode_lb(tree, node, key, &exact) << 2; // The index of the low bit of the current element
  if(exact) return 0; // Cannot insert if the same key exists
  uint64_t lower_mask = (0x1UL << bit_lower) - 1;  // This masks off all higher bits than bit_lower (inclusive)
  node->permute = ((node->permute & ~lower_mask) << 4) | ((uint64_t)node->size << bit_lower) | (node->permute & lower_mask);
  node->size++;
  return 1;
}

// Removing is a heavyweight operation because we must rearrange the elements
void btnode_removeat(btnode_t *node, int index) {
  assert(index >= 0 && index < node->size);
  assert(!(node->property & BTNODE_INNER) || index != 0); // Should not remove the leftmost separator for inner node
  uint64_t physical_slot = (node->permute >> (index << 2)) & 0xFUL;
  memmove(node->data + (physical_slot << 1), 
          node->data + (physical_slot << 1) + 2, 
          sizeof(uint64_t) * ((node->size - physical_slot - 1) << 1));
  uint64_t new_permute = 0UL;
  int new_offset = 0;
  for(int i = 0;i < node->size;i++) {
    uint64_t slot = (node->permute >> (i << 2)) & 0xF;  // Physical slot of logical location i
    if(slot < physical_slot) {
      new_permute |= (slot << (new_offset << 2));
      new_offset++;
    } else if(slot > physical_slot) {
      new_permute |= ((slot - 1) << (new_offset << 2));
      new_offset++;
    }
  }
  node->permute = new_permute;
  node->size--;
}

int btnode_remove(btree_t *tree, btnode_t *node, uint64_t key) {
  assert(node->size > 0);
  int exact;
  int index = btnode_lb(tree, node, key, &exact);
  if(!exact) return 0; // Cannot remove because the key is not found
  btnode_removeat(node, index);
  return 1;
}

// Split the node into two; if it is an uneven split then the lower half is smaller
btnode_t *btnode_split(btnode_t *node) {
  int mid = node->size / 2; // First node in the upper half
  btnode_t *new_node = btnode_init(node->property);
  new_node->size = node->size - mid;
  btnode_setnext(new_node, btnode_getnext(node)); // new_node->next = node->next;
  btnode_setprev(new_node, node); // new_node->prev = node;
  new_node->level = node->level;
  for(int i = 0;i < new_node->size;i++) {
    new_node->data[i << 1] = *btnode_at(node, mid + i, BTNODE_KEY);
    new_node->data[(i << 1) + 1] = *btnode_at(node, mid + i, BTNODE_VALUE);
    new_node->permute |= ((uint64_t)i << (i << 2));
  }
  uint64_t temp[BTNODE_CAPACITY * 2];
  uint64_t temp_permute = 0UL;
  for(int i = 0;i < mid;i++) {
    temp[i << 1] = *btnode_at(node, i, BTNODE_KEY);
    temp[(i << 1) + 1] = *btnode_at(node, i, BTNODE_VALUE);
    temp_permute |= ((uint64_t)i << (i << 2));
  }
  memcpy(node->data, temp, sizeof(uint64_t) * (mid << 1));
  btnode_setnext(node, new_node); // node->next = new_node;
  node->permute = temp_permute;
  node->size = mid;
  return new_node;
}

// Merges right node with left node, and returns the node after merge
// Do not assume the returned value is left node; Right node will be freed
btnode_t *btnode_merge(btnode_t *left, btnode_t *right) {
  assert(left->size + right->size < BTNODE_CAPACITY);
  uint64_t temp[BTNODE_CAPACITY * 2];
  uint64_t temp_permute = 0UL;
  for(int i = 0;i < left->size;i++) {
    temp[i << 1] = *btnode_at(left, i, BTNODE_KEY);
    temp[(i << 1) + 1] = *btnode_at(left, i, BTNODE_VALUE);
    temp_permute |= ((uint64_t)i << (i << 2));
  }
  for(int i = left->size;i < left->size + right->size;i++) {
    temp[i << 1] = *btnode_at(right, i - left->size, BTNODE_KEY);
    temp[(i << 1) + 1] = *btnode_at(right, i - left->size, BTNODE_VALUE);
    temp_permute |= ((uint64_t)i << (i << 2));
  }
  left->size += right->size;
  memcpy(left->data, temp, sizeof(uint64_t) * (left->size << 1));
  left->permute = temp_permute;
  btnode_setnext(left, btnode_getnext(right)); // left->next = right->next;
  right->size = right->permute = 0; // For TSX: Abort any txn that has accessed right node
  btnode_free(right);
  return left;
}

// This function performs SMO based on the size of the node; return a node that the key is in
// Argument "parent_index" is the index of the node in the parent
btnode_t *btnode_smo(btree_t *tree, btnode_t *node, uint64_t key, btnode_t *parent, int parent_index) {
  // First check if the current node needs spliting and perform node split if necessary
  if(node->size == BTNODE_CAPACITY) {
    btnode_t *new_node = btnode_split(node);
    if(parent == NULL) {
      assert((node->property & BTNODE_ROOT) && (new_node->property & BTNODE_ROOT));
      parent = btnode_init(BTNODE_ROOT | BTNODE_INNER);
      parent->level = node->level + 1;
      node->property &= ~BTNODE_ROOT;
      new_node->property &= ~BTNODE_ROOT;
      tree->root = parent;
      btnode_insert(tree, parent, *btnode_at(node, 0, BTNODE_KEY), (uint64_t)node);
    }
    btnode_insert(tree, parent, *btnode_at(new_node, 0, BTNODE_KEY), (uint64_t)new_node);
    if(tree->cmp(key, *btnode_at(new_node, 0, BTNODE_KEY)) >= 0) node = new_node; // Search new node if key is in it
  } else if(parent && node->size < BTNODE_MERGE_THRESHOLD) { // Consider merging only when it is not root
    assert(parent_index != -1);
    int merged = 0;
    if(parent_index != 0) { // Left merge
      btnode_t *left = (btnode_t *)*btnode_at(parent, parent_index - 1, BTNODE_VALUE);
      if(left->size + node->size < BTNODE_CAPACITY) {
        *btnode_at(parent, parent_index - 1, BTNODE_VALUE) = (uint64_t)btnode_merge(left, node);
        btnode_removeat(parent, parent_index); // Remove the separator of the current node
        node = left; // Search this node for next level
        merged = 1;
      }
    }
    if(!merged && parent_index < parent->size - 1) { // Right merge
      btnode_t *right = (btnode_t *)*btnode_at(parent, parent_index + 1, BTNODE_VALUE);
      if(node->size + right->size < BTNODE_CAPACITY) {
        *btnode_at(parent, parent_index, BTNODE_VALUE) = (uint64_t)btnode_merge(node, right);
        btnode_removeat(parent, parent_index + 1); // Remove the separator of the next node
        merged = 1;
      }
    }
  }
  return node;
}

// Returns the leaf node after SMO, even for read-only op (SMO should be relatively infrequent)
btnode_t *bt_findleaf(btree_t *tree, uint64_t key) {
  btnode_t *parent = NULL, *curr = tree->root;
  int parent_index = -1;
  while(curr->property & BTNODE_INNER) {
    curr = btnode_smo(tree, curr, key, parent, parent_index); // May adjust the node we need to search
    assert(btnode_ub(tree, curr, key) != 0);
    parent = curr;
    parent_index = btnode_ub(tree, curr, key) - 1; // The index of the child node we will visit
    curr = (btnode_t *)*btnode_at(curr, parent_index, BTNODE_VALUE);
    assert(curr->level + 1 == parent->level); // Must do it here because parent may be NULL at first call
  }
  assert(curr->property & BTNODE_LEAF);
  return btnode_smo(tree, curr, key, parent, parent_index);
}

int bt_insert(btree_t *tree, uint64_t key, uint64_t value) {
  return btnode_insert(tree, bt_findleaf(tree, key), key, value);
}

int bt_remove(btree_t *tree, uint64_t key) {
  return btnode_remove(tree, bt_findleaf(tree, key), key);
}

uint64_t bt_find(btree_t *tree, uint64_t key, int *success) {
  btnode_t *leaf = bt_findleaf(tree, key);
  int index = btnode_lb(tree, leaf, key, success);
  if(!*success) return 0;
  return *btnode_at(leaf, index, BTNODE_VALUE);
}

// Update if key exists, insert if not; return 1 if insert happens, 0 if not
int bt_upsert(btree_t *tree, uint64_t key, uint64_t value) {
  btnode_t *leaf = bt_findleaf(tree, key);
  if(leaf->size == 0) { btnode_insert(tree, leaf, key, value); return 1; }
  int success;
  int index = btnode_lb(tree, leaf, key, &success);
  if(!success) { btnode_insert(tree, leaf, key, value); return 1; }
  *btnode_at(leaf, index, BTNODE_VALUE) = value;
  return 0;
}