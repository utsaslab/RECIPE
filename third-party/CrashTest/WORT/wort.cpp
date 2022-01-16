#ifndef STRING_TYPE

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <emmintrin.h>
#include <assert.h>
#include <x86intrin.h>
#include <math.h>
#include <mutex>
#include <shared_mutex>
#include "wort.h"

static std::shared_mutex mutex;

/* If you want to change the number of entries,
 * change the values of NODE_BITS & MAX_DEPTH */
static const unsigned long NODE_BITS = 4;
static const unsigned long MAX_DEPTH = 15;
static const unsigned long NUM_NODE_ENTRIES = (0x1UL << 4);
static const unsigned long LOW_BIT_MASK = ((0x1UL << 4) - 1);

static const unsigned long MAX_HEIGHT = (15 + 1);

/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (((uintptr_t)x & 1))
#define SET_LEAF(x) ((void*)((uintptr_t)x | 1))
#define LEAF_RAW(x) ((wort_leaf*)((void*)((uintptr_t)x & ~1)))

static unsigned long LATENCY = 0;
static unsigned long CPU_FREQ_MHZ = 2100;
static unsigned long CACHE_LINE_SIZE = 64;

static inline void cpu_pause()
{
	__asm__ volatile ("pause" ::: "memory");
}

static inline unsigned long read_tsc(void)
{
	unsigned long var;
	unsigned int hi, lo;

	asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
	var = ((unsigned long long int) hi << 32) | lo;

	return var;
}

static inline void mfence() {
    asm volatile("mfence" ::: "memory");
}

static inline void flush_buffer(void *buf, unsigned long len, bool fence)
{
	unsigned long i, etsc;
	len = len + ((unsigned long)(buf) & (CACHE_LINE_SIZE - 1));
	if (fence) {
		mfence();
		for (i = 0; i < len; i += CACHE_LINE_SIZE) {
			etsc = read_tsc() + (unsigned long)(LATENCY * CPU_FREQ_MHZ / 1000);
#ifdef CLFLUSH
            asm volatile("clflush %0" : "+m" (*(volatile char *)(buf+i)));
#elif CLFLUSH_OPT
            asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(buf+i)));
#elif CLWB
            asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(buf+i)));
#endif
			while (read_tsc() < etsc)
				cpu_pause();
		}
		mfence();
	} else {
		for (i = 0; i < len; i += CACHE_LINE_SIZE) {
			etsc = read_tsc() + (unsigned long)(LATENCY * CPU_FREQ_MHZ / 1000);
#ifdef CLFLUSH
            asm volatile("clflush %0" : "+m" (*(volatile char *)(buf+i)));
#elif CLFLUSH_OPT
            asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(buf+i)));
#elif CLWB
            asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(buf+i)));
#endif
			while (read_tsc() < etsc)
				cpu_pause();
		}
	}
}

static int get_index(unsigned long key, int depth)
{
	int index;

	index = ((key >> ((MAX_DEPTH - depth) * NODE_BITS)) & LOW_BIT_MASK);
	return index;
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
static wort_node* alloc_node() {
    void *ret;
	wort_node* n;
	//n = (wort_node*)calloc(1, sizeof(wort_node16));
	posix_memalign(&ret, 64, sizeof(wort_node16));
    n = (wort_node *) ret;
	memset(n, 0, sizeof(wort_node16));
	return n;
}

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int wort_tree_init(wort_tree *t) {
	t->root = NULL;
	t->size = 0;
	return 0;
}

// Recursively destroys the tree
/*
static void destroy_node(wort_node *n) {
	// Break if null
	if (!n) return;

	// Special case leafs
	if (IS_LEAF(n)) {
		free(LEAF_RAW(n));
		return;
	}

	// Handle each node type
	int i;
	union {
		wort_node4 *p1;
		wort_node16 *p2;
		wort_node48 *p3;
		wort_node256 *p4;
	} p;
	switch (n->type) {
		case NODE4:
			p.p1 = (wort_node4*)n;
			for (i=0;i<n->num_children;i++) {
				destroy_node(p.p1->children[i]);
			}
			break;

		case NODE16:
			p.p2 = (wort_node16*)n;
			for (i=0;i<n->num_children;i++) {
				destroy_node(p.p2->children[i]);
			}
			break;

		case NODE48:
			p.p3 = (wort_node48*)n;
			for (i=0;i<n->num_children;i++) {
				destroy_node(p.p3->children[i]);
			}
			break;

		case NODE256:
			p.p4 = (wort_node256*)n;
			for (i=0;i<256;i++) {
				if (p.p4->children[i])
					destroy_node(p.p4->children[i]);
			}
			break;

		default:
			abort();
	}

	// Free ourself on the way up
	free(n);
}
*/
/**
 * Destroys an ART tree
 * @return 0 on success.
 */
/*
int wort_tree_destroy(wort_tree *t) {
	destroy_node(t->root);
	return 0;
}
*/

/**
 * Returns the size of the ART tree.
 */
#if 0
#ifndef BROKEN_GCC_C99_INLINE
extern inline uint64_t wort_size(wort_tree *t);
#endif
#endif

static wort_node** find_child(wort_node *n, unsigned char c) {
	wort_node16 *p;

	p = (wort_node16 *)n;
	if (p->children[c])
		return &p->children[c];

	return NULL;
}

// Simple inlined if
static inline int min(int a, int b) {
	return (a < b) ? a : b;
}

/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */
static int check_prefix(const wort_node *n, const unsigned long key, int key_len, int depth) {
//	int max_cmp = min(min(n->partial_len, MAX_PREFIX_LEN), (key_len * INDEX_BITS) - depth);
	int max_cmp = min(min(n->partial_len, MAX_PREFIX_LEN), MAX_HEIGHT - depth);
	int idx;
	for (idx=0; idx < max_cmp; idx++) {
		if (n->partial[idx] != get_index(key, depth + idx))
			return idx;
	}
	return idx;
}

/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
static int leaf_matches(const wort_leaf *n, unsigned long key, int key_len, int depth) {
	(void)depth;
	// Fail if the key lengths are different
	if (n->key_len != (uint32_t)key_len) return 1;

	// Compare the keys starting at the depth
//	return memcmp(n->key, key, key_len);
	return !(n->key == key);
}

// Find the minimum leaf under a node
static wort_leaf* minimum(const wort_node *n) {
	// Handle base cases
	if (!n) return NULL;
	if (IS_LEAF(n)) return LEAF_RAW(n);

	int idx = 0;

	while (!((wort_node16 *)n)->children[idx]) idx++;
	return minimum(((wort_node16 *)n)->children[idx]);
}

static int longest_common_prefix(wort_leaf *l1, wort_leaf *l2, int depth) {
//	int idx, max_cmp = (min(l1->key_len, l2->key_len) * INDEX_BITS) - depth;
	int idx, max_cmp = MAX_HEIGHT - depth;

	for (idx=0; idx < max_cmp; idx++) {
		if (get_index(l1->key, depth + idx) != get_index(l2->key, depth + idx))
			return idx;
	}
	return idx;
}

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* wort_search(const wort_tree *t, const unsigned long key, int key_len) {
    std::shared_lock<std::shared_mutex> lock(mutex);
	wort_node **child;
	wort_node *n = t->root;
	int prefix_len, depth = 0;

	while (n) {
		// Might be a leaf
		if (IS_LEAF(n)) {
			n = (wort_node*)LEAF_RAW(n);
			// Check if the expanded path matches
			if (!leaf_matches((wort_leaf*)n, key, key_len, depth)) {
				return ((wort_leaf*)n)->value;
			}
			return NULL;
		}

		if (n->depth == depth) {
			// Bail if the prefix does not match
			if (n->partial_len) {
				prefix_len = check_prefix(n, key, key_len, depth);
				if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len))
					return NULL;
				depth = depth + n->partial_len;
			}
		} else {
			wort_leaf *leaf[2];
			int cnt, pos, i;

			for (pos = 0, cnt = 0; pos < 16; pos++) {
				if (((wort_node16*)n)->children[pos]) {
					leaf[cnt] = minimum(((wort_node16*)n)->children[pos]);
					cnt++;
					if (cnt == 2)
						break;
				}
			}

			int prefix_diff = longest_common_prefix(leaf[0], leaf[1], depth);
			wort_node old_path;
			old_path.partial_len = prefix_diff;
			for (i = 0; i < min(MAX_PREFIX_LEN, prefix_diff); i++)
				old_path.partial[i] = get_index(leaf[1]->key, depth + i);

			prefix_len = check_prefix(&old_path, key, key_len, depth);
			if (prefix_len != min(MAX_PREFIX_LEN, old_path.partial_len))
				return NULL;
			depth = depth + old_path.partial_len;
		}

		// Recursively search
		child = find_child(n, get_index(key, depth));
		n = (child) ? *child : NULL;
		depth++;
	}
	return NULL;
}

// Find the maximum leaf under a node
/*
static wort_leaf* maximum(const wort_node *n) {
	// Handle base cases
	if (!n) return NULL;
	if (IS_LEAF(n)) return LEAF_RAW(n);

	int idx;
	switch (n->type) {
		case NODE4:
			return maximum(((wort_node4*)n)->children[n->num_children-1]);
		case NODE16:
			return maximum(((wort_node16*)n)->children[n->num_children-1]);
		case NODE48:
			idx=255;
			while (!((wort_node48*)n)->keys[idx]) idx--;
			idx = ((wort_node48*)n)->keys[idx] - 1;
			return maximum(((wort_node48*)n)->children[idx]);
		case NODE256:
			idx=255;
			while (!((wort_node256*)n)->children[idx]) idx--;
			return maximum(((wort_node256*)n)->children[idx]);
		default:
			abort();
	}
}
*/

/**
 * Returns the minimum valued leaf
 */
/*
wort_leaf* wort_minimum(wort_tree *t) {
	return minimum((wort_node*)t->root);
}
*/

/**
 * Returns the maximum valued leaf
 */
/*
wort_leaf* wort_maximum(wort_tree *t) {
	return maximum((wort_node*)t->root);
}
*/

static wort_leaf* make_leaf(const unsigned long key, int key_len, void *value, bool flush) {
    void *ret;
	wort_leaf *l;
	posix_memalign(&ret, 64, sizeof(wort_leaf));
    l = (wort_leaf *)ret;
	l->value = value;
	l->key_len = key_len;
	l->key = key;

    if (flush == true)
        flush_buffer(l, sizeof(wort_leaf), true);
	return l;
}

/*
static void copy_header(wort_node *dest, wort_node *src) {
	dest->num_children = src->num_children;
	dest->partial_len = src->partial_len;
	memcpy(dest->partial, src->partial, min(MAX_PREFIX_LEN, src->partial_len));
}
*/

static void add_child(wort_node16 *n, wort_node **ref, unsigned char c, void *child) {
	(void)ref;
	n->children[c] = (wort_node*)child;
}

/**
 * Calculates the index at which the prefixes mismatch
 */
static int prefix_mismatch(const wort_node *n, const unsigned long key, int key_len, int depth, wort_leaf **l) {
	int max_cmp = min(min(MAX_PREFIX_LEN, n->partial_len), MAX_HEIGHT - depth);
	int idx;
	for (idx=0; idx < max_cmp; idx++) {
		if (n->partial[idx] != get_index(key, depth + idx))
			return idx;
	}

	// If the prefix is short we can avoid finding a leaf
	if (n->partial_len > MAX_PREFIX_LEN) {
		// Prefix is longer than what we've checked, find a leaf
		*l = minimum(n);
		max_cmp = MAX_HEIGHT - depth;
		for (; idx < max_cmp; idx++) {
			if (get_index((*l)->key, idx + depth) != get_index(key, depth + idx))
				return idx;
		}
	}
	return idx;
}

static void recovery_prefix(wort_node *n, int depth) {
	wort_leaf *leaf[2];
	int cnt, pos, i, j;

	for (pos = 0, cnt = 0; pos < 16; pos++) {
		if (((wort_node16*)n)->children[pos]) {
			leaf[cnt] = minimum(((wort_node16*)n)->children[pos]);
			cnt++;
			if (cnt == 2)
				break;
		}
	}

	int prefix_diff = longest_common_prefix(leaf[0], leaf[1], depth);
	wort_node old_path;
	old_path.partial_len = prefix_diff;
	for (i = 0; i < min(MAX_PREFIX_LEN, prefix_diff); i++)
		old_path.partial[i] = get_index(leaf[1]->key, depth + i);
	old_path.depth = depth;
	*((uint64_t *)n) = *((uint64_t *)&old_path);
	flush_buffer(n, sizeof(wort_node), true);
}

static void* recursive_insert(wort_node *n, wort_node **ref, const unsigned long key,
		int key_len, void *value, int depth, int *old)
{
	// If we are at a NULL node, inject a leaf
	if (!n) {
		*ref = (wort_node*)SET_LEAF(make_leaf(key, key_len, value, true));
		flush_buffer(ref, sizeof(uintptr_t), true);
		return NULL;
	}

	// If we are at a leaf, we need to replace it with a node
	if (IS_LEAF(n)) {
		wort_leaf *l = LEAF_RAW(n);

		// Check if we are updating an existing value
		if (!leaf_matches(l, key, key_len, depth)) {
			*old = 1;
			void *old_val = l->value;
			l->value = value;
			flush_buffer(&l->value, sizeof(uintptr_t), true);
			return old_val;
		}

		// New value, we must split the leaf into a node4
		wort_node16 *new_node = (wort_node16 *)alloc_node();
		new_node->n.depth = depth;

		// Create a new leaf
		wort_leaf *l2 = make_leaf(key, key_len, value, false);

		// Determine longest prefix
		int i, longest_prefix = longest_common_prefix(l, l2, depth);
		new_node->n.partial_len = longest_prefix;
		for (i = 0; i < min(MAX_PREFIX_LEN, longest_prefix); i++)
			new_node->n.partial[i] = get_index(key, depth + i);

		// Add the leafs to the new node4
		add_child(new_node, ref, get_index(l->key, depth + longest_prefix), SET_LEAF(l));
		add_child(new_node, ref, get_index(l2->key, depth + longest_prefix), SET_LEAF(l2));

        mfence();
		flush_buffer(new_node, sizeof(wort_node16), false);
		flush_buffer(l2, sizeof(wort_leaf), false);
        mfence();

		*ref = (wort_node*)new_node;
		flush_buffer(ref, 8, true);
		return NULL;
	}

	if (n->depth != depth) {
		recovery_prefix(n, depth);
	}

	// Check if given node has a prefix
	if (n->partial_len) {
		// Determine if the prefixes differ, since we need to split
		wort_leaf *l = NULL;
		int prefix_diff = prefix_mismatch(n, key, key_len, depth, &l);
		if ((uint32_t)prefix_diff >= n->partial_len) {
			depth += n->partial_len;
			goto RECURSE_SEARCH;
		}

		// Create a new node
		wort_node16 *new_node = (wort_node16 *)alloc_node();
		new_node->n.depth = depth;
		new_node->n.partial_len = prefix_diff;
		memcpy(new_node->n.partial, n->partial, min(MAX_PREFIX_LEN, prefix_diff));

		// Adjust the prefix of the old node
        wort_node temp_path;
        if (n->partial_len <= MAX_PREFIX_LEN) {
			add_child(new_node, ref, n->partial[prefix_diff], n);
			temp_path.partial_len = n->partial_len - (prefix_diff + 1);
			temp_path.depth = (depth + prefix_diff + 1);
			memcpy(temp_path.partial, n->partial + prefix_diff + 1,
					min(MAX_PREFIX_LEN, temp_path.partial_len));
		} else {
			int i;
			if (l == NULL)
				l = minimum(n);
			add_child(new_node, ref, get_index(l->key, depth + prefix_diff), n);
			temp_path.partial_len = n->partial_len - (prefix_diff + 1);
			for (i = 0; i < min(MAX_PREFIX_LEN, temp_path.partial_len); i++)
				temp_path.partial[i] = get_index(l->key, depth + prefix_diff + 1 +i);
			temp_path.depth = (depth + prefix_diff + 1);
		}

		// Insert the new leaf
		l = make_leaf(key, key_len, value, false);
		add_child(new_node, ref, get_index(key, depth + prefix_diff), SET_LEAF(l));

        mfence();
		flush_buffer(new_node, sizeof(wort_node16), false);
		flush_buffer(l, sizeof(wort_leaf), false);
        mfence();

        *ref = (wort_node*)new_node;
        *((uint64_t *)n) = *((uint64_t *)&temp_path);

        mfence();
		flush_buffer(n, sizeof(wort_node), false);
		flush_buffer(ref, sizeof(uintptr_t), false);
        mfence();

		return NULL;
	}

RECURSE_SEARCH:;

	// Find a child to recurse to
	wort_node **child = find_child(n, get_index(key, depth));
	if (child) {
		return recursive_insert(*child, child, key, key_len, value, depth + 1, old);
	}

	// No child, node goes within us
	wort_leaf *l = make_leaf(key, key_len, value, true);

	add_child((wort_node16 *)n, ref, get_index(key, depth), SET_LEAF(l));
	flush_buffer(&((wort_node16 *)n)->children[get_index(key, depth)], sizeof(uintptr_t), true);
	return NULL;
}

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* wort_insert(wort_tree *t, const unsigned long key, int key_len, void *value) {
    std::unique_lock<std::shared_mutex> lock(mutex);
	int old_val = 0;
	void *old = recursive_insert(t->root, &t->root, key, key_len, value, 0, &old_val);
	if (!old_val) t->size++;
	return old;
}

/*
static void remove_child256(wort_node256 *n, wort_node **ref, unsigned char c) {
	n->children[c] = NULL;
	n->n.num_children--;

	// Resize to a node48 on underflow, not immediately to prevent
	// trashing if we sit on the 48/49 boundary
	if (n->n.num_children == 37) {
		wort_node48 *new_node = (wort_node48*)alloc_node(NODE48);
		*ref = (wort_node*)new_node;
		copy_header((wort_node*)new_node, (wort_node*)n);

		int i, pos = 0;
		for (i=0;i<256;i++) {
			if (n->children[i]) {
				new_node->children[pos] = n->children[i];
				new_node->keys[i] = pos + 1;
				pos++;
			}
		}
		free(n);
	}
}

static void remove_child48(wort_node48 *n, wort_node **ref, unsigned char c) {
	int pos = n->keys[c];
	n->keys[c] = 0;
	n->children[pos-1] = NULL;
	n->n.num_children--;

	if (n->n.num_children == 12) {
		wort_node16 *new_node = (wort_node16*)alloc_node(NODE16);
		*ref = (wort_node*)new_node;
		copy_header((wort_node*)new_node, (wort_node*)n);

		int i, child = 0;
		for (i=0;i<256;i++) {
			pos = n->keys[i];
			if (pos) {
				new_node->keys[child] = i;
				new_node->children[child] = n->children[pos - 1];
				child++;
			}
		}
		free(n);
	}
}

static void remove_child16(wort_node16 *n, wort_node **ref, wort_node **l) {
	int pos = l - n->children;
	memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
	memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
	n->n.num_children--;

	if (n->n.num_children == 3) {
		wort_node4 *new_node = (wort_node4*)alloc_node(NODE4);
		*ref = (wort_node*)new_node;
		copy_header((wort_node*)new_node, (wort_node*)n);
		memcpy(new_node->keys, n->keys, 4);
		memcpy(new_node->children, n->children, 4*sizeof(void*));
		free(n);
	}
}

static void remove_child4(wort_node4 *n, wort_node **ref, wort_node **l) {
	int pos = l - n->children;
	memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
	memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
	n->n.num_children--;

	// Remove nodes with only a single child
	if (n->n.num_children == 1) {
		wort_node *child = n->children[0];
		if (!IS_LEAF(child)) {
			// Concatenate the prefixes
			int prefix = n->n.partial_len;
			if (prefix < MAX_PREFIX_LEN) {
				n->n.partial[prefix] = n->keys[0];
				prefix++;
			}
			if (prefix < MAX_PREFIX_LEN) {
				int sub_prefix = min(child->partial_len, MAX_PREFIX_LEN - prefix);
				memcpy(n->n.partial+prefix, child->partial, sub_prefix);
				prefix += sub_prefix;
			}

			// Store the prefix in the child
			memcpy(child->partial, n->n.partial, min(prefix, MAX_PREFIX_LEN));
			child->partial_len += n->n.partial_len + 1;
		}
		*ref = child;
		free(n);
	}
}

static void remove_child(wort_node *n, wort_node **ref, unsigned char c, wort_node **l) {
	switch (n->type) {
		case NODE4:
			return remove_child4((wort_node4*)n, ref, l);
		case NODE16:
			return remove_child16((wort_node16*)n, ref, l);
		case NODE48:
			return remove_child48((wort_node48*)n, ref, c);
		case NODE256:
			return remove_child256((wort_node256*)n, ref, c);
		default:
			abort();
	}
}


static wort_leaf* recursive_delete(wort_node *n, wort_node **ref, const unsigned char *key, int key_len, int depth) {
	// Search terminated
	if (!n) return NULL;

	// Handle hitting a leaf node
	if (IS_LEAF(n)) {
		wort_leaf *l = LEAF_RAW(n);
		if (!leaf_matches(l, key, key_len, depth)) {
			*ref = NULL;
			return l;
		}
		return NULL;
	}

	// Bail if the prefix does not match
	if (n->partial_len) {
		int prefix_len = check_prefix(n, key, key_len, depth);
		if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len)) {
			return NULL;
		}
		depth = depth + n->partial_len;
	}

	// Find child node
	wort_node **child = find_child(n, key[depth]);
	if (!child) return NULL;

	// If the child is leaf, delete from this node
	if (IS_LEAF(*child)) {
		wort_leaf *l = LEAF_RAW(*child);
		if (!leaf_matches(l, key, key_len, depth)) {
			remove_child(n, ref, key[depth], child);
			return l;
		}
		return NULL;

		// Recurse
	} else {
		return recursive_delete(*child, child, key, key_len, depth+1);
	}
}
*/

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
/*
void* wort_delete(wort_tree *t, const unsigned char *key, int key_len) {
	wort_leaf *l = recursive_delete(t->root, &t->root, key, key_len, 0);
	if (l) {
		t->size--;
		void *old = l->value;
		free(l);
		return old;
	}
	return NULL;
}
*/

// Recursively iterates over the tree
/*
static int recursive_iter(wort_node *n, wort_callback cb, void *data) {
	// Handle base cases
	if (!n) return 0;
	if (IS_LEAF(n)) {
		wort_leaf *l = LEAF_RAW(n);
		return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
	}

	int i, idx, res;
	switch (n->type) {
		case NODE4:
			for (i=0; i < n->num_children; i++) {
				res = recursive_iter(((wort_node4*)n)->children[i], cb, data);
				if (res) return res;
			}
			break;

		case NODE16:
			for (i=0; i < n->num_children; i++) {
				res = recursive_iter(((wort_node16*)n)->children[i], cb, data);
				if (res) return res;
			}
			break;

		case NODE48:
			for (i=0; i < 256; i++) {
				idx = ((wort_node48*)n)->keys[i];
				if (!idx) continue;

				res = recursive_iter(((wort_node48*)n)->children[idx-1], cb, data);
				if (res) return res;
			}
			break;

		case NODE256:
			for (i=0; i < 256; i++) {
				if (!((wort_node256*)n)->children[i]) continue;
				res = recursive_iter(((wort_node256*)n)->children[i], cb, data);
				if (res) return res;
			}
			break;

		default:
			abort();
	}
	return 0;
}
*/

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
/*
int wort_iter(wort_tree *t, wort_callback cb, void *data) {
	return recursive_iter(t->root, cb, data);
}
*/

#if 0
static void recursive_lookup(wort_node *n, unsigned long num,
		unsigned long *search_count, unsigned long buf[]) {
	if (!n) return ;
	if (IS_LEAF(n)) {
		wort_leaf *l = LEAF_RAW(n);
		buf[*search_count] = *(unsigned long *)l->value;
		(*search_count)++;
		return ;
	}

	int i;
	for (i = 0; i < NUM_NODE_ENTRIES; i++){
		if (!((wort_node16 *)n)->children[i]) continue;
		recursive_lookup(((wort_node16 *)n)->children[i], num, search_count, buf);
		if (*search_count == num)
			break;
	}
	return ;
}

static int new_recursive_lookup(wort_node *n, unsigned long minKey, unsigned
        long maxKey, char** output) {
    wort_node **child;
    int prefix_len, depth = 0;
    wort_node *stack[100000];
    int idx[100000];
    int lev = 0;
    int output_cnt = 0;

    while (n) {
        //sum_depth++;
        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (wort_node*)LEAF_RAW(n);
            // Check if the expanded path matches
            if (!leaf_matches((wort_leaf*)n, minKey, sizeof(unsigned long), depth)) {
                //output[output_cnt++] =
                (char*)(((wort_leaf*)n)->value);
                break;
                //return ((wort_leaf*)n)->value;
            }
            break;
            //      return NULL;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            //comp_path++;
            prefix_len = check_prefix(n, minKey, sizeof(unsigned long), depth);
            if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len)){
                break;
            }
            //        return NULL;
            depth = depth + n->partial_len;
        }
        // Recursively search
        stack[lev] = n;
        idx[lev]= get_index(minKey,depth);
        lev++;
        child = find_child(n, get_index(minKey, depth));
        n = (child) ? *child : NULL;
        depth++;
    }

    wort_node* nn;
    int pos = -1,i;
    bool zero_flag = false;
    int cur_lev = -1;
    for(i=lev-1; i>=0; i--){
        if(idx[i]!=(NUM_NODE_ENTRIES-1)){
            nn = stack[i];
            pos  = idx[i]+1;
            zero_flag=true;
            cur_lev = i;
            break;
        }
    }

    //        pos++;
    bool flag=false;
    while(1){
        flag=false;
        if(IS_LEAF(nn)){
            nn = (wort_node*)LEAF_RAW(nn);
            if(((wort_leaf*)nn)->key > minKey){
                if(((wort_leaf*)nn)->key >= maxKey){
                    return output_cnt;
                }
                else{
                    output[output_cnt++]=(char*)(((wort_leaf*)nn)->key);
                    nn = stack[--cur_lev];
                    pos = idx[cur_lev]+1;
                }
            }
        }

        int rightmost = -1;
        for(i=pos; i<NUM_NODE_ENTRIES; i++){
            if(((wort_node16*)nn)->children[i]!=NULL){
                flag = true;
                rightmost = i;
                stack[cur_lev]=nn;
                idx[cur_lev++]=i;
                break;
            }
        }

        if(rightmost!=-1){
            nn=((wort_node16*)nn)->children[rightmost];
            pos=0;
        }

        if(flag==false){
            nn = stack[--cur_lev];
            pos = idx[cur_lev]+1;
        }

        //pos = NUM_NODE_ENTRIES;
    }
    return output_cnt;
}

void Range_Lookup(wort_tree *t, unsigned long num, unsigned long buf[]){
	unsigned long search_count = 0;
	return recursive_lookup(t->root, num, &search_count, buf);
}
#endif

/**
 * Checks if a leaf prefix matches
 * @return 0 on success.
 */
/*
static int leaf_prefix_matches(const wort_leaf *n, const unsigned char *prefix, int prefix_len) {
	// Fail if the key length is too short
	if (n->key_len < (uint32_t)prefix_len) return 1;

	// Compare the keys
	return memcmp(n->key, prefix, prefix_len);
}
*/

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each that matches a given prefix.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg prefix The prefix of keys to read
 * @arg prefix_len The length of the prefix
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
/*
int wort_iter_prefix(wort_tree *t, const unsigned char *key, int key_len, wort_callback cb, void *data) {
	wort_node **child;
	wort_node *n = t->root;
	int prefix_len, depth = 0;
	while (n) {
		// Might be a leaf
		if (IS_LEAF(n)) {
			n = (wort_node*)LEAF_RAW(n);
			// Check if the expanded path matches
			if (!leaf_prefix_matches((wort_leaf*)n, key, key_len)) {
				wort_leaf *l = (wort_leaf*)n;
				return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
			}
			return 0;
		}

		// If the depth matches the prefix, we need to handle this node
		if (depth == key_len) {
			wort_leaf *l = minimum(n);
			if (!leaf_prefix_matches(l, key, key_len))
				return recursive_iter(n, cb, data);
			return 0;
		}

		// Bail if the prefix does not match
		if (n->partial_len) {
			prefix_len = prefix_mismatch(n, key, key_len, depth);

			// If there is no match, search is terminated
			if (!prefix_len)
				return 0;

			// If we've matched the prefix, iterate on this node
			else if (depth + prefix_len == key_len) {
				return recursive_iter(n, cb, data);
			}

			// if there is a full match, go deeper
			depth = depth + n->partial_len;
		}

		// Recursively search
		child = find_child(n, key[depth]);
		n = (child) ? *child : NULL;
		depth++;
	}
	return 0;
}
*/

#else

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <emmintrin.h>
#include <assert.h>
#include <x86intrin.h>
#include <mutex>
#include <shared_mutex>
#include "wort.h"

static std::shared_mutex mutex;

static const unsigned long NODE_BITS = 4;
static const unsigned long NUM_NODE_ENTRIES = (0x1UL << 4);

static const unsigned long LOW_BIT_MASK = ((0x1UL << 4) - 1);
static const unsigned long HIGH_BIT_MASK = (((0x1UL << 8) - 1) ^ ((0x1UL << 4) - 1));

/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (((uintptr_t)x & 1))
#define SET_LEAF(x) ((void*)((uintptr_t)x | 1))
#define LEAF_RAW(x) ((wort_leaf*)((void*)((uintptr_t)x & ~1)))

static unsigned long LATENCY = 0;
static unsigned long CPU_FREQ_MHZ = 2100;
static unsigned long CACHE_LINE_SIZE = 64;

static inline void cpu_pause()
{
	__asm__ volatile ("pause" ::: "memory");
}

static inline unsigned long read_tsc(void)
{
	unsigned long var;
	unsigned int hi, lo;

	asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
	var = ((unsigned long long int) hi << 32) | lo;

	return var;
}

static inline void mfence() {
    asm volatile("mfence" ::: "memory");
}

static inline void flush_buffer(void *buf, unsigned long len, bool fence)
{
	unsigned long i, etsc;
	len = len + ((unsigned long)(buf) & (CACHE_LINE_SIZE - 1));
	if (fence) {
		mfence();
		for (i = 0; i < len; i += CACHE_LINE_SIZE) {
			etsc = read_tsc() + (unsigned long)(LATENCY * CPU_FREQ_MHZ / 1000);
#ifdef CLFLUSH
            asm volatile("clflush %0" : "+m" (*(volatile char *)(buf+i)));
#elif CLFLUSH_OPT
            asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(buf+i)));
#elif CLWB
            asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(buf+i)));
#endif
			while (read_tsc() < etsc)
				cpu_pause();
		}
		mfence();
	} else {
		for (i = 0; i < len; i += CACHE_LINE_SIZE) {
			etsc = read_tsc() + (unsigned long)(LATENCY * CPU_FREQ_MHZ / 1000);
#ifdef CLFLUSH
            asm volatile("clflush %0" : "+m" (*(volatile char *)(buf+i)));
#elif CLFLUSH_OPT
            asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(buf+i)));
#elif CLWB
            asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(buf+i)));
#endif
            while (read_tsc() < etsc)
				cpu_pause();
		}
	}
}


static unsigned char get_partial(const unsigned char *key, int depth)
{
	unsigned char partial;
	if ((depth % 2) == 0)
		partial = ((key[depth / 2] & HIGH_BIT_MASK) >> 4);
	else
		partial = key[depth / 2] & LOW_BIT_MASK;
	return partial;
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
static wort_node* alloc_node() {
    void *ret;
	wort_node* n;
//	n = (wort_node*)calloc(1, sizeof(wort_node16));
	posix_memalign(&ret, 64, sizeof(wort_node16));
    n = (wort_node *) ret;
	memset(n, 0, sizeof(wort_node16));
	return n;
}

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int wort_tree_init(wort_tree *t) {
	t->root = NULL;
	t->size = 0;
	return 0;
}

// Recursively destroys the tree
/*
static void destroy_node(wort_node *n) {
	// Break if null
	if (!n) return;

	// Special case leafs
	if (IS_LEAF(n)) {
		free(LEAF_RAW(n));
		return;
	}

	// Handle each node type
	int i;
	union {
		wort_node4 *p1;
		wort_node16 *p2;
		wort_node48 *p3;
		wort_node256 *p4;
	} p;
	switch (n->type) {
		case NODE4:
			p.p1 = (wort_node4*)n;
			for (i=0;i<n->num_children;i++) {
				destroy_node(p.p1->children[i]);
			}
			break;

		case NODE16:
			p.p2 = (wort_node16*)n;
			for (i=0;i<n->num_children;i++) {
				destroy_node(p.p2->children[i]);
			}
			break;

		case NODE48:
			p.p3 = (wort_node48*)n;
			for (i=0;i<n->num_children;i++) {
				destroy_node(p.p3->children[i]);
			}
			break;

		case NODE256:
			p.p4 = (wort_node256*)n;
			for (i=0;i<256;i++) {
				if (p.p4->children[i])
					destroy_node(p.p4->children[i]);
			}
			break;

		default:
			abort();
	}

	// Free ourself on the way up
	free(n);
}
*/
/**
 * Destroys an ART tree
 * @return 0 on success.
 */
/*
int wort_tree_destroy(wort_tree *t) {
	destroy_node(t->root);
	return 0;
}
*/

/**
 * Returns the size of the ART tree.
 */

#ifndef BROKEN_GCC_C99_INLINE
extern inline uint64_t wort_size(wort_tree *t);
#endif

static wort_node** find_child(wort_node *n, unsigned char c) {
	wort_node16 *p;

	p = (wort_node16 *)n;
	if (p->children[c])
		return &p->children[c];

	return NULL;
}

// Simple inlined if
static inline int min(int a, int b) {
	return (a < b) ? a : b;
}

/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */
static int check_prefix(const wort_node *n, const unsigned char *key, int key_len, int depth) {
/*
	int max_cmp = min(min(n->partial_len, MAX_PREFIX_LEN), key_len - depth);
	int idx;
	for (idx=0; idx < max_cmp; idx++) {
		if (n->partial[idx] != key[depth + idx])
			return idx;
	}
	return idx;
*/
	int max_cmp = min(min(n->partial_len, MAX_PREFIX_LEN), (key_len * 2) - depth);
	int idx;
	for (idx = 0; idx < max_cmp; idx++) {
		if (n->partial[idx] != get_partial(key, depth + idx))
			return idx;
	}
	return idx;
}

/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
static int leaf_matches(const wort_leaf *n, const unsigned char *key, int key_len, int depth) {
	(void)depth;
	// Fail if the key lengths are different
	if (n->key_len != (uint32_t)key_len) return 1;

	// Compare the keys starting at the depth
	return memcmp(n->key, key, key_len);
}

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* wort_search(const wort_tree *t, const unsigned char *key, int key_len) {
    std::shared_lock<std::shared_mutex> lock(mutex);
	wort_node **child;
	wort_node *n = t->root;
	int prefix_len, depth = 0;

	while (n) {
		// Might be a leaf
		if (IS_LEAF(n)) {
			n = (wort_node*)LEAF_RAW(n);
			// Check if the expanded path matches
			if (!leaf_matches((wort_leaf*)n, key, key_len, depth)) {
				return ((wort_leaf*)n)->value;
			}
			return NULL;
		}

		if (n->depth == depth) {
			// Bail if the prefix does not match
			if (n->partial_len) {
				prefix_len = check_prefix(n, key, key_len, depth);
				if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len))
					return NULL;
				depth = depth + n->partial_len;
			}
		} else {
			printf("[Search] failure occurs\n");
			exit(0);
		}

		// Recursively search
//		child = find_child(n, key[depth]);
		child = find_child(n, get_partial(key, depth));
		n = (child) ? *child : NULL;
		depth++;
	}
	return NULL;
}

// Find the minimum leaf under a node
static wort_leaf* minimum(const wort_node *n) {
	// Handle base cases
	if (!n) return NULL;
	if (IS_LEAF(n)) return LEAF_RAW(n);

	int idx = 0;

	while (!((wort_node16 *)n)->children[idx]) idx++;
	return minimum(((wort_node16 *)n)->children[idx]);
}

// Find the maximum leaf under a node
/*
static wort_leaf* maximum(const wort_node *n) {
	// Handle base cases
	if (!n) return NULL;
	if (IS_LEAF(n)) return LEAF_RAW(n);

	int idx;
	switch (n->type) {
		case NODE4:
			return maximum(((wort_node4*)n)->children[n->num_children-1]);
		case NODE16:
			return maximum(((wort_node16*)n)->children[n->num_children-1]);
		case NODE48:
			idx=255;
			while (!((wort_node48*)n)->keys[idx]) idx--;
			idx = ((wort_node48*)n)->keys[idx] - 1;
			return maximum(((wort_node48*)n)->children[idx]);
		case NODE256:
			idx=255;
			while (!((wort_node256*)n)->children[idx]) idx--;
			return maximum(((wort_node256*)n)->children[idx]);
		default:
			abort();
	}
}
*/

/**
 * Returns the minimum valued leaf
 */
/*
wort_leaf* wort_minimum(wort_tree *t) {
	return minimum((wort_node*)t->root);
}
*/

/**
 * Returns the maximum valued leaf
 */
/*
wort_leaf* wort_maximum(wort_tree *t) {
	return maximum((wort_node*)t->root);
}
*/

static wort_leaf* make_leaf(const unsigned char *key, int key_len, void *value) {
    void *ret;
	wort_leaf *l;
	posix_memalign(&ret, 64, sizeof(wort_leaf) + key_len);
    l = (wort_leaf *) ret;
	l->value = value;
	l->key_len = key_len;
	memcpy(l->key, key, key_len);
	return l;
}

static int longest_common_prefix(wort_leaf *l1, wort_leaf *l2, int depth) {
/*
	int idx, max_cmp = min(l1->key_len, l2->key_len) - depth;

	for (idx=0; idx < max_cmp; idx++) {
		if (l1->key[depth+idx] != l2->key[depth+idx])
			return idx;
	}
	return idx;
*/
	int idx, max_cmp = (min(l1->key_len, l2->key_len) * 2) - depth;

	for (idx = 0; idx < max_cmp; idx++) {
		if (get_partial(l1->key, depth + idx) !=
				get_partial(l2->key, depth + idx))
			return idx;
	}
	return idx;
}

/*
static void copy_header(wort_node *dest, wort_node *src) {
	dest->num_children = src->num_children;
	dest->partial_len = src->partial_len;
	memcpy(dest->partial, src->partial, min(MAX_PREFIX_LEN, src->partial_len));
}
*/

static void add_child(wort_node16 *n, wort_node **ref, unsigned char c, void *child) {
	(void)ref;
	n->children[c] = (wort_node*)child;
}

/**
 * Calculates the index at which the prefixes mismatch
 */
static int prefix_mismatch(const wort_node *n, const unsigned char *key, int key_len, int depth, wort_leaf **l) {
/*
	int max_cmp = min(min(MAX_PREFIX_LEN, n->partial_len), key_len - depth);
	int idx;
	for (idx=0; idx < max_cmp; idx++) {
		if (n->partial[idx] != key[depth + idx])
			return idx;
	}
*/
	int max_cmp = min(min(MAX_PREFIX_LEN, n->partial_len), (key_len * 2) - depth);
	int idx;
	for (idx = 0; idx < max_cmp; idx++) {
		if (n->partial[idx] != get_partial(key, depth + idx))
			return idx;
	}
/*
	// If the prefix is short we can avoid finding a leaf
	if (n->partial_len > MAX_PREFIX_LEN) {
		// Prefix is longer than what we've checked, find a leaf
		*l = minimum(n);
		max_cmp = min((*l)->key_len, key_len) - depth;
		for (; idx < max_cmp; idx++) {
			if ((*l)->key[idx + depth] != key[depth + idx])
				return idx;
		}
	}
	return idx;
*/
	if (n->partial_len > MAX_PREFIX_LEN) {
		*l = minimum(n);
		max_cmp = (min((*l)->key_len, key_len) * 2) - depth;
		for (; idx < max_cmp; idx++) {
			if (get_partial((*l)->key, idx + depth) !=
					get_partial(key, depth + idx))
				return idx;
		}
	}
	return idx;
}

static void* recursive_insert(wort_node *n, wort_node **ref, const unsigned char *key,
		int key_len, void *value, int depth, int *old)
{
	// If we are at a NULL node, inject a leaf
	if (!n) {
		*ref = (wort_node*)SET_LEAF(make_leaf(key, key_len, value));
		flush_buffer(*ref, sizeof(wort_leaf) + key_len, false);
		flush_buffer(ref, 8, true);
		return NULL;
	}

	// If we are at a leaf, we need to replace it with a node
	if (IS_LEAF(n)) {
		wort_leaf *l = LEAF_RAW(n);

		// Check if we are updating an existing value
		if (!leaf_matches(l, key, key_len, depth)) {
			*old = 1;
			void *old_val = l->value;
			l->value = value;
			flush_buffer(&l->value, 8, true);
			return old_val;
		}

		// New value, we must split the leaf into a node4
		wort_node16 *new_node = (wort_node16 *)alloc_node();
		new_node->n.depth = depth;

		// Create a new leaf
		wort_leaf *l2 = make_leaf(key, key_len, value);

		// Determine longest prefix
		int i, longest_prefix = longest_common_prefix(l, l2, depth);
		new_node->n.partial_len = longest_prefix;
//		memcpy(new_node->n.partial, key+depth, min(MAX_PREFIX_LEN, longest_prefix));
		for (i = 0; i < min(MAX_PREFIX_LEN, longest_prefix); i++)
			new_node->n.partial[i] = get_partial(key, depth + i);

		// Add the leafs to the new node4
		*ref = (wort_node*)new_node;
//		add_child(new_node, ref, l->key[depth + longest_prefix], SET_LEAF(l));
//		add_child(new_node, ref, l2->key[depth + longest_prefix], SET_LEAF(l2));
		add_child(new_node, ref, get_partial(l->key, depth + longest_prefix), SET_LEAF(l));
		add_child(new_node, ref, get_partial(l2->key, depth + longest_prefix), SET_LEAF(l2));

		flush_buffer(new_node, sizeof(wort_node16), false);
		flush_buffer(l2, sizeof(wort_leaf) + key_len, false);
		flush_buffer(ref, 8, true);
		return NULL;
	}

	if (n->depth != depth) {
		printf("[Insert] failure occurs\n");
		exit(0);
	}

	// Check if given node has a prefix
	if (n->partial_len) {
		// Determine if the prefixes differ, since we need to split
		wort_leaf *l = NULL;
		int prefix_diff = prefix_mismatch(n, key, key_len, depth, &l);
		if ((uint32_t)prefix_diff >= n->partial_len) {
			depth += n->partial_len;
			goto RECURSE_SEARCH;
		}

		// Create a new node
		wort_node16 *new_node = (wort_node16 *)alloc_node();
		new_node->n.depth = depth;
		*ref = (wort_node*)new_node;
		new_node->n.partial_len = prefix_diff;
		memcpy(new_node->n.partial, n->partial, min(MAX_PREFIX_LEN, prefix_diff));

		// Adjust the prefix of the old node
		if (n->partial_len <= MAX_PREFIX_LEN) {
			wort_node temp_path;
			add_child(new_node, ref, n->partial[prefix_diff], n);
			temp_path.partial_len = n->partial_len - (prefix_diff + 1);
			temp_path.depth = (depth + prefix_diff + 1);
			memmove(temp_path.partial, n->partial + prefix_diff + 1,
					min(MAX_PREFIX_LEN, temp_path.partial_len));
			*((uint64_t *)n) = *((uint64_t *)&temp_path);
		} else {
			int i;
			wort_node temp_path;
			temp_path.partial_len = n->partial_len - (prefix_diff + 1);
//			n->partial_len -= (prefix_diff + 1);
			if (l == NULL)
				l = minimum(n);
//			add_child(new_node, ref, l->key[depth + prefix_diff], n);
			add_child(new_node, ref, get_partial(l->key, depth + prefix_diff), n);
//			memcpy(n->partial, l->key+depth+prefix_diff+1,
//					min(MAX_PREFIX_LEN, n->partial_len));
			for (i = 0; i < min(MAX_PREFIX_LEN, temp_path.partial_len); i++)
				temp_path.partial[i] = get_partial(l->key, depth + prefix_diff + 1 + i);
			temp_path.depth = (depth + prefix_diff + 1);
			*((uint64_t *)n) = *((uint64_t *)&temp_path);
		}

		// Insert the new leaf
		l = make_leaf(key, key_len, value);
//		add_child(new_node, ref, key[depth + prefix_diff], SET_LEAF(l));
		add_child(new_node, ref, get_partial(key, depth + prefix_diff), SET_LEAF(l));

		flush_buffer(new_node, sizeof(wort_node16), false);
		flush_buffer(l, sizeof(wort_leaf) + key_len, false);
		flush_buffer(n, sizeof(wort_node), false);
		flush_buffer(ref, 8, true);
		return NULL;
	}

RECURSE_SEARCH:;

	// Find a child to recurse to
//	wort_node **child = find_child(n, key[depth]);
	wort_node **child = find_child(n, get_partial(key, depth));
	if (child) {
		return recursive_insert(*child, child, key, key_len, value, depth + 1, old);
	}

	// No child, node goes within us
	wort_leaf *l = make_leaf(key, key_len, value);
//	add_child((wort_node16 *)n, ref, key[depth], SET_LEAF(l));
	add_child((wort_node16 *)n, ref, get_partial(key, depth), SET_LEAF(l));

	flush_buffer(l, sizeof(wort_leaf) + key_len, false);
//	flush_buffer(&((wort_node16 *)n)->children[key[depth]], 8, true);
	flush_buffer(&((wort_node16 *)n)->children[get_partial(key, depth)], 8, true);
	return NULL;
}

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* wort_insert(wort_tree *t, const unsigned char *key, int key_len, void *value) {
    std::unique_lock<std::shared_mutex> lock(mutex);
	int old_val = 0;
	void *old = recursive_insert(t->root, &t->root, key, key_len, value, 0, &old_val);
	if (!old_val) t->size++;
	return old;
}

/*
static void remove_child256(wort_node256 *n, wort_node **ref, unsigned char c) {
	n->children[c] = NULL;
	n->n.num_children--;

	// Resize to a node48 on underflow, not immediately to prevent
	// trashing if we sit on the 48/49 boundary
	if (n->n.num_children == 37) {
		wort_node48 *new_node = (wort_node48*)alloc_node(NODE48);
		*ref = (wort_node*)new_node;
		copy_header((wort_node*)new_node, (wort_node*)n);

		int i, pos = 0;
		for (i=0;i<256;i++) {
			if (n->children[i]) {
				new_node->children[pos] = n->children[i];
				new_node->keys[i] = pos + 1;
				pos++;
			}
		}
		free(n);
	}
}

static void remove_child48(wort_node48 *n, wort_node **ref, unsigned char c) {
	int pos = n->keys[c];
	n->keys[c] = 0;
	n->children[pos-1] = NULL;
	n->n.num_children--;

	if (n->n.num_children == 12) {
		wort_node16 *new_node = (wort_node16*)alloc_node(NODE16);
		*ref = (wort_node*)new_node;
		copy_header((wort_node*)new_node, (wort_node*)n);

		int i, child = 0;
		for (i=0;i<256;i++) {
			pos = n->keys[i];
			if (pos) {
				new_node->keys[child] = i;
				new_node->children[child] = n->children[pos - 1];
				child++;
			}
		}
		free(n);
	}
}

static void remove_child16(wort_node16 *n, wort_node **ref, wort_node **l) {
	int pos = l - n->children;
	memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
	memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
	n->n.num_children--;

	if (n->n.num_children == 3) {
		wort_node4 *new_node = (wort_node4*)alloc_node(NODE4);
		*ref = (wort_node*)new_node;
		copy_header((wort_node*)new_node, (wort_node*)n);
		memcpy(new_node->keys, n->keys, 4);
		memcpy(new_node->children, n->children, 4*sizeof(void*));
		free(n);
	}
}

static void remove_child4(wort_node4 *n, wort_node **ref, wort_node **l) {
	int pos = l - n->children;
	memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
	memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
	n->n.num_children--;

	// Remove nodes with only a single child
	if (n->n.num_children == 1) {
		wort_node *child = n->children[0];
		if (!IS_LEAF(child)) {
			// Concatenate the prefixes
			int prefix = n->n.partial_len;
			if (prefix < MAX_PREFIX_LEN) {
				n->n.partial[prefix] = n->keys[0];
				prefix++;
			}
			if (prefix < MAX_PREFIX_LEN) {
				int sub_prefix = min(child->partial_len, MAX_PREFIX_LEN - prefix);
				memcpy(n->n.partial+prefix, child->partial, sub_prefix);
				prefix += sub_prefix;
			}

			// Store the prefix in the child
			memcpy(child->partial, n->n.partial, min(prefix, MAX_PREFIX_LEN));
			child->partial_len += n->n.partial_len + 1;
		}
		*ref = child;
		free(n);
	}
}

static void remove_child(wort_node *n, wort_node **ref, unsigned char c, wort_node **l) {
	switch (n->type) {
		case NODE4:
			return remove_child4((wort_node4*)n, ref, l);
		case NODE16:
			return remove_child16((wort_node16*)n, ref, l);
		case NODE48:
			return remove_child48((wort_node48*)n, ref, c);
		case NODE256:
			return remove_child256((wort_node256*)n, ref, c);
		default:
			abort();
	}
}


static wort_leaf* recursive_delete(wort_node *n, wort_node **ref, const unsigned char *key, int key_len, int depth) {
	// Search terminated
	if (!n) return NULL;

	// Handle hitting a leaf node
	if (IS_LEAF(n)) {
		wort_leaf *l = LEAF_RAW(n);
		if (!leaf_matches(l, key, key_len, depth)) {
			*ref = NULL;
			return l;
		}
		return NULL;
	}

	// Bail if the prefix does not match
	if (n->partial_len) {
		int prefix_len = check_prefix(n, key, key_len, depth);
		if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len)) {
			return NULL;
		}
		depth = depth + n->partial_len;
	}

	// Find child node
	wort_node **child = find_child(n, key[depth]);
	if (!child) return NULL;

	// If the child is leaf, delete from this node
	if (IS_LEAF(*child)) {
		wort_leaf *l = LEAF_RAW(*child);
		if (!leaf_matches(l, key, key_len, depth)) {
			remove_child(n, ref, key[depth], child);
			return l;
		}
		return NULL;

		// Recurse
	} else {
		return recursive_delete(*child, child, key, key_len, depth+1);
	}
}
*/

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
/*
void* wort_delete(wort_tree *t, const unsigned char *key, int key_len) {
	wort_leaf *l = recursive_delete(t->root, &t->root, key, key_len, 0);
	if (l) {
		t->size--;
		void *old = l->value;
		free(l);
		return old;
	}
	return NULL;
}
*/

// Recursively iterates over the tree
/*
static int recursive_iter(wort_node *n, wort_callback cb, void *data) {
	// Handle base cases
	if (!n) return 0;
	if (IS_LEAF(n)) {
		wort_leaf *l = LEAF_RAW(n);
		return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
	}

	int i, idx, res;
	switch (n->type) {
		case NODE4:
			for (i=0; i < n->num_children; i++) {
				res = recursive_iter(((wort_node4*)n)->children[i], cb, data);
				if (res) return res;
			}
			break;

		case NODE16:
			for (i=0; i < n->num_children; i++) {
				res = recursive_iter(((wort_node16*)n)->children[i], cb, data);
				if (res) return res;
			}
			break;

		case NODE48:
			for (i=0; i < 256; i++) {
				idx = ((wort_node48*)n)->keys[i];
				if (!idx) continue;

				res = recursive_iter(((wort_node48*)n)->children[idx-1], cb, data);
				if (res) return res;
			}
			break;

		case NODE256:
			for (i=0; i < 256; i++) {
				if (!((wort_node256*)n)->children[i]) continue;
				res = recursive_iter(((wort_node256*)n)->children[i], cb, data);
				if (res) return res;
			}
			break;

		default:
			abort();
	}
	return 0;
}
*/

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
/*
int wort_iter(wort_tree *t, wort_callback cb, void *data) {
	return recursive_iter(t->root, cb, data);
}
*/

/**
 * Checks if a leaf prefix matches
 * @return 0 on success.
 */
/*
static int leaf_prefix_matches(const wort_leaf *n, const unsigned char *prefix, int prefix_len) {
	// Fail if the key length is too short
	if (n->key_len < (uint32_t)prefix_len) return 1;

	// Compare the keys
	return memcmp(n->key, prefix, prefix_len);
}
*/

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each that matches a given prefix.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg prefix The prefix of keys to read
 * @arg prefix_len The length of the prefix
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
/*
int wort_iter_prefix(wort_tree *t, const unsigned char *key, int key_len, wort_callback cb, void *data) {
	wort_node **child;
	wort_node *n = t->root;
	int prefix_len, depth = 0;
	while (n) {
		// Might be a leaf
		if (IS_LEAF(n)) {
			n = (wort_node*)LEAF_RAW(n);
			// Check if the expanded path matches
			if (!leaf_prefix_matches((wort_leaf*)n, key, key_len)) {
				wort_leaf *l = (wort_leaf*)n;
				return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
			}
			return 0;
		}

		// If the depth matches the prefix, we need to handle this node
		if (depth == key_len) {
			wort_leaf *l = minimum(n);
			if (!leaf_prefix_matches(l, key, key_len))
				return recursive_iter(n, cb, data);
			return 0;
		}

		// Bail if the prefix does not match
		if (n->partial_len) {
			prefix_len = prefix_mismatch(n, key, key_len, depth);

			// If there is no match, search is terminated
			if (!prefix_len)
				return 0;

			// If we've matched the prefix, iterate on this node
			else if (depth + prefix_len == key_len) {
				return recursive_iter(n, cb, data);
			}

			// if there is a full match, go deeper
			depth = depth + n->partial_len;
		}

		// Recursively search
		child = find_child(n, key[depth]);
		n = (child) ? *child : NULL;
		depth++;
	}
	return 0;
}
*/

#endif
