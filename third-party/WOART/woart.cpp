#ifndef STRING_TYPE
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <emmintrin.h>
#include <assert.h>
#include <x86intrin.h>
#include <mutex>
#include <shared_mutex>
#include <vector>
#include <utility>
#include "woart.h"

static std::shared_mutex mutex;

#define NODE4		1
#define NODE16		2
#define NODE48		3
#define NODE256		4

#define BITS_PER_LONG		64

static const unsigned long NODE_BITS = 8;
static const unsigned long MAX_DEPTH = 7;
static const unsigned long NUM_NODE_ENTRIES = (0x1UL << 8);
static const unsigned long LOW_BIT_MASK = ((0x1UL << 8) - 1);

static const unsigned long MAX_HEIGHT = (7 + 1);

#define BITOP_WORD(nr)	((nr) / BITS_PER_LONG)

/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (((uintptr_t)x & 1))
#define SET_LEAF(x) ((void*)((uintptr_t)x | 1))
#define LEAF_RAW(x) ((woart_leaf*)((void*)((uintptr_t)x & ~1)))

static unsigned long CACHE_LINE_SIZE = 64;
static unsigned long LATENCY = 0;
static unsigned long CPU_FREQ_MHZ = 2100;

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

static inline unsigned long __ffs(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "rm" (word));
	return word;
}

static inline unsigned long ffz(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "r" (~word));
	return word;
}

static int get_index(unsigned long key, int depth)
{
	int index;

	index = ((key >> ((MAX_DEPTH - depth) * NODE_BITS)) & LOW_BIT_MASK);
	return index;
}

/*
 * Find the next set bit in a memory region.
 */
static unsigned long find_next_bit(const unsigned long *addr, unsigned long size,
			    unsigned long offset)
{
	const unsigned long *p = addr + BITOP_WORD(offset);
	unsigned long result = offset & ~(BITS_PER_LONG-1);
	unsigned long tmp;

	if (offset >= size)
		return size;
	size -= result;
	offset %= BITS_PER_LONG;
	if (offset) {
		tmp = *(p++);
		tmp &= (~0UL << offset);
		if (size < BITS_PER_LONG)
			goto found_first;
		if (tmp)
			goto found_middle;
		size -= BITS_PER_LONG;
		result += BITS_PER_LONG;
	}
	while (size & ~(BITS_PER_LONG-1)) {
		if ((tmp = *(p++)))
			goto found_middle;
		result += BITS_PER_LONG;
		size -= BITS_PER_LONG;
	}
	if (!size)
		return result;
	tmp = *p;

found_first:
	tmp &= (~0UL >> (BITS_PER_LONG - size));
	if (tmp == 0UL)		/* Are any bits set? */
		return result + size;	/* Nope. */
found_middle:
	return result + __ffs(tmp);
}

/*
 * Find the next zero bit in a memory region
 */
static unsigned long find_next_zero_bit(const unsigned long *addr, unsigned long size,
		unsigned long offset)
{
	const unsigned long *p = addr + BITOP_WORD(offset);
	unsigned long result = offset & ~(BITS_PER_LONG - 1);
	unsigned long tmp;

	if (offset >= size)
		return size;
	size -= result;
	offset %= BITS_PER_LONG;
	if (offset) {
		tmp = *(p++);
		tmp |= ~0UL >> (BITS_PER_LONG - offset);
		if (size < BITS_PER_LONG)
			goto found_first;
		if (~tmp)
			goto found_middle;
		size -= BITS_PER_LONG;
		result += BITS_PER_LONG;
	}
	while (size & ~(BITS_PER_LONG - 1)) {
		if (~(tmp = *(p++)))
			goto found_middle;
		result += BITS_PER_LONG;
		size -= BITS_PER_LONG;
	}
	if (!size)
		return result;
	tmp = *p;

found_first:
	tmp |= ~0UL << size;
	if (tmp == ~0UL)
		return result + size;
found_middle:
	return result + ffz(tmp);
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
static woart_node* alloc_node(uint8_t type) {
    void *ret;
	woart_node* n;
	int i;
	switch (type) {
		case NODE4:
			posix_memalign(&ret, 64, sizeof(woart_node4));
            n = (woart_node *)ret;
			for (i = 0; i < 4; i++)
				((woart_node4 *)n)->slot[i].i_ptr = -1;
			break;
		case NODE16:
			posix_memalign(&ret, 64, sizeof(woart_node16));
            n = (woart_node *)ret;
			((woart_node16 *)n)->bitmap = 0;
			break;
		case NODE48:
			posix_memalign(&ret, 64, sizeof(woart_node48));
            n = (woart_node *)ret;
			memset(n, 0, sizeof(woart_node48));
			break;
		case NODE256:
			posix_memalign(&ret, 64, sizeof(woart_node256));
            n = (woart_node *)ret;
			memset(n, 0, sizeof(woart_node256));
			break;
		default:
			abort();
	}
	n->type = type;
	return n;
}

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int woart_tree_init(woart_tree *t) {
	t->root = NULL;
	t->size = 0;
	return 0;
}

// Recursively destroys the tree
/*
static void destroy_node(woart_node *n) {
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
		woart_node4 *p1;
		woart_node16 *p2;
		woart_node48 *p3;
		woart_node256 *p4;
	} p;
	switch (n->type) {
		case NODE4:
			p.p1 = (woart_node4*)n;
			for (i=0;i<n->num_children;i++) {
				destroy_node(p.p1->children[i]);
			}
			break;

		case NODE16:
			p.p2 = (woart_node16*)n;
			for (i=0;i<n->num_children;i++) {
				destroy_node(p.p2->children[i]);
			}
			break;

		case NODE48:
			p.p3 = (woart_node48*)n;
			for (i=0;i<n->num_children;i++) {
				destroy_node(p.p3->children[i]);
			}
			break;

		case NODE256:
			p.p4 = (woart_node256*)n;
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
int woart_tree_destroy(woart_tree *t) {
	destroy_node(t->root);
	return 0;
}
*/

/**
 * Returns the size of the ART tree.

#ifndef BROKEN_GCC_C99_INLINE
extern inline uint64_t woart_size(woart_tree *t);
#endif
*/

static woart_node** find_child(woart_node *n, unsigned char c) {
	int i;
	union {
		woart_node4 *p1;
		woart_node16 *p2;
		woart_node48 *p3;
		woart_node256 *p4;
	} p;
	switch (n->type) {
		case NODE4:
			p.p1 = (woart_node4 *)n;
			for (i = 0; (i < 4 && (p.p1->slot[i].i_ptr != -1)); i++) {
				if (p.p1->slot[i].key == c)
					return &p.p1->children[p.p1->slot[i].i_ptr];
			}
			break;
		case NODE16:
			p.p2 = (woart_node16 *)n;
			for (i = 0; i < 16; i++) {
				i = find_next_bit(&p.p2->bitmap, 16, i);
				if (i < 16 && p.p2->keys[i] == c)
					return &p.p2->children[i];
			}
			break;
		case NODE48:
			p.p3 = (woart_node48 *)n;
			i = p.p3->keys[c];
			if (i)
				return &p.p3->children[i - 1];
			break;
		case NODE256:
			p.p4 = (woart_node256 *)n;
			if (p.p4->children[c])
				return &p.p4->children[c];
			break;
		default:
			abort();
	}
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
static int check_prefix(const woart_node *n, const unsigned long key, int key_len, int depth) {
//	int max_cmp = min(min(n->partial_len, MAX_PREFIX_LEN), (key_len * INDEX_BITS) - depth);
	int max_cmp = min(min(n->path.partial_len, MAX_PREFIX_LEN), MAX_HEIGHT - depth);
	int idx;
	for (idx=0; idx < max_cmp; idx++) {
		if (n->path.partial[idx] != get_index(key, depth + idx))
			return idx;
	}
	return idx;
}

/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
static int leaf_matches(const woart_leaf *n, unsigned long key, int key_len, int depth) {
	(void)depth;
	// Fail if the key lengths are different
	if (n->key_len != (uint32_t)key_len) return 1;

	// Compare the keys starting at the depth
//	return memcmp(n->key, key, key_len);
	return !(n->key == key);
}

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* woart_search(const woart_tree *t, const unsigned long key, int key_len) {
    std::shared_lock<std::shared_mutex> lock(mutex);
	woart_node **child;
	woart_node *n = t->root;
	int prefix_len, depth = 0;

	while (n) {
		// Might be a leaf
		if (IS_LEAF(n)) {
			n = (woart_node*)LEAF_RAW(n);
			// Check if the expanded path matches
			if (!leaf_matches((woart_leaf*)n, key, key_len, depth)) {
				return ((woart_leaf*)n)->value;
			}
			return NULL;
		}

		if (n->path.depth == depth) {
			// Bail if the prefix does not match
			if (n->path.partial_len) {
				prefix_len = check_prefix(n, key, key_len, depth);
				if (prefix_len != min(MAX_PREFIX_LEN, n->path.partial_len))
					return NULL;
				depth = depth + n->path.partial_len;
			}
		} else {
			printf("Search: Crash occured\n");
			exit(0);
		}

		// Recursively search
		child = find_child(n, get_index(key, depth));
		n = (child) ? *child : NULL;
		depth++;
	}
	return NULL;
}

// Find the minimum leaf under a node
static woart_leaf* minimum(const woart_node *n) {
	// Handle base cases
	if (!n) return NULL;
	if (IS_LEAF(n)) return LEAF_RAW(n);

	int i, j, idx, min;
	switch (n->type) {
		case NODE4:
			return minimum(((woart_node4 *)n)->children[((woart_node4 *)n)->slot[0].i_ptr]);
		case NODE16:
			i = find_next_bit(&((woart_node16 *)n)->bitmap, 16, 0);
			min = ((woart_node16 *)n)->keys[i];
			idx = i;
			for (i = i + 1; i < 16; i++) {
				i = find_next_bit(&((woart_node16 *)n)->bitmap, 16, i);
				if(((woart_node16 *)n)->keys[i] < min && i < 16) {
					min = ((woart_node16 *)n)->keys[i];
					idx = i;
				}
			}
			return minimum(((woart_node16 *)n)->children[idx]);
		case NODE48:
			idx = 0;
			while (!((woart_node48*)n)->keys[idx]) idx++;
			idx = ((woart_node48*)n)->keys[idx] - 1;
			return minimum(((woart_node48 *)n)->children[idx]);
		case NODE256:
			idx = 0;
			while (!((woart_node256 *)n)->children[idx]) idx++;
			return minimum(((woart_node256 *)n)->children[idx]);
		default:
			abort();
	}
}

// Find the maximum leaf under a node
/*
static woart_leaf* maximum(const woart_node *n) {
	// Handle base cases
	if (!n) return NULL;
	if (IS_LEAF(n)) return LEAF_RAW(n);

	int idx;
	switch (n->type) {
		case NODE4:
			return maximum(((woart_node4*)n)->children[n->num_children-1]);
		case NODE16:
			return maximum(((woart_node16*)n)->children[n->num_children-1]);
		case NODE48:
			idx=255;
			while (!((woart_node48*)n)->keys[idx]) idx--;
			idx = ((woart_node48*)n)->keys[idx] - 1;
			return maximum(((woart_node48*)n)->children[idx]);
		case NODE256:
			idx=255;
			while (!((woart_node256*)n)->children[idx]) idx--;
			return maximum(((woart_node256*)n)->children[idx]);
		default:
			abort();
	}
}
*/

/**
 * Returns the minimum valued leaf
 */
/*
woart_leaf* woart_minimum(woart_tree *t) {
	return minimum((woart_node*)t->root);
}
*/

/**
 * Returns the maximum valued leaf
 */
/*
woart_leaf* woart_maximum(woart_tree *t) {
	return maximum((woart_node*)t->root);
}
*/

static woart_leaf* make_leaf(const unsigned long key, int key_len, void *value, bool flush) {
    void *ret;
	woart_leaf *l;
	posix_memalign(&ret, 64, sizeof(woart_leaf));
    l = (woart_leaf *)ret;
	l->value = value;
	l->key_len = key_len;
	l->key = key;

    if (flush == true)
        flush_buffer(l, sizeof(woart_leaf), true);
	return l;
}

static int longest_common_prefix(woart_leaf *l1, woart_leaf *l2, int depth) {
//	int idx, max_cmp = (min(l1->key_len, l2->key_len) * INDEX_BITS) - depth;
	int idx, max_cmp = MAX_HEIGHT - depth;

	for (idx=0; idx < max_cmp; idx++) {
		if (get_index(l1->key, depth + idx) != get_index(l2->key, depth + idx))
			return idx;
	}
	return idx;
}

static void copy_header(woart_node *dest, woart_node *src) {
	memcpy(&dest->path, &src->path, sizeof(path_comp));
}

static void add_child256(woart_node256 *n, woart_node **ref, unsigned char c, void *child) {
	(void)ref;
	n->children[c] = (woart_node *)child;
	flush_buffer(&n->children[c], 8, true);
}

static void add_child256_noflush(woart_node256 *n, woart_node **ref, unsigned char c, void *child) {
	(void)ref;
	n->children[c] = (woart_node *)child;
}

static void add_child48(woart_node48 *n, woart_node **ref, unsigned char c, void *child) {
	unsigned long bitmap = 0;
	int i, num = 0;

	for (i = 0; i < 256; i++) {
		if (n->keys[i]) {
			bitmap += (0x1UL << (n->keys[i] - 1));
			num++;
			if (num == 48)
				break;
		}
	}

	if (num < 48) {
		unsigned long pos = find_next_zero_bit(&bitmap, 48, 0);
		n->children[pos] = (woart_node *)child;
		flush_buffer(&n->children[pos], 8, true);
		n->keys[c] = pos + 1;
		flush_buffer(&n->keys[c], sizeof(unsigned char), true);
	} else {
		woart_node256 *new_node = (woart_node256 *)alloc_node(NODE256);
		for (i = 0; i < 256; i++) {
			if (n->keys[i]) {
				new_node->children[i] = n->children[n->keys[i] - 1];
				num--;
				if (num == 0)
					break;
			}
		}		
		copy_header((woart_node *)new_node, (woart_node *)n);
		add_child256_noflush(new_node, ref, c, child);
		flush_buffer(new_node, sizeof(woart_node256), true);

		*ref = (woart_node *)new_node;
		flush_buffer(ref, 8, true);

		free(n);
	}
}

static void add_child16(woart_node16 *n, woart_node **ref, unsigned char c, void *child) {
	if (n->bitmap != ((0x1UL << 16) - 1)) {
		int empty_idx;

		empty_idx = find_next_zero_bit(&n->bitmap, 16, 0);
		if (empty_idx == 16) {
			printf("find next zero bit error add_child16\n");
			abort();
		}

		n->keys[empty_idx] = c;
		n->children[empty_idx] = (woart_node *)child;
        mfence();
		flush_buffer(&n->keys[empty_idx], sizeof(unsigned char), false);
		flush_buffer(&n->children[empty_idx], sizeof(uintptr_t), false);
        mfence();

		n->bitmap += (0x1UL << empty_idx);
		flush_buffer(&n->bitmap, sizeof(unsigned long), true);
	} else {
		int idx;
		woart_node48 *new_node = (woart_node48 *)alloc_node(NODE48);

		memcpy(new_node->children, n->children,
				sizeof(void *) * 16);
		for (idx = 0; idx < 16; idx++) {
			new_node->keys[n->keys[idx]] = idx + 1;
		}
		copy_header((woart_node *)new_node, (woart_node *)n);

		new_node->keys[c] = 17;
		new_node->children[16] = (woart_node *)child;
		flush_buffer(new_node, sizeof(woart_node48), true);

		*ref = (woart_node *)new_node;
		flush_buffer(ref, sizeof(uintptr_t), true);

		free(n);
	}
}

static void add_child4(woart_node4 *n, woart_node **ref, unsigned char c, void *child) {
	if (n->slot[3].i_ptr == -1) {
		slot_array temp_slot[4];
		int i, idx, mid = -1;
		unsigned long p_idx = 0;

		for (idx = 0; (idx < 4 && (n->slot[idx].i_ptr != -1)); idx++) {
			p_idx = p_idx + (0x1UL << n->slot[idx].i_ptr);
			if (mid == -1 && c < n->slot[idx].key)
				mid = idx;
		}

		if (mid == -1)
			mid = idx;

		p_idx = find_next_zero_bit(&p_idx, 4, 0);
		if (p_idx == 4) {
			printf("find next zero bit error in child4\n");
			abort();
		}
		n->children[p_idx] = (woart_node *)child;
		flush_buffer(&n->children[p_idx], sizeof(uintptr_t), true);

		for (i = idx - 1; i >= mid; i--) {
			temp_slot[i + 1].key = n->slot[i].key;
			temp_slot[i + 1].i_ptr = n->slot[i].i_ptr;
		}

		if (idx < 3) {
			for (i = idx + 1; i < 4; i++)
				temp_slot[i].i_ptr = -1;
		}

		temp_slot[mid].key = c;
		temp_slot[mid].i_ptr = p_idx;

		for (i = mid - 1; i >=0; i--) {
			temp_slot[i].key = n->slot[i].key;
			temp_slot[i].i_ptr = n->slot[i].i_ptr;
		}

		*((uint64_t *)n->slot) = *((uint64_t *)temp_slot);
		flush_buffer(n->slot, sizeof(uintptr_t), true);
	} else {
		int idx;
		woart_node16 *new_node = (woart_node16 *)alloc_node(NODE16);

		for (idx = 0; idx < 4; idx++) {
			new_node->keys[n->slot[idx].i_ptr] = n->slot[idx].key;
			new_node->children[n->slot[idx].i_ptr] = n->children[n->slot[idx].i_ptr];
			new_node->bitmap += (0x1UL << n->slot[idx].i_ptr);
		}
		copy_header((woart_node *)new_node, (woart_node *)n);

		new_node->keys[4] = c;
		new_node->children[4] = (woart_node *)child;
		new_node->bitmap += (0x1UL << 4);
		flush_buffer(new_node, sizeof(woart_node16), true);

		*ref = (woart_node *)new_node;
		flush_buffer(ref, 8, true);

		free(n);
	}
}

static void add_child4_noflush(woart_node4 *n, woart_node **ref, unsigned char c, void *child) {
	slot_array temp_slot[4];
	int i, idx, mid = -1;
	unsigned long p_idx = 0;

	for (idx = 0; (idx < 4 && (n->slot[idx].i_ptr != -1)); idx++) {
		p_idx = p_idx + (0x1UL << n->slot[idx].i_ptr);
		if (mid == -1 && c < n->slot[idx].key)
			mid = idx;
	}

	if (mid == -1)
		mid = idx;

	p_idx = find_next_zero_bit(&p_idx, 4, 0);
	if (p_idx == 4) {
		printf("find next zero bit error in child4\n");
		abort();
	}

	n->children[p_idx] = (woart_node *)child;

	for (i = idx - 1; i >= mid; i--) {
		temp_slot[i + 1].key = n->slot[i].key;
		temp_slot[i + 1].i_ptr = n->slot[i].i_ptr;
	}

	if (idx < 3) {
		for (i = idx + 1; i < 4; i++)
			temp_slot[i].i_ptr = -1;
	}

	temp_slot[mid].key = c;
	temp_slot[mid].i_ptr = p_idx;

	for (i = mid - 1; i >=0; i--) {
		temp_slot[i].key = n->slot[i].key;
		temp_slot[i].i_ptr = n->slot[i].i_ptr;
	}

	*((uint64_t *)n->slot) = *((uint64_t *)temp_slot);
}

static void add_child(woart_node *n, woart_node **ref, unsigned char c, void *child) {
	switch (n->type) {
		case NODE4:
			return add_child4((woart_node4 *)n, ref, c, child);
		case NODE16:
			return add_child16((woart_node16 *)n, ref, c, child);
		case NODE48:
			return add_child48((woart_node48 *)n, ref, c, child);
		case NODE256:
			return add_child256((woart_node256 *)n, ref, c, child);
		default:
			abort();
	}
}

/**
 * Calculates the index at which the prefixes mismatch
 */
static int prefix_mismatch(const woart_node *n, const unsigned long key, int key_len, int depth, woart_leaf **l) {
//	int max_cmp = min(min(MAX_PREFIX_LEN, n->partial_len), (key_len * INDEX_BITS) - depth);
	int max_cmp = min(min(MAX_PREFIX_LEN, n->path.partial_len), MAX_HEIGHT - depth);
	int idx;
	for (idx=0; idx < max_cmp; idx++) {
		if (n->path.partial[idx] != get_index(key, depth + idx))
			return idx;
	}

	// If the prefix is short we can avoid finding a leaf
	if (n->path.partial_len > MAX_PREFIX_LEN) {
		// Prefix is longer than what we've checked, find a leaf
		*l = minimum(n);
//		max_cmp = (min((*l)->key_len, key_len) * INDEX_BITS) - depth;
		max_cmp = MAX_HEIGHT - depth;
		for (; idx < max_cmp; idx++) {
			if (get_index((*l)->key, idx + depth) != get_index(key, depth + idx))
				return idx;
		}
	}
	return idx;
}

static void* recursive_insert(woart_node *n, woart_node **ref, const unsigned long key,
		int key_len, void *value, int depth, int *old)
{
	// If we are at a NULL node, inject a leaf
	if (!n) {
		*ref = (woart_node*)SET_LEAF(make_leaf(key, key_len, value, true));
		flush_buffer(ref, sizeof(uintptr_t), true);
		return NULL;
	}

	// If we are at a leaf, we need to replace it with a node
	if (IS_LEAF(n)) {
		woart_leaf *l = LEAF_RAW(n);

		// Check if we are updating an existing value
		if (!leaf_matches(l, key, key_len, depth)) {
			*old = 1;
			void *old_val = l->value;
			l->value = value;
			flush_buffer(&l->value, sizeof(uintptr_t), true);
			return old_val;
		}

		// New value, we must split the leaf into a node4
		woart_node4 *new_node = (woart_node4 *)alloc_node(NODE4);
		new_node->n.path.depth = depth;

		// Create a new leaf
		woart_leaf *l2 = make_leaf(key, key_len, value, false);

		// Determine longest prefix
		int i, longest_prefix = longest_common_prefix(l, l2, depth);
		new_node->n.path.partial_len = longest_prefix;
		for (i = 0; i < min(MAX_PREFIX_LEN, longest_prefix); i++)
			new_node->n.path.partial[i] = get_index(key, depth + i);

		add_child4_noflush(new_node, ref, get_index(l->key, depth + longest_prefix), SET_LEAF(l));
		add_child4_noflush(new_node, ref, get_index(l2->key, depth + longest_prefix), SET_LEAF(l2));

        mfence();
		flush_buffer(new_node, sizeof(woart_node4), false);
		flush_buffer(l2, sizeof(woart_leaf), false);
        mfence();

		// Add the leafs to the new node4
		*ref = (woart_node*)new_node;
		flush_buffer(ref, sizeof(uintptr_t), true);
		return NULL;
	}

	if (n->path.depth != depth) {
		printf("Insert: system is previously crashed!!\n");
		exit(0);
	}

	// Check if given node has a prefix
	if (n->path.partial_len) {
		// Determine if the prefixes differ, since we need to split
		woart_leaf *l = NULL;
		int prefix_diff = prefix_mismatch(n, key, key_len, depth, &l);
		if ((uint32_t)prefix_diff >= n->path.partial_len) {
			depth += n->path.partial_len;
			goto RECURSE_SEARCH;
		}

		// Create a new node
		woart_node4 *new_node = (woart_node4*)alloc_node(NODE4);
		new_node->n.path.depth = depth;
		new_node->n.path.partial_len = prefix_diff;
		memcpy(new_node->n.path.partial, n->path.partial, min(MAX_PREFIX_LEN, prefix_diff));

		// Adjust the prefix of the old node
        path_comp temp_path;
        if (n->path.partial_len <= MAX_PREFIX_LEN) {
			add_child4_noflush(new_node, ref, n->path.partial[prefix_diff], n);
			temp_path.partial_len = n->path.partial_len - (prefix_diff + 1);
			temp_path.depth = (depth + prefix_diff + 1);
			memmove(temp_path.partial, n->path.partial + prefix_diff + 1,
					min(MAX_PREFIX_LEN, temp_path.partial_len));
		} else {
			int i;
			if (l == NULL)
				l = minimum(n);
			add_child4_noflush(new_node, ref, get_index(l->key, depth + prefix_diff), n);
			temp_path.partial_len = n->path.partial_len - (prefix_diff + 1);
			for (i = 0; i < min(MAX_PREFIX_LEN, temp_path.partial_len); i++)
				temp_path.partial[i] = get_index(l->key, depth + prefix_diff + 1 + i);
			temp_path.depth = (depth + prefix_diff + 1);
		}

		// Insert the new leaf
		l = make_leaf(key, key_len, value, false);
		add_child4_noflush(new_node, ref, get_index(key, depth + prefix_diff), SET_LEAF(l));

        mfence();
		flush_buffer(new_node, sizeof(woart_node4), false);
		flush_buffer(l, sizeof(woart_leaf), false);
        mfence();

		*ref = (woart_node*)new_node;
        *((uint64_t *)&n->path) = *((uint64_t *)&temp_path);

        mfence();
		flush_buffer(&n->path, sizeof(path_comp), false);
		flush_buffer(ref, sizeof(uintptr_t), false);
        mfence();

		return NULL;
	}

RECURSE_SEARCH:;

	// Find a child to recurse to
	woart_node **child = find_child(n, get_index(key, depth));
	if (child) {
		return recursive_insert(*child, child, key, key_len, value, depth + 1, old);
	}

	// No child, node goes within us
	woart_leaf *l = make_leaf(key, key_len, value, true);

	add_child(n, ref, get_index(key, depth), SET_LEAF(l));

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
void* woart_insert(woart_tree *t, const unsigned long key, int key_len, void *value) {
    std::unique_lock<std::shared_mutex> lock(mutex);
	int old_val = 0;
	void *old = recursive_insert(t->root, &t->root, key, key_len, value, 0, &old_val);
	if (!old_val) t->size++;
	return old;
}

/*
static void remove_child256(woart_node256 *n, woart_node **ref, unsigned char c) {
	n->children[c] = NULL;
	n->n.num_children--;

	// Resize to a node48 on underflow, not immediately to prevent
	// trashing if we sit on the 48/49 boundary
	if (n->n.num_children == 37) {
		woart_node48 *new_node = (woart_node48*)alloc_node(NODE48);
		*ref = (woart_node*)new_node;
		copy_header((woart_node*)new_node, (woart_node*)n);

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

static void remove_child48(woart_node48 *n, woart_node **ref, unsigned char c) {
	int pos = n->keys[c];
	n->keys[c] = 0;
	n->children[pos-1] = NULL;
	n->n.num_children--;

	if (n->n.num_children == 12) {
		woart_node16 *new_node = (woart_node16*)alloc_node(NODE16);
		*ref = (woart_node*)new_node;
		copy_header((woart_node*)new_node, (woart_node*)n);

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

static void remove_child16(woart_node16 *n, woart_node **ref, woart_node **l) {
	int pos = l - n->children;
	memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
	memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
	n->n.num_children--;

	if (n->n.num_children == 3) {
		woart_node4 *new_node = (woart_node4*)alloc_node(NODE4);
		*ref = (woart_node*)new_node;
		copy_header((woart_node*)new_node, (woart_node*)n);
		memcpy(new_node->keys, n->keys, 4);
		memcpy(new_node->children, n->children, 4*sizeof(void*));
		free(n);
	}
}

static void remove_child4(woart_node4 *n, woart_node **ref, woart_node **l) {
	int pos = l - n->children;
	memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
	memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
	n->n.num_children--;

	// Remove nodes with only a single child
	if (n->n.num_children == 1) {
		woart_node *child = n->children[0];
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

static void remove_child(woart_node *n, woart_node **ref, unsigned char c, woart_node **l) {
	switch (n->type) {
		case NODE4:
			return remove_child4((woart_node4*)n, ref, l);
		case NODE16:
			return remove_child16((woart_node16*)n, ref, l);
		case NODE48:
			return remove_child48((woart_node48*)n, ref, c);
		case NODE256:
			return remove_child256((woart_node256*)n, ref, c);
		default:
			abort();
	}
}


static woart_leaf* recursive_delete(woart_node *n, woart_node **ref, const unsigned char *key, int key_len, int depth) {
	// Search terminated
	if (!n) return NULL;

	// Handle hitting a leaf node
	if (IS_LEAF(n)) {
		woart_leaf *l = LEAF_RAW(n);
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
	woart_node **child = find_child(n, key[depth]);
	if (!child) return NULL;

	// If the child is leaf, delete from this node
	if (IS_LEAF(*child)) {
		woart_leaf *l = LEAF_RAW(*child);
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
void* woart_delete(woart_tree *t, const unsigned char *key, int key_len) {
	woart_leaf *l = recursive_delete(t->root, &t->root, key, key_len, 0);
	if (l) {
		t->size--;
		void *old = l->value;
		free(l);
		return old;
	}
	return NULL;
}
*/

/*
// Recursively iterates over the tree
static int recursive_iter(woart_node *n, woart_callback cb, void *data) {
	// Handle base cases
	if (!n) return 0;
	if (IS_LEAF(n)) {
		woart_leaf *l = LEAF_RAW(n);
		return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
	}

	int i, idx, res;
	switch (n->type) {
		case NODE4:
			for (i=0; i < n->num_children; i++) {
				res = recursive_iter(((woart_node4*)n)->children[i], cb, data);
				if (res) return res;
			}
			break;

		case NODE16:
			for (i=0; i < n->num_children; i++) {
				res = recursive_iter(((woart_node16*)n)->children[i], cb, data);
				if (res) return res;
			}
			break;

		case NODE48:
			for (i=0; i < 256; i++) {
				idx = ((woart_node48*)n)->keys[i];
				if (!idx) continue;

				res = recursive_iter(((woart_node48*)n)->children[idx-1], cb, data);
				if (res) return res;
			}
			break;

		case NODE256:
			for (i=0; i < 256; i++) {
				if (!((woart_node256*)n)->children[i]) continue;
				res = recursive_iter(((woart_node256*)n)->children[i], cb, data);
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
int woart_iter(woart_tree *t, woart_callback cb, void *data) {
	return recursive_iter(t->root, cb, data);
}
*/

static void insertion_sort(key_pos *base, int num)
{
	int i, j;
	key_pos temp;

	for (i = 1; i < num; i++) {
		for (j = i; j > 0; j--) {
			if (base[j - 1].key > base[j].key) {
				temp = base[j - 1];
				base[j - 1] = base[j];
				base[j] = temp;
			} else
				break;
		}
	}
}

static void recursive_traverse(woart_node *n, int num, int *search_count, unsigned long buf[]) {
	// Handle base cases
	if (!n || ((*search_count) == num)) return ;
	if (IS_LEAF(n)) {
		woart_leaf *l = LEAF_RAW(n);
		buf[*search_count] = *(unsigned long *)l->value;
		(*search_count)++;
		return ;
	}

	int i, j, idx, count48;
	key_pos *sorted_pos;
	switch (n->type) {
		case NODE4:
			for (i = 0; i < ((((woart_node4*)n)->slot[i].i_ptr != -1) && i < 4); i++) {
				recursive_traverse(((woart_node4*)n)->children[((woart_node4*)n)->slot[i].i_ptr],
						num, search_count, buf);
				if (*search_count == num)
					break;
			}
			break;
		case NODE16:
			sorted_pos = (key_pos *)malloc(sizeof(key_pos) * 16);
			for (i = 0, j = 0; i < 16; i++) {
				i = find_next_bit(&((woart_node16*)n)->bitmap, 16, i);
				if (i < 16) {
					sorted_pos[j].key = ((woart_node16*)n)->keys[i];
					sorted_pos[j].child = ((woart_node16*)n)->children[i];
					j++;
				}
			}
			insertion_sort(sorted_pos, j);

			for (i = 0; i < j; i++) {
				recursive_traverse(sorted_pos[i].child, num, search_count, buf);
				if (*search_count == num)
					break;
			}
			free(sorted_pos);
			break;
		case NODE48:
			count48 = 0;
			for (i = 0; i < 256; i++) {
				idx = ((woart_node48*)n)->keys[i];
				if (!idx) continue;
				count48++;
				recursive_traverse(((woart_node48*)n)->children[idx - 1], num,
						search_count, buf);
				if (*search_count == num || count48 == 48)
					break;
			}
			break;
		case NODE256:
			for (i=0; i < 256; i++) {
				if (!((woart_node256*)n)->children[i]) continue;
				recursive_traverse(((woart_node256*)n)->children[i], num,
						search_count, buf);
				if (*search_count == num)
					break;
			}
			break;
		default:
			abort();
	}
	return ;
}

void* recursive_lookup(const woart_tree *t, const unsigned long key, int key_len, int num, int *search_count, unsigned long buf[]) {
	woart_node **child;
	woart_node *n = t->root;
	int prefix_len, depth = 0;
    std::vector<std::pair<woart_node *, int>> stack;
    stack.reserve(30);

	while (n) {
		// Might be a leaf
		if (IS_LEAF(n)) {
			n = (woart_node*)LEAF_RAW(n);
            stack.pop_back();
            break;
		}

		if (n->path.depth == depth) {
			// Bail if the prefix does not match
			if (n->path.partial_len) {
				prefix_len = check_prefix(n, key, key_len, depth);
				if (prefix_len != min(MAX_PREFIX_LEN, n->path.partial_len))
					return NULL;
				depth = depth + n->path.partial_len;
			}
		} else {
			printf("Search: Crash occured\n");
			exit(0);
		}

		// Recursively search
        stack.push_back(std::make_pair(n, get_index(key, depth)));
		child = find_child(n, get_index(key, depth));
		n = (child) ? *child : NULL;
		depth++;
	}

    while (!stack.empty()) {
        recursive_traverse((stack.back()).first, num, search_count, buf);
        stack.pop_back();
    }

	return NULL;
}

void* woart_scan(woart_tree *t, unsigned long min, int num, unsigned long buf[]) {
    std::shared_lock<std::shared_mutex> lock(mutex);
	int search_count = 0;
	return recursive_lookup(t, min, sizeof(unsigned long), num, &search_count, buf);
}

/**
 * Checks if a leaf prefix matches
 * @return 0 on success.
 */
/*
static int leaf_prefix_matches(const woart_leaf *n, const unsigned char *prefix, int prefix_len) {
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
int woart_iter_prefix(woart_tree *t, const unsigned char *key, int key_len, woart_callback cb, void *data) {
	woart_node **child;
	woart_node *n = t->root;
	int prefix_len, depth = 0;
	while (n) {
		// Might be a leaf
		if (IS_LEAF(n)) {
			n = (woart_node*)LEAF_RAW(n);
			// Check if the expanded path matches
			if (!leaf_prefix_matches((woart_leaf*)n, key, key_len)) {
				woart_leaf *l = (woart_leaf*)n;
				return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
			}
			return 0;
		}

		// If the depth matches the prefix, we need to handle this node
		if (depth == key_len) {
			woart_leaf *l = minimum(n);
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
#include <vector>
#include <utility>
#include "woart.h"

static std::shared_mutex mutex;

#define NODE4		1
#define NODE16		2
#define NODE48		3
#define NODE256		4

#define BITS_PER_LONG		64

#define BITOP_WORD(nr)	((nr) / BITS_PER_LONG)

/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (((uintptr_t)x & 1))
#define SET_LEAF(x) ((void*)((uintptr_t)x | 1))
#define LEAF_RAW(x) ((woart_leaf*)((void*)((uintptr_t)x & ~1)))

static unsigned long CACHE_LINE_SIZE = 64;
static unsigned long LATENCY = 0;
static unsigned long CPU_FREQ_MHZ = 2100;

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

static inline unsigned long __ffs(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "rm" (word));
	return word;
}

static inline unsigned long ffz(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "r" (~word));
	return word;
}

/*
 * Find the next set bit in a memory region.
 */
static unsigned long find_next_bit(const unsigned long *addr, unsigned long size,
			    unsigned long offset)
{
	const unsigned long *p = addr + BITOP_WORD(offset);
	unsigned long result = offset & ~(BITS_PER_LONG-1);
	unsigned long tmp;

	if (offset >= size)
		return size;
	size -= result;
	offset %= BITS_PER_LONG;
	if (offset) {
		tmp = *(p++);
		tmp &= (~0UL << offset);
		if (size < BITS_PER_LONG)
			goto found_first;
		if (tmp)
			goto found_middle;
		size -= BITS_PER_LONG;
		result += BITS_PER_LONG;
	}
	while (size & ~(BITS_PER_LONG-1)) {
		if ((tmp = *(p++)))
			goto found_middle;
		result += BITS_PER_LONG;
		size -= BITS_PER_LONG;
	}
	if (!size)
		return result;
	tmp = *p;

found_first:
	tmp &= (~0UL >> (BITS_PER_LONG - size));
	if (tmp == 0UL)		/* Are any bits set? */
		return result + size;	/* Nope. */
found_middle:
	return result + __ffs(tmp);
}

/*
 * Find the next zero bit in a memory region
 */
static unsigned long find_next_zero_bit(const unsigned long *addr, unsigned long size,
		unsigned long offset)
{
	const unsigned long *p = addr + BITOP_WORD(offset);
	unsigned long result = offset & ~(BITS_PER_LONG - 1);
	unsigned long tmp;

	if (offset >= size)
		return size;
	size -= result;
	offset %= BITS_PER_LONG;
	if (offset) {
		tmp = *(p++);
		tmp |= ~0UL >> (BITS_PER_LONG - offset);
		if (size < BITS_PER_LONG)
			goto found_first;
		if (~tmp)
			goto found_middle;
		size -= BITS_PER_LONG;
		result += BITS_PER_LONG;
	}
	while (size & ~(BITS_PER_LONG - 1)) {
		if (~(tmp = *(p++)))
			goto found_middle;
		result += BITS_PER_LONG;
		size -= BITS_PER_LONG;
	}
	if (!size)
		return result;
	tmp = *p;

found_first:
	tmp |= ~0UL << size;
	if (tmp == ~0UL)
		return result + size;
found_middle:
	return result + ffz(tmp);
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
static woart_node* alloc_node(uint8_t type) {
    void *ret;
	woart_node* n;
	int i;
	switch (type) {
		case NODE4:
			posix_memalign(&ret, 64, sizeof(woart_node4));
            n = (woart_node *)ret;
			for (i = 0; i < 4; i++)
				((woart_node4 *)n)->slot[i].i_ptr = -1;
			break;
		case NODE16:
			posix_memalign(&ret, 64, sizeof(woart_node16));
            n = (woart_node *)ret;
			((woart_node16 *)n)->bitmap = 0;
			break;
		case NODE48:
			posix_memalign(&ret, 64, sizeof(woart_node48));
            n = (woart_node *)ret;
			memset(((woart_node48 *)n)->bits_arr, 0, sizeof(((woart_node48 *)n)->bits_arr));
			break;
		case NODE256:
			posix_memalign(&ret, 64, sizeof(woart_node256));
            n = (woart_node *)ret;
			memset(n, 0, sizeof(woart_node256));
			break;
		default:
			abort();
	}
	n->type = type;
	return n;
}

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int woart_tree_init(woart_tree *t) {
	t->root = NULL;
	t->size = 0;
	return 0;
}

// Recursively destroys the tree
/*
static void destroy_node(woart_node *n) {
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
		woart_node4 *p1;
		woart_node16 *p2;
		woart_node48 *p3;
		woart_node256 *p4;
	} p;
	switch (n->type) {
		case NODE4:
			p.p1 = (woart_node4*)n;
			for (i=0;i<n->num_children;i++) {
				destroy_node(p.p1->children[i]);
			}
			break;

		case NODE16:
			p.p2 = (woart_node16*)n;
			for (i=0;i<n->num_children;i++) {
				destroy_node(p.p2->children[i]);
			}
			break;

		case NODE48:
			p.p3 = (woart_node48*)n;
			for (i=0;i<n->num_children;i++) {
				destroy_node(p.p3->children[i]);
			}
			break;

		case NODE256:
			p.p4 = (woart_node256*)n;
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
int woart_tree_destroy(woart_tree *t) {
	destroy_node(t->root);
	return 0;
}
*/

/**
 * Returns the size of the ART tree.

#ifndef BROKEN_GCC_C99_INLINE
extern inline uint64_t woart_size(woart_tree *t);
#endif
*/

static woart_node** find_child(woart_node *n, unsigned char c) {
	int i;
	union {
		woart_node4 *p1;
		woart_node16 *p2;
		woart_node48 *p3;
		woart_node256 *p4;
	} p;
	switch (n->type) {
		case NODE4:
			p.p1 = (woart_node4 *)n;
			for (i = 0; (i < 4 && (p.p1->slot[i].i_ptr != -1)); i++) {
				if (p.p1->slot[i].key == c)
					return &p.p1->children[p.p1->slot[i].i_ptr];
			}
			break;
		case NODE16:
			p.p2 = (woart_node16 *)n;
			for (i = 0; i < 16; i++) {
				i = find_next_bit(&p.p2->bitmap, 16, i);
				if (i < 16 && p.p2->keys[i] == c)
					return &p.p2->children[i];
			}
			break;
		case NODE48:
			p.p3 = (woart_node48 *)n;
			if (p.p3->bits_arr[c / 16].k_bits & (0x1UL << (c % 16)))
				return &p.p3->children[p.p3->keys[c]];
			break;
		case NODE256:
			p.p4 = (woart_node256 *)n;
			if (p.p4->children[c])
				return &p.p4->children[c];
			break;
		default:
			abort();
	}
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
static int check_prefix(const woart_node *n, const unsigned char *key, int key_len, int depth) {
	int max_cmp = min(min(n->path.partial_len, MAX_PREFIX_LEN), key_len - depth);
	int idx;
	for (idx=0; idx < max_cmp; idx++) {
		if (n->path.partial[idx] != key[depth + idx])
			return idx;
	}
	return idx;
}

/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
static int leaf_matches(const woart_leaf *n, const unsigned char *key, int key_len, int depth) {
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
void* woart_search(const woart_tree *t, const unsigned char *key, int key_len) {
    std::shared_lock<std::shared_mutex> lock(mutex);
	woart_node **child;
	woart_node *n = t->root;
	int prefix_len, depth = 0;

	while (n) {
		// Might be a leaf
		if (IS_LEAF(n)) {
			n = (woart_node*)LEAF_RAW(n);
			// Check if the expanded path matches
			if (!leaf_matches((woart_leaf*)n, key, key_len, depth)) {
				return ((woart_leaf*)n)->value;
			}
			return NULL;
		}

		if (n->path.depth == depth) {
			// Bail if the prefix does not match
			if (n->path.partial_len) {
				prefix_len = check_prefix(n, key, key_len, depth);
				if (prefix_len != min(MAX_PREFIX_LEN, n->path.partial_len))
					return NULL;
				depth = depth + n->path.partial_len;
			}
		} else {
			printf("Search: Crash occured\n");
			exit(0);
		}

		// Recursively search
		child = find_child(n, key[depth]);
		n = (child) ? *child : NULL;
		depth++;
	}
	return NULL;
}

// Find the minimum leaf under a node
static woart_leaf* minimum(const woart_node *n) {
	// Handle base cases
	if (!n) return NULL;
	if (IS_LEAF(n)) return LEAF_RAW(n);

	int i, j, idx, min;
	switch (n->type) {
		case NODE4:
			return minimum(((woart_node4 *)n)->children[((woart_node4 *)n)->slot[0].i_ptr]);
		case NODE16:
			i = find_next_bit(&((woart_node16 *)n)->bitmap, 16, 0);
			min = ((woart_node16 *)n)->keys[i];
			idx = i;
			for (i = i + 1; i < 16; i++) {
				i = find_next_bit(&((woart_node16 *)n)->bitmap, 16, i);
				if(((woart_node16 *)n)->keys[i] < min && i < 16) {
					min = ((woart_node16 *)n)->keys[i];
					idx = i;
				}
			}
			return minimum(((woart_node16 *)n)->children[idx]);
		case NODE48:
			for (i = 0; i < 16; i++) {
				for (j = 0; j < 16; j++) { 
					j = find_next_bit((unsigned long *)&((woart_node48 *)n)->bits_arr[i], 64, j);
					if (j < 16)
						return minimum(((woart_node48 *)n)->children[((woart_node48 *)n)->keys[j + (i * 16)]]);
				}
			}
		case NODE256:
			idx = 0;
			while (!((woart_node256 *)n)->children[idx]) idx++;
			return minimum(((woart_node256 *)n)->children[idx]);
		default:
			abort();
	}
}

// Find the maximum leaf under a node
/*
static woart_leaf* maximum(const woart_node *n) {
	// Handle base cases
	if (!n) return NULL;
	if (IS_LEAF(n)) return LEAF_RAW(n);

	int idx;
	switch (n->type) {
		case NODE4:
			return maximum(((woart_node4*)n)->children[n->num_children-1]);
		case NODE16:
			return maximum(((woart_node16*)n)->children[n->num_children-1]);
		case NODE48:
			idx=255;
			while (!((woart_node48*)n)->keys[idx]) idx--;
			idx = ((woart_node48*)n)->keys[idx] - 1;
			return maximum(((woart_node48*)n)->children[idx]);
		case NODE256:
			idx=255;
			while (!((woart_node256*)n)->children[idx]) idx--;
			return maximum(((woart_node256*)n)->children[idx]);
		default:
			abort();
	}
}
*/

/**
 * Returns the minimum valued leaf
 */
/*
woart_leaf* woart_minimum(woart_tree *t) {
	return minimum((woart_node*)t->root);
}
*/

/**
 * Returns the maximum valued leaf
 */
/*
woart_leaf* woart_maximum(woart_tree *t) {
	return maximum((woart_node*)t->root);
}
*/

static woart_leaf* make_leaf(const unsigned char *key, int key_len, void *value) {
    void *ret;
	woart_leaf *l;
	posix_memalign(&ret, 64, sizeof(woart_leaf) + key_len);
    l = (woart_leaf *)ret;
	l->value = value;
	l->key_len = key_len;
	memcpy(l->key, key, key_len);
	return l;
}

static int longest_common_prefix(woart_leaf *l1, woart_leaf *l2, int depth) {
	int max_cmp = min(l1->key_len, l2->key_len) - depth;
	int idx;
	for (idx = 0; idx < max_cmp; idx++) {
		if (l1->key[depth + idx] != l2->key[depth + idx])
			return idx;
	}
	return idx;
}

static void copy_header(woart_node *dest, woart_node *src) {
	memcpy(&dest->path, &src->path, sizeof(path_comp));
}

static void add_child256(woart_node256 *n, woart_node **ref, unsigned char c, void *child) {
	(void)ref;
	n->children[c] = (woart_node *)child;
	flush_buffer(&n->children[c], 8, true);
}

static void add_child256_noflush(woart_node256 *n, woart_node **ref, unsigned char c, void *child) {
	(void)ref;
	n->children[c] = (woart_node *)child;
}

static void add_child48(woart_node48 *n, woart_node **ref, unsigned char c, void *child) {
	int idx;
	unsigned long p_bitmap = 0;

	for (idx = 0; idx < 16; idx++)
		p_bitmap += n->bits_arr[idx].p_bits;

	if (p_bitmap != ((0x1UL << 48) - 1)) {
		idx = find_next_zero_bit(&p_bitmap, 48, 0);
		if (idx == 48) {
			printf("find next zero bit error in child 16\n");
			abort();
		}

		n->keys[c] = idx;
		n->children[idx] = (woart_node *)child;

		n->bits_arr[c / 16].k_bits += (0x1UL << (c % 16));
		n->bits_arr[c / 16].p_bits += (0x1UL << idx);

		flush_buffer(&n->keys[c], sizeof(unsigned char), false);
		flush_buffer(&n->children[idx], 8, false);
		flush_buffer(&n->bits_arr[c / 16], 8, true);
	} else {
		int i, j, num = 0;
		woart_node256 *new_node = (woart_node256 *)alloc_node(NODE256);

		for (i = 0; i < 16; i++) {
			for (j = 0; j < 16; j++) { 
				j = find_next_bit((unsigned long *)&n->bits_arr[i], 64, j);
				if (j < 16) {
					new_node->children[j + (i * 16)] = n->children[n->keys[j + (i * 16)]];
					num++;
					if (num == 48)
						break;
				}
			}
			if (num == 48)
				break;
		}
		copy_header((woart_node *)new_node, (woart_node *)n);
		*ref = (woart_node *)new_node;
		add_child256_noflush(new_node, ref, c, child);

		flush_buffer(new_node, sizeof(woart_node256), false);
		flush_buffer(ref, 8, true);

		free(n);
	}
}

static void add_child48_noflush(woart_node48 *n, woart_node **ref, unsigned char c, void *child) {
	int idx;
	unsigned long p_bitmap = 0;

	for (idx = 0; idx < 16; idx++)
		p_bitmap += n->bits_arr[idx].p_bits;

	idx = find_next_zero_bit(&p_bitmap, 48, 0);
	if (idx == 48) {
		printf("find next zero bit error in child 16\n");
		abort();
	}

	n->keys[c] = idx;
	n->children[idx] = (woart_node *)child;

	n->bits_arr[c / 16].k_bits += (0x1UL << (c % 16));
	n->bits_arr[c / 16].p_bits += (0x1UL << idx);
}

static void add_child16(woart_node16 *n, woart_node **ref, unsigned char c, void *child) {
	if (n->bitmap != ((0x1UL << 16) - 1)) {
		int empty_idx;

		empty_idx = find_next_zero_bit(&n->bitmap, 16, 0);
		if (empty_idx == 16) {
			printf("find next zero bit error add_child16\n");
			abort();
		}

		n->keys[empty_idx] = c;
		n->children[empty_idx] = (woart_node *)child;

		n->bitmap += (0x1UL << empty_idx);

		flush_buffer(&n->keys[empty_idx], sizeof(unsigned char), false);
		flush_buffer(&n->children[empty_idx], 8, false);
		flush_buffer(&n->bitmap, sizeof(unsigned long), true);
	} else {
		int idx;
		woart_node48 *new_node = (woart_node48 *)alloc_node(NODE48);

		for (idx = 0; idx < 16; idx++) {
			new_node->bits_arr[n->keys[idx] / 16].k_bits += (0x1UL << (n->keys[idx] % 16));
			new_node->bits_arr[n->keys[idx] / 16].p_bits += (0x1UL << idx);
			new_node->keys[n->keys[idx]] = idx;
			new_node->children[idx] = n->children[idx];
		}
		copy_header((woart_node *)new_node, (woart_node *)n);
		*ref = (woart_node *)new_node;
		add_child48_noflush(new_node, ref, c, child);

		flush_buffer(new_node, sizeof(woart_node48), false);
		flush_buffer(ref, 8, true);

		free(n);
	}
}

static void add_child16_noflush(woart_node16 *n, woart_node **ref, unsigned char c, void *child) {
	int empty_idx;

	empty_idx = find_next_zero_bit(&n->bitmap, 16, 0);
	if (empty_idx == 16) {
		printf("find next zero bit error add_child16\n");
		abort();
	}

	n->keys[empty_idx] = c;
	n->children[empty_idx] = (woart_node *)child;

	n->bitmap += (0x1UL << empty_idx);
}

static void add_child4(woart_node4 *n, woart_node **ref, unsigned char c, void *child) {
	if (n->slot[3].i_ptr == -1) {
		slot_array temp_slot[4];
		int i, idx, mid = -1;
		unsigned long p_idx = 0;

		for (idx = 0; (idx < 4 && (n->slot[idx].i_ptr != -1)); idx++) {
			p_idx = p_idx + (0x1UL << n->slot[idx].i_ptr);
			if (mid == -1 && c < n->slot[idx].key)
				mid = idx;
		}

		if (mid == -1)
			mid = idx;

		p_idx = find_next_zero_bit(&p_idx, 4, 0);
		if (p_idx == 4) {
			printf("find next zero bit error in child4\n");
			abort();
		}
		n->children[p_idx] = (woart_node *)child;

		for (i = idx - 1; i >= mid; i--) {
			temp_slot[i + 1].key = n->slot[i].key;
			temp_slot[i + 1].i_ptr = n->slot[i].i_ptr;
		}

		if (idx < 3) {
			for (i = idx + 1; i < 4; i++)
				temp_slot[i].i_ptr = -1;
		}

		temp_slot[mid].key = c;
		temp_slot[mid].i_ptr = p_idx;

		for (i = mid - 1; i >=0; i--) {
			temp_slot[i].key = n->slot[i].key;
			temp_slot[i].i_ptr = n->slot[i].i_ptr;
		}

		*((uint64_t *)n->slot) = *((uint64_t *)temp_slot);

		flush_buffer(&n->children[p_idx], 8, false);
		flush_buffer(n->slot, 8, true);
	} else {
		int idx;
		woart_node16 *new_node = (woart_node16 *)alloc_node(NODE16);

		for (idx = 0; idx < 4; idx++) {
			new_node->keys[n->slot[idx].i_ptr] = n->slot[idx].key;
			new_node->children[n->slot[idx].i_ptr] = n->children[n->slot[idx].i_ptr];
			new_node->bitmap += (0x1UL << n->slot[idx].i_ptr);
		}
		copy_header((woart_node *)new_node, (woart_node *)n);
		*ref = (woart_node *)new_node;
		add_child16_noflush(new_node, ref, c, child);

		flush_buffer(new_node, sizeof(woart_node16), false);
		flush_buffer(ref, 8, true);

		free(n);
	}
}

static void add_child4_noflush(woart_node4 *n, woart_node **ref, unsigned char c, void *child) {
	slot_array temp_slot[4];
	int i, idx, mid = -1;
	unsigned long p_idx = 0;

	for (idx = 0; (idx < 4 && (n->slot[idx].i_ptr != -1)); idx++) {
		p_idx = p_idx + (0x1UL << n->slot[idx].i_ptr);
		if (mid == -1 && c < n->slot[idx].key)
			mid = idx;
	}

	if (mid == -1)
		mid = idx;

	p_idx = find_next_zero_bit(&p_idx, 4, 0);
	if (p_idx == 4) {
		printf("find next zero bit error in child4\n");
		abort();
	}

	n->children[p_idx] = (woart_node *)child;

	for (i = idx - 1; i >= mid; i--) {
		temp_slot[i + 1].key = n->slot[i].key;
		temp_slot[i + 1].i_ptr = n->slot[i].i_ptr;
	}

	if (idx < 3) {
		for (i = idx + 1; i < 4; i++)
			temp_slot[i].i_ptr = -1;
	}

	temp_slot[mid].key = c;
	temp_slot[mid].i_ptr = p_idx;

	for (i = mid - 1; i >=0; i--) {
		temp_slot[i].key = n->slot[i].key;
		temp_slot[i].i_ptr = n->slot[i].i_ptr;
	}

	*((uint64_t *)n->slot) = *((uint64_t *)temp_slot);
}

static void add_child(woart_node *n, woart_node **ref, unsigned char c, void *child) {
	switch (n->type) {
		case NODE4:
			return add_child4((woart_node4 *)n, ref, c, child);
		case NODE16:
			return add_child16((woart_node16 *)n, ref, c, child);
		case NODE48:
			return add_child48((woart_node48 *)n, ref, c, child);
		case NODE256:
			return add_child256((woart_node256 *)n, ref, c, child);
		default:
			abort();
	}
}

/**
 * Calculates the index at which the prefixes mismatch
 */
static int prefix_mismatch(const woart_node *n, const unsigned char *key, int key_len, int depth, woart_leaf **l) {
	int max_cmp = min(min(MAX_PREFIX_LEN, n->path.partial_len), key_len - depth);
	int idx;
	for (idx=0; idx < max_cmp; idx++) {
		if (n->path.partial[idx] != key[depth + idx])
			return idx;
	}

	// If the prefix is short we can avoid finding a leaf
	if (n->path.partial_len > MAX_PREFIX_LEN) {
		// Prefix is longer than what we've checked, find a leaf
		*l = minimum(n);
		max_cmp = min((*l)->key_len, key_len) - depth;
		for (; idx < max_cmp; idx++) {
			if ((*l)->key[idx + depth] != key[depth + idx])
				return idx;
		}
	}
	return idx;
}

static void* recursive_insert(woart_node *n, woart_node **ref, const unsigned char *key,
		int key_len, void *value, int depth, int *old)
{
	// If we are at a NULL node, inject a leaf
	if (!n) {
		*ref = (woart_node*)SET_LEAF(make_leaf(key, key_len, value));
		flush_buffer(*ref, sizeof(woart_leaf) + key_len, false);
		flush_buffer(ref, 8, true);
		return NULL;
	}

	// If we are at a leaf, we need to replace it with a node
	if (IS_LEAF(n)) {
		woart_leaf *l = LEAF_RAW(n);

		// Check if we are updating an existing value
		if (!leaf_matches(l, key, key_len, depth)) {
			*old = 1;
			void *old_val = l->value;
			l->value = value;
			flush_buffer(&l->value, 8, true);
			return old_val;
		}

		// New value, we must split the leaf into a node4
		woart_node4 *new_node = (woart_node4 *)alloc_node(NODE4);
		new_node->n.path.depth = depth;

		// Create a new leaf
		woart_leaf *l2 = make_leaf(key, key_len, value);

		// Determine longest prefix
		int i, longest_prefix = longest_common_prefix(l, l2, depth);
		new_node->n.path.partial_len = longest_prefix;
		memcpy(new_node->n.path.partial, key+depth, min(MAX_PREFIX_LEN, longest_prefix));
		// Add the leafs to the new node4
		*ref = (woart_node*)new_node;
		add_child4_noflush(new_node, ref, l->key[depth + longest_prefix], SET_LEAF(l));
		add_child4_noflush(new_node, ref, l2->key[depth + longest_prefix], SET_LEAF(l2));

		flush_buffer(new_node, sizeof(woart_node4), false);
		flush_buffer(l2, sizeof(woart_leaf) + key_len, false);
		flush_buffer(ref, 8, true);
		return NULL;
	}

	if (n->path.depth != depth) {
		printf("Insert: system is previously crashed!!\n");
		exit(0);
	}

	// Check if given node has a prefix
	if (n->path.partial_len) {
		// Determine if the prefixes differ, since we need to split
		woart_leaf *l = NULL;
		int prefix_diff = prefix_mismatch(n, key, key_len, depth, &l);
		if ((uint32_t)prefix_diff >= n->path.partial_len) {
			depth += n->path.partial_len;
			goto RECURSE_SEARCH;
		}

		// Create a new node
		woart_node4 *new_node = (woart_node4*)alloc_node(NODE4);
		new_node->n.path.depth = depth;
		*ref = (woart_node*)new_node;
		new_node->n.path.partial_len = prefix_diff;
		memcpy(new_node->n.path.partial, n->path.partial, min(MAX_PREFIX_LEN, prefix_diff));

		// Adjust the prefix of the old node
		if (n->path.partial_len <= MAX_PREFIX_LEN) {
			path_comp temp_path;
			add_child4_noflush(new_node, ref, n->path.partial[prefix_diff], n);
			temp_path.partial_len = n->path.partial_len - (prefix_diff + 1);
			temp_path.depth = (depth + prefix_diff + 1);
			memmove(temp_path.partial, n->path.partial + prefix_diff + 1,
					min(MAX_PREFIX_LEN, temp_path.partial_len));
			*((uint64_t *)&n->path) = *((uint64_t *)&temp_path);
		} else {
			int i;
			path_comp temp_path;
			if (l == NULL)
				l = minimum(n);
			add_child4_noflush(new_node, ref, l->key[depth + prefix_diff], n);
			temp_path.partial_len = n->path.partial_len - (prefix_diff + 1);
			temp_path.depth = (depth + prefix_diff + 1);
			memcpy(temp_path.partial, l->key+depth+prefix_diff+1,
					min(MAX_PREFIX_LEN, temp_path.partial_len));
			*((uint64_t *)&n->path) = *((uint64_t *)&temp_path);
		}
	
		// Insert the new leaf
		l = make_leaf(key, key_len, value);
		add_child4_noflush(new_node, ref, key[depth + prefix_diff], SET_LEAF(l));

		flush_buffer(new_node, sizeof(woart_node4), false);
		flush_buffer(l, sizeof(woart_leaf) + key_len, false);
		flush_buffer(&n->path, sizeof(path_comp), false);
		flush_buffer(ref, 8, true);
		return NULL;
	}

RECURSE_SEARCH:;

	// Find a child to recurse to
	woart_node **child = find_child(n, key[depth]);
	if (child) {
		return recursive_insert(*child, child, key, key_len, value, depth + 1, old);
	}

	// No child, node goes within us
	woart_leaf *l = make_leaf(key, key_len, value);
	flush_buffer(l, sizeof(woart_leaf) + key_len, false);

	add_child(n, ref, key[depth], SET_LEAF(l));

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
void* woart_insert(woart_tree *t, const unsigned char *key, int key_len, void *value) {
    std::unique_lock<std::shared_mutex> lock(mutex);
	int old_val = 0;
	void *old = recursive_insert(t->root, &t->root, key, key_len, value, 0, &old_val);
	if (!old_val) t->size++;
	return old;
}

/*
static void remove_child256(woart_node256 *n, woart_node **ref, unsigned char c) {
	n->children[c] = NULL;
	n->n.num_children--;

	// Resize to a node48 on underflow, not immediately to prevent
	// trashing if we sit on the 48/49 boundary
	if (n->n.num_children == 37) {
		woart_node48 *new_node = (woart_node48*)alloc_node(NODE48);
		*ref = (woart_node*)new_node;
		copy_header((woart_node*)new_node, (woart_node*)n);

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

static void remove_child48(woart_node48 *n, woart_node **ref, unsigned char c) {
	int pos = n->keys[c];
	n->keys[c] = 0;
	n->children[pos-1] = NULL;
	n->n.num_children--;

	if (n->n.num_children == 12) {
		woart_node16 *new_node = (woart_node16*)alloc_node(NODE16);
		*ref = (woart_node*)new_node;
		copy_header((woart_node*)new_node, (woart_node*)n);

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

static void remove_child16(woart_node16 *n, woart_node **ref, woart_node **l) {
	int pos = l - n->children;
	memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
	memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
	n->n.num_children--;

	if (n->n.num_children == 3) {
		woart_node4 *new_node = (woart_node4*)alloc_node(NODE4);
		*ref = (woart_node*)new_node;
		copy_header((woart_node*)new_node, (woart_node*)n);
		memcpy(new_node->keys, n->keys, 4);
		memcpy(new_node->children, n->children, 4*sizeof(void*));
		free(n);
	}
}

static void remove_child4(woart_node4 *n, woart_node **ref, woart_node **l) {
	int pos = l - n->children;
	memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
	memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
	n->n.num_children--;

	// Remove nodes with only a single child
	if (n->n.num_children == 1) {
		woart_node *child = n->children[0];
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

static void remove_child(woart_node *n, woart_node **ref, unsigned char c, woart_node **l) {
	switch (n->type) {
		case NODE4:
			return remove_child4((woart_node4*)n, ref, l);
		case NODE16:
			return remove_child16((woart_node16*)n, ref, l);
		case NODE48:
			return remove_child48((woart_node48*)n, ref, c);
		case NODE256:
			return remove_child256((woart_node256*)n, ref, c);
		default:
			abort();
	}
}


static woart_leaf* recursive_delete(woart_node *n, woart_node **ref, const unsigned char *key, int key_len, int depth) {
	// Search terminated
	if (!n) return NULL;

	// Handle hitting a leaf node
	if (IS_LEAF(n)) {
		woart_leaf *l = LEAF_RAW(n);
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
	woart_node **child = find_child(n, key[depth]);
	if (!child) return NULL;

	// If the child is leaf, delete from this node
	if (IS_LEAF(*child)) {
		woart_leaf *l = LEAF_RAW(*child);
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
void* woart_delete(woart_tree *t, const unsigned char *key, int key_len) {
	woart_leaf *l = recursive_delete(t->root, &t->root, key, key_len, 0);
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
static int recursive_iter(woart_node *n, woart_callback cb, void *data) {
	// Handle base cases
	if (!n) return 0;
	if (IS_LEAF(n)) {
		woart_leaf *l = LEAF_RAW(n);
		return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
	}

	int i, idx, res;
	switch (n->type) {
		case NODE4:
			for (i=0; i < n->num_children; i++) {
				res = recursive_iter(((woart_node4*)n)->children[i], cb, data);
				if (res) return res;
			}
			break;

		case NODE16:
			for (i=0; i < n->num_children; i++) {
				res = recursive_iter(((woart_node16*)n)->children[i], cb, data);
				if (res) return res;
			}
			break;

		case NODE48:
			for (i=0; i < 256; i++) {
				idx = ((woart_node48*)n)->keys[i];
				if (!idx) continue;

				res = recursive_iter(((woart_node48*)n)->children[idx-1], cb, data);
				if (res) return res;
			}
			break;

		case NODE256:
			for (i=0; i < 256; i++) {
				if (!((woart_node256*)n)->children[i]) continue;
				res = recursive_iter(((woart_node256*)n)->children[i], cb, data);
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
int woart_iter(woart_tree *t, woart_callback cb, void *data) {
	return recursive_iter(t->root, cb, data);
}
*/

/**
 * Checks if a leaf prefix matches
 * @return 0 on success.
 */
/*
static int leaf_prefix_matches(const woart_leaf *n, const unsigned char *prefix, int prefix_len) {
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
int woart_iter_prefix(woart_tree *t, const unsigned char *key, int key_len, woart_callback cb, void *data) {
	woart_node **child;
	woart_node *n = t->root;
	int prefix_len, depth = 0;
	while (n) {
		// Might be a leaf
		if (IS_LEAF(n)) {
			n = (woart_node*)LEAF_RAW(n);
			// Check if the expanded path matches
			if (!leaf_prefix_matches((woart_leaf*)n, key, key_len)) {
				woart_leaf *l = (woart_leaf*)n;
				return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
			}
			return 0;
		}

		// If the depth matches the prefix, we need to handle this node
		if (depth == key_len) {
			woart_leaf *l = minimum(n);
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

static void insertion_sort(key_pos *base, int num)
{
	int i, j;
	key_pos temp;

	for (i = 1; i < num; i++) {
		for (j = i; j > 0; j--) {
			if (base[j - 1].key > base[j].key) {
				temp = base[j - 1];
				base[j - 1] = base[j];
				base[j] = temp;
			} else
				break;
		}
	}
}

static void recursive_traverse(woart_node *n, int num, int *search_count, unsigned long buf[]) {
	// Handle base cases
	if (!n || ((*search_count) == num)) return ;
	if (IS_LEAF(n)) {
		woart_leaf *l = LEAF_RAW(n);
		buf[*search_count] = *(unsigned long *)l->value;
		(*search_count)++;
		return ;
	}

	int i, j, idx, count48;
	key_pos *sorted_pos;
	switch (n->type) {
		case NODE4:
			for (i = 0; i < ((((woart_node4*)n)->slot[i].i_ptr != -1) && i < 4); i++) {
				recursive_traverse(((woart_node4*)n)->children[((woart_node4*)n)->slot[i].i_ptr],
						num, search_count, buf);
				if (*search_count == num)
					break;
			}
			break;
		case NODE16:
			sorted_pos = (key_pos *)malloc(sizeof(key_pos) * 16);
			for (i = 0, j = 0; i < 16; i++) {
				i = find_next_bit(&((woart_node16*)n)->bitmap, 16, i);
				if (i < 16) {
					sorted_pos[j].key = ((woart_node16*)n)->keys[i];
					sorted_pos[j].child = ((woart_node16*)n)->children[i];
					j++;
				}
			}
			insertion_sort(sorted_pos, j);

			for (i = 0; i < j; i++) {
				recursive_traverse(sorted_pos[i].child, num, search_count, buf);
				if (*search_count == num)
					break;
			}
			free(sorted_pos);
			break;
		case NODE48:
			count48 = 0;
			for (i = 0; i < 256; i++) {
				idx = ((woart_node48*)n)->keys[i];
				if (!idx) continue;
				count48++;
				recursive_traverse(((woart_node48*)n)->children[idx - 1], num,
						search_count, buf);
				if (*search_count == num || count48 == 48)
					break;
			}
			break;
		case NODE256:
			for (i=0; i < 256; i++) {
				if (!((woart_node256*)n)->children[i]) continue;
				recursive_traverse(((woart_node256*)n)->children[i], num,
						search_count, buf);
				if (*search_count == num)
					break;
			}
			break;
		default:
			abort();
	}
	return ;
}

void* recursive_lookup(const woart_tree *t, const unsigned char *key, int key_len, int num, int *search_count, unsigned long buf[]) {
	woart_node **child;
	woart_node *n = t->root;
	int prefix_len, depth = 0;
    std::vector<std::pair<woart_node *, int>> stack;
    stack.reserve(30);

	while (n) {
		// Might be a leaf
		if (IS_LEAF(n)) {
			n = (woart_node*)LEAF_RAW(n);
            stack.pop_back();
            break;
		}

		if (n->path.depth == depth) {
			// Bail if the prefix does not match
			if (n->path.partial_len) {
				prefix_len = check_prefix(n, key, key_len, depth);
				if (prefix_len != min(MAX_PREFIX_LEN, n->path.partial_len))
					return NULL;
				depth = depth + n->path.partial_len;
			}
		} else {
			printf("Search: Crash occured\n");
			exit(0);
		}

		// Recursively search
        stack.push_back(std::make_pair(n, key[depth]));
		child = find_child(n, key[depth]);
		n = (child) ? *child : NULL;
		depth++;
	}

    while (!stack.empty()) {
        recursive_traverse((stack.back()).first, num, search_count, buf);
        stack.pop_back();
    }

	return NULL;
}

void* woart_scan(woart_tree *t, const unsigned char *min, int key_len, int num, unsigned long buf[]) {
    std::shared_lock<std::shared_mutex> lock(mutex);
	int search_count = 0;
	return recursive_lookup(t, min, key_len, num, &search_count, buf);
}

#endif
