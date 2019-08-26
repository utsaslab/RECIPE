#ifndef STRING_TYPE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <x86intrin.h>
#include <malloc.h>
#include <stdint.h>
#include <time.h>
#include <mutex>
#include <shared_mutex>
#include "wbtree.h"

namespace wbtree {

static std::shared_mutex mutex;

#define BITS_PER_LONG		64

#define BITOP_WORD(nr)	((nr) / BITS_PER_LONG)

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

static node *find_leaf_node(node *curr, unsigned long key);
static int Search(node *curr, char *temp, unsigned long key);
static int insert_in_leaf_noflush(node *curr, unsigned long key, void *value);
static void insert_in_parent(tree *t, node *curr, unsigned long key, node *splitNode);
static void insert_in_leaf(node *curr, unsigned long key, void *value);

static inline unsigned long ffz(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "r" (~word));
	return word;
}

static void add_log_entry(tree *t, void *addr, unsigned int size, unsigned char type)
{
	log_entry *log;
	int i, remain_size;

	remain_size = size - ((size / LOG_DATA_SIZE) * LOG_DATA_SIZE);

	if ((char *)t->start_log->next_offset == 
			(t->start_log->log_data + LOG_AREA_SIZE))
		t->start_log->next_offset = (log_entry *)t->start_log->log_data;

	if (size <= LOG_DATA_SIZE) {
		log = t->start_log->next_offset;
		log->size = size;
		log->type = type;
		log->addr = addr;
		memcpy(log->data, addr, size);

		if (type == LE_DATA)
			flush_buffer(log, sizeof(log_entry), false);
		else
			flush_buffer(log, sizeof(log_entry), true);

		t->start_log->next_offset = t->start_log->next_offset + 1;
	} else {
		void *next_addr = addr;

//		mfence();
		for (i = 0; i < size / LOG_DATA_SIZE; i++) {
			log = t->start_log->next_offset;
			log->size = LOG_DATA_SIZE;
			log->type = type;
			log->addr = next_addr;
			memcpy(log->data, next_addr, LOG_DATA_SIZE);

			flush_buffer(log, sizeof(log_entry), false);

			t->start_log->next_offset = t->start_log->next_offset + 1;
			if ((char *)t->start_log->next_offset == 
					(t->start_log->log_data + LOG_AREA_SIZE))
				t->start_log->next_offset = (log_entry *)t->start_log->log_data;

			next_addr = (char *)next_addr + LOG_DATA_SIZE;
		}

		if (remain_size > 0) {
			log = t->start_log->next_offset;
			log->size = LOG_DATA_SIZE;
			log->type = type;
			log->addr = next_addr;
			memcpy(log->data, next_addr, remain_size);

			flush_buffer(log, sizeof(log_entry), false);
			
			t->start_log->next_offset = t->start_log->next_offset + 1;
		}
//		mfence();
	}
}

static node *allocNode()
{
    void *ret;
	node *n;
	posix_memalign(&ret, 64, sizeof(node));
    n = (node *)ret;
	memset(n->slot,0,sizeof(n->slot));
	n->bitmap = 1;
	n->isleaf = 1;
	return n;
}

tree *initTree()
{
	tree *t = (tree *)malloc(sizeof(tree));
	t->root = allocNode();
    void *ret;
	posix_memalign(&ret, 64, sizeof(log_area));
    t->start_log = (log_area *)ret;
	t->start_log->next_offset = (log_entry *)t->start_log->log_data;
	return t;
}

void *Lookup(tree *t, unsigned long key)
{
    std::shared_lock<std::shared_mutex> lock(mutex);
	node *curr = t->root;
	curr = find_leaf_node(curr, key);
	int loc = Search(curr, curr->slot, key);

	if (loc > curr->slot[0]) 
		loc = curr->slot[0];

	if (curr->entries[curr->slot[loc]].key != key || loc > curr->slot[0])
		return NULL;

	return curr->entries[curr->slot[loc]].ptr;
}

void Range_Lookup(tree *t, unsigned long start_key, unsigned int num, 
		unsigned long buf[])
{
	int loc, i;
	unsigned long search_count = 0;
	struct timespec t1, t2;
	unsigned long elapsed_time;
	node *curr = t->root;

//	clock_gettime(CLOCK_MONOTONIC, &t1);
	curr = find_leaf_node(curr, start_key);
	loc = Search(curr, curr->slot, start_key);
//	printf("loc = %d\n", loc);
//	clock_gettime(CLOCK_MONOTONIC, &t2);
//	elapsed_time = (t2.tv_sec - t1.tv_sec) * 1000000000;
//	elapsed_time += (t2.tv_nsec - t1.tv_nsec);
//	printf("Range lookup 1 = %lu\n", elapsed_time);

//	clock_gettime(CLOCK_MONOTONIC, &t1);
	while (search_count < num) {
		for (i = loc; i <= curr->slot[0]; i++) {
			buf[search_count] = *(unsigned long *)curr->entries[curr->slot[i]].ptr;
			search_count++;
			if(search_count == num) {
//				clock_gettime(CLOCK_MONOTONIC, &t2);
//				elapsed_time = (t2.tv_sec - t1.tv_sec) * 1000000000;
//				elapsed_time += (t2.tv_nsec - t1.tv_nsec);
//				printf("Range lookup 2 = %lu\n", elapsed_time);
				return ;
			}
		}

		curr = curr->leftmostPtr;
		if (curr == NULL) {
			printf("error\n");
			return ;
		}
		loc = 1;
	}
//	clock_gettime(CLOCK_MONOTONIC, &t2);
//	elapsed_time = (t2.tv_sec - t1.tv_sec) * 1000000000;
//	elapsed_time += (t2.tv_nsec - t1.tv_nsec);
//	printf("Range lookup 2 = %lu\n", elapsed_time);
}

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
	if (tmp == ~0UL)	/* Are any bits zero? */
		return result + size;	/* Nope */
found_middle:
	return result + ffz(tmp);
}

static int Append(node *n, unsigned long key, void *value)
{
	int errval = -1;
	unsigned long index;

	index = find_next_zero_bit(&n->bitmap, BITMAP_SIZE, 1) - 1;
	if (index == BITMAP_SIZE - 1)
		return errval;

	n->entries[index].key = key;
	n->entries[index].ptr = value;
	return index;
}

static int Append_in_inner(node *n, unsigned long key, void *value)
{
	int errval = -1;
	unsigned long index;

	index = find_next_zero_bit(&n->bitmap, BITMAP_SIZE, 1) - 1;
	if (index == BITMAP_SIZE - 1)
		return errval;

	n->entries[index].key = key;
	n->entries[index].ptr = value;
	return index;
}

static int Search(node *curr, char *temp, unsigned long key)
{
	int low = 1, mid = 1;
	int high = temp[0];

	while (low <= high){
		mid = (low + high) / 2;
		if (curr->entries[temp[mid]].key > key)
			high = mid - 1;
		else if (curr->entries[temp[mid]].key < key)
			low = mid + 1;
		else
			break;
	}

	if (low > mid) 
		mid = low;

	return mid;
}

static node *find_leaf_node(node *curr, unsigned long key) 
{
	int loc;

	if (curr->isleaf) 
		return curr;
	loc = Search(curr, curr->slot, key);

	if (loc > curr->slot[0]) 
		return find_leaf_node((node *)curr->entries[curr->slot[loc - 1]].ptr, key);
	else if (curr->entries[curr->slot[loc]].key <= key) 
		return find_leaf_node((node *)curr->entries[curr->slot[loc]].ptr, key);
	else if (loc == 1) 
		return find_leaf_node((node *)curr->leftmostPtr, key);
	else 
		return find_leaf_node((node *)curr->entries[curr->slot[loc - 1]].ptr, key);
}


void Insert(tree *t, unsigned long key, void *value)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
	int numEntries;
	node *curr = t->root;
	/* Find proper leaf */
	curr = find_leaf_node(curr, key);

	/* Check overflow & split */
	numEntries = curr->slot[0];
	if(numEntries == NODE_SIZE) {
		add_log_entry(t, curr, sizeof(node), LE_DATA);
		node *splitNode = allocNode();
		int j, loc, cp = curr->slot[0];
		splitNode->leftmostPtr = curr->leftmostPtr;

		//overflown node
		for (j = MIN_LIVE_ENTRIES; j > 0; j--) {
			loc = Append(splitNode, curr->entries[curr->slot[cp]].key, 
					curr->entries[curr->slot[cp]].ptr);
			splitNode->slot[j] = loc;
			splitNode->slot[0]++;
			splitNode->bitmap = splitNode->bitmap + (0x1UL << (loc + 1));
			curr->bitmap = curr->bitmap & (~(0x1UL << (curr->slot[cp] + 1)));
			cp--;
		}

		curr->slot[0] -= MIN_LIVE_ENTRIES;

		if (splitNode->entries[splitNode->slot[1]].key > key) {
			loc = insert_in_leaf_noflush(curr, key, value);
			flush_buffer(&(curr->entries[loc]), sizeof(entry), false);
		}
		else
			insert_in_leaf_noflush(splitNode, key, value);

		insert_in_parent(t, curr, splitNode->entries[splitNode->slot[1]].key, splitNode);

		curr->leftmostPtr = splitNode;
		
		flush_buffer(curr->slot, (char *)curr->entries - (char *)curr->slot, false);
		flush_buffer(&curr->leftmostPtr, 8, false);

		add_log_entry(t, NULL, 0, LE_COMMIT);
	}
	else{
		insert_in_leaf(curr, key, value);
	}
}

static int insert_in_leaf_noflush(node *curr, unsigned long key, void *value)
{
	int loc, mid, j;

	curr->bitmap = curr->bitmap - 1;
	loc = Append(curr, key, value);

	mid = Search(curr, curr->slot, key);

	for (j = curr->slot[0]; j >= mid; j--)
		curr->slot[j + 1] = curr->slot[j];

	curr->slot[mid] = loc;

	curr->slot[0] = curr->slot[0] + 1;

	curr->bitmap = curr->bitmap + 1 + (0x1UL << (loc + 1));
	return loc;
}

static void insert_in_leaf(node *curr, unsigned long key, void *value)
{
	int loc, mid, j;

	curr->bitmap = curr->bitmap - 1;
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
	loc = Append(curr, key, value);
	flush_buffer(&(curr->entries[loc]), sizeof(entry), false);

	mid = Search(curr, curr->slot, key);

	for (j = curr->slot[0]; j >= mid; j--)
		curr->slot[j + 1] = curr->slot[j];

	curr->slot[mid] = loc;

	curr->slot[0] = curr->slot[0] + 1;
	flush_buffer(curr->slot, sizeof(curr->slot), false);
	
	curr->bitmap = curr->bitmap + 1 + (0x1UL << (loc + 1));
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
}

static void insert_in_inner(node *curr, unsigned long key, void *value)
{
	int loc, mid, j;

	curr->bitmap = curr->bitmap - 1;
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
	loc = Append_in_inner(curr, key, value);
	flush_buffer(&(curr->entries[loc]), sizeof(entry), false);

	mid = Search(curr, curr->slot, key);

	for (j = curr->slot[0]; j >= mid; j--)
		curr->slot[j + 1] = curr->slot[j];

	curr->slot[mid] = loc;

	curr->slot[0] = curr->slot[0] + 1;
	flush_buffer(curr->slot, sizeof(curr->slot), false);
	
	curr->bitmap = curr->bitmap + 1 + (0x1UL << (loc + 1));
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
}

static int insert_in_inner_noflush(node *curr, unsigned long key, void *value)
{
	int loc, mid, j;

	curr->bitmap = curr->bitmap - 1;
	loc = Append_in_inner(curr, key, value);

	mid = Search(curr, curr->slot, key);

	for (j = curr->slot[0]; j >= mid; j--)
		curr->slot[j + 1] = curr->slot[j];

	curr->slot[mid] = loc;

	curr->slot[0] = curr->slot[0] + 1;

	curr->bitmap = curr->bitmap + 1 + (0x1UL << (loc + 1));
	return loc;
}

static void insert_in_parent(tree *t, node *curr, unsigned long key, node *splitNode) {
	if (curr == t->root) {
		node *root = allocNode();
		root->isleaf = 0;
		root->leftmostPtr = curr;
		root->bitmap = root->bitmap + (0x1UL << 1);
		root->entries[0].ptr = splitNode;
		root->entries[0].key = key;
		splitNode->parent = root;

		root->slot[1] = 0;
		root->slot[0] = 1;
		flush_buffer(root, sizeof(node), false);
		flush_buffer(splitNode, sizeof(node), false);

		curr->parent = root;
		flush_buffer(&curr->parent, 8, false);

		t->root = root;
		return ;
	}

	node *parent = curr->parent;

	if (parent->slot[0] < NODE_SIZE) {
		int mid, j, loc;

		add_log_entry(t, parent->slot, (char *)parent->entries - (char *)parent->slot, LE_DATA);
		
		parent->bitmap = parent->bitmap - 1;

		loc = Append_in_inner(parent, key, splitNode);
		flush_buffer(&(parent->entries[loc]), sizeof(entry), false);

		splitNode->parent = parent;
		flush_buffer(splitNode, sizeof(node), false);

		mid = Search(parent, parent->slot, key);

		for (j = parent->slot[0]; j >= mid; j--)
			parent->slot[j + 1] = parent->slot[j];

		parent->slot[mid] = loc;
		parent->slot[0] = parent->slot[0] + 1;

		parent->bitmap = parent->bitmap + 1 + (0x1UL << (loc + 1));
		flush_buffer(parent->slot, (char *)parent->entries - (char *)parent->slot, false);
	} else {
		int j, loc, cp = parent->slot[0];
		node *splitParent = allocNode();
		splitParent->isleaf = 0;

		add_log_entry(t, parent, sizeof(node), LE_DATA);

		for (j = MIN_LIVE_ENTRIES; j > 0; j--) {
			loc = Append_in_inner(splitParent,parent->entries[parent->slot[cp]].key, parent->entries[parent->slot[cp]].ptr);
			node *child = (node *) parent->entries[parent->slot[cp]].ptr;
			add_log_entry(t, &child->parent, 8, LE_DATA);
			child->parent = splitParent;
			flush_buffer(&child->parent, 8, false);
			splitParent->slot[j] = loc;
			splitParent->slot[0]++;
			splitParent->bitmap = splitParent->bitmap + (0x1UL << (loc + 1));
			parent->bitmap = parent->bitmap & (~(0x1UL << (parent->slot[cp] + 1)));
			cp--;
		}

		parent->slot[0] -= MIN_LIVE_ENTRIES;

		if (splitParent->entries[splitParent->slot[1]].key > key) {
			loc = insert_in_inner_noflush(parent, key, splitNode);
			flush_buffer(&(parent->entries[loc]), sizeof(entry), false);
			splitNode->parent = parent;
			flush_buffer(splitNode, sizeof(node), false);
		}
		else {
			splitNode->parent = splitParent;
			flush_buffer(splitNode, sizeof(node), false);
			insert_in_inner_noflush(splitParent, key, splitNode);
		}

		flush_buffer(parent->slot, (char *)parent->entries - (char *)parent->slot, false);

		insert_in_parent(t, parent, 
				splitParent->entries[splitParent->slot[1]].key, splitParent);
	}
}

void *Update(tree *t, unsigned long key, void *value)
{
	node *curr = t->root;
	curr = find_leaf_node(curr, key);
	int loc = Search(curr, curr->slot, key);

	if (loc > curr->slot[0]) 
		loc = curr->slot[0];

	if (curr->entries[curr->slot[loc]].key != key || loc > curr->slot[0])
		return NULL;

	curr->entries[curr->slot[loc]].ptr = value;
	flush_buffer(&curr->entries[curr->slot[loc]].ptr, 8, true);

	return curr->entries[curr->slot[loc]].ptr;
}

static int delete_in_leaf(node *curr, unsigned long key)
{
	int mid, j;

	curr->bitmap = curr->bitmap - 1;
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);

	mid = Search(curr, curr->slot, key);

	for (j = curr->slot[0]; j > mid; j--)
		curr->slot[j - 1] = curr->slot[j];

	curr->slot[0] = curr->slot[0] - 1;

	flush_buffer(curr->slot, sizeof(curr->slot), false);
	
	curr->bitmap = curr->bitmap + 1 - (0x1UL << (mid + 1));
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
	return 0;
}

int Delete(tree *t, unsigned long key)
{
	int numEntries, errval = 0;
	node *curr = t->root;
	
	curr = find_leaf_node(curr, key);
/*
	numEntries = curr->slot[0];
	if (numEntries <= 1)
		errval = -1;
	else
		errval = delete_in_leaf(curr, key);
*/
	errval = delete_in_leaf(curr, key);

	return errval;
}
}
#else

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <x86intrin.h>
#include <malloc.h>
#include <stdint.h>
#include <time.h>
#include <mutex>
#include <shared_mutex>
#include "wbtree.h"

namespace wbtree {
static std::shared_mutex mutex;

#define BITS_PER_LONG		64

#define BITOP_WORD(nr)	((nr) / BITS_PER_LONG)

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

static int insert_in_inner_noflush(node *curr, key_item *inserted_item, void *value);
static int insert_in_leaf_noflush(node *curr, key_item *new_item, void *value);
static node *find_leaf_node(node *curr, unsigned char *key, int key_len);
static int Search(node *curr, char *temp, unsigned char *key, int key_len);
static void insert_in_leaf(node *curr, key_item *new_item, void *value);
static void insert_in_parent(tree *t, node *curr, key_item *inserted_item, node *splitNode);

static inline unsigned long ffz(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "r" (~word));
	return word;
}

static inline int max(int a, int b) {
	return (a > b) ? a : b;
}

static void add_log_entry(tree *t, void *addr, unsigned int size, unsigned char type)
{
	log_entry *log;
	int i, remain_size;

	remain_size = size - ((size / LOG_DATA_SIZE) * LOG_DATA_SIZE);

	if ((char *)t->start_log->next_offset == 
			(t->start_log->log_data + LOG_AREA_SIZE))
		t->start_log->next_offset = (log_entry *)t->start_log->log_data;

	if (size <= LOG_DATA_SIZE) {
		log = t->start_log->next_offset;
		log->size = size;
		log->type = type;
		log->addr = addr;
		memcpy(log->data, addr, size);

		if (type == LE_DATA)
			flush_buffer(log, sizeof(log_entry), false);
		else
			flush_buffer(log, sizeof(log_entry), true);

		t->start_log->next_offset = t->start_log->next_offset + 1;
	} else {
		void *next_addr = addr;

		for (i = 0; i < size / LOG_DATA_SIZE; i++) {
			log = t->start_log->next_offset;
			log->size = LOG_DATA_SIZE;
			log->type = type;
			log->addr = next_addr;
			memcpy(log->data, next_addr, LOG_DATA_SIZE);

			flush_buffer(log, sizeof(log_entry), false);

			t->start_log->next_offset = t->start_log->next_offset + 1;
			if ((char *)t->start_log->next_offset == 
					(t->start_log->log_data + LOG_AREA_SIZE))
				t->start_log->next_offset = (log_entry *)t->start_log->log_data;

			next_addr = (char *)next_addr + LOG_DATA_SIZE;
		}

		if (remain_size > 0) {
			log = t->start_log->next_offset;
			log->size = LOG_DATA_SIZE;
			log->type = type;
			log->addr = next_addr;
			memcpy(log->data, next_addr, remain_size);

			flush_buffer(log, sizeof(log_entry), false);

			t->start_log->next_offset = t->start_log->next_offset + 1;
		}
	}
}

static key_item *make_key_item(unsigned char *key, int key_len)
{
    void *ret;
	key_item *new_key;
    posix_memalign(&ret, 64, sizeof(key_item) + key_len);
    new_key = (key_item *)ret;
	new_key->key_len = key_len;
	memcpy(new_key->key, key, key_len);

	flush_buffer(new_key, sizeof(key_item) + key_len, false);

	return new_key;
}

static node *allocNode()
{
    void *ret;
	node *n;
    posix_memalign(&ret, 64, sizeof(node));
    n = (node *)ret;
	memset(n->slot,0,sizeof(n->slot));
	n->bitmap = 1;
	n->isleaf = 1;
	return n;
}

tree *initTree()
{
	tree *t = (tree *)malloc(sizeof(tree));
	t->root = allocNode();
    void *ret;
    posix_memalign(&ret, 64, sizeof(log_area));
	t->start_log = (log_area *)ret;
	t->start_log->next_offset = (log_entry *)t->start_log->log_data;
	return t;
}

void *Lookup(tree *t, unsigned char *key, int key_len)
{
    std::shared_lock<std::shared_mutex> lock(mutex);
	node *curr = t->root;
	curr = find_leaf_node(curr, key, key_len);
	int loc = Search(curr, curr->slot, key, key_len);

	if (loc > curr->slot[0]) 
		loc = curr->slot[0];

	if (key_len != (curr->entries[curr->slot[loc]].key)->key_len)
		return NULL;

	if (memcmp((curr->entries[curr->slot[loc]].key)->key, key, key_len) != 0 || loc > curr->slot[0])
		return NULL;

	return curr->entries[curr->slot[loc]].ptr;
}

/*
void Range_Lookup(tree *t, unsigned long start_key, unsigned int num, 
		unsigned long buf[])
{
	int loc, i;
	unsigned long search_count = 0;
	struct timespec t1, t2;
	unsigned long elapsed_time;
	node *curr = t->root;

//	clock_gettime(CLOCK_MONOTONIC, &t1);
	curr = find_leaf_node(curr, start_key);
	loc = Search(curr, curr->slot, start_key);
//	printf("loc = %d\n", loc);
//	clock_gettime(CLOCK_MONOTONIC, &t2);
//	elapsed_time = (t2.tv_sec - t1.tv_sec) * 1000000000;
//	elapsed_time += (t2.tv_nsec - t1.tv_nsec);
//	printf("Range lookup 1 = %lu\n", elapsed_time);

//	clock_gettime(CLOCK_MONOTONIC, &t1);
	while (search_count < num) {
		for (i = loc; i <= curr->slot[0]; i++) {
			buf[search_count] = *(unsigned long *)curr->entries[curr->slot[i]].ptr;
			search_count++;
			if(search_count == num) {
//				clock_gettime(CLOCK_MONOTONIC, &t2);
//				elapsed_time = (t2.tv_sec - t1.tv_sec) * 1000000000;
//				elapsed_time += (t2.tv_nsec - t1.tv_nsec);
//				printf("Range lookup 2 = %lu\n", elapsed_time);
				return ;
			}
		}

		curr = curr->leftmostPtr;
		if (curr == NULL) {
			printf("error\n");
			return ;
		}
		loc = 1;
	}
//	clock_gettime(CLOCK_MONOTONIC, &t2);
//	elapsed_time = (t2.tv_sec - t1.tv_sec) * 1000000000;
//	elapsed_time += (t2.tv_nsec - t1.tv_nsec);
//	printf("Range lookup 2 = %lu\n", elapsed_time);
}
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
	if (tmp == ~0UL)	/* Are any bits zero? */
		return result + size;	/* Nope */
found_middle:
	return result + ffz(tmp);
}

static int Append(node *n, key_item *new_item, void *value)
{
	int errval = -1;
	unsigned long index;

	index = find_next_zero_bit(&n->bitmap, BITMAP_SIZE, 1) - 1;
	if (index == BITMAP_SIZE - 1)
		return errval;

	n->entries[index].key = new_item;
	n->entries[index].ptr = value;
	return index;
}

static int Append_in_inner(node *n, key_item *inserted_item, void *value)
{
	int errval = -1;
	unsigned long index;

	index = find_next_zero_bit(&n->bitmap, BITMAP_SIZE, 1) - 1;
	if (index == BITMAP_SIZE - 1)
		return errval;

	n->entries[index].key = inserted_item;
	n->entries[index].ptr = value;
	return index;
}

static int Search(node *curr, char *temp, unsigned char *key, int key_len)
{
	int low = 1, mid = 1;
	int high = temp[0];
	int len, decision;

	while (low <= high){
		mid = (low + high) / 2;
		len = max((curr->entries[temp[mid]].key)->key_len, key_len);
		decision = memcmp((curr->entries[temp[mid]].key)->key, key, len);
		if (decision > 0)
			high = mid - 1;
		else if (decision < 0)
			low = mid + 1;
		else
			break;
	}

	if (low > mid)
		mid = low;

	return mid;
}

static node *find_leaf_node(node *curr, unsigned char *key, int key_len) 
{
	int loc;

	if (curr->isleaf) 
		return curr;
	loc = Search(curr, curr->slot, key, key_len);

	if (loc > curr->slot[0]) 
		return find_leaf_node((node *)curr->entries[curr->slot[loc - 1]].ptr, key, key_len);
	else if (memcmp((curr->entries[curr->slot[loc]].key)->key, key, max((curr->entries[curr->slot[loc]].key)->key_len, key_len)) <= 0) 
		return find_leaf_node((node *)curr->entries[curr->slot[loc]].ptr, key, key_len);
	else if (loc == 1) 
		return find_leaf_node((node *)curr->leftmostPtr, key, key_len);
	else 
		return find_leaf_node((node *)curr->entries[curr->slot[loc - 1]].ptr, key, key_len);
}

void Insert(tree *t, unsigned char *key, int key_len, void *value)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
	int numEntries;
	node *curr = t->root;
	/* Find proper leaf */
	curr = find_leaf_node(curr, key, key_len);

	/* Make new key item */
	key_item *new_item = make_key_item(key, key_len);

	/* Check overflow & split */
	numEntries = curr->slot[0];
	if(numEntries == NODE_SIZE) {
		add_log_entry(t, curr, sizeof(node), LE_DATA);
		node *splitNode = allocNode();
		int len, j, loc, cp = curr->slot[0];
		splitNode->leftmostPtr = curr->leftmostPtr;

		//overflown node
		for (j = MIN_LIVE_ENTRIES; j > 0; j--) {
			loc = Append_in_inner(splitNode, curr->entries[curr->slot[cp]].key, 
					curr->entries[curr->slot[cp]].ptr);
			splitNode->slot[j] = loc;
			splitNode->slot[0]++;
			splitNode->bitmap = splitNode->bitmap + (0x1UL << (loc + 1));
			curr->bitmap = curr->bitmap & (~(0x1UL << (curr->slot[cp] + 1)));
			cp--;
		}

		curr->slot[0] -= MIN_LIVE_ENTRIES;

		len = max((splitNode->entries[splitNode->slot[1]].key)->key_len, key_len);
		if (memcmp((splitNode->entries[splitNode->slot[1]].key)->key, key, len) > 0) {
			loc = insert_in_leaf_noflush(curr, new_item, value);
			flush_buffer(&(curr->entries[loc]), sizeof(entry), false);
		}
		else
			insert_in_leaf_noflush(splitNode, new_item, value);

		insert_in_parent(t, curr, splitNode->entries[splitNode->slot[1]].key, splitNode);

		curr->leftmostPtr = splitNode;
		
		flush_buffer(curr->slot, (char *)curr->entries - (char *)curr->slot, false);
		flush_buffer(&curr->leftmostPtr, 8, false);

		add_log_entry(t, NULL, 0, LE_COMMIT);
	}
	else{
		insert_in_leaf(curr, new_item, value);
	}
}

static int insert_in_leaf_noflush(node *curr, key_item *new_item, void *value)
{
	int loc, mid, j;

	curr->bitmap = curr->bitmap - 1;
	loc = Append(curr, new_item, value);

	mid = Search(curr, curr->slot, new_item->key, new_item->key_len);

	for (j = curr->slot[0]; j >= mid; j--)
		curr->slot[j + 1] = curr->slot[j];

	curr->slot[mid] = loc;

	curr->slot[0] = curr->slot[0] + 1;

	curr->bitmap = curr->bitmap + 1 + (0x1UL << (loc + 1));
	return loc;
}

static void insert_in_leaf(node *curr, key_item *new_item, void *value)
{
	int loc, mid, j;

	curr->bitmap = curr->bitmap - 1;
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
	loc = Append(curr, new_item, value);
	flush_buffer(&(curr->entries[loc]), sizeof(entry), false);

	mid = Search(curr, curr->slot, new_item->key, new_item->key_len);

	for (j = curr->slot[0]; j >= mid; j--)
		curr->slot[j + 1] = curr->slot[j];

	curr->slot[mid] = loc;

	curr->slot[0] = curr->slot[0] + 1;
	flush_buffer(curr->slot, sizeof(curr->slot), false);
	
	curr->bitmap = curr->bitmap + 1 + (0x1UL << (loc + 1));
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
}

/*
void insert_in_inner(node *curr, unsigned long key, void *value)
{
	int loc, mid, j;

	curr->bitmap = curr->bitmap - 1;
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
	loc = Append_in_inner(curr, key, value);
	flush_buffer(&(curr->entries[loc]), sizeof(entry), false);

	mid = Search(curr, curr->slot, key);

	for (j = curr->slot[0]; j >= mid; j--)
		curr->slot[j + 1] = curr->slot[j];

	curr->slot[mid] = loc;

	curr->slot[0] = curr->slot[0] + 1;
	flush_buffer(curr->slot, sizeof(curr->slot), false);
	
	curr->bitmap = curr->bitmap + 1 + (0x1UL << (loc + 1));
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
}
*/

static int insert_in_inner_noflush(node *curr, key_item *inserted_item, void *value)
{
	int loc, mid, j;

	curr->bitmap = curr->bitmap - 1;
	loc = Append_in_inner(curr, inserted_item, value);

	mid = Search(curr, curr->slot, inserted_item->key, inserted_item->key_len);

	for (j = curr->slot[0]; j >= mid; j--)
		curr->slot[j + 1] = curr->slot[j];

	curr->slot[mid] = loc;

	curr->slot[0] = curr->slot[0] + 1;

	curr->bitmap = curr->bitmap + 1 + (0x1UL << (loc + 1));
	return loc;
}

static void insert_in_parent(tree *t, node *curr, key_item *inserted_item, node *splitNode) {
	if (curr == t->root) {
		node *root = allocNode();
		root->isleaf = 0;
		root->leftmostPtr = curr;
		root->bitmap = root->bitmap + (0x1UL << 1);
		root->entries[0].ptr = splitNode;
		root->entries[0].key = inserted_item;
		splitNode->parent = root;

		root->slot[1] = 0;
		root->slot[0] = 1;
		flush_buffer(root, sizeof(node), false);
		flush_buffer(splitNode, sizeof(node), false);

		curr->parent = root;
		flush_buffer(&curr->parent, 8, false);

		t->root = root;
		return ;
	}

	node *parent = curr->parent;

	if (parent->slot[0] < NODE_SIZE) {
		int mid, j, loc;

		add_log_entry(t, parent->slot, (char *)parent->entries - (char *)parent->slot, LE_DATA);
		
		parent->bitmap = parent->bitmap - 1;

		loc = Append_in_inner(parent, inserted_item, splitNode);
		flush_buffer(&(parent->entries[loc]), sizeof(entry), false);

		splitNode->parent = parent;
		flush_buffer(splitNode, sizeof(node), false);

		mid = Search(parent, parent->slot, inserted_item->key, inserted_item->key_len);

		for (j = parent->slot[0]; j >= mid; j--)
			parent->slot[j + 1] = parent->slot[j];

		parent->slot[mid] = loc;
		parent->slot[0] = parent->slot[0] + 1;

		parent->bitmap = parent->bitmap + 1 + (0x1UL << (loc + 1));
		flush_buffer(parent->slot, (char *)parent->entries - (char *)parent->slot, false);
	} else {
		int len, j, loc, cp = parent->slot[0];
		node *splitParent = allocNode();
		splitParent->isleaf = 0;

		add_log_entry(t, parent, sizeof(node), LE_DATA);

		for (j = MIN_LIVE_ENTRIES; j > 0; j--) {
			loc = Append_in_inner(splitParent, parent->entries[parent->slot[cp]].key, parent->entries[parent->slot[cp]].ptr);
			node *child = (node *)parent->entries[parent->slot[cp]].ptr;
			add_log_entry(t, &child->parent, 8, LE_DATA);
			child->parent = splitParent;
			flush_buffer(&child->parent, 8, false);
			splitParent->slot[j] = loc;
			splitParent->slot[0]++;
			splitParent->bitmap = splitParent->bitmap + (0x1UL << (loc + 1));
			parent->bitmap = parent->bitmap & (~(0x1UL << (parent->slot[cp] + 1)));
			cp--;
		}

		parent->slot[0] -= MIN_LIVE_ENTRIES;

		len = max((splitParent->entries[splitParent->slot[1]].key)->key_len, inserted_item->key_len);
		if (memcmp((splitParent->entries[splitParent->slot[1]].key)->key, inserted_item->key, len) > 0) {
			loc = insert_in_inner_noflush(parent, inserted_item, splitNode);
			flush_buffer(&(parent->entries[loc]), sizeof(entry), false);
			splitNode->parent = parent;
			flush_buffer(splitNode, sizeof(node), false);
		}
		else {
			splitNode->parent = splitParent;
			flush_buffer(splitNode, sizeof(node), false);
			insert_in_inner_noflush(splitParent, inserted_item, splitNode);
		}

		flush_buffer(parent->slot, (char *)parent->entries - (char *)parent->slot, false);

		insert_in_parent(t, parent, 
				splitParent->entries[splitParent->slot[1]].key, splitParent);
	}
}

void *Update(tree *t, unsigned char *key, int key_len, void *value)
{
	node *curr = t->root;
	curr = find_leaf_node(curr, key, key_len);
	int loc = Search(curr, curr->slot, key, key_len);

	if (loc > curr->slot[0]) 
		loc = curr->slot[0];

	if (key_len != (curr->entries[curr->slot[loc]].key)->key_len)
		return NULL;

	if (memcmp((curr->entries[curr->slot[loc]].key)->key, key, key_len) != 0 || loc > curr->slot[0])
		return NULL;

	curr->entries[curr->slot[loc]].ptr = value;
	flush_buffer(&curr->entries[curr->slot[loc]].ptr, 8, true);

	return curr->entries[curr->slot[loc]].ptr;
}

/*
int delete_in_leaf(node *curr, unsigned long key)
{
	int mid, j;

	curr->bitmap = curr->bitmap - 1;
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);

	mid = Search(curr, curr->slot, key);

	for (j = curr->slot[0]; j > mid; j--)
		curr->slot[j - 1] = curr->slot[j];

	curr->slot[0] = curr->slot[0] - 1;

	flush_buffer(curr->slot, sizeof(curr->slot), false);
	
	curr->bitmap = curr->bitmap + 1 - (0x1UL << (mid + 1));
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
	return 0;
}

int Delete(tree *t, unsigned long key)
{
	int numEntries, errval = 0;
	node *curr = t->root;
	
	curr = find_leaf_node(curr, key);

//	numEntries = curr->slot[0];
//	if (numEntries <= 1)
//		errval = -1;
//	else
//		errval = delete_in_leaf(curr, key);

	errval = delete_in_leaf(curr, key);

	return errval;
}
*/
}
#endif
