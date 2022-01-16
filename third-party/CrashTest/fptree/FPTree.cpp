#ifndef STRING_TYPE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <x86intrin.h>
#include <malloc.h>
#include <stdint.h>
#include <time.h>
#include <immintrin.h>
#include <tbb/spin_mutex.h>
#include "FPTree.h"

namespace fptree {

tbb::speculative_spin_mutex mutex;

static const unsigned long BITMAP_SIZE = NUM_LN_ENTRY;

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
            asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(but+i)));
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

void add_log_entry(tree *t, void *addr, unsigned int size, unsigned char type)
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

size_t off = 0;
void *malloc_aligned(size_t alignment, size_t bytes)
{
    // we need to allocate enough storage for the requested bytes, some 
    // book-keeping (to store the location returned by malloc) and some extra
    // padding to allow us to find an aligned byte.  im not entirely sure if 
    // 2 * alignment is enough here, its just a guess.
    const size_t total_size = bytes + (2 * alignment) + sizeof(size_t);

    // use malloc to allocate the memory.
    char *data = (char *)malloc(total_size);
    if (data)
    {
        // store the original start of the malloc'd data.
        const void * const data_start = data;

        // dedicate enough space to the book-keeping.
        data += sizeof(size_t);

        // find a memory location with correct alignment.  the alignment minus 
        // the remainder of this mod operation is how many bytes forward we need 
        // to move to find an aligned byte.
        const size_t offset = alignment - (((size_t)data) % alignment);

        // set data to the aligned memory.
        data += offset;

        // write the book-keeping.
        size_t *book_keeping = (size_t*)(data - sizeof(size_t));
        *book_keeping = (size_t)data_start;
    }

    return data;
}

LN *allocLNode()
{
	LN *node;
	posix_memalign((void **)&node, 64, sizeof(LN));

	node->type = THIS_LN;
	node->bitmap = 0;
	return node;
}

IN *allocINode()
{
	IN *node;
	posix_memalign((void **)&node, 64, sizeof(IN));
	node->type = THIS_IN;
	node->nKeys = 0;
	return node;
}

tree *initTree()
{
  tree *t = (tree *)malloc(sizeof(tree)); 
  t->root = allocLNode();
  ((LN *)t->root)->pNext = NULL;
  posix_memalign((void **)&(t->start_log), 64, sizeof(log_area));
  t->start_log->next_offset = (log_entry *)t->start_log->log_data;
  return t;
}

unsigned char hash(unsigned long key) {
	unsigned char hash_key = key % 256;
	return hash_key;
}

void insertion_sort(entry *base, int num)
{
	int i, j;
	entry temp;

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

/*
 * Find the next set bit in a memory region.
 */
unsigned long find_next_bit(const unsigned long *addr, unsigned long size,
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
 * Find the next zero bit in a memory region.
 */
unsigned long find_next_zero_bit(const unsigned long *addr, unsigned long size,
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

void *Lookup(tree *t, unsigned long key)
{
  void *value = NULL;
  while(true) {
    tbb::speculative_spin_mutex::scoped_lock speculative_lock(mutex);

    unsigned long loc = 0;
    LN *curr = (LN *)find_leaf_node(t->root, key);
    if(curr->lock == 1) {
      _xabort(0xff);
      continue;
    }

    while (loc < NUM_LN_ENTRY) {
      loc = find_next_bit(&curr->bitmap, BITMAP_SIZE, loc);
      if (loc == BITMAP_SIZE)
        break;

      if (curr->fingerprints[loc] == hash(key) &&
          curr->entries[loc].key == key) {
        value = curr->entries[loc].ptr;
        break;
      }
      loc++;
    }

    speculative_lock.release();
  	return value;
  }
}

void Range_Lookup(tree *t, unsigned long start_key, unsigned int num, 
		unsigned long buf[])
{
	unsigned long i, entry_num, loc, search_count = 0;
	LN *curr = (LN *)(t->root);
	entry *sorted_entry = (entry *)malloc(NUM_LN_ENTRY * sizeof(entry));
	curr = (LN *)find_leaf_node((void *)curr, start_key);

	while (curr != NULL) {
		loc = 0;
		entry_num = 0;

		while (loc < NUM_LN_ENTRY) {
			loc = find_next_bit(&curr->bitmap, BITMAP_SIZE, loc);
			if (loc == BITMAP_SIZE)
				break;

			sorted_entry[entry_num] = curr->entries[loc];
			loc++;
			entry_num++;
		}
		insertion_sort(sorted_entry, entry_num);

		for (i = 0; i < entry_num; i++) {
			buf[search_count] = *(unsigned long *)sorted_entry[i].ptr;
			search_count++;
			if (search_count == num)
				return ;
		}
		curr = curr->pNext;
	}
}

int Search(IN *curr, unsigned long key)
{
	int low = 0, mid = 0;
	int high = curr->nKeys - 1;

	while (low <= high){
		mid = (low + high) / 2;
		if (curr->keys[mid] > key)
			high = mid - 1;
		else if (curr->keys[mid] < key)
			low = mid + 1;
		else
			break;
	}

	if (low > mid) 
		mid = low;

	return mid;
}

void *find_leaf_node(void *curr, unsigned long key) 
{
	unsigned long loc;

	if (((LN *)curr)->type == THIS_LN) 
		return curr;
	loc = Search((IN *)curr, key);

	if (loc > ((IN *)curr)->nKeys - 1) 
		return find_leaf_node(((IN *)curr)->ptr[loc - 1], key);
	else if (((IN *)curr)->keys[loc] <= key) 
		return find_leaf_node(((IN *)curr)->ptr[loc], key);
	else if (loc == 0) 
		return find_leaf_node(((IN *)curr)->leftmostPtr, key);
	else 
		return find_leaf_node(((IN *)curr)->ptr[loc - 1], key);
}

void Insert(tree *t, unsigned long key, void *value)
{
  bool should_split = false;
  LN *curr = (LN *)(t->root);
  while(true) {
    tbb::speculative_spin_mutex::scoped_lock speculative_lock(mutex);

    /* Find proper leaf */
    curr = (LN *)find_leaf_node((LN *)(t->root), key);

    if(curr->lock == 1) {
      continue;
    }

    curr->lock = 1;
    should_split = (curr->bitmap == IS_FULL);

    speculative_lock.release();
    break;
  }

	/* Check overflow & split */
	if(should_split) {
		int j;
		LN *split_LNode = allocLNode();
		entry *valid_entry = (entry *)malloc(NUM_LN_ENTRY * sizeof(entry));

		add_log_entry(t, curr, sizeof(LN), LE_DATA);

		split_LNode->pNext = curr->pNext;

		for (j = 0; j < NUM_LN_ENTRY; j++)
			valid_entry[j] = curr->entries[j];

		insertion_sort(valid_entry, NUM_LN_ENTRY);

		curr->bitmap = 0;
		for (j = 0; j < MIN_LN_ENTRIES; j++)
			insert_in_leaf_noflush(curr, valid_entry[j].key,
					valid_entry[j].ptr);

		for (j = MIN_LN_ENTRIES; j < NUM_LN_ENTRY; j++)
			insert_in_leaf_noflush(split_LNode, valid_entry[j].key,
					valid_entry[j].ptr);

		free(valid_entry);

		if (split_LNode->entries[0].key > key) {
			insert_in_leaf_noflush(curr, key, value);
		} else
			insert_in_leaf_noflush(split_LNode, key, value);

		curr->pNext = split_LNode;

		flush_buffer(curr, sizeof(LN), false);
		flush_buffer(split_LNode, sizeof(LN), false);

    tbb::speculative_spin_mutex::scoped_lock speculative_lock(mutex);
		insert_in_parent(t, curr, split_LNode->entries[0].key, split_LNode);
    speculative_lock.release();

		add_log_entry(t, NULL, 0, LE_COMMIT);
	}
	else{
		insert_in_leaf(curr, key, value);
	}
  curr->lock = 0;
}

int insert_in_leaf_noflush(LN *curr, unsigned long key, void *value)
{
	int errval = -1;
	unsigned long index;
	index = find_next_zero_bit(&curr->bitmap, BITMAP_SIZE, 0);
	if (index == BITMAP_SIZE)
		return errval;

	curr->entries[index].key = key;
	curr->entries[index].ptr = value;
	curr->fingerprints[index] = hash(key);
	curr->bitmap = curr->bitmap + (0x1UL << index);
	return index;
}

void insert_in_leaf(LN *curr, unsigned long key, void *value)
{
	unsigned long index;
	index = find_next_zero_bit(&curr->bitmap, BITMAP_SIZE, 0);
	if (index == BITMAP_SIZE)
		return ;

	curr->entries[index].key = key;
	curr->entries[index].ptr = value;
	flush_buffer(&curr->entries[index], sizeof(entry), false);
	curr->fingerprints[index] = hash(key);
	flush_buffer(&curr->fingerprints[index], sizeof(unsigned char), false);
	curr->bitmap = curr->bitmap + (0x1UL << index);
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
}

void insert_in_inner(IN *curr, unsigned long key, void *child)
{
	int loc, mid, j;

	mid = Search(curr, key);

	for (j = (curr->nKeys - 1); j >= mid; j--) {
		curr->keys[j + 1] = curr->keys[j];
		curr->ptr[j + 1] = curr->ptr[j];
	}

	curr->keys[mid] = key;
	curr->ptr[mid] = child;

	curr->nKeys++;
}

void insert_in_parent(tree *t, void *curr, unsigned long key, void *splitNode) {
	if (curr == t->root) {
		IN *root = allocINode();
		root->leftmostPtr = curr;
		root->keys[0] = key;
		root->ptr[0] = splitNode;
		root->nKeys++;

		((IN *)splitNode)->parent = root;
		((IN *)curr)->parent = root;
		t->root = root;
		return ;
	}

	IN *parent;

	if (((IN *)curr)->type == THIS_IN)
		parent = ((IN *)curr)->parent;
	else
		parent = ((LN *)curr)->parent;

	if (parent->nKeys < NUM_IN_ENTRY) {
		insert_in_inner(parent, key, splitNode);
		((IN *)splitNode)->parent = parent;
	} else {
		int i, j, loc, parent_nKeys;
		IN *split_INode = allocINode();
		parent_nKeys = parent->nKeys;

		for (j = MIN_IN_ENTRIES, i = 0; j < parent_nKeys; j++, i++) {
			split_INode->keys[i] = parent->keys[j];
			split_INode->ptr[i] = parent->ptr[j];
			((IN *)split_INode->ptr[i])->parent = split_INode;
			split_INode->nKeys++;
			parent->nKeys--;
		}

		if (split_INode->keys[0] > key) {
			insert_in_inner(parent, key, splitNode);
			((IN *)splitNode)->parent = parent;
		}
		else {
			((IN *)splitNode)->parent = split_INode;
			insert_in_inner(split_INode, key, splitNode);
		}

		insert_in_parent(t, parent, split_INode->keys[0], split_INode);
	}
}


void *Update(tree *t, unsigned long key, void *value)
{
	unsigned long loc = 0;
	LN *curr = (LN *)(t->root);
	curr = (LN *)find_leaf_node((void *)curr, key);

	while (loc < NUM_LN_ENTRY) {
		loc = find_next_bit(&curr->bitmap, BITMAP_SIZE, loc);
		if (loc == BITMAP_SIZE)
			break;
		
		if (curr->fingerprints[loc] == hash(key) &&
				curr->entries[loc].key == key) {
			curr->entries[loc].ptr = value;
			flush_buffer(&curr->entries[loc].ptr, 8, true);
			return curr->entries[loc].ptr;
		}
		loc++;
	}

	return NULL;
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

	flush_buffer(curr->slot, sizeof(curr->slot), true);
	
	curr->bitmap = curr->bitmap + 1 - (0x1UL << (mid + 1));
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
	return 0;
}

int Delete(tree *t, unsigned long key)
{
	int numEntries, errval = 0;
	node *curr = t->root;
	
	curr = find_leaf_node(curr, key);

	numEntries = curr->slot[0];
	if (numEntries <= 1)
		errval = -1;
	else
		errval = delete_in_leaf(curr, key);

	return errval;
}
*/
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
#include <immintrin.h>
#include <tbb/spin_mutex.h>
#include "FPTree.h"

namespace fptree {
tbb::speculative_spin_mutex mutex;

static const unsigned long BITMAP_SIZE = NUM_LN_ENTRY;

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

static inline int max(int a, int b) {
    return (a > b) ? a : b;
}

void add_log_entry(tree *t, void *addr, unsigned int size, unsigned char type)
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

key_item *make_key_item(unsigned char *key, int key_len)
{
	key_item *new_key;
    posix_memalign((void **)&new_key, 64, (sizeof(key_item) + key_len));
	new_key->key_len = key_len;
	memcpy(new_key->key, key, key_len);

	flush_buffer(new_key, sizeof(key_item) + key_len, false);

	return new_key;
}

LN *allocLNode()
{
	LN *node;
    posix_memalign((void **)&node, 64, sizeof(LN));
	node->type = THIS_LN;
	node->bitmap = 0;
	return node;
}

IN *allocINode()
{
	IN *node;
    posix_memalign((void **)&node, 64, sizeof(IN));
	node->type = THIS_IN;
	node->nKeys = 0;
	return node;
}

tree *initTree()
{
	tree *t =(tree *)malloc(sizeof(tree)); 
	t->root = allocLNode();
	((LN *)t->root)->pNext = NULL;
    posix_memalign((void **)&(t->start_log), 64, sizeof(log_area));
	t->start_log->next_offset = (log_entry *)t->start_log->log_data;
	return t;
}

uint32_t jenkins_hash(const void *key, size_t length);

unsigned char hash(unsigned char *key, int key_len) {
	uint32_t jenkins = jenkins_hash(key, key_len);
	unsigned char hash_key = jenkins % 256;
	return hash_key;
}

void insertion_sort(entry *base, int num)
{
	int i, j;
	entry temp;

	for (i = 1; i < num; i++) {
		for (j = i; j > 0; j--) {
			if (memcmp((base[j - 1].key)->key, (base[j].key)->key, max((base[j - 1].key)->key_len, (base[j].key)->key_len)) > 0) {
				temp = base[j - 1];
				base[j - 1] = base[j];
				base[j] = temp;
			} else
				break;
		}
	}
}

/*
 * Find the next set bit in a memory region.
 */
unsigned long find_next_bit(const unsigned long *addr, unsigned long size,
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
 * Find the next zero bit in a memory region.
 */
unsigned long find_next_zero_bit(const unsigned long *addr, unsigned long size,
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

void *Lookup(tree *t, unsigned char *key, int key_len)
{
    void *value = NULL;
    while (true) {
        tbb::speculative_spin_mutex::scoped_lock speculative_lock(mutex);

        unsigned long loc = 0;
        LN *curr = (LN *)find_leaf_node(t->root, key, key_len);
        if (curr->lock == 1) {
            _xabort(0xff);
            continue;
        }

        while (loc < NUM_LN_ENTRY) {
            loc = find_next_bit(&curr->bitmap, BITMAP_SIZE, loc);
            if (loc == BITMAP_SIZE)
                break;

            if (curr->fingerprints[loc] == hash(key, key_len) &&
                    memcmp((curr->entries[loc].key)->key, key, 
                        max((curr->entries[loc].key)->key_len, key_len)) == 0) {
                value = curr->entries[loc].ptr;
                break;
            }
            loc++;
        }

        speculative_lock.release();
        return value;
    }
}
/*
void Range_Lookup(tree *t, unsigned long start_key, unsigned int num, 
		unsigned long buf[])
{
	unsigned long i, entry_num, loc, search_count = 0;
	LN *curr = t->root;
	entry *sorted_entry = malloc(NUM_LN_ENTRY * sizeof(entry));
	curr = find_leaf_node(curr, start_key);

	while (curr != NULL) {
		loc = 0;
		entry_num = 0;

		while (loc < NUM_LN_ENTRY) {
			loc = find_next_bit(&curr->bitmap, BITMAP_SIZE, loc);
			if (loc == BITMAP_SIZE)
				break;

			sorted_entry[entry_num] = curr->entries[loc];
			loc++;
			entry_num++;
		}
		insertion_sort(sorted_entry, entry_num);

		for (i = 0; i < entry_num; i++) {
			buf[search_count] = *(unsigned long *)sorted_entry[i].ptr;
			search_count++;
			if (search_count == num)
				return ;
		}
		curr = curr->pNext;
	}
}
*/
int Search(IN *curr, unsigned char *key, int key_len)
{
	int low = 0, mid = 0;
	int high = curr->nKeys - 1;
	int len, decision;

	while (low <= high){
		mid = (low + high) / 2;
		len = max((curr->keys[mid])->key_len, key_len);
		decision = memcmp((curr->keys[mid])->key, key, len);
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

void *find_leaf_node(void *curr, unsigned char *key, int key_len) 
{
	int loc;

	if (((LN *)curr)->type == THIS_LN) 
		return curr;
	loc = Search((IN *)curr, key, key_len);

	if (loc > ((IN *)curr)->nKeys - 1) 
		return find_leaf_node(((IN *)curr)->ptr[loc - 1], key, key_len);
	else if (memcmp((((IN *)curr)->keys[loc])->key, key, max((((IN *)curr)->keys[loc])->key_len, key_len)) <= 0)
		return find_leaf_node(((IN *)curr)->ptr[loc], key, key_len);
	else if (loc == 0) 
		return find_leaf_node(((IN *)curr)->leftmostPtr, key, key_len);
	else 
		return find_leaf_node(((IN *)curr)->ptr[loc - 1], key, key_len);
}


void Insert(tree *t, unsigned char *key, int key_len, void *value)
{
    bool should_split = false;
	LN *curr = (LN *)(t->root);
    while (true) {
        tbb::speculative_spin_mutex::scoped_lock speculative_lock(mutex);

        /* Find proper leaf */
        curr = (LN *)find_leaf_node((LN *)(t->root), key, key_len);

        if (curr->lock == 1) {
            continue;
        }

        curr->lock = 1;
        should_split = (curr->bitmap == IS_FULL);

        speculative_lock.release();
        break;
    }

	/* Make new key item */
	key_item *new_item = make_key_item(key, key_len);

	/* Check overflow & split */
	if(should_split) {
		int j;
		LN *split_LNode = allocLNode();
		entry *valid_entry = (entry *)malloc(NUM_LN_ENTRY * sizeof(entry));

		add_log_entry(t, curr, sizeof(LN), LE_DATA);

		split_LNode->pNext = curr->pNext;

		for (j = 0; j < NUM_LN_ENTRY; j++)
			valid_entry[j] = curr->entries[j];

		insertion_sort(valid_entry, NUM_LN_ENTRY);

		curr->bitmap = 0;
		for (j = 0; j < MIN_LN_ENTRIES; j++)
			insert_in_leaf_noflush(curr, valid_entry[j].key,
					valid_entry[j].ptr);

		for (j = MIN_LN_ENTRIES; j < NUM_LN_ENTRY; j++)
			insert_in_leaf_noflush(split_LNode, valid_entry[j].key,
					valid_entry[j].ptr);

		free(valid_entry);

		if (memcmp((split_LNode->entries[0].key)->key, new_item->key, max((split_LNode->entries[0].key)->key_len, key_len)) > 0) {
			insert_in_leaf_noflush(curr, new_item, value);
		} else
			insert_in_leaf_noflush(split_LNode, new_item, value);

		curr->pNext = split_LNode;

		flush_buffer(curr, sizeof(LN), false);
		flush_buffer(split_LNode, sizeof(LN), false);

    tbb::speculative_spin_mutex::scoped_lock speculative_lock(mutex);
		insert_in_parent(t, curr, split_LNode->entries[0].key, split_LNode);
    speculative_lock.release();

		add_log_entry(t, NULL, 0, LE_COMMIT);
	}
	else{
		insert_in_leaf(curr, new_item, value);
	}

    curr->lock = 0;
}

int insert_in_leaf_noflush(LN *curr, key_item *new_item, void *value)
{
	int errval = -1;
	unsigned long index;
	index = find_next_zero_bit(&curr->bitmap, BITMAP_SIZE, 0);
	if (index == BITMAP_SIZE)
		return errval;

	curr->entries[index].key = new_item;
	curr->entries[index].ptr = value;
	curr->fingerprints[index] = hash(new_item->key, new_item->key_len);
	curr->bitmap = curr->bitmap + (0x1UL << index);
	return index;
}

void insert_in_leaf(LN *curr, key_item *new_item, void *value)
{
	unsigned long index;
	index = find_next_zero_bit(&curr->bitmap, BITMAP_SIZE, 0);
	if (index == BITMAP_SIZE)
		return ;

	curr->entries[index].key = new_item;
	curr->entries[index].ptr = value;
	curr->fingerprints[index] = hash(new_item->key, new_item->key_len);		//change with jenkins
	curr->bitmap = curr->bitmap + (0x1UL << index);

	flush_buffer(&curr->entries[index], sizeof(entry), false);
	flush_buffer(&curr->fingerprints[index], sizeof(unsigned char), false);
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
}

void insert_in_inner(IN *curr, key_item *inserted_item, void *child)
{
	int loc, mid, j;

	mid = Search(curr, inserted_item->key, inserted_item->key_len);

	for (j = (curr->nKeys - 1); j >= mid; j--) {
		curr->keys[j + 1] = curr->keys[j];
		curr->ptr[j + 1] = curr->ptr[j];
	}

	curr->keys[mid] = inserted_item;
	curr->ptr[mid] = child;

	curr->nKeys++;
}

void insert_in_parent(tree *t, void *curr, key_item *inserted_item, void *splitNode) {
	if (curr == t->root) {
		IN *root = allocINode();
		root->leftmostPtr = curr;
		root->keys[0] = inserted_item;
		root->ptr[0] = splitNode;
		root->nKeys++;

		((IN *)splitNode)->parent = root;
		((IN *)curr)->parent = root;
		t->root = root;
		return ;
	}

	IN *parent;

	if (((IN *)curr)->type == THIS_IN)
		parent = ((IN *)curr)->parent;
	else
		parent = ((LN *)curr)->parent;

	if (parent->nKeys < NUM_IN_ENTRY) {
		insert_in_inner(parent, inserted_item, splitNode);
		((IN *)splitNode)->parent = parent;
	} else {
		int i, j, loc, parent_nKeys;
		IN *split_INode = allocINode();
		parent_nKeys = parent->nKeys;

		for (j = MIN_IN_ENTRIES, i = 0; j < parent_nKeys; j++, i++) {
			split_INode->keys[i] = parent->keys[j];
			split_INode->ptr[i] = parent->ptr[j];
			((IN *)split_INode->ptr[i])->parent = split_INode;
			split_INode->nKeys++;
			parent->nKeys--;
		}

		if (memcmp((split_INode->keys[0])->key, inserted_item->key,
					max((split_INode->keys[0])->key_len, inserted_item->key_len)) > 0) {
			insert_in_inner(parent, inserted_item, splitNode);
			((IN *)splitNode)->parent = parent;
		}
		else {
			((IN *)splitNode)->parent = split_INode;
			insert_in_inner(split_INode, inserted_item, splitNode);
		}

		insert_in_parent(t, parent, split_INode->keys[0], split_INode);
	}
}

/*
void *Update(tree *t, unsigned long key, void *value)
{
	unsigned long loc = 0;
	LN *curr = t->root;
	curr = find_leaf_node(curr, key);

	while (loc < NUM_LN_ENTRY) {
		loc = find_next_bit(&curr->bitmap, BITMAP_SIZE, loc);
		if (loc == BITMAP_SIZE)
			break;
		
		if (curr->fingerprints[loc] == hash(key) &&
				curr->entries[loc].key == key) {
			curr->entries[loc].ptr = value;
			flush_buffer(&curr->entries[loc].ptr, 8, true);
			return curr->entries[loc].ptr;
		}
		loc++;
	}

	return NULL;
}
*/

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

	flush_buffer(curr->slot, sizeof(curr->slot), true);
	
	curr->bitmap = curr->bitmap + 1 - (0x1UL << (mid + 1));
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
	return 0;
}

int Delete(tree *t, unsigned long key)
{
	int numEntries, errval = 0;
	node *curr = t->root;
	
	curr = find_leaf_node(curr, key);

	numEntries = curr->slot[0];
	if (numEntries <= 1)
		errval = -1;
	else
		errval = delete_in_leaf(curr, key);

	return errval;
}
*/

/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Hash table
 *
 * The hash function used here is by Bob Jenkins, 1996:
 *    <http://burtleburtle.net/bob/hash/doobs.html>
 *       "By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.
 *       You may use this code any way you wish, private, educational,
 *       or commercial.  It's free."
 *
 */
#define HASH_LITTLE_ENDIAN 1
#define HASH_BIG_ENDIAN 0

#define rot(x,k) (((x)<<(k)) ^ ((x)>>(32-(k))))

/*
-------------------------------------------------------------------------------
mix -- mix 3 32-bit values reversibly.

This is reversible, so any information in (a,b,c) before mix() is
still in (a,b,c) after mix().

If four pairs of (a,b,c) inputs are run through mix(), or through
mix() in reverse, there are at least 32 bits of the output that
are sometimes the same for one pair and different for another pair.
This was tested for:
* pairs that differed by one bit, by two bits, in any combination
  of top bits of (a,b,c), or in any combination of bottom bits of
  (a,b,c).
* "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
  the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
  is commonly produced by subtraction) look like a single 1-bit
  difference.
* the base values were pseudorandom, all zero but one bit set, or
  all zero plus a counter that starts at zero.

Some k values for my "a-=c; a^=rot(c,k); c+=b;" arrangement that
satisfy this are
    4  6  8 16 19  4
    9 15  3 18 27 15
   14  9  3  7 17  3
Well, "9 15 3 18 27 15" didn't quite get 32 bits diffing
for "differ" defined as + with a one-bit base and a two-bit delta.  I
used http://burtleburtle.net/bob/hash/avalanche.html to choose
the operations, constants, and arrangements of the variables.

This does not achieve avalanche.  There are input bits of (a,b,c)
that fail to affect some output bits of (a,b,c), especially of a.  The
most thoroughly mixed value is c, but it doesn't really even achieve
avalanche in c.

This allows some parallelism.  Read-after-writes are good at doubling
the number of bits affected, so the goal of mixing pulls in the opposite
direction as the goal of parallelism.  I did what I could.  Rotates
seem to cost as much as shifts on every machine I could lay my hands
on, and rotates are much kinder to the top and bottom bits, so I used
rotates.
-------------------------------------------------------------------------------
*/
#define mix(a,b,c) \
{ \
  a -= c;  a ^= rot(c, 4);  c += b; \
  b -= a;  b ^= rot(a, 6);  a += c; \
  c -= b;  c ^= rot(b, 8);  b += a; \
  a -= c;  a ^= rot(c,16);  c += b; \
  b -= a;  b ^= rot(a,19);  a += c; \
  c -= b;  c ^= rot(b, 4);  b += a; \
}

/*
-------------------------------------------------------------------------------
final -- final mixing of 3 32-bit values (a,b,c) into c

Pairs of (a,b,c) values differing in only a few bits will usually
produce values of c that look totally different.  This was tested for
* pairs that differed by one bit, by two bits, in any combination
  of top bits of (a,b,c), or in any combination of bottom bits of
  (a,b,c).
* "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
  the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
  is commonly produced by subtraction) look like a single 1-bit
  difference.
* the base values were pseudorandom, all zero but one bit set, or
  all zero plus a counter that starts at zero.

These constants passed:
 14 11 25 16 4 14 24
 12 14 25 16 4 14 24
and these came close:
  4  8 15 26 3 22 24
 10  8 15 26 3 22 24
 11  8 15 26 3 22 24
-------------------------------------------------------------------------------
*/
#define final(a,b,c) \
{ \
  c ^= b; c -= rot(b,14); \
  a ^= c; a -= rot(c,11); \
  b ^= a; b -= rot(a,25); \
  c ^= b; c -= rot(b,16); \
  a ^= c; a -= rot(c,4);  \
  b ^= a; b -= rot(a,14); \
  c ^= b; c -= rot(b,24); \
}

uint32_t jenkins_hash(
  const void *key,       /* the key to hash */
  size_t length)    /* length of the key */
{
  uint32_t a,b,c;                                          /* internal state */
  union { const void *ptr; size_t i; } u;     /* needed for Mac Powerbook G4 */

  /* Set up the internal state */
  a = b = c = 0xdeadbeef + ((uint32_t)length) + 0;

  u.ptr = key;
  if (HASH_LITTLE_ENDIAN && ((u.i & 0x3) == 0)) {
    const uint32_t *k = (uint32_t *)key;                           /* read 32-bit chunks */

    /*------ all but last block: aligned reads and affect 32 bits of (a,b,c) */
    while (length > 12)
    {
      a += k[0];
      b += k[1];
      c += k[2];
      mix(a,b,c);
      length -= 12;
      k += 3;
    }

    /*----------------------------- handle the last (probably partial) block */
    /*
     * "k[2]&0xffffff" actually reads beyond the end of the string, but
     * then masks off the part it's not allowed to read.  Because the
     * string is aligned, the masked-off tail is in the same word as the
     * rest of the string.  Every machine with memory protection I've seen
     * does it on word boundaries, so is OK with this.  But VALGRIND will
     * still catch it and complain.  The masking trick does make the hash
     * noticeably faster for short strings (like English words).
     */
    switch(length)
    {
    case 12: c+=k[2]; b+=k[1]; a+=k[0]; break;
    case 11: c+=k[2]&0xffffff; b+=k[1]; a+=k[0]; break;
    case 10: c+=k[2]&0xffff; b+=k[1]; a+=k[0]; break;
    case 9 : c+=k[2]&0xff; b+=k[1]; a+=k[0]; break;
    case 8 : b+=k[1]; a+=k[0]; break;
    case 7 : b+=k[1]&0xffffff; a+=k[0]; break;
    case 6 : b+=k[1]&0xffff; a+=k[0]; break;
    case 5 : b+=k[1]&0xff; a+=k[0]; break;
    case 4 : a+=k[0]; break;
    case 3 : a+=k[0]&0xffffff; break;
    case 2 : a+=k[0]&0xffff; break;
    case 1 : a+=k[0]&0xff; break;
    case 0 : return c;  /* zero length strings require no mixing */
    }
  } else if (HASH_LITTLE_ENDIAN && ((u.i & 0x1) == 0)) {
    const uint16_t *k = (uint16_t *)key;                           /* read 16-bit chunks */
    const uint8_t  *k8;

    /*--------------- all but last block: aligned reads and different mixing */
    while (length > 12)
    {
      a += k[0] + (((uint32_t)k[1])<<16);
      b += k[2] + (((uint32_t)k[3])<<16);
      c += k[4] + (((uint32_t)k[5])<<16);
      mix(a,b,c);
      length -= 12;
      k += 6;
    }

    /*----------------------------- handle the last (probably partial) block */
    k8 = (const uint8_t *)k;
    switch(length)
    {
    case 12: c+=k[4]+(((uint32_t)k[5])<<16);
             b+=k[2]+(((uint32_t)k[3])<<16);
             a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 11: c+=((uint32_t)k8[10])<<16;     /* @fallthrough */
    case 10: c+=k[4];                       /* @fallthrough@ */
             b+=k[2]+(((uint32_t)k[3])<<16);
             a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 9 : c+=k8[8];                      /* @fallthrough */
    case 8 : b+=k[2]+(((uint32_t)k[3])<<16);
             a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 7 : b+=((uint32_t)k8[6])<<16;      /* @fallthrough */
    case 6 : b+=k[2];
             a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 5 : b+=k8[4];                      /* @fallthrough */
    case 4 : a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 3 : a+=((uint32_t)k8[2])<<16;      /* @fallthrough */
    case 2 : a+=k[0];
             break;
    case 1 : a+=k8[0];
             break;
    case 0 : return c;  /* zero length strings require no mixing */
    }

  } else {                        /* need to read the key one byte at a time */
    const uint8_t *k = (uint8_t *)key;

    /*--------------- all but the last block: affect some 32 bits of (a,b,c) */
    while (length > 12)
    {
      a += k[0];
      a += ((uint32_t)k[1])<<8;
      a += ((uint32_t)k[2])<<16;
      a += ((uint32_t)k[3])<<24;
      b += k[4];
      b += ((uint32_t)k[5])<<8;
      b += ((uint32_t)k[6])<<16;
      b += ((uint32_t)k[7])<<24;
      c += k[8];
      c += ((uint32_t)k[9])<<8;
      c += ((uint32_t)k[10])<<16;
      c += ((uint32_t)k[11])<<24;
      mix(a,b,c);
      length -= 12;
      k += 12;
    }

    /*-------------------------------- last block: affect all 32 bits of (c) */
    switch(length)                   /* all the case statements fall through */
    {
    case 12: c+=((uint32_t)k[11])<<24;
    case 11: c+=((uint32_t)k[10])<<16;
    case 10: c+=((uint32_t)k[9])<<8;
    case 9 : c+=k[8];
    case 8 : b+=((uint32_t)k[7])<<24;
    case 7 : b+=((uint32_t)k[6])<<16;
    case 6 : b+=((uint32_t)k[5])<<8;
    case 5 : b+=k[4];
    case 4 : a+=((uint32_t)k[3])<<24;
    case 3 : a+=((uint32_t)k[2])<<16;
    case 2 : a+=((uint32_t)k[1])<<8;
    case 1 : a+=k[0];
             break;
    case 0 : return c;  /* zero length strings require no mixing */
    }
  }

  final(a,b,c);
  return c;             /* zero length strings require no mixing */
}
}
#endif
