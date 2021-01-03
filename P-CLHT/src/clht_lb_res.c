/*   
 *   File: clht_lb_res.c
 *   Author: Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>
 *   Description: lock-based cache-line hash table with resizing.
 *   clht_lb_res.c is part of ASCYLIB
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>
 *	      	      Distributed Programming Lab (LPD), EPFL
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#include <math.h>
#include <stdlib.h>
#include <malloc.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <emmintrin.h>

#include "clht_lb_res.h"

//#define CLHTDEBUG

__thread ssmem_allocator_t* clht_alloc;

#ifdef DEBUG
__thread uint32_t put_num_restarts = 0;
__thread uint32_t put_num_failed_expand = 0;
__thread uint32_t put_num_failed_on_new = 0;
#endif

__thread size_t check_ht_status_steps = CLHT_STATUS_INVOK_IN;

#include "stdlib.h"
#include "assert.h"

#if defined(CLHTDEBUG)
 #define DEBUG_PRINT(fmt, args...) fprintf(stderr, "CLHT_DEBUG: %s:%d:%s(): " fmt, \
    __FILE__, __LINE__, __func__, ##args)
#else
 #define DEBUG_PRINT(fmt, args...)
#endif

    const char*
clht_type_desc()
{
    return "CLHT-LB-RESIZE";
}

    inline int
is_power_of_two (unsigned int x) 
{
    return ((x != 0) && !(x & (x - 1)));
}

    static inline
int is_odd (int x)
{
    return x & 1;
}

/** Jenkins' hash function for 64-bit integers. */
    inline uint64_t
__ac_Jenkins_hash_64(uint64_t key)
{
    key += ~(key << 32);
    key ^= (key >> 22);
    key += ~(key << 13);
    key ^= (key >> 8);
    key += (key << 3);
    key ^= (key >> 15);
    key += ~(key << 27);
    key ^= (key >> 31);
    return key;
}

inline void *clht_ptr_from_off(uint64_t offset, bool alignment)
{
    PMEMoid oid = {pool_uuid, offset};
    void *vaddr = pmemobj_direct(oid);
    if (!alignment)
        return vaddr;
    else {
        if (vaddr)
            return (void *)((uint64_t)vaddr + ALIGNMENT_PADDING);
        else
            return vaddr;
    }
}

static inline void mfence() {
    asm volatile("sfence":::"memory");
}

static inline void clflush(char *data, int len, bool front, bool back)
{
    volatile char *ptr = (char *)((unsigned long)data &~(CACHE_LINE_SIZE-1));
    if (front)
        mfence();
    for(; ptr<data+len; ptr+=CACHE_LINE_SIZE){
#ifdef CLFLUSH
        asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
    }
    if (back)
        mfence();
}

// Implemented while assuming all allocations are cacheline-aligned
static inline void clflush_next_check(char *data, int len, bool fence)
{
    volatile char *ptr = (char *)((unsigned long)data &~(CACHE_LINE_SIZE-1));
    if (fence)
        mfence();
    for(; ptr<data+len; ptr+=CACHE_LINE_SIZE){
#ifdef CLFLUSH
        asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
		if (((bucket_t *)ptr)->next_off != OID_NULL.off)
            clflush_next_check((char *)clht_ptr_from_off((((bucket_t *)ptr)->next_off), true), sizeof(bucket_t), false);
    }
    if (fence)
        mfence();
}

// Implemented without assuming cacheline-aligned allocation
static inline void clflush_new_hashtable(bucket_t *buckets, size_t num_buckets) {
    bucket_t *bucket;

    clflush((char *)buckets, num_buckets * sizeof(bucket_t), false, false);
    for (uint64_t i = 0; i < num_buckets; i++) {
        bucket = &buckets[i];
        while (bucket->next_off != OID_NULL.off) {
            bucket = (bucket_t *)clht_ptr_from_off(bucket->next_off, true);
            clflush((char *)bucket, sizeof(bucket_t), false, false);
        }
    }
}

static inline void movnt64(uint64_t *dest, uint64_t const src, bool front, bool back) {
    assert(((uint64_t)dest & 7) == 0);
    if (front) mfence();
    _mm_stream_si64((long long int *)dest, (long long int) src);
    if (back) mfence();
}

static int bucket_init(PMEMobjpool *pop_arg, void *ptr, void *arg)
{
    bucket_t *bucket = (bucket_t *)((uint64_t)ptr + ALIGNMENT_PADDING);
    bucket->lock = 0;

    uint32_t j;
    for (j = 0; j < ENTRIES_PER_BUCKET; j++)
        bucket->key[j] = 0;
    bucket->next_off = OID_NULL.off;
    return 0;
}

/* Create a new bucket. */
    bucket_t*
clht_bucket_create() 
{
    bucket_t* bucket = NULL;
    PMEMoid bucket_oid;
    if (pmemobj_alloc(pop, &bucket_oid, sizeof(bucket_t) + ALIGNMENT_PADDING, TOID_TYPE_NUM(bucket_t), bucket_init, 0)) {
        fprintf(stderr, "pmemobj_alloc failed for clht_bucket_create\n");
        assert(0);
    }

    bucket = pmemobj_direct(bucket_oid);
    if (bucket == NULL)
        return NULL;
    return (bucket_t *)((uint64_t)bucket + ALIGNMENT_PADDING);
}

    bucket_t*
clht_bucket_create_stats(clht_hashtable_t* h, int* resize)
{
    bucket_t* b = clht_bucket_create();
    if (((uint64_t)b & (CACHE_LINE_SIZE - 1)) != 0)
        fprintf(stderr, "cacheline-unaligned bucket allocation\n");

    //if (IAF_U32(&h->num_expands) == h->num_expands_threshold)
    if (IAF_U32(&h->num_expands) >= h->num_expands_threshold)
    {
        /* printf("      -- hit threshold (%u ~ %u)\n", h->num_expands, h->num_expands_threshold); */
        *resize = 1;
    }
    return b;
}

clht_hashtable_t* clht_hashtable_create(uint64_t num_buckets);

    clht_t* 
clht_create(uint64_t num_buckets)
{
    // Enable prefault
    int arg_open = 1, arg_create = 1;
    if ((pmemobj_ctl_set(pop, "prefault.at_open", &arg_open)) != 0)
        perror("failed to configure prefaults at open\n");
    if ((pmemobj_ctl_set(pop, "prefault.at_create", &arg_create)) != 0)
        perror("failed to configure prefaults at create\n");

    // Open the PMEMpool if it exists, otherwise create it
    size_t pool_size = 32*1024*1024*1024UL;
    if (access("/dev/shm/pool", F_OK) != -1)
        pop = pmemobj_open("/dev/shm/pool", POBJ_LAYOUT_NAME(clht));
    else
        pop = pmemobj_create("/dev/shm/pool", POBJ_LAYOUT_NAME(clht), pool_size, 0666);

    if (pop == NULL)
        perror("failed to open the pool\n");

    // Create the root pointer
    PMEMoid my_root = pmemobj_root(pop, sizeof(clht_t));
    if (pmemobj_direct(my_root) == NULL)
        perror("root pointer is null\n");
    pool_uuid = my_root.pool_uuid_lo;

    clht_t *w = pmemobj_direct(my_root);
    if (w == NULL)
        return NULL;

    if (w->ht_off == 0) {
        clht_hashtable_t *ht_ptr;
        ht_ptr = clht_hashtable_create(num_buckets);
        assert(ht_ptr != NULL);

        w->ht_off = pmemobj_oid(ht_ptr).off;
        w->resize_lock = LOCK_FREE;
        w->gc_lock = LOCK_FREE;
        w->status_lock = LOCK_FREE;
        w->version_list = NULL;
        w->version_min = 0;
        w->ht_oldest = ht_ptr;

        // This should flush everything to persistent memory
        clflush((char *)clht_ptr_from_off(ht_ptr->table_off, true), num_buckets * sizeof(bucket_t), false, true);
        clflush((char *)ht_ptr, sizeof(clht_hashtable_t), false, true);
        clflush((char *)w, sizeof(clht_t), false, true);
    } else {
        w->resize_lock = LOCK_FREE;
        w->gc_lock = LOCK_FREE;
        w->status_lock = LOCK_FREE;
    }

    return w;
}

    clht_hashtable_t*
clht_hashtable_create(uint64_t num_buckets)
{
    clht_hashtable_t* hashtable = NULL;

    if (num_buckets == 0)
    {
        return NULL;
    }

    /* Allocate the table itself. */
    PMEMoid ht_oid;
    if (pmemobj_alloc(pop, &ht_oid, sizeof(clht_hashtable_t), TOID_TYPE_NUM(clht_hashtable_t), 0, 0)) {
        fprintf(stderr, "pmemobj_alloc failed for clht_hashtable_create\n");
        assert(0);
    }

    hashtable = pmemobj_direct(ht_oid);
    if (hashtable == NULL) {
        printf("** malloc @ hashtable\n");
        return NULL;
    }

    PMEMoid table_oid;
    if (pmemobj_zalloc(pop, &table_oid, (num_buckets * sizeof(bucket_t)) + ALIGNMENT_PADDING, TOID_TYPE_NUM(bucket_t))) {
        fprintf(stderr, "pmemobj_alloc failed for table_oid in clht_hashtable_create\n");
        assert(0);
    }

    hashtable->table_off = table_oid.off;
    bucket_t *bucket_ptr = (bucket_t *)clht_ptr_from_off(hashtable->table_off, true);
    if (bucket_ptr == NULL) {
        fprintf(stderr, "** alloc: hashtable->table\n");
        perror("bucket_ptr is null\n");
        return NULL;
    } else if (((uint64_t)bucket_ptr & (CACHE_LINE_SIZE -1)) != 0) {
        fprintf(stderr, "cacheline-unaligned hash table allocation\n");
    }

    uint64_t i;
    for (i = 0; i < num_buckets; i++) {
        bucket_ptr[i].lock = LOCK_FREE;
        uint32_t j;
        for (j = 0; j < ENTRIES_PER_BUCKET; j++)
            bucket_ptr[i].key[j] = 0;
    }

    hashtable->num_buckets = num_buckets;
    hashtable->hash = num_buckets - 1;
    hashtable->version = 0;
    hashtable->table_tmp = NULL;
    hashtable->table_new = NULL;
    hashtable->table_prev = NULL;
    hashtable->num_expands = 0;
    hashtable->num_expands_threshold = (CLHT_PERC_EXPANSIONS * num_buckets);
    if (hashtable->num_expands_threshold == 0)
    {
        hashtable->num_expands_threshold = 1;
    }
    hashtable->is_helper = 1;
    hashtable->helper_done = 0;

    return hashtable;
}


/* Hash a key for a particular hash table. */
    uint64_t
clht_hash(clht_hashtable_t* hashtable, clht_addr_t key) 
{
    /* uint64_t hashval; */
    /* return __ac_Jenkins_hash_64(key) & (hashtable->hash); */
    /* return hashval % hashtable->num_buckets; */
    /* return key % hashtable->num_buckets; */
    /* return key & (hashtable->num_buckets - 1); */
    return key & (hashtable->hash);
}

/* Retrieve a key-value entry from a hash table. */
    clht_val_t
clht_get(clht_t* h, clht_addr_t key)
{
    clht_hashtable_t *hashtable = clht_ptr_from_off(h->ht_off, false);
    size_t bin = clht_hash(hashtable, key);
    CLHT_GC_HT_VERSION_USED(hashtable);
    volatile bucket_t *bucket = ((bucket_t *)clht_ptr_from_off(hashtable->table_off, true)) + bin;

    uint32_t j;
    clht_val_t val;
    do
    {
        for (j = 0; j < ENTRIES_PER_BUCKET; j++)
        {
retry:
            val = bucket->val[j];
#ifdef __tile__
            _mm_lfence();
#endif
            if (bucket->key[j] == key)
            {
                if (likely(bucket->val[j] == val))
                {
                    return val;
                }
                else
                {
                    goto retry;
                }
            }
        }

        bucket = (bucket_t *)clht_ptr_from_off(bucket->next_off, true);
    }
    while (unlikely(bucket != NULL));
    return 0;
}

    static inline int
bucket_exists(volatile bucket_t* bucket, clht_addr_t key)
{
    uint32_t j;
    do 
    {
        for (j = 0; j < ENTRIES_PER_BUCKET; j++)
        {
            if (bucket->key[j] == key)
            {
                return true;
            }
        }
        bucket = (bucket_t *)clht_ptr_from_off(bucket->next_off, true);
    } 
    while (unlikely(bucket != NULL));
    return false;
}

/* Insert a key-value entry into a hash table. */
    int
clht_put(clht_t* h, clht_addr_t key, clht_val_t val)
{
    clht_hashtable_t* hashtable = clht_ptr_from_off(h->ht_off, false);
    size_t bin = clht_hash(hashtable, key);
    volatile bucket_t *bucket = ((bucket_t *)clht_ptr_from_off(hashtable->table_off, true)) + bin;

#if CLHT_READ_ONLY_FAIL == 1
    if (bucket_exists(bucket, key))
    {
        return false;
    }
#endif

    clht_lock_t* lock = &bucket->lock;
    while (!LOCK_ACQ(lock, hashtable))
    {
        hashtable = clht_ptr_from_off(h->ht_off, false);
        size_t bin = clht_hash(hashtable, key);

        bucket = ((bucket_t *)clht_ptr_from_off(hashtable->table_off, true)) + bin;
        lock = &bucket->lock;
    }

    CLHT_GC_HT_VERSION_USED(hashtable);
    CLHT_CHECK_STATUS(h);
    clht_addr_t* empty = NULL;
    clht_val_t* empty_v = NULL;

    uint32_t j;
    do 
    {
        for (j = 0; j < ENTRIES_PER_BUCKET; j++)
        {
            if (bucket->key[j] == key) 
            {
                LOCK_RLS(lock);
                return false;
            }
            else if (empty == NULL && bucket->key[j] == 0)
            {
                empty = (clht_addr_t*) &bucket->key[j];
                empty_v = &bucket->val[j];
            }
        }

        int resize = 0;
        if (likely(clht_ptr_from_off(bucket->next_off, true) == NULL))
        {
            if (unlikely(empty == NULL))
            {
                DPP(put_num_failed_expand);

                bucket_t* b = clht_bucket_create_stats(hashtable, &resize);
                b->val[0] = val;
#ifdef __tile__
                /* keep the writes in order */
                _mm_sfence();
#endif
                b->key[0] = key;
#ifdef __tile__
                /* make sure they are visible */
                _mm_sfence();
#endif
                clflush((char *)b, sizeof(bucket_t), false, true);
                movnt64((uint64_t *)&bucket->next_off, (uint64_t)(pmemobj_oid(b).off - ALIGNMENT_PADDING), false, true);
            }
            else
            {
                *empty_v = val;
#ifdef __tile__
                /* keep the writes in order */
                _mm_sfence();
#endif
                clflush((char *)empty_v, sizeof(clht_val_t), false, true);
                movnt64((uint64_t *)empty, (uint64_t)key, false, true);
            }

            LOCK_RLS(lock);
            if (unlikely(resize))
            {
                /* ht_resize_pes(h, 1); */
				DEBUG_PRINT("Calling ht_status for key %ld\n", (long)key);
                int ret = ht_status(h, 1, 0);
				
				// if crash, return true, because the insert anyway succeeded
				if (ret == 0)
					return true;
            }
            return true;
        }
        bucket = (bucket_t *)clht_ptr_from_off(bucket->next_off, true);
    }
    while (true);
}

/* Update a value entry associated with given key. */
    int
clht_update(clht_t* h, clht_addr_t key, clht_val_t val)
{
    clht_hashtable_t* hashtable = (clht_hashtable_t *)clht_ptr_from_off(h->ht_off, false);
    size_t bin = clht_hash(hashtable, key);
    volatile bucket_t *bucket = ((bucket_t *)clht_ptr_from_off(hashtable->table_off, true)) + bin;

#if CLHT_READ_ONLY_FAIL == 1
    if (!bucket_exists(bucket, key))
    {
        return false;
    }
#endif

    clht_lock_t* lock = &bucket->lock;
    while (!LOCK_ACQ(lock, hashtable))
    {
        hashtable = (clht_hashtable_t *)clht_ptr_from_off(h->ht_off, false);
        size_t bin = clht_hash(hashtable, key);

        bucket = ((bucket_t *)clht_ptr_from_off(hashtable->table_off, true)) + bin;
        lock = &bucket->lock;
    }

    CLHT_GC_HT_VERSION_USED(hashtable);
    CLHT_CHECK_STATUS(h);

    uint32_t j;
    do 
    {
        for (j = 0; j < ENTRIES_PER_BUCKET; j++)
        {
            if (bucket->key[j] == key)
            {
                movnt64((uint64_t *)&bucket->val[j], val, true, true);
                LOCK_RLS(lock);
                return true;
            }
        }
        bucket = (bucket_t *)clht_ptr_from_off(bucket->next_off, true);
    } 
    while (unlikely(bucket != NULL));

    return false;
}

/* Remove a key-value entry from a hash table. */
    clht_val_t
clht_remove(clht_t* h, clht_addr_t key)
{
    clht_hashtable_t* hashtable = (clht_hashtable_t *)clht_ptr_from_off(h->ht_off, false);
    size_t bin = clht_hash(hashtable, key);
    volatile bucket_t* bucket = ((bucket_t *)clht_ptr_from_off(hashtable->table_off, true)) + bin;

#if CLHT_READ_ONLY_FAIL == 1
    if (!bucket_exists(bucket, key))
    {
        return false;
    }
#endif

    clht_lock_t* lock = &bucket->lock;
    while (!LOCK_ACQ(lock, hashtable))
    {
        hashtable = (clht_hashtable_t *)clht_ptr_from_off(h->ht_off, false);
        size_t bin = clht_hash(hashtable, key);

        bucket = ((bucket_t *)clht_ptr_from_off(hashtable->table_off, true)) + bin;
        lock = &bucket->lock;
    }

    CLHT_GC_HT_VERSION_USED(hashtable);
    CLHT_CHECK_STATUS(h);

    uint32_t j;
    uint64_t emptyMarker = 0;
    do
    {
        for (j = 0; j < ENTRIES_PER_BUCKET; j++)
        {
            if (bucket->key[j] == key)
            {
                clht_val_t val = bucket->val[j];
                movnt64((uint64_t *)&bucket->key[j], emptyMarker, true, true);
                LOCK_RLS(lock);
                return val;
            }
        }
        bucket = (bucket_t *)clht_ptr_from_off(bucket->next_off, true);
    }
    while (unlikely(bucket != NULL));
    LOCK_RLS(lock);
    return false;
}

    static uint32_t
clht_put_seq(clht_hashtable_t* hashtable, clht_addr_t key, clht_val_t val, uint64_t bin)
{
    volatile bucket_t* bucket = ((bucket_t *)clht_ptr_from_off(hashtable->table_off, true)) + bin;
    uint32_t j;

    do
    {
        for (j = 0; j < ENTRIES_PER_BUCKET; j++)
        {
            if (bucket->key[j] == 0)
            {
                bucket->val[j] = val;
                bucket->key[j] = key;
                return true;
            }
        }

        if (clht_ptr_from_off(bucket->next_off, true) == NULL)
        {
            DPP(put_num_failed_expand);
            int null;
            bucket->next_off = pmemobj_oid(clht_bucket_create_stats(hashtable, &null)).off - ALIGNMENT_PADDING;
            bucket_t *bucket_ptr = (bucket_t *)clht_ptr_from_off(bucket->next_off, true);
            bucket_ptr->val[0] = val;
            bucket_ptr->key[0] = key;
            return true;
        }

        bucket = (bucket_t *)clht_ptr_from_off(bucket->next_off, true);
    }
    while (true);
}


    static int
bucket_cpy(clht_t* h, volatile bucket_t* bucket, clht_hashtable_t* ht_new)
{
    if (!LOCK_ACQ_RES(&bucket->lock))
    {
        return 0;
    }
    uint32_t j;
    do
    {
        for (j = 0; j < ENTRIES_PER_BUCKET; j++)
        {
            clht_addr_t key = bucket->key[j];
            if (key != 0)
            {
                uint64_t bin = clht_hash(ht_new, key);
                clht_put_seq(ht_new, key, bucket->val[j], bin);
            }
        }
        bucket = (bucket_t *)clht_ptr_from_off(bucket->next_off, true);
    }
    while (bucket != NULL);

    return 1;
}


    void
ht_resize_help(clht_hashtable_t* h)
{
    if ((int32_t) FAD_U32((volatile uint32_t*) &h->is_helper) <= 0)
    {
        return;
    }

    int32_t b;
    /* hash = num_buckets - 1 */
    for (b = h->hash; b >= 0; b--)
    {
        bucket_t* bu_cur = ((bucket_t *)clht_ptr_from_off(h->table_off, true)) + b;
        if (!bucket_cpy((clht_t *)h, bu_cur, h->table_tmp))
        {	    /* reached a point where the resizer is handling */
            /* printf("[GC-%02d] helped  #buckets: %10zu = %5.1f%%\n",  */
            /* 	 clht_gc_get_id(), h->num_buckets - b, 100.0 * (h->num_buckets - b) / h->num_buckets); */
            break;
        }
    }

    h->helper_done = 1;
}

// return -1 if crash is simulated.
    int
ht_resize_pes(clht_t* h, int is_increase, int by)
{
//    ticks s = getticks();

    check_ht_status_steps = CLHT_STATUS_INVOK;

    clht_hashtable_t* ht_old = (clht_hashtable_t *)clht_ptr_from_off(h->ht_off, false);

    if (TRYLOCK_ACQ(&h->resize_lock))
    {
        return 0;
    }

    size_t num_buckets_new;
    if (is_increase == true)
    {
        /* num_buckets_new = CLHT_RATIO_DOUBLE * ht_old->num_buckets; */
        num_buckets_new = by * ht_old->num_buckets;
    }
    else
    {
#if CLHT_HELP_RESIZE == 1
        ht_old->is_helper = 0;
#endif
        num_buckets_new = ht_old->num_buckets / CLHT_RATIO_HALVE;
    }

    /* printf("// resizing: from %8zu to %8zu buckets\n", ht_old->num_buckets, num_buckets_new); */

    clht_hashtable_t* ht_new = clht_hashtable_create(num_buckets_new);
    ht_new->version = ht_old->version + 1;

#if CLHT_HELP_RESIZE == 1
    ht_old->table_tmp = ht_new;

    size_t b;
    for (b = 0; b < ht_old->num_buckets; b++)
    {
        bucket_t* bu_cur = (bucket_t *)(clht_ptr_from_off(ht_old->table_off, true)) + b;
        int ret = bucket_cpy(h, bu_cur, ht_new); /* reached a point where the helper is handling */
		if (ret == -1)
			return -1;

		if (!ret)
            break;
    }

    if (is_increase && ht_old->is_helper != 1)	/* there exist a helper */
    {
        while (ht_old->helper_done != 1)
        {
            _mm_pause();
        }
    }

#else

    size_t b;
    for (b = 0; b < ht_old->num_buckets; b++)
    {
        bucket_t* bu_cur = ((bucket_t *)clht_ptr_from_off(ht_old->table_off, true)) + b;
        int ret = bucket_cpy(h, bu_cur, ht_new);
		if (ret == -1)
			return -1;
    }
#endif

#if defined(DEBUG)
    /* if (clht_size(ht_old) != clht_size(ht_new)) */
    /*   { */
    /*     printf("**clht_size(ht_old) = %zu != clht_size(ht_new) = %zu\n", clht_size(ht_old), clht_size(ht_new)); */
    /*   } */
#endif

    ht_new->table_prev = ht_old;

    int ht_resize_again = 0;
    if (ht_new->num_expands >= ht_new->num_expands_threshold)
    {
        /* printf("--problem: have already %u expands\n", ht_new->num_expands); */
        ht_resize_again = 1;
        /* ht_new->num_expands_threshold = ht_new->num_expands + 1; */
    }

    mfence();
    clflush((char *)ht_new, sizeof(clht_hashtable_t), false, false);
    clflush_new_hashtable((bucket_t *)clht_ptr_from_off(ht_new->table_off, true), num_buckets_new);
    mfence();

	// atomically swap the root pointer
    // Presume the head of "h" contains the pointer (offset) to the hash table
    SWAP_U64((uint64_t*) h, (uint64_t) pmemobj_oid(ht_new).off);
    clflush((char *)h, sizeof(uint64_t), false, true);

	DEBUG_PRINT("Parent reached correctly\n"); 
    ht_old->table_new = ht_new;
    TRYLOCK_RLS(h->resize_lock);

//    ticks e = getticks() - s;
//    double mba = (ht_new->num_buckets * 64) / (1024.0 * 1024);
//    printf("[RESIZE-%02d] to #bu %7zu = MB: %7.2f    | took: %13llu ti = %8.6f s\n",
//            clht_gc_get_id(), ht_new->num_buckets, mba, (unsigned long long) e, e / 2.1e9);

#if defined(CLHTDEBUG)
	DEBUG_PRINT("-------------ht old------------\n");
	clht_print(ht_old);
	DEBUG_PRINT("-------------ht new------------\n");
	clht_print(ht_new);
    DEBUG_PRINT("-------------ht current------------\n");
    clht_print(h->ht);
	DEBUG_PRINT("-------------------------\n");
#endif

#if CLHT_DO_GC == 1
    clht_gc_collect(h);
#else
    clht_gc_release(ht_old);
#endif

    if (ht_resize_again)
    {
        ht_status(h, 1, 0);
    }
    return 1;
}

    size_t
clht_size(clht_hashtable_t* hashtable)
{
    uint64_t num_buckets = hashtable->num_buckets;
    volatile bucket_t* bucket = NULL;
    size_t size = 0;

    uint64_t bin;
    for (bin = 0; bin < num_buckets; bin++)
    {
        bucket = ((bucket_t *)clht_ptr_from_off(hashtable->table_off, true)) + bin;

        uint32_t j;
        do
        {
            for (j = 0; j < ENTRIES_PER_BUCKET; j++)
            {
                if (bucket->key[j] > 0)
                {
                    size++;
                }
            }

            bucket = (bucket_t *)clht_ptr_from_off(bucket->next_off, true);
        }
        while (bucket != NULL);
    }
    return size;
}

    size_t
ht_status(clht_t* h, int resize_increase, int just_print)
{
    if (TRYLOCK_ACQ(&h->status_lock) && !resize_increase)
    {
        return 0;
    }

    clht_hashtable_t* hashtable = (clht_hashtable_t *)clht_ptr_from_off(h->ht_off, false);
    uint64_t num_buckets = hashtable->num_buckets;
    volatile bucket_t* bucket = NULL;
    size_t size = 0;
    int expands = 0;
    int expands_max = 0;

    uint64_t bin;
    for (bin = 0; bin < num_buckets; bin++)
    {
        bucket = ((bucket_t *)clht_ptr_from_off(hashtable->table_off, true)) + bin;

        int expands_cont = -1;
        expands--;
        uint32_t j;
        do
        {
            expands_cont++;
            expands++;
            for (j = 0; j < ENTRIES_PER_BUCKET; j++)
            {
                if (bucket->key[j] > 0)
                {
                    size++;
                }
            }

            bucket = (bucket_t *)clht_ptr_from_off(bucket->next_off, true);
        }
        while (bucket != NULL);

        if (expands_cont > expands_max)
        {
            expands_max = expands_cont;
        }
    }

    double full_ratio = 100.0 * size / ((hashtable->num_buckets) * ENTRIES_PER_BUCKET);

    if (just_print)
    {
        printf("[STATUS-%02d] #bu: %7zu / #elems: %7zu / full%%: %8.4f%% / expands: %4d / max expands: %2d\n",
                99, hashtable->num_buckets, size, full_ratio, expands, expands_max);
    }
    else
    {
        if (full_ratio > 0 && full_ratio < CLHT_PERC_FULL_HALVE)
        {
//            printf("[STATUS-%02d] #bu: %7zu / #elems: %7zu / full%%: %8.4f%% / expands: %4d / max expands: %2d\n",
//                    clht_gc_get_id(), hashtable->num_buckets, size, full_ratio, expands, expands_max);
            ht_resize_pes(h, 0, 33);
        }
        else if ((full_ratio > 0 && full_ratio > CLHT_PERC_FULL_DOUBLE) || expands_max > CLHT_MAX_EXPANSIONS ||
                resize_increase)
        {
            int inc_by = (full_ratio / CLHT_OCCUP_AFTER_RES);
            int inc_by_pow2 = pow2roundup(inc_by);

//            printf("[STATUS-%02d] #bu: %7zu / #elems: %7zu / full%%: %8.4f%% / expands: %4d / max expands: %2d\n",
//                    clht_gc_get_id(), hashtable->num_buckets, size, full_ratio, expands, expands_max);
            if (inc_by_pow2 == 1)
            {
                inc_by_pow2 = 2;
            }
			DEBUG_PRINT("Callig ht_resize_pes\n");
            int ret = ht_resize_pes(h, 1, inc_by_pow2);
			// return if crashed
			if (ret == -1)
				return 0;
        }
    }

    if (!just_print)
    {
        clht_gc_collect(h);
    }

    TRYLOCK_RLS(h->status_lock);
    return size;
}


    size_t
clht_size_mem(clht_hashtable_t* h) /* in bytes */
{
    if (h == NULL)
    {
        return 0;
    }

    size_t size_tot = sizeof(clht_hashtable_t**);
    size_tot += (h->num_buckets + h->num_expands) * sizeof(bucket_t);
    return size_tot;
}

    size_t
clht_size_mem_garbage(clht_hashtable_t* h) /* in bytes */
{
    if (h == NULL)
    {
        return 0;
    }

    size_t size_tot = 0;
    clht_hashtable_t* cur = h->table_prev;
    while (cur != NULL)
    {
        size_tot += clht_size_mem(cur);
        cur = cur->table_prev;
    }

    return size_tot;
}


    void
clht_print(clht_hashtable_t* hashtable)
{
    uint64_t num_buckets = hashtable->num_buckets;
    volatile bucket_t* bucket;

    printf("Number of buckets: %zu\n", num_buckets);

    uint64_t bin;
    for (bin = 0; bin < num_buckets; bin++)
    {
        bucket = ((bucket_t *)clht_ptr_from_off(hashtable->table_off, true)) + bin;

        printf("[[%05zu]] ", bin);

        uint32_t j;
        do
        {
            for (j = 0; j < ENTRIES_PER_BUCKET; j++)
            {
                if (bucket->key[j])
                {
                    printf("(%-5llu/%p)-> ", (long long unsigned int) bucket->key[j], (void*) bucket->val[j]);
                }
            }

            bucket = (bucket_t *)clht_ptr_from_off(bucket->next_off, true);
            printf(" ** -> ");
        }
        while (bucket != NULL);
        printf("\n");
    }
    fflush(stdout);
}

void clht_lock_initialization(clht_t *h)
{
	DEBUG_PRINT("Performing Lock initialization\n");
    clht_hashtable_t *ht = (clht_hashtable_t *)clht_ptr_from_off(h->ht_off, false);
    volatile bucket_t *next;

    h->resize_lock = LOCK_FREE;
    h->status_lock = LOCK_FREE;
    h->gc_lock = LOCK_FREE;

    int i;
    bucket_t *buckets = (bucket_t *)clht_ptr_from_off(ht->table_off, true);
    for (i = 0; i < ht->num_buckets; i++) {
        buckets[i].lock = LOCK_FREE;
        for (next = clht_ptr_from_off(buckets[i].next_off, true); 
                next != NULL; next = clht_ptr_from_off(next->next_off, true)) {
            next->lock = LOCK_FREE;
        }
    }
}
