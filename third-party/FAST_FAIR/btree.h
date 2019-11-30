#ifndef BTREE_H_
#define BTREE_H_

#include <unistd.h>
#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <stdlib.h>
#include <math.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string.h>
#include <cassert>
#include <climits>
#include <future>
#include <mutex>
#ifdef LOCK_INIT
#include "tbb/concurrent_vector.h"
#endif

namespace fastfair {

#define PAGESIZE 512

static uint64_t CPU_FREQ_MHZ = 2100;
static uint64_t CACHE_LINE_SIZE = 64;
#define QUERY_NUM 25

#define IS_FORWARD(c) (c % 2 == 0)

pthread_mutex_t print_mtx;

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

static unsigned long write_latency_in_ns=0;
unsigned long long search_time_in_insert=0;
unsigned int gettime_cnt= 0;
unsigned long long clflush_time_in_insert=0;
unsigned long long update_time_in_insert=0;
int node_cnt=0;

using namespace std;

static inline void mfence()
{
    asm volatile("mfence":::"memory");
}

static inline void clflush(char *data, int len)
{
    volatile char *ptr = (char *)((unsigned long)data &~(CACHE_LINE_SIZE-1));
    mfence();
    for(; ptr<data+len; ptr+=CACHE_LINE_SIZE){
        unsigned long etsc = read_tsc() + 
            (unsigned long)(write_latency_in_ns*CPU_FREQ_MHZ/1000);
#ifdef CLFLUSH
        asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
        while (read_tsc() < etsc) cpu_pause();
    }
    mfence();
}

#ifdef LOCK_INIT
static tbb::concurrent_vector<std::mutex *> lock_initializer;
static void lock_initialization()
{
    printf("lock table size = %lu\n", lock_initializer.size());
    for (uint64_t i = 0; i < lock_initializer.size(); i++) {
        lock_initializer[i]->unlock();
    }
}
#endif

typedef struct key_item {
    size_t key_len;
    char key[];
} key_item;

//using entry_key_t = uint64_t;

union Key {
    uint64_t ikey;
    key_item *skey;
};

class page;

class btree{
    private:
        int height;
        char* root;

    public:

        btree();
        ~btree() {
        }
        void setNewRoot(char *) __attribute__((optimize(0)));
        void getNumberOfNodes() __attribute__((optimize(0)));
        void btree_insert(uint64_t, char*) __attribute__((optimize(0)));
        void btree_insert(char*, char*) __attribute__((optimize(0)));
        void btree_insert_internal(char *, uint64_t, char *, uint32_t) __attribute__((optimize(0)));
        void btree_insert_internal(char *, key_item *, char *, uint32_t) __attribute__((optimize(0)));
        void btree_delete(uint64_t) __attribute__((optimize(0)));
        //void btree_delete_internal
        //    (entry_key_t, char *, uint32_t, entry_key_t *, bool *, page **);
        char *btree_search(uint64_t) __attribute__((optimize(0)));
        char *btree_search(char *) __attribute__((optimize(0)));
        void btree_search_range(uint64_t, uint64_t, unsigned long *, int, int &) __attribute__((optimize(0)));
        void btree_search_range(char *, char *, unsigned long *, int, int &) __attribute__((optimize(0)));
        key_item *make_key_item(char *, size_t, bool) __attribute__((optimize(0)));

        friend class page;
};

class header{
    private:
        page* leftmost_ptr;         // 8 bytes
        page* sibling_ptr;          // 8 bytes
        uint32_t level;             // 4 bytes
        uint32_t switch_counter;    // 4 bytes
        std::mutex *mtx;            // 8 bytes
        union Key highest;          // 8 bytes
        uint8_t is_deleted;         // 1 bytes
        int16_t last_index;         // 2 bytes
        uint8_t dummy[5];           // 5 bytes

        friend class page;
        friend class btree;

    public:
        header() {
            mtx = new std::mutex();

            leftmost_ptr = NULL;  
            sibling_ptr = NULL;
            switch_counter = 0;
            last_index = -1;
            is_deleted = false;
#ifdef LOCK_INIT
            lock_initializer.push_back(mtx);
#endif
        }

        ~header() {
            delete mtx;
        }
};

class entry{
    private:
        union Key key; // 8 bytes
        char* ptr;     // 8 bytes

    public :
        entry(){
            key.ikey = UINT64_MAX;
            ptr = NULL;
        }

        friend class page;
        friend class btree;
};

const int cardinality = (PAGESIZE-sizeof(header))/sizeof(entry);
const int count_in_line = CACHE_LINE_SIZE / sizeof(entry);

class page{
    private:
        header hdr;  // header in persistent memory, 16 bytes
        entry records[cardinality]; // slots in persistent memory, 16 bytes * n

    public:
        friend class btree;

        page(uint32_t level = 0) {
            hdr.level = level;
            records[0].ptr = NULL;
        }

        // this is called when tree grows
        page(page* left, uint64_t key, page* right, uint32_t level = 0) {
            hdr.leftmost_ptr = left;
            hdr.level = level;
            records[0].key.ikey = key;
            records[0].ptr = (char*) right;
            records[1].ptr = NULL;

            hdr.last_index = 0;

            clflush((char*)this, sizeof(page));
        }

        // this is called when tree grows
        page(page* left, key_item *key, page* right, uint32_t level = 0) {
            hdr.leftmost_ptr = left;
            hdr.level = level;
            records[0].key.skey = key;
            records[0].ptr = (char*) right;
            records[1].ptr = NULL;

            hdr.last_index = 0;

            clflush((char*)this, sizeof(page));
        }

        void *operator new(size_t size) {
            void *ret;
            posix_memalign(&ret,64,size);
            return ret;
        }

        inline int count() __attribute__((optimize(0))) {
            uint32_t previous_switch_counter;
            int count = 0;
            do {
                previous_switch_counter = hdr.switch_counter;
                count = hdr.last_index + 1;

                while(count >= 0 && records[count].ptr != NULL) {
                    if(IS_FORWARD(previous_switch_counter))
                        ++count;
                    else
                        --count;
                }

                if(count < 0) {
                    count = 0;
                    while(records[count].ptr != NULL) {
                        ++count;
                    }
                }

            } while(IS_FORWARD(previous_switch_counter) != IS_FORWARD(hdr.switch_counter));

            return count;
        }

        inline bool remove_key(uint64_t key) __attribute__((optimize(0))) {
            // Set the switch_counter
            if(IS_FORWARD(hdr.switch_counter))
                ++hdr.switch_counter;
            else
                hdr.switch_counter += 2;

            bool shift = false;
            int i;
            for(i = 0; records[i].ptr != NULL; ++i) {
                if(!shift && records[i].key.ikey == key) {
                    records[i].ptr = (i == 0) ?
                        (char *)hdr.leftmost_ptr : records[i - 1].ptr;
                    shift = true;
                }

                if(shift) {
                    records[i].key.ikey = records[i + 1].key.ikey;
                    records[i].ptr = records[i + 1].ptr;

                    // flush
                    uint64_t records_ptr = (uint64_t)(&records[i]);
                    int remainder = records_ptr % CACHE_LINE_SIZE;
                    bool do_flush = (remainder == 0) ||
                        ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) == 1) &&
                         ((remainder + sizeof(entry)) % CACHE_LINE_SIZE) != 0);
                    if(do_flush) {
                        clflush((char *)records_ptr, CACHE_LINE_SIZE);
                    }
                }
            }

            if(shift) {
                --hdr.last_index;
            }
            return shift;
        }

        bool remove(btree* bt, uint64_t key, bool only_rebalance = false, bool with_lock = true) __attribute__((optimize(0))) {
            hdr.mtx->lock();

            bool ret = remove_key(key);

            hdr.mtx->unlock();

            return ret;
        }

#if 0
        /*
         * Although we implemented the rebalancing of B+-Tree, it is currently blocked for the performance.
         * Please refer to the follow.
         * Chi, P., Lee, W. C., & Xie, Y. (2014, August).
         * Making B+-tree efficient in PCM-based main memory. In Proceedings of the 2014
         * international symposium on Low power electronics and design (pp. 69-74). ACM.
         */
        bool remove_rebalancing(btree* bt, entry_key_t key, bool only_rebalance = false, bool with_lock = true) {
            if(with_lock) {
                hdr.mtx->lock();
            }
            if(hdr.is_deleted) {
                if(with_lock) {
                    hdr.mtx->unlock();
                }
                return false;
            }

            if(!only_rebalance) {
                register int num_entries_before = count();

                // This node is root
                if(this == (page *)bt->root) {
                    if(hdr.level > 0) {
                        if(num_entries_before == 1 && !hdr.sibling_ptr) {
                            bt->root = (char *)hdr.leftmost_ptr;
                            clflush((char *)&(bt->root), sizeof(char *));

                            hdr.is_deleted = 1;
                        }
                    }

                    // Remove the key from this node
                    bool ret = remove_key(key);

                    if(with_lock) {
                        hdr.mtx->unlock();
                    }
                    return true;
                }

                bool should_rebalance = true;
                // check the node utilization
                if(num_entries_before - 1 >= (int)((cardinality - 1) * 0.5)) { 
                    should_rebalance = false;
                }

                // Remove the key from this node
                bool ret = remove_key(key);

                if(!should_rebalance) {
                    if(with_lock) {
                        hdr.mtx->unlock();
                    }
                    return (hdr.leftmost_ptr == NULL) ? ret : true;
                }
            } 

            //Remove a key from the parent node
            entry_key_t deleted_key_from_parent = 0;
            bool is_leftmost_node = false;
            page *left_sibling;
            bt->btree_delete_internal(key, (char *)this, hdr.level + 1,
                    &deleted_key_from_parent, &is_leftmost_node, &left_sibling);

            if(is_leftmost_node) {
                if(with_lock) {
                    hdr.mtx->unlock();
                }

                if(!with_lock) {
                    hdr.sibling_ptr->hdr.mtx->lock();
                }
                hdr.sibling_ptr->remove(bt, hdr.sibling_ptr->records[0].key, true, with_lock);
                if(!with_lock) {
                    hdr.sibling_ptr->hdr.mtx->unlock();
                }
                return true;
            }

            if(with_lock) {
                left_sibling->hdr.mtx->lock();
            }

            while(left_sibling->hdr.sibling_ptr != this) {
                if(with_lock) {
                    page *t = left_sibling->hdr.sibling_ptr;
                    left_sibling->hdr.mtx->unlock();
                    left_sibling = t;
                    left_sibling->hdr.mtx->lock();
                }
                else
                    left_sibling = left_sibling->hdr.sibling_ptr;
            }

            register int num_entries = count();
            register int left_num_entries = left_sibling->count();

            // Merge or Redistribution
            int total_num_entries = num_entries + left_num_entries;
            if(hdr.leftmost_ptr)
                ++total_num_entries;

            entry_key_t parent_key;

            if(total_num_entries > cardinality - 1) { // Redistribution
                register int m = (int) ceil(total_num_entries / 2);

                if(num_entries < left_num_entries) { // left -> right
                    if(hdr.leftmost_ptr == nullptr){
                        for(int i=left_num_entries - 1; i>=m; i--){
                            insert_key
                                (left_sibling->records[i].key, left_sibling->records[i].ptr, &num_entries); 
                        } 

                        left_sibling->records[m].ptr = nullptr;
                        clflush((char *)&(left_sibling->records[m].ptr), sizeof(char *));

                        left_sibling->hdr.last_index = m - 1;
                        clflush((char *)&(left_sibling->hdr.last_index), sizeof(int16_t));

                        parent_key = records[0].key; 
                    }
                    else{
                        insert_key(deleted_key_from_parent, (char*)hdr.leftmost_ptr,
                                &num_entries); 

                        for(int i=left_num_entries - 1; i>m; i--){
                            insert_key
                                (left_sibling->records[i].key, left_sibling->records[i].ptr, &num_entries); 
                        }

                        parent_key = left_sibling->records[m].key; 

                        hdr.leftmost_ptr = (page*)left_sibling->records[m].ptr; 
                        clflush((char *)&(hdr.leftmost_ptr), sizeof(page *));

                        left_sibling->records[m].ptr = nullptr;
                        clflush((char *)&(left_sibling->records[m].ptr), sizeof(char *));

                        left_sibling->hdr.last_index = m - 1;
                        clflush((char *)&(left_sibling->hdr.last_index), sizeof(int16_t));
                    }

                    if(left_sibling == ((page *)bt->root)) {
                        page* new_root = new page(left_sibling, parent_key, this, hdr.level + 1);
                        bt->setNewRoot((char *)new_root);
                    }
                    else {
                        bt->btree_insert_internal
                            ((char *)left_sibling, parent_key, (char *)this, hdr.level + 1);
                    }
                }
                else{ // from leftmost case
                    hdr.is_deleted = 1;
                    clflush((char *)&(hdr.is_deleted), sizeof(uint8_t));

                    page* new_sibling = new page(hdr.level); 
                    new_sibling->hdr.mtx->lock();
                    new_sibling->hdr.sibling_ptr = hdr.sibling_ptr;

                    int num_dist_entries = num_entries - m;
                    int new_sibling_cnt = 0;

                    if(hdr.leftmost_ptr == nullptr){
                        for(int i=0; i<num_dist_entries; i++){
                            left_sibling->insert_key(records[i].key, records[i].ptr,
                                    &left_num_entries); 
                        } 

                        for(int i=num_dist_entries; records[i].ptr != NULL; i++){
                            new_sibling->insert_key(records[i].key, records[i].ptr,
                                    &new_sibling_cnt, false); 
                        } 

                        clflush((char *)(new_sibling), sizeof(page));

                        left_sibling->hdr.sibling_ptr = new_sibling;
                        clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(page *));

                        parent_key = new_sibling->records[0].key; 
                    }
                    else{
                        left_sibling->insert_key(deleted_key_from_parent,
                                (char*)hdr.leftmost_ptr, &left_num_entries);

                        for(int i=0; i<num_dist_entries - 1; i++){
                            left_sibling->insert_key(records[i].key, records[i].ptr,
                                    &left_num_entries); 
                        } 

                        parent_key = records[num_dist_entries - 1].key;

                        new_sibling->hdr.leftmost_ptr = (page*)records[num_dist_entries - 1].ptr;
                        for(int i=num_dist_entries; records[i].ptr != NULL; i++){
                            new_sibling->insert_key(records[i].key, records[i].ptr,
                                    &new_sibling_cnt, false); 
                        } 
                        clflush((char *)(new_sibling), sizeof(page));

                        left_sibling->hdr.sibling_ptr = new_sibling;
                        clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(page *));
                    }

                    if(left_sibling == ((page *)bt->root)) {
                        page* new_root = new page(left_sibling, parent_key, new_sibling, hdr.level + 1);
                        bt->setNewRoot((char *)new_root);
                    }
                    else {
                        bt->btree_insert_internal
                            ((char *)left_sibling, parent_key, (char *)new_sibling, hdr.level + 1);
                    }

                    new_sibling->hdr.mtx->unlock();
                }
            }
            else {
                hdr.is_deleted = 1;
                clflush((char *)&(hdr.is_deleted), sizeof(uint8_t));

                if(hdr.leftmost_ptr)
                    left_sibling->insert_key(deleted_key_from_parent, 
                            (char *)hdr.leftmost_ptr, &left_num_entries);

                for(int i = 0; records[i].ptr != NULL; ++i) { 
                    left_sibling->insert_key(records[i].key, records[i].ptr, &left_num_entries);
                }

                left_sibling->hdr.sibling_ptr = hdr.sibling_ptr;
                clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(page *));
            }

            if(with_lock) {
                left_sibling->hdr.mtx->unlock();
                hdr.mtx->unlock();
            }

            return true;
        }
#endif
        inline void
            insert_key(uint64_t key, char* ptr, int *num_entries, bool flush = true,
                    bool update_last_index = true) __attribute__((optimize(0))) {
                // update switch_counter
                if(!IS_FORWARD(hdr.switch_counter))
                    ++hdr.switch_counter;
                else
                    hdr.switch_counter += 2;

                // FAST
                if(*num_entries == 0) {  // this page is empty
                    entry* new_entry = (entry*) &records[0];
                    entry* array_end = (entry*) &records[1];
                    new_entry->key.ikey = (uint64_t) key;
                    new_entry->ptr = (char*) ptr;

                    array_end->ptr = (char*)NULL;

                    if(flush) {
                        clflush((char*) this, CACHE_LINE_SIZE);
                    }
                }
                else {
                    int i = *num_entries - 1, inserted = 0, to_flush_cnt = 0;
                    records[*num_entries+1].ptr = records[*num_entries].ptr;
                    if(flush) {
                        if((uint64_t)&(records[*num_entries+1].ptr) % CACHE_LINE_SIZE == 0)
                            clflush((char*)&(records[*num_entries+1].ptr), sizeof(char*));
                    }

                    // FAST
                    for(i = *num_entries - 1; i >= 0; i--) {
                        if(key < records[i].key.ikey) {
                            records[i+1].ptr = records[i].ptr;
                            records[i+1].key.ikey = records[i].key.ikey;

                            if(flush) {
                                uint64_t records_ptr = (uint64_t)(&records[i+1]);

                                int remainder = records_ptr % CACHE_LINE_SIZE;
                                bool do_flush = (remainder == 0) ||
                                    ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) == 1)
                                     && ((remainder+sizeof(entry))%CACHE_LINE_SIZE)!=0);
                                if(do_flush) {
                                    clflush((char*)records_ptr,CACHE_LINE_SIZE);
                                    to_flush_cnt = 0;
                                }
                                else
                                    ++to_flush_cnt;
                            }
                        }
                        else{
                            records[i+1].ptr = records[i].ptr;
                            records[i+1].key.ikey = key;
                            records[i+1].ptr = ptr;

                            if(flush)
                                clflush((char*)&records[i+1],sizeof(entry));
                            inserted = 1;
                            break;
                        }
                    }
                    if(inserted==0){
                        records[0].ptr =(char*) hdr.leftmost_ptr;
                        records[0].key.ikey = key;
                        records[0].ptr = ptr;
                        if(flush)
                            clflush((char*) &records[0], sizeof(entry));
                    }
                }

                if(update_last_index) {
                    hdr.last_index = *num_entries;
                }
                ++(*num_entries);
            }

        inline void
            insert_key(key_item *key, char* ptr, int *num_entries, bool flush = true,
                    bool update_last_index = true) __attribute__((optimize(0))) {
                // update switch_counter
                if(!IS_FORWARD(hdr.switch_counter))
                    ++hdr.switch_counter;
                else
                    hdr.switch_counter += 2;

                // FAST
                if(*num_entries == 0) {  // this page is empty
                    entry* new_entry = (entry*) &records[0];
                    entry* array_end = (entry*) &records[1];
                    new_entry->key.skey = key;
                    new_entry->ptr = (char*) ptr;

                    array_end->ptr = (char*)NULL;

                    if(flush) {
                        clflush((char*) this, CACHE_LINE_SIZE);
                    }
                }
                else {
                    int i = *num_entries - 1, inserted = 0, to_flush_cnt = 0;
                    records[*num_entries+1].ptr = records[*num_entries].ptr;
                    if(flush) {
                        if((uint64_t)&(records[*num_entries+1].ptr) % CACHE_LINE_SIZE == 0)
                            clflush((char*)&(records[*num_entries+1].ptr), sizeof(char*));
                    }

                    // FAST
                    for(i = *num_entries - 1; i >= 0; i--) {
                        if(memcmp(key->key, records[i].key.skey->key,
                                    std::min(key->key_len, records[i].key.skey->key_len)) < 0) {
                            records[i+1].ptr = records[i].ptr;
                            records[i+1].key.skey = records[i].key.skey;

                            if(flush) {
                                uint64_t records_ptr = (uint64_t)(&records[i+1]);

                                int remainder = records_ptr % CACHE_LINE_SIZE;
                                bool do_flush = (remainder == 0) ||
                                    ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) == 1)
                                     && ((remainder+sizeof(entry))%CACHE_LINE_SIZE)!=0);
                                if(do_flush) {
                                    clflush((char*)records_ptr,CACHE_LINE_SIZE);
                                    to_flush_cnt = 0;
                                }
                                else
                                    ++to_flush_cnt;
                            }
                        }
                        else{
                            records[i+1].ptr = records[i].ptr;
                            records[i+1].key.skey = key;
                            records[i+1].ptr = ptr;

                            if(flush)
                                clflush((char*)&records[i+1],sizeof(entry));
                            inserted = 1;
                            break;
                        }
                    }
                    if(inserted==0){
                        records[0].ptr =(char*) hdr.leftmost_ptr;
                        records[0].key.skey = key;
                        records[0].ptr = ptr;
                        if(flush)
                            clflush((char*) &records[0], sizeof(entry));
                    }
                }

                if(update_last_index) {
                    hdr.last_index = *num_entries;
                }
                ++(*num_entries);
            }

        // Insert a new integer key - FAST and FAIR
        page *store
            (btree* bt, char* left, uint64_t key, char* right,
             bool flush, bool with_lock, page *invalid_sibling = NULL) __attribute__((optimize(0))) {
                if(with_lock) {
                    hdr.mtx->lock(); // Lock the write lock
                }
                if(hdr.is_deleted) {
                    if(with_lock) {
                        hdr.mtx->unlock();
                    }

                    return NULL;
                }

                // If this node has a sibling node,
                if(hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
                    // Compare this key with the first key of the sibling
                    if (hdr.level > 0) {
                        if(key >= hdr.sibling_ptr->hdr.highest.ikey) {
                            if(with_lock) {
                                hdr.mtx->unlock(); // Unlock the write lock
                            }
                            return hdr.sibling_ptr->store(bt, left, key, right,
                                    true, with_lock, invalid_sibling);
                        }
                    } else {
                        if(key >= hdr.sibling_ptr->hdr.highest.ikey) {
                            if(with_lock) {
                                hdr.sibling_ptr->hdr.mtx->lock();
                                bt->btree_insert_internal((char *)this, hdr.sibling_ptr->hdr.highest.ikey,
                                        (char *)hdr.sibling_ptr, hdr.level + 1);
                                hdr.sibling_ptr->hdr.mtx->unlock();
                                hdr.mtx->unlock(); // Unlock the write lock
                            }
                            return hdr.sibling_ptr->store(bt, left, key, right,
                                    true, with_lock, invalid_sibling);
                        }
                    }
                }

                if (left != NULL) {
                    char *_ret = this->linear_search(key);
                    if (_ret == left || _ret == right) {
                        hdr.mtx->unlock();
                        return this;
                    } else {
                        printf("Need to recover\n");
                        hdr.mtx->unlock();
                        return this;
                    }
                }

                register int num_entries = count();

                // FAST
                if(num_entries < cardinality - 1) {
                    insert_key(key, right, &num_entries, flush);

                    if(with_lock) {
                        hdr.mtx->unlock(); // Unlock the write lock
                    }

                    return this;
                }
                else {// FAIR
                    // overflow
                    // create a new node
                    page* sibling = new page(hdr.level);
                    register int m = (int) ceil(num_entries/2);
                    uint64_t split_key = records[m].key.ikey;

                    // migrate half of keys into the sibling
                    int sibling_cnt = 0;
                    if(hdr.leftmost_ptr == NULL){ // leaf node
                        for(int i=m; i<num_entries; ++i){
                            sibling->insert_key(records[i].key.ikey, records[i].ptr, &sibling_cnt, false);
                        }
                    }
                    else{ // internal node
                        for(int i=m+1;i<num_entries;++i){
                            sibling->insert_key(records[i].key.ikey, records[i].ptr, &sibling_cnt, false);
                        }
                        sibling->hdr.leftmost_ptr = (page*) records[m].ptr;
                    }

                    sibling->hdr.highest.ikey = records[m].key.ikey;
                    sibling->hdr.sibling_ptr = hdr.sibling_ptr;
                    clflush((char *)sibling, sizeof(page));

                    if (hdr.leftmost_ptr == NULL)
                        sibling->hdr.mtx->lock();

                    if(IS_FORWARD(hdr.switch_counter))
                        hdr.switch_counter++;
                    else
                        hdr.switch_counter += 2;
                    mfence();
                    hdr.sibling_ptr = sibling;
                    clflush((char*) &hdr, sizeof(hdr));

                    // set to NULL
                    records[m].ptr = NULL;
                    clflush((char*) &records[m], sizeof(entry));

                    hdr.last_index = m - 1;
                    clflush((char *)&(hdr.last_index), sizeof(int16_t));

                    num_entries = hdr.last_index + 1;

                    page *ret;

                    // insert the key for internal node
                    if (hdr.leftmost_ptr != NULL) {
                        if(key < split_key) {
                            insert_key(key, right, &num_entries);
                            ret = this;
                        }
                        else {
                            sibling->insert_key(key, right, &sibling_cnt);
                            ret = sibling;
                        }
                    }

                    // Set a new root or insert the split key to the parent
                    if(bt->root == (char *)this) { // only one node can update the root ptr
                        page* new_root = new page((page*)this, split_key, sibling,
                                hdr.level + 1);
                        bt->setNewRoot((char *)new_root);

                        if(with_lock && hdr.leftmost_ptr != NULL) {
                            hdr.mtx->unlock(); // Unlock the write lock
                        }
                    }
                    else {
                        if(with_lock && hdr.leftmost_ptr != NULL) {
                            hdr.mtx->unlock(); // Unlock the write lock
                        }

                        bt->btree_insert_internal(NULL, split_key, (char *)sibling,
                                hdr.level + 1);
                    }

                    // insert the key for leaf node
                    if (hdr.leftmost_ptr == NULL) {
                        if(key < split_key) {
                            insert_key(key, right, &num_entries);
                            ret = this;
                        }
                        else {
                            sibling->insert_key(key, right, &sibling_cnt);
                            ret = sibling;
                        }

                        if (with_lock) {
                            hdr.mtx->unlock();
                            sibling->hdr.mtx->unlock();
                        }
                    }

                    return ret;
                }

            }

        // Insert a new string key - FAST and FAIR
        page *store
            (btree* bt, char* left, key_item *key, char* right,
             bool flush, bool with_lock, page *invalid_sibling = NULL) __attribute__((optimize(0))) {
                if(with_lock) {
                    hdr.mtx->lock(); // Lock the write lock
                }
                if(hdr.is_deleted) {
                    if(with_lock) {
                        hdr.mtx->unlock();
                    }

                    return NULL;
                }

                // If this node has a sibling node,
                if(hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
                    // Compare this key with the first key of the sibling
                    if (hdr.level > 0) {
                        if(memcmp(key->key, hdr.sibling_ptr->hdr.highest.skey->key,
                                    std::min(key->key_len, hdr.sibling_ptr->hdr.highest.skey->key_len)) >= 0) {
                            if(with_lock) {
                                hdr.mtx->unlock(); // Unlock the write lock
                            }
                            return hdr.sibling_ptr->store(bt, left, key, right,
                                    true, with_lock, invalid_sibling);
                        }
                    } else {
                        if(memcmp(key->key, hdr.sibling_ptr->hdr.highest.skey->key,
                                    std::min(key->key_len, hdr.sibling_ptr->hdr.highest.skey->key_len)) >= 0) {
                            if(with_lock) {
                                hdr.sibling_ptr->hdr.mtx->lock();
                                bt->btree_insert_internal((char *)this, hdr.sibling_ptr->hdr.highest.skey,
                                        (char *)hdr.sibling_ptr, hdr.level + 1);
                                hdr.sibling_ptr->hdr.mtx->unlock();
                                hdr.mtx->unlock(); // Unlock the write lock
                            }
                            return hdr.sibling_ptr->store(bt, left, key, right,
                                    true, with_lock, invalid_sibling);
                        }
                    }
                }

                if (left != NULL) {
                    char *_ret = this->linear_search(key);
                    if (_ret == left || _ret == right) {
                        hdr.mtx->unlock();
                        return this;
                    } else {
                        printf("Need to recover\n");
                        hdr.mtx->unlock();
                        return this;
                    }
                }

                register int num_entries = count();

                // FAST
                if(num_entries < cardinality - 1) {
                    insert_key(key, right, &num_entries, flush);

                    if(with_lock) {
                        hdr.mtx->unlock(); // Unlock the write lock
                    }

                    return this;
                }
                else {// FAIR
                    // overflow
                    // create a new node
                    page* sibling = new page(hdr.level);
                    register int m = (int) ceil(num_entries/2);
                    key_item *split_key = records[m].key.skey;

                    // migrate half of keys into the sibling
                    int sibling_cnt = 0;
                    if(hdr.leftmost_ptr == NULL){ // leaf node
                        for(int i=m; i<num_entries; ++i){
                            sibling->insert_key(records[i].key.skey, records[i].ptr, &sibling_cnt, false);
                        }
                    }
                    else{ // internal node
                        for(int i=m+1;i<num_entries;++i){
                            sibling->insert_key(records[i].key.skey, records[i].ptr, &sibling_cnt, false);
                        }
                        sibling->hdr.leftmost_ptr = (page*) records[m].ptr;
                    }

                    sibling->hdr.highest.skey = records[m].key.skey;
                    sibling->hdr.sibling_ptr = hdr.sibling_ptr;
                    clflush((char *)sibling, sizeof(page));

                    if (hdr.leftmost_ptr == NULL)
                        sibling->hdr.mtx->lock();

                    // set to NULL
                    if(IS_FORWARD(hdr.switch_counter))
                        hdr.switch_counter++;
                    else
                        hdr.switch_counter += 2;
                    mfence();
                    hdr.sibling_ptr = sibling;
                    clflush((char*) &hdr, sizeof(hdr));

                    records[m].ptr = NULL;
                    clflush((char*) &records[m], sizeof(entry));

                    hdr.last_index = m - 1;
                    clflush((char *)&(hdr.last_index), sizeof(int16_t));

                    num_entries = hdr.last_index + 1;

                    page *ret;

                    if (hdr.leftmost_ptr != NULL) {
                        // insert the key for internal node
                        if(memcmp(key->key, split_key->key,
                                    std::min(key->key_len, split_key->key_len)) < 0) {
                            insert_key(key, right, &num_entries);
                            ret = this;
                        }
                        else {
                            sibling->insert_key(key, right, &sibling_cnt);
                            ret = sibling;
                        }
                    }

                    // Set a new root or insert the split key to the parent
                    if(bt->root == (char *)this) { // only one node can update the root ptr
                        page* new_root = new page((page*)this, split_key, sibling,
                                hdr.level + 1);
                        bt->setNewRoot((char *)new_root);

                        if(with_lock && hdr.leftmost_ptr != NULL) {
                            hdr.mtx->unlock(); // Unlock the write lock
                        }
                    }
                    else {
                        if(with_lock && hdr.leftmost_ptr != NULL) {
                            hdr.mtx->unlock(); // Unlock the write lock
                        }

                        bt->btree_insert_internal(NULL, split_key, (char *)sibling,
                                hdr.level + 1);
                    }

                    if (hdr.leftmost_ptr == NULL) {
                        // insert the key for leafnode node
                        if(memcmp(key->key, split_key->key,
                                    std::min(key->key_len, split_key->key_len)) < 0) {
                            insert_key(key, right, &num_entries);
                            ret = this;
                        }
                        else {
                            sibling->insert_key(key, right, &sibling_cnt);
                            ret = sibling;
                        }

                        if (with_lock) {
                            hdr.mtx->unlock();
                            sibling->hdr.mtx->unlock();
                        }
                    }

                    return ret;
                }

            }

        // Search integer keys with linear search
        void linear_search_range
            (uint64_t min, uint64_t max, unsigned long *buf, int num, int &off) __attribute__((optimize(0))) {
                int i;
                uint32_t previous_switch_counter;
                page *current = this;
                void *snapshot_n;
                off = 0;

                while(current) {
                    int old_off = off;
                    snapshot_n = current->hdr.sibling_ptr;
                    mfence();
                    do {
                        previous_switch_counter = current->hdr.switch_counter;
                        off = old_off;

                        uint64_t tmp_key;
                        char *tmp_ptr;

                        if(IS_FORWARD(previous_switch_counter)) {
                            if((tmp_key = current->records[0].key.ikey) > min) {
                                if(tmp_key < max && off < num) {
                                    if((tmp_ptr = current->records[0].ptr) != NULL) {
                                        if(tmp_key == current->records[0].key.ikey) {
                                            if(tmp_ptr) {
                                                buf[off++] = (unsigned long)tmp_ptr;
                                            }
                                        }
                                    }
                                }
                                else
                                    return;
                            }

                            for(i=1; current->records[i].ptr != NULL; ++i) {
                                if((tmp_key = current->records[i].key.ikey) > min) {
                                    if(tmp_key < max && off < num) {
                                        if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                            if(tmp_key == current->records[i].key.ikey) {
                                                if(tmp_ptr) {
                                                    buf[off++] = (unsigned long)tmp_ptr;
                                                }
                                            }
                                        }
                                    }
                                    else
                                        return;
                                }
                            }
                        }
                        else {
                            for(i=count() - 1; i > 0; --i) {
                                if((tmp_key = current->records[i].key.ikey) > min) {
                                    if(tmp_key < max && off < num) {
                                        if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                            if(tmp_key == current->records[i].key.ikey) {
                                                if(tmp_ptr) {
                                                    buf[off++] = (unsigned long)tmp_ptr;
                                                }
                                            }
                                        }
                                    }
                                    else
                                        return;
                                }
                            }

                            if((tmp_key = current->records[0].key.ikey) > min) {
                                if(tmp_key < max && off < num) {
                                    if((tmp_ptr = current->records[0].ptr) != NULL) {
                                        if(tmp_key == current->records[0].key.ikey) {
                                            if(tmp_ptr) {
                                                buf[off++] = (unsigned long)tmp_ptr;
                                            }
                                        }
                                    }
                                }
                                else
                                    return;
                            }
                        }
                    } while(previous_switch_counter != current->hdr.switch_counter);

                    if (snapshot_n == current->hdr.sibling_ptr)
                        current = current->hdr.sibling_ptr;
                    else
                        off = old_off;
                }
            }

        // Search string keys with linear search
        void linear_search_range
            (key_item *min, key_item *max, unsigned long *buf, int num, int &off) __attribute__((optimize(0))) {
                int i;
                uint32_t previous_switch_counter;
                page *current = this;
                void *snapshot_n;
                off = 0;

                while(current) {
                    int old_off = off;
                    snapshot_n = current->hdr.sibling_ptr;
                    mfence();
                    do {
                        previous_switch_counter = current->hdr.switch_counter;
                        off = old_off;

                        key_item *tmp_key;
                        char *tmp_ptr;

                        if(IS_FORWARD(previous_switch_counter)) {
                            tmp_key = current->records[0].key.skey;
                            if(memcmp(tmp_key->key, min->key, std::min(tmp_key->key_len, min->key_len)) > 0) {
                                //if(memcmp(tmp_key->key, max->key, std::min(tmp_key->key_len, max->key_len)) < 0 && off < num) {
                                if(off < num) {
                                    if((tmp_ptr = current->records[0].ptr) != NULL) {
                                        if(memcmp(tmp_key->key, current->records[0].key.skey->key,
                                                    std::min(tmp_key->key_len, current->records[0].key.skey->key_len)) == 0) {
                                            if(tmp_ptr) {
                                                buf[off++] = (unsigned long)tmp_ptr;
                                            }
                                        }
                                    }
                                }
                                else
                                    return;
                            }

                            for(i=1; current->records[i].ptr != NULL; ++i) {
                                tmp_key = current->records[i].key.skey;
                                if(memcmp(tmp_key->key, min->key, std::min(tmp_key->key_len, min->key_len)) > 0) {
                                    //if(memcmp(tmp_key->key, max->key, std::min(tmp_key->key_len, max->key_len)) < 0 && off < num) {
                                    if(off < num) {
                                        if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                            if(memcmp(tmp_key->key, current->records[i].key.skey->key,
                                                        std::min(tmp_key->key_len, current->records[i].key.skey->key_len)) == 0) {
                                                if(tmp_ptr) {
                                                    buf[off++] = (unsigned long)tmp_ptr;
                                                }
                                            }
                                        }
                                    }
                                    else
                                        return;
                                }
                            }
                        }
                        else {
                            for(i=count() - 1; i > 0; --i) {
                                tmp_key = current->records[i].key.skey;
                                if(memcmp(tmp_key->key, min->key, std::min(tmp_key->key_len, min->key_len)) > 0 && off < num) {
                                    //if(memcmp(tmp_key->key, max->key, std::min(tmp_key->key_len, max->key_len)) < 0 && off < num) {
                                    if(off < num) {
                                        if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                            if(memcmp(tmp_key->key, current->records[i].key.skey->key, std::min(tmp_key->key_len, current->records[i].key.skey->key_len)) == 0) {
                                                if(tmp_ptr) {
                                                    buf[off++] = (unsigned long)tmp_ptr;
                                                }
                                            }
                                        }
                                    }
                                    else
                                        return;
                                }
                            }

                            tmp_key = current->records[0].key.skey;
                            if(memcmp(tmp_key->key, min->key, std::min(tmp_key->key_len, min->key_len)) > 0 && off < num) {
                                //if(memcmp(tmp_key->key, max->key, std::min(tmp_key->key_len, min->key_len)) < 0 && off < num) {
                                if(off < num) {
                                    if((tmp_ptr = current->records[0].ptr) != NULL) {
                                        if(memcmp(tmp_key->key, current->records[0].key.skey->key, std::min(tmp_key->key_len, current->records[0].key.skey->key_len)) == 0) {
                                            if(tmp_ptr) {
                                                buf[off++] = (unsigned long)tmp_ptr;
                                            }
                                        }
                                    }
                                }
                                else
                                    return;
                            }
                        }
                    } while(previous_switch_counter != current->hdr.switch_counter);

                    if (snapshot_n == current->hdr.sibling_ptr)
                        current = current->hdr.sibling_ptr;
                    else
                        off = old_off;
                }
            }

        char *linear_search(btree *bt, uint64_t key) __attribute__((optimize(0))) {
            int i = 1;
            uint32_t previous_switch_counter;
            char *ret = NULL;
            char *t;
            uint64_t k;

            if(hdr.leftmost_ptr == NULL) { // Search a leaf node
                do {
                    previous_switch_counter = hdr.switch_counter;
                    ret = NULL;

                    // search from left ro right
                    if(IS_FORWARD(previous_switch_counter)) {
                        if((k = records[0].key.ikey) == key) {
                            if((t = records[0].ptr) != NULL) {
                                if(k == records[0].key.ikey) {
                                    ret = t;
                                    continue;
                                }
                            }
                        }

                        for(i=1; records[i].ptr != NULL; ++i) {
                            if((k = records[i].key.ikey) == key) {
                                if(records[i-1].ptr != (t = records[i].ptr)) {
                                    if(k == records[i].key.ikey) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    else { // search from right to left
                        for(i = count() - 1; i > 0; --i) {
                            if((k = records[i].key.ikey) == key) {
                                if(records[i - 1].ptr != (t = records[i].ptr) && t) {
                                    if(k == records[i].key.ikey) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }

                        if(!ret) {
                            if((k = records[0].key.ikey) == key) {
                                if(NULL != (t = records[0].ptr) && t) {
                                    if(k == records[0].key.ikey) {
                                        ret = t;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                } while(IS_FORWARD(hdr.switch_counter) != IS_FORWARD(previous_switch_counter));

                if(ret) {
                    return ret;
                }

                if((t = (char *)hdr.sibling_ptr) && key >= ((page *)t)->hdr.highest.ikey) {
                    hdr.mtx->lock();
                    hdr.sibling_ptr->hdr.mtx->lock();
                    bt->btree_insert_internal((char *)this, hdr.sibling_ptr->hdr.highest.ikey,
                            (char *)hdr.sibling_ptr, hdr.level + 1);
                    hdr.sibling_ptr->hdr.mtx->unlock();
                    hdr.mtx->unlock();
                    return t;
                }

                return NULL;
            }
            else { // internal node
                do {
                    previous_switch_counter = hdr.switch_counter;
                    ret = NULL;

                    if(IS_FORWARD(previous_switch_counter)) {
                        if(key < (k = records[0].key.ikey)) {
                            if((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
                                ret = t;
                                continue;
                            }
                        }

                        for(i = 1; records[i].ptr != NULL; ++i) {
                            if(key < (k = records[i].key.ikey)) {
                                if((t = records[i-1].ptr) != records[i].ptr) {
                                    ret = t;
                                    break;
                                }
                            }
                        }

                        if(!ret) {
                            ret = records[i - 1].ptr;
                            continue;
                        }
                    }
                    else { // search from right to left
                        for(i = count() - 1; i >= 0; --i) {
                            if(key >= (k = records[i].key.ikey)) {
                                if(i == 0) {
                                    if((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                                        ret = t;
                                        break;
                                    }
                                }
                                else {
                                    if(records[i - 1].ptr != (t = records[i].ptr)) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } while(IS_FORWARD(hdr.switch_counter) != IS_FORWARD(previous_switch_counter));

                if((t = (char *)hdr.sibling_ptr) != NULL) {
                    if(key >= ((page *)t)->hdr.highest.ikey) {
                        hdr.mtx->lock();
                        hdr.sibling_ptr->hdr.mtx->lock();
                        bt->btree_insert_internal((char *)this, hdr.sibling_ptr->hdr.highest.ikey,
                                (char *)hdr.sibling_ptr, hdr.level + 1);
                        hdr.sibling_ptr->hdr.mtx->unlock();
                        hdr.mtx->unlock();
                        return t;
                    }
                }

                if(ret) {
                    return ret;
                }
                else
                    return (char *)hdr.leftmost_ptr;
            }

            return NULL;
        }


        char *linear_search(uint64_t key) __attribute__((optimize(0))) {
            int i = 1;
            uint32_t previous_switch_counter;
            char *ret = NULL;
            char *t;
            uint64_t k;

            if(hdr.leftmost_ptr == NULL) { // Search a leaf node
                do {
                    previous_switch_counter = hdr.switch_counter;
                    ret = NULL;

                    // search from left ro right
                    if(IS_FORWARD(previous_switch_counter)) {
                        if((k = records[0].key.ikey) == key) {
                            if((t = records[0].ptr) != NULL) {
                                if(k == records[0].key.ikey) {
                                    ret = t;
                                    continue;
                                }
                            }
                        }

                        for(i=1; records[i].ptr != NULL; ++i) {
                            if((k = records[i].key.ikey) == key) {
                                if(records[i-1].ptr != (t = records[i].ptr)) {
                                    if(k == records[i].key.ikey) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    else { // search from right to left
                        for(i = count() - 1; i > 0; --i) {
                            if((k = records[i].key.ikey) == key) {
                                if(records[i - 1].ptr != (t = records[i].ptr) && t) {
                                    if(k == records[i].key.ikey) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }

                        if(!ret) {
                            if((k = records[0].key.ikey) == key) {
                                if(NULL != (t = records[0].ptr) && t) {
                                    if(k == records[0].key.ikey) {
                                        ret = t;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                } while(IS_FORWARD(hdr.switch_counter) != IS_FORWARD(previous_switch_counter));

                if(ret) {
                    return ret;
                }

                if((t = (char *)hdr.sibling_ptr) && key >= ((page *)t)->hdr.highest.ikey) {
                    return t;
                }

                return NULL;
            }
            else { // internal node
                do {
                    previous_switch_counter = hdr.switch_counter;
                    ret = NULL;

                    if(IS_FORWARD(previous_switch_counter)) {
                        if(key < (k = records[0].key.ikey)) {
                            if((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
                                ret = t;
                                continue;
                            }
                        }

                        for(i = 1; records[i].ptr != NULL; ++i) {
                            if(key < (k = records[i].key.ikey)) {
                                if((t = records[i-1].ptr) != records[i].ptr) {
                                    ret = t;
                                    break;
                                }
                            }
                        }

                        if(!ret) {
                            ret = records[i - 1].ptr;
                            continue;
                        }
                    }
                    else { // search from right to left
                        for(i = count() - 1; i >= 0; --i) {
                            if(key >= (k = records[i].key.ikey)) {
                                if(i == 0) {
                                    if((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                                        ret = t;
                                        break;
                                    }
                                }
                                else {
                                    if(records[i - 1].ptr != (t = records[i].ptr)) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } while(IS_FORWARD(hdr.switch_counter) != IS_FORWARD(previous_switch_counter));

                if((t = (char *)hdr.sibling_ptr) != NULL) {
                    if(key >= ((page *)t)->hdr.highest.ikey) {
                        return t;
                    }
                }

                if(ret) {
                    return ret;
                }
                else
                    return (char *)hdr.leftmost_ptr;
            }

            return NULL;
        }

        char *linear_search(btree *bt, key_item *key) __attribute__((optimize(0))) {
            int i = 1;
            uint32_t previous_switch_counter;
            char *ret = NULL;
            char *t;
            key_item *k;

            if(hdr.leftmost_ptr == NULL) { // Search a leaf node
                do {
                    previous_switch_counter = hdr.switch_counter;
                    ret = NULL;

                    // search from left ro right
                    if(IS_FORWARD(previous_switch_counter)) {
                        k = records[0].key.skey;
                        if(memcmp(k->key, key->key, std::min(k->key_len, key->key_len)) == 0) {
                            if((t = records[0].ptr) != NULL) {
                                if(memcmp(k->key, records[0].key.skey->key, std::min(k->key_len, records[0].key.skey->key_len)) == 0) {
                                    ret = t;
                                    continue;
                                }
                            }
                        }

                        for(i=1; records[i].ptr != NULL; ++i) {
                            k = records[i].key.skey;
                            if(memcmp(k->key, key->key, std::min(k->key_len, key->key_len)) == 0) {
                                if(records[i-1].ptr != (t = records[i].ptr)) {
                                    if(memcmp(k->key, records[i].key.skey->key, std::min(k->key_len, records[i].key.skey->key_len)) == 0) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    else { // search from right to left
                        for(i = count() - 1; i > 0; --i) {
                            k = records[i].key.skey;
                            if(memcmp(k->key, key->key, std::min(k->key_len, key->key_len)) == 0) {
                                if(records[i - 1].ptr != (t = records[i].ptr) && t) {
                                    if(memcmp(k->key, records[i].key.skey->key, std::min(k->key_len, records[i].key.skey->key_len)) == 0) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }

                        if(!ret) {
                            k = records[0].key.skey;
                            if(memcmp(k->key, key->key, std::min(k->key_len, key->key_len)) == 0) {
                                if(NULL != (t = records[0].ptr) && t) {
                                    if(memcmp(k->key, records[0].key.skey->key, std::min(k->key_len, records[0].key.skey->key_len)) == 0) {
                                        ret = t;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                } while(IS_FORWARD(hdr.switch_counter) != IS_FORWARD(previous_switch_counter));

                if(ret) {
                    return ret;
                }

                if((t = (char *)hdr.sibling_ptr) && memcmp(key->key, ((page *)t)->hdr.highest.skey->key, std::min(key->key_len, ((page *)t)->hdr.highest.skey->key_len)) >= 0) {
                    hdr.mtx->lock();
                    hdr.sibling_ptr->hdr.mtx->lock();
                    bt->btree_insert_internal((char *)this, hdr.sibling_ptr->hdr.highest.skey,
                            (char *)hdr.sibling_ptr, hdr.level + 1);
                    hdr.sibling_ptr->hdr.mtx->unlock();
                    hdr.mtx->unlock();
                    return t;
                }

                return NULL;
            }
            else { // internal node
                do {
                    previous_switch_counter = hdr.switch_counter;
                    ret = NULL;

                    if(IS_FORWARD(previous_switch_counter)) {
                        k = records[0].key.skey;
                        if(memcmp(key->key, k->key, std::min(key->key_len, k->key_len)) < 0) {
                            if((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
                                ret = t;
                                continue;
                            }
                        }

                        for(i = 1; records[i].ptr != NULL; ++i) {
                            k = records[i].key.skey;
                            if(memcmp(key->key, k->key, std::min(key->key_len, k->key_len)) < 0) {
                                if((t = records[i-1].ptr) != records[i].ptr) {
                                    ret = t;
                                    break;
                                }
                            }
                        }

                        if(!ret) {
                            ret = records[i - 1].ptr;
                            continue;
                        }
                    }
                    else { // search from right to left
                        for(i = count() - 1; i >= 0; --i) {
                            k = records[i].key.skey;
                            if(memcmp(key->key, k->key, std::min(key->key_len, k->key_len)) >= 0) {
                                if(i == 0) {
                                    if((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                                        ret = t;
                                        break;
                                    }
                                }
                                else {
                                    if(records[i - 1].ptr != (t = records[i].ptr)) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } while(IS_FORWARD(hdr.switch_counter) != IS_FORWARD(previous_switch_counter));

                if((t = (char *)hdr.sibling_ptr) != NULL) {
                    if(memcmp(key->key, ((page *)t)->hdr.highest.skey->key, std::min(key->key_len, ((page *)t)->hdr.highest.skey->key_len)) >= 0) {
                        hdr.mtx->lock();
                        hdr.sibling_ptr->hdr.mtx->lock();
                        bt->btree_insert_internal((char *)this, hdr.sibling_ptr->hdr.highest.skey,
                                (char *)hdr.sibling_ptr, hdr.level + 1);
                        hdr.sibling_ptr->hdr.mtx->unlock();
                        hdr.mtx->unlock();
                        return t;
                    }
                }

                if(ret) {
                    return ret;
                }
                else
                    return (char *)hdr.leftmost_ptr;
            }

            return NULL;
        }

        char *linear_search(key_item *key) __attribute__((optimize(0))) {
            int i = 1;
            uint32_t previous_switch_counter;
            char *ret = NULL;
            char *t;
            key_item *k;

            if(hdr.leftmost_ptr == NULL) { // Search a leaf node
                do {
                    previous_switch_counter = hdr.switch_counter;
                    ret = NULL;

                    // search from left ro right
                    if(IS_FORWARD(previous_switch_counter)) {
                        k = records[0].key.skey;
                        if(memcmp(k->key, key->key, std::min(k->key_len, key->key_len)) == 0) {
                            if((t = records[0].ptr) != NULL) {
                                if(memcmp(k->key, records[0].key.skey->key, std::min(k->key_len, records[0].key.skey->key_len)) == 0) {
                                    ret = t;
                                    continue;
                                }
                            }
                        }

                        for(i=1; records[i].ptr != NULL; ++i) {
                            k = records[i].key.skey;
                            if(memcmp(k->key, key->key, std::min(k->key_len, key->key_len)) == 0) {
                                if(records[i-1].ptr != (t = records[i].ptr)) {
                                    if(memcmp(k->key, records[i].key.skey->key, std::min(k->key_len, records[i].key.skey->key_len)) == 0) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    else { // search from right to left
                        for(i = count() - 1; i > 0; --i) {
                            k = records[i].key.skey;
                            if(memcmp(k->key, key->key, std::min(k->key_len, key->key_len)) == 0) {
                                if(records[i - 1].ptr != (t = records[i].ptr) && t) {
                                    if(memcmp(k->key, records[i].key.skey->key, std::min(k->key_len, records[i].key.skey->key_len)) == 0) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }

                        if(!ret) {
                            k = records[0].key.skey;
                            if(memcmp(k->key, key->key, std::min(k->key_len, key->key_len)) == 0) {
                                if(NULL != (t = records[0].ptr) && t) {
                                    if(memcmp(k->key, records[0].key.skey->key, std::min(k->key_len, records[0].key.skey->key_len)) == 0) {
                                        ret = t;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                } while(IS_FORWARD(hdr.switch_counter) != IS_FORWARD(previous_switch_counter));

                if(ret) {
                    return ret;
                }

                if((t = (char *)hdr.sibling_ptr) && memcmp(key->key, ((page *)t)->hdr.highest.skey->key, std::min(key->key_len, ((page *)t)->hdr.highest.skey->key_len)) >= 0) {
                    return t;
                }

                return NULL;
            }
            else { // internal node
                do {
                    previous_switch_counter = hdr.switch_counter;
                    ret = NULL;

                    if(IS_FORWARD(previous_switch_counter)) {
                        k = records[0].key.skey;
                        if(memcmp(key->key, k->key, std::min(key->key_len, k->key_len)) < 0) {
                            if((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
                                ret = t;
                                continue;
                            }
                        }

                        for(i = 1; records[i].ptr != NULL; ++i) {
                            k = records[i].key.skey;
                            if(memcmp(key->key, k->key, std::min(key->key_len, k->key_len)) < 0) {
                                if((t = records[i-1].ptr) != records[i].ptr) {
                                    ret = t;
                                    break;
                                }
                            }
                        }

                        if(!ret) {
                            ret = records[i - 1].ptr;
                            continue;
                        }
                    }
                    else { // search from right to left
                        for(i = count() - 1; i >= 0; --i) {
                            k = records[i].key.skey;
                            if(memcmp(key->key, k->key, std::min(key->key_len, k->key_len)) >= 0) {
                                if(i == 0) {
                                    if((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                                        ret = t;
                                        break;
                                    }
                                }
                                else {
                                    if(records[i - 1].ptr != (t = records[i].ptr)) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } while(IS_FORWARD(hdr.switch_counter) != IS_FORWARD(previous_switch_counter));

                if((t = (char *)hdr.sibling_ptr) != NULL) {
                    if(memcmp(key->key, ((page *)t)->hdr.highest.skey->key, std::min(key->key_len, ((page *)t)->hdr.highest.skey->key_len)) >= 0) {
                        return t;
                    }
                }

                if(ret) {
                    return ret;
                }
                else
                    return (char *)hdr.leftmost_ptr;
            }

            return NULL;
        }
};

/*
 * class btree
 */
btree::btree() {
    root = (char*)new page();
    clflush((char *)root, sizeof(page));
    height = 1;
}

void btree::setNewRoot(char *new_root) {
    this->root = (char*)new_root;
    clflush((char *)&this->root, sizeof(char *));
    ++height;
}

key_item *btree::make_key_item(char *key, size_t key_len, bool flush)
{
    void *aligned_alloc;
    posix_memalign(&aligned_alloc, 64, sizeof(key_item) + key_len);
    key_item *new_key = (key_item *)aligned_alloc;
    new_key->key_len = key_len;
    memcpy(new_key->key, key, key_len);     // copy including NULL character

    if (flush)
        clflush((char *)new_key, sizeof(key_item) + key_len);

    return new_key;
}

char *btree::btree_search(uint64_t key) {
    page* p = (page*)root;

    while(p->hdr.leftmost_ptr != NULL) {
        p = (page *)p->linear_search(key);
    }

    page *t;
    while((t = (page *)p->linear_search(key)) == p->hdr.sibling_ptr) {
        p = t;
        if(!p) {
            break;
        }
    }

    return (char *)t;
}

char *btree::btree_search(char *key) {
    page* p = (page*)root;

    key_item *new_item = make_key_item(key, strlen(key) + 1, false);

    while(p->hdr.leftmost_ptr != NULL) {
        p = (page *)p->linear_search(new_item);
    }

    page *t;
    while((t = (page *)p->linear_search(new_item)) == p->hdr.sibling_ptr) {
        p = t;
        if(!p) {
            break;
        }
    }

    return (char *)t;
}

// insert the key in the leaf node
void btree::btree_insert(uint64_t key, char* right) { //need to be string
    page* p = (page*)root;

    while(p->hdr.leftmost_ptr != NULL) {
        p = (page*)p->linear_search(this, key);
    }

    if(!p->store(this, NULL, key, right, true, true)) { // store
        btree_insert(key, right);
    }
}

// insert the key in the leaf node
void btree::btree_insert(char *key, char* right) { //need to be string
    page* p = (page*)root;

    key_item *new_item = make_key_item(key, strlen(key) + 1, true);

    while(p->hdr.leftmost_ptr != NULL) {
        p = (page*)p->linear_search(new_item);
    }

    if(!p->store(this, NULL, new_item, right, true, true)) { // store
        btree_insert(key, right);
    }
}

// store the integer key into the node at the given level
void btree::btree_insert_internal
(char *left, uint64_t key, char *right, uint32_t level) {
    if(level > ((page *)root)->hdr.level)
        return;

    page *p = (page *)this->root;

    while(p->hdr.level > level)
        p = (page *)p->linear_search(key);

    if(!p->store(this, left, key, right, true, true)) {
        btree_insert_internal(left, key, right, level);
    }
}

// store the string key into the node at the given level
void btree::btree_insert_internal
(char *left, key_item *key, char *right, uint32_t level) {
    if(level > ((page *)root)->hdr.level)
        return;

    page *p = (page *)this->root;

    while(p->hdr.level > level)
        p = (page *)p->linear_search(key);

    if(!p->store(this, NULL, key, right, true, true)) {
        btree_insert_internal(left, key, right, level);
    }
}

void btree::btree_delete(uint64_t key) {
    page* p = (page*)root;

    while(p->hdr.leftmost_ptr != NULL){
        p = (page*) p->linear_search(key);
    }

    page *t;
    while((t = (page *)p->linear_search(key)) == p->hdr.sibling_ptr) {
        p = t;
        if(!p)
            break;
    }

    if(p) {
        if(!p->remove(this, key)) {
            btree_delete(key);
        }
    }
    else {
        printf("not found the key to delete %lu\n", key);
    }
}
#if 0
void btree::btree_delete_internal
(entry_key_t key, char *ptr, uint32_t level, entry_key_t *deleted_key,
 bool *is_leftmost_node, page **left_sibling) {
    if(level > ((page *)this->root)->hdr.level)
        return;

    page *p = (page *)this->root;

    while(p->hdr.level > level) {
        p = (page *)p->linear_search(key);
    }

    p->hdr.mtx->lock();

    if((char *)p->hdr.leftmost_ptr == ptr) {
        *is_leftmost_node = true;
        p->hdr.mtx->unlock();
        return;
    }

    *is_leftmost_node = false;

    for(int i=0; p->records[i].ptr != NULL; ++i) {
        if(p->records[i].ptr == ptr) {
            if(i == 0) {
                if((char *)p->hdr.leftmost_ptr != p->records[i].ptr) {
                    *deleted_key = p->records[i].key;
                    *left_sibling = p->hdr.leftmost_ptr;
                    p->remove(this, *deleted_key, false, false);
                    break;
                }
            }
            else {
                if(p->records[i - 1].ptr != p->records[i].ptr) {
                    *deleted_key = p->records[i].key;
                    *left_sibling = (page *)p->records[i - 1].ptr;
                    p->remove(this, *deleted_key, false, false);
                    break;
                }
            }
        }
    }

    p->hdr.mtx->unlock();
}
#endif
// Function to search integer keys from "min" to "max"
void btree::btree_search_range (uint64_t min, uint64_t max, unsigned long *buf, int num, int &off) {
    page *p = (page *)root;

    while(p) {
        if(p->hdr.leftmost_ptr != NULL) {
            // The current page is internal
            p = (page *)p->linear_search(min);
        }
        else {
            // Found a leaf
            p->linear_search_range(min, max, buf, num, off);

            break;
        }
    }
}

// Function to search string keys from "min" to "max"
void btree::btree_search_range (char *min, char *max, unsigned long *buf, int num, int &off) {
    page *p = (page *)root;
    key_item *min_item = make_key_item(min, strlen(min) + 1, false);
    key_item *max_item = make_key_item(max, strlen(max) + 1, false);

    while(p) {
        if(p->hdr.leftmost_ptr != NULL) {
            // The current page is internal
            p = (page *)p->linear_search(min_item);
        }
        else {
            // Found a leaf
            p->linear_search_range(min_item, max_item, buf, num, off);

            break;
        }
    }
}
}
#endif
