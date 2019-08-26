#ifndef MASSTREE_H_
#define MASSTREE_H_

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <math.h>
#include <iostream>
#include <mutex>
#include <atomic>
#include <assert.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "../mt_crash_test.h"

#ifdef LOCK_INIT
#include "tbb/concurrent_vector.h"
#endif

namespace masstree {

static constexpr uint64_t CACHE_LINE_SIZE = 64;
static uint64_t CPU_FREQ_MHZ = 2100;
static unsigned long write_latency = 0;

#define NODESIZE            512
#define CARDINALITY         (NODESIZE - CACHE_LINE_SIZE) / 16
#define LEAF_WIDTH          15
#define LEAF_THRESHOLD      1

#define INITIAL_VALUE       0x0123456789ABCDE0ULL
#define FULL_VALUE          0xEDCBA98765432100ULL

#define INTER_BITS            (1ULL << 3)
//#define LEAF_BITS             (1ULL << 2)
#define LAYER_BITS            (1ULL << 1)
#define LV_BITS               (1ULL << 0)

#define IS_INTER(x)         ((uintptr_t)x & INTER_BITS)
//#define IS_LEAF(x)          ((uintptr_t)x & LEAF_BITS)
#define IS_LAYER(x)         ((uintptr_t)x & LAYER_BITS)
#define IS_LV(x)            ((uintptr_t)x & LV_BITS)

//#define LEAF_PTR(x)         (leafnode*)((void*)((uintptr_t)x & ~LEAF_BITS))
#define LAYER_PTR(x)        (layer*)((void*)((uintptr_t)x & ~LAYER_BITS))
#define LV_PTR(x)           (leafvalue*)((void*)((uintptr_t)x & ~LV_BITS))

#define SET_INTER(x)        ((void*)((uintptr_t)x | INTER_BITS))
//#define SET_LEAF(x)         ((void*)((uintptr_t)x | LEAF_BITS))
#define SET_LAYER(x)        ((void*)((uintptr_t)x | LAYER_BITS))
#define SET_LV(x)           ((void*)((uintptr_t)x | LV_BITS))

#ifdef MASSTREEDEBUG
    std::ostream &mt_cout = std::cout;
#else
    std::ofstream dev_null("/dev/null");
    std::ostream &mt_cout = dev_null;
#endif

#define DUMMY_PTR (void*)0xffffffff

#define DUMMY_MERGE_RETURN (int)INT_MAX

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

static inline void fence() {
    asm volatile("" : : : "memory");
}

static inline void mfence() {
    asm volatile("mfence":::"memory");
}

static inline void clflush(char *data, int len, bool fence)
{
    volatile char *ptr = (char *)((unsigned long)data &~(CACHE_LINE_SIZE-1));
    if (fence)
        mfence();
    for(; ptr<data+len; ptr+=CACHE_LINE_SIZE){
        unsigned long etsc = read_tsc() + (unsigned long)(write_latency*CPU_FREQ_MHZ/1000);
/*#ifndef CLWB
        //            asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#else
        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
*/
		asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
        while(read_tsc() < etsc) cpu_pause();
    }
    if (fence)
        mfence();
}

static inline void prefetch_(const void *ptr)
{
    typedef struct { char x[CACHE_LINE_SIZE]; } cacheline_t;
    asm volatile("prefetcht0 %0" : : "m" (*(const cacheline_t *)ptr));
}

#ifdef LOCK_INIT
static tbb::concurrent_vector<std::mutex *> lock_initializer;
void lock_initialization()
{
    printf("lock table size = %lu\n", lock_initializer.size());
    for (uint64_t i = 0; i < lock_initializer.size(); i++) {
        lock_initializer[i]->unlock();
    }
}
#endif

class kv {
    private:
        uint64_t key;
        void *value;
    public:
        kv() {
            key = UINT64_MAX;
            value = NULL;
        }

        friend class leafnode;
};

typedef struct leafvalue {
    uint64_t value;
    size_t key_len;
    uint64_t fkey[];
} leafvalue;

typedef struct key_indexed_position {
    int i;
    int p;
    inline key_indexed_position() {
    }
    inline constexpr key_indexed_position(int i_, int p_)
        : i(i_), p(p_) {
    }
} key_indexed_position;

class masstree {
    private:
        void *root_;
    public:
        masstree (void *new_root) {
            root_ = new_root;
        }

        ~masstree() {
        }

        void *root() {return root_;}

        void setNewRoot(void *new_root);

        bool put(uint64_t key, void *value);

        bool put(char *key, uint64_t value);

        bool del(uint64_t key);

        bool del(char *key);

        void *get(uint64_t key);

        void *get(char *key);

        bool split(void *left, void *root, uint32_t depth, leafvalue *lv, uint64_t key, void *right, uint32_t level, void *child);

        int merge(void *left, void *root, uint32_t depth, leafvalue *lv, uint64_t key, uint32_t level, void *child);

        leafvalue *make_leaf(char *key, size_t key_len, uint64_t value);

        int scan(uint64_t min, int num, uint64_t *buf);

        int scan(char *min, int num, leafvalue *buf[]);
};

class permuter {
    public:
        permuter() {
            x_ = 0ULL;
        }

        permuter(uint64_t x) : x_(x) {
        }

        /** @brief Return an empty permuter with size 0.

          Elements will be allocated in order 0, 1, ..., @a width - 1. */
        static inline uint64_t make_empty() {
            uint64_t p = (uint64_t) INITIAL_VALUE;
            return p & ~(uint64_t) (LEAF_WIDTH);
        }
        /** @brief Return a permuter with size @a n.

          The returned permutation has size() @a n. For 0 <= i < @a n,
          (*this)[i] == i. Elements n through @a width - 1 are free, and will be
          allocated in that order. */
        static inline uint64_t make_sorted(int n) {
            uint64_t mask = (n == LEAF_WIDTH ? (uint64_t) 0 : (uint64_t) 16 << (n << 2)) - 1;
            return (make_empty() << (n << 2))
                | ((uint64_t) FULL_VALUE & mask)
                | n;
        }

        /** @brief Return the permuter's size. */
        int size() const {
            return x_ & LEAF_WIDTH;
        }

        /** @brief Return the permuter's element @a i.
          @pre 0 <= i < width */
        int operator[](int i) const {
            return (x_ >> ((i << 2) + 4)) & LEAF_WIDTH;
        }

        int back() const {
            return (*this)[LEAF_WIDTH - 1];
        }

        uint64_t value() const {
            return x_;
        }

        uint64_t value_from(int i) const {
            return x_ >> ((i + 1) << 2);
        }

        void set_size(int n) {
            x_ = (x_ & ~(uint64_t)LEAF_WIDTH) | n;
        }

        /** @brief Allocate a new element and insert it at position @a i.
          @pre 0 <= @a i < @a width
          @pre size() < @a width
          @return The newly allocated element.

          Consider the following code:
          <code>
          kpermuter<...> p = ..., q = p;
          int x = q.insert_from_back(i);
          </code>

          The modified permuter, q, has the following properties.
          <ul>
          <li>q.size() == p.size() + 1</li>
          <li>Given j with 0 <= j < i, q[j] == p[j] && q[j] != x</li>
          <li>Given j with j == i, q[j] == x</li>
          <li>Given j with i < j < q.size(), q[j] == p[j-1] && q[j] != x</li>
          </ul> */
        int insert_from_back(int i) {
            int value = back();
            // increment size, leave lower slots unchanged
            x_ = ((x_ + 1) & (((uint64_t) 16 << (i << 2)) - 1))
                // insert slot
                | ((uint64_t) value << ((i << 2) + 4))
                // shift up unchanged higher entries & empty slots
                | ((x_ << 4) & ~(((uint64_t) 256 << (i << 2)) - 1));
            return value;
        }

        /** @brief Insert an unallocated element from position @a si at position @a di.
          @pre 0 <= @a di < @a width
          @pre size() < @a width
          @pre size() <= @a si
          @return The newly allocated element. */
        void insert_selected(int di, int si) {
            int value = (*this)[si];
            uint64_t mask = ((uint64_t) 256 << (si << 2)) - 1;
            // increment size, leave lower slots unchanged
            x_ = ((x_ + 1) & (((uint64_t) 16 << (di << 2)) - 1))
                // insert slot
                | ((uint64_t) value << ((di << 2) + 4))
                // shift up unchanged higher entries & empty slots
                | ((x_ << 4) & mask & ~(((uint64_t) 256 << (di << 2)) - 1))
                // leave uppermost slots alone
                | (x_ & ~mask);
        }
        /** @brief Remove the element at position @a i.
          @pre 0 <= @a i < @a size()
          @pre size() < @a width

          Consider the following code:
          <code>
          kpermuter<...> p = ..., q = p;
          q.remove(i);
          </code>

          The modified permuter, q, has the following properties.
          <ul>
          <li>q.size() == p.size() - 1</li>
          <li>Given j with 0 <= j < i, q[j] == p[j]</li>
          <li>Given j with i <= j < q.size(), q[j] == p[j+1]</li>
          <li>q[q.size()] == p[i]</li>
          </ul> */
        void remove(int i) {
            if (int(x_ & 15) == i + 1)
                --x_;
            else {
                int rot_amount = ((x_ & 15) - i - 1) << 2;
                uint64_t rot_mask =
                    (((uint64_t) 16 << rot_amount) - 1) << ((i + 1) << 2);
                // decrement size, leave lower slots unchanged
                x_ = ((x_ - 1) & ~rot_mask)
                    // shift higher entries down
                    | (((x_ & rot_mask) >> 4) & rot_mask)
                    // shift value up
                    | (((x_ & rot_mask) << rot_amount) & rot_mask);
            }
        }
        /** @brief Remove the element at position @a i to the back.
          @pre 0 <= @a i < @a size()
          @pre size() < @a width

          Consider the following code:
          <code>
          kpermuter<...> p = ..., q = p;
          q.remove_to_back(i);
          </code>

          The modified permuter, q, has the following properties.
          <ul>
          <li>q.size() == p.size() - 1</li>
          <li>Given j with 0 <= j < i, q[j] == p[j]</li>
          <li>Given j with i <= j < @a width - 1, q[j] == p[j+1]</li>
          <li>q.back() == p[i]</li>
          </ul> */
        void remove_to_back(int i) {
            uint64_t mask = ~(((uint64_t) 16 << (i << 2)) - 1);
            // clear unused slots
            uint64_t x = x_ & (((uint64_t) 16 << (LEAF_WIDTH << 2)) - 1);
            // decrement size, leave lower slots unchanged
            x_ = ((x - 1) & ~mask)
                // shift higher entries down
                | ((x >> 4) & mask)
                // shift removed element up
                | ((x & mask) << ((LEAF_WIDTH - i - 1) << 2));
        }
        /** @brief Rotate the permuter's elements between @a i and size().
          @pre 0 <= @a i <= @a j <= size()

          Consider the following code:
          <code>
          kpermuter<...> p = ..., q = p;
          q.rotate(i, j);
          </code>

          The modified permuter, q, has the following properties.
          <ul>
          <li>q.size() == p.size()</li>
          <li>Given k with 0 <= k < i, q[k] == p[k]</li>
          <li>Given k with i <= k < q.size(), q[k] == p[i + (k - i + j - i) mod (size() - i)]</li>
          </ul> */
        void rotate(int i, int j) {
            uint64_t mask = (i == LEAF_WIDTH ? (uint64_t) 0 : (uint64_t) 16 << (i << 2)) - 1;
            // clear unused slots
            uint64_t x = x_ & (((uint64_t) 16 << (LEAF_WIDTH << 2)) - 1);
            x_ = (x & mask)
                | ((x >> ((j - i) << 2)) & ~mask)
                | ((x & ~mask) << ((LEAF_WIDTH - j) << 2));
        }
        /** @brief Exchange the elements at positions @a i and @a j. */
        void exchange(int i, int j) {
            uint64_t diff = ((x_ >> (i << 2)) ^ (x_ >> (j << 2))) & 240;
            x_ ^= (diff << (i << 2)) | (diff << (j << 2));
        }
        /** @brief Exchange positions of values @a x and @a y. */
        void exchange_values(int x, int y) {
            uint64_t diff = 0, p = x_;
            for (int i = 0; i < LEAF_WIDTH; ++i, diff <<= 4, p <<= 4) {
                int v = (p >> (LEAF_WIDTH << 2)) & 15;
                diff ^= -((v == x) | (v == y)) & (x ^ y);
            }
            x_ ^= diff;
        }

        bool operator==(const permuter& x) const {
            return x_ == x.x_;
        }
        bool operator!=(const permuter& x) const {
            return !(*this == x);
        }

        int operator&(uint64_t mask) {
            return x_ & mask;
        }

        void operator>>=(uint64_t mask) {
            x_ = (x_ >> mask);
        }

        static inline int size(uint64_t p) {
            return p & 15;
        }

    private:
        uint64_t x_;
};

class leafnode {
    private:
        uint32_t level_;        // 4bytes
        uint32_t version_;      // 4bytes
        std::mutex *wlock;      // 8bytes
        leafnode *next;         // 8bytes
        leafnode *leftmost_ptr; // 8bytes
        uint64_t highest;       // 8bytes
        permuter permutation;   // 8bytes
        uint64_t dummy[2];      // 16bytes
        kv entry[LEAF_WIDTH];

    public:
        leafnode(uint32_t level) : permutation(permuter::make_empty()) {
            level_ = level;
            version_ = 0;
            wlock = new std::mutex();
            next = NULL;
            leftmost_ptr = NULL;
            highest = 0;
#ifdef LOCK_INIT
            lock_initializer.push_back(wlock);
#endif
        }

        leafnode(void *left, uint64_t key, void *right, uint32_t level = 1) : permutation(permuter::make_empty()) {
            level_ = level;
            version_ = 0;
            wlock = new std::mutex();
            next = NULL;
            highest = 0;

            leftmost_ptr = reinterpret_cast<leafnode *> (left);
            entry[0].key = key;
            entry[0].value = right;

            permutation = permuter::make_sorted(1);
#ifdef LOCK_INIT
            lock_initializer.push_back(wlock);
#endif
        }

        void *operator new(size_t size) {
            void *ret;
            posix_memalign(&ret, CACHE_LINE_SIZE, size);
            return ret;
        }

        permuter permute();

        key_indexed_position key_lower_bound_by(uint64_t key);

        key_indexed_position key_lower_bound(uint64_t key);

        void lock() {wlock->lock();}

        void unlock() {wlock->unlock();}

        bool trylock() {return wlock->try_lock();}

        int compare_key(const uint64_t a, const uint64_t b);

        leafnode *advance_to_key(const uint64_t& key, bool checker);

        void assign(int p, const uint64_t& key, void *value);

        void assign_value(int p, void *value);

        inline void assign_initialize(int p, const uint64_t& key, void *value);

        inline void assign_initialize(int p, leafnode *x, int xp);

        inline void assign_initialize_for_layer(int p, const uint64_t& key);

        int split_into(leafnode *nr, int p, const uint64_t& key, void *value, uint64_t& split_key);

        void split_into_inter(leafnode *nr, int p, const uint64_t& key, void *value, uint64_t& split_key);

        void *leaf_insert(masstree *t, void *root, uint32_t depth, leafvalue *lv, uint64_t key,
                void *value, key_indexed_position &kx_, bool flush, bool with_lock, leafnode *invalid_sibling);

        void *leaf_delete(masstree *t, void *root, uint32_t depth, leafvalue *lv, uint64_t key,
                key_indexed_position &kx_, bool flush, bool with_lock, leafnode *invalid_sibling);

        void *inter_insert(masstree *t, void *root, uint32_t depth, leafvalue *lv, uint64_t key, void *value,
                key_indexed_position &kx_, bool flush, bool with_lock, leafnode *invalid_sibling, leafnode *child);

        int inter_delete(masstree *t, void *root, uint32_t depth, leafvalue *lv, uint64_t key,
                key_indexed_position &kx_, bool flush, bool with_lock, leafnode *invalid_sibling, leafnode *child);

        bool has_changed(uint32_t oldv);

        void prefetch() const;

        uint32_t version();

        uint32_t level() {return level_;}

        uint64_t key(int i) {return entry[i].key;}

        void *value(int i) {return entry[i].value;}

        leafnode *leftmost() {return leftmost_ptr;}

        leafnode *next_() {return next;}

        uint64_t highest_() {return highest;}

        void make_new_layer(leafnode *p, key_indexed_position &kx_, leafvalue *olv, leafvalue *nlv, uint32_t depth);

        leafnode *correct_layer_root(void *root, leafvalue *lv, uint32_t depth, key_indexed_position &pkx_);

        void *entry_addr(int p);

        bool check_for_recovery(masstree *t, leafnode *left, leafnode *right, void *root, uint32_t depth, leafvalue *lv);

        void get_range(leafvalue * &lv, int num, int &count, leafvalue *buf[], leafnode *root, uint32_t depth);

        leafvalue *smallest_leaf(size_t key_len, uint64_t value);

        leafnode *search_for_leftsibling(void *root, uint64_t key, uint32_t level, leafnode *right);
};

int leafnode::compare_key(const uint64_t a, const uint64_t b)
{
    if (a == b)
        return 0;
    else
        return a < b ? -1 : 1;
}

key_indexed_position leafnode::key_lower_bound_by(uint64_t key)
{
    permuter perm = permutation;
    int l = 0, r = perm.size();
    while (l < r) {
        int m = (l + r) >> 1;
        int mp = perm[m];
        int cmp = compare_key(key, entry[mp].key);
        if (cmp < 0)
            r = m;
        else if (cmp == 0)
            return key_indexed_position(m, mp);
        else
            l = m + 1;
    }
    return key_indexed_position(l, -1);
}

key_indexed_position leafnode::key_lower_bound(uint64_t key)
{
    permuter perm = permutation;
    int l = 0, r = perm.size();
    while (l < r) {
        int m = (l + r) >> 1;
        int mp = perm[m];
        int cmp = compare_key(key, entry[mp].key);
        if (cmp < 0)
            r = m;
        else if (cmp == 0)
            return key_indexed_position(m, mp);
        else
            l = m + 1;
    }

    return (l-1 < 0 ? key_indexed_position(l-1, -1) : key_indexed_position(l-1, perm[l-1]));
}

leafnode *leafnode::advance_to_key(const uint64_t& key, bool checker)
{
    const leafnode *n = this;

    leafnode *next;
    if ((next = n->next) && compare_key(key, next->highest) >= 0) {
//        if (!checker) {
//            printf("Reader must not come here\n");
//            exit(0);
//        }
        n = next;
    }

    return const_cast<leafnode *> (n);
}

permuter leafnode::permute()
{
    return permutation;
}

uint32_t leafnode::version()
{
    return version_;
}

bool leafnode::has_changed(uint32_t oldv)
{
    fence();
    return (version_ != oldv);
}

void leafnode::prefetch() const
{
    for (int i = 64; i < std::min((16 * LEAF_WIDTH) + 1, 4 * 64); i += 64)
        prefetch_((const char *) this + i);
}

leafvalue *masstree::make_leaf(char *key, size_t key_len, uint64_t value)
{
    void *aligned_alloc;
    size_t len = (key_len % sizeof(uint64_t)) == 0 ? key_len : (((key_len) / sizeof(uint64_t)) + 1) * sizeof(uint64_t);

    posix_memalign(&aligned_alloc, CACHE_LINE_SIZE, sizeof(leafvalue) + len);
    leafvalue *lv = reinterpret_cast<leafvalue *> (aligned_alloc);
    memset(lv, 0, sizeof(leafvalue) + len);

    lv->value = value;
    lv->key_len = key_len;          // key_len or len??
    memcpy(lv->fkey, key, key_len);

    for (int i = 0; i < (len / sizeof(uint64_t)); i++)
        lv->fkey[i] = __builtin_bswap64(lv->fkey[i]);

    if (value != 0)
        clflush((char *) lv, sizeof(leafvalue) + len, true);
    return lv;
}

leafvalue *leafnode::smallest_leaf(size_t key_len, uint64_t value)
{
    void *aligned_alloc;
    size_t len = (key_len % sizeof(uint64_t)) == 0 ? key_len : (((key_len) / sizeof(uint64_t)) + 1) * sizeof(uint64_t);

    posix_memalign(&aligned_alloc, CACHE_LINE_SIZE, sizeof(leafvalue) + len);
    leafvalue *lv = reinterpret_cast<leafvalue *> (aligned_alloc);
    memset(lv, 0, sizeof(leafvalue) + len);

    lv->value = value;
    lv->key_len = key_len;          // key_len or len??

    for (int i = 0; i < (len / sizeof(uint64_t)); i++)
        lv->fkey[i] = 0ULL;

    if (value != 0)
        clflush((char *) lv, sizeof(leafvalue) + len, true);
    return lv;
}

void leafnode::make_new_layer(leafnode *l, key_indexed_position &kx_, leafvalue *olv,
        leafvalue *nlv, uint32_t depth)
{
    uint32_t sdepth = depth;
    int kcmp = compare_key(olv->fkey[depth], nlv->fkey[depth]);

    leafnode *twig_head = l;
    leafnode *twig_tail = l;
    while (kcmp == 0) {
        leafnode *nl = new leafnode(0);
        nl->assign_initialize_for_layer(0, olv->fkey[depth]);
        if (twig_head != l)
            twig_tail->entry[0].value = nl;
        else
            twig_head = nl;
        nl->permutation = permuter::make_sorted(1);
        twig_tail = nl;
        depth++;
        kcmp = compare_key(olv->fkey[depth], nlv->fkey[depth]);
    }

    leafnode *nl = new leafnode(0);
    nl->assign_initialize(0, kcmp < 0 ? olv->fkey[depth] : nlv->fkey[depth], kcmp < 0 ? SET_LV(olv) : SET_LV(nlv));
    nl->assign_initialize(1, kcmp < 0 ? nlv->fkey[depth] : olv->fkey[depth], kcmp < 0 ? SET_LV(nlv) : SET_LV(olv));
    //nl->entry[kcml > 0].value = l->entry[kx_.p].value;
    if (kcmp < 0)
        nl->permutation = permuter::make_sorted(1);
    else {
        permuter permnl = permuter::make_sorted(2);
        permnl.remove_to_back(0);
        nl->permutation = permnl.value();
    }

    fence();
    if (twig_tail != l)
        twig_tail->entry[0].value = nl;
    twig_tail = nl;
    if (twig_head != l) {
        mfence();
        for ( ; twig_head != twig_tail && twig_head != NULL; twig_head = reinterpret_cast <leafnode *>(twig_head->entry[0].value)) {
            clflush((char *)twig_head, sizeof(leafnode), false);
        }
        clflush((char *)twig_tail, sizeof(leafnode), false);
        mfence();

        l->entry[kx_.p].value = twig_head;
        clflush((char *)l->entry_addr(kx_.p) + 8, sizeof(uintptr_t), true);
    } else {
        clflush((char *)nl, sizeof(leafnode), true);

        l->entry[kx_.p].value = nl;
        clflush((char *)l->entry_addr(kx_.p) + 8, sizeof(uintptr_t), true);
    }

    l = nl;

    kx_.i = kx_.p = kcmp < 0;

    permuter cp = l->permutation.value();
    cp.insert_from_back(kx_.i);
    fence();
    l->permutation = cp.value();
}

bool leafnode::check_for_recovery(masstree *t, leafnode *left, leafnode *right, void *root, uint32_t depth, leafvalue *lv)
{
	bool status;
	mt_cout << __func__ << ": Checking for recovery" << std::endl;
    permuter perm = left->permute();

    for (int i = perm.size() - 1; i >= 0; i--) {
        if (left->key(perm[i]) >= right->highest) {
            perm.remove_to_back(i);
        } else {
            break;
        }
    }

    if (left->permutation.size() != perm.size()) {
        left->permutation = perm.value();
        clflush((char *)&left->permutation, sizeof(permuter), true);
    }

    if (depth > 0) {
        key_indexed_position pkx_;
        leafnode *p = correct_layer_root(root, lv, depth, pkx_);
        if (p->value(pkx_.p) == left) {
            leafnode *new_root = new leafnode(left, right->highest, right, left->level() + 1);
            clflush((char *) new_root, sizeof(leafnode), true);
            p->entry[pkx_.p].value = new_root;
            clflush((char *) &p->entry[pkx_.p].value, sizeof(uintptr_t), true);
            p->unlock();

            right->unlock();
            left->unlock();
        } else {
            root = p;
            status = t->split(p->entry[pkx_.p].value, root, depth, lv, right->highest, right, left->level() + 1, left);
			if (!status)
				return false;
        }
    } else {
        if (t->root() == left) {
            leafnode *new_root = new leafnode(left, right->highest, right, left->level() + 1);
            clflush((char *) new_root, sizeof(leafnode), true);
            t->setNewRoot(new_root);

            right->unlock();
            left->unlock();
        } else {
            status = t->split(NULL, NULL, 0, NULL, right->highest, right, left->level() + 1, left);
			if (!status)
				return false;
        }
    }
	return true;
}

bool masstree::put(uint64_t key, void *value)
{
    key_indexed_position kx_;
    uint32_t depth = 0;
    leafnode *next;
	bool status;

from_root:
    leafnode *p = reinterpret_cast<leafnode *> (this->root_);
    while (p->level() != 0) {
inter_retry:
        next = p->advance_to_key(key, true);
        if (next != p) {
            // check for recovery
            if (p->trylock()) {
                if (next->trylock()){
                    status = p->check_for_recovery(this, p, next, NULL, 0, NULL);
					if (!status)
						return false;
				}
                else
                    p->unlock();
            }
            p = next;
            goto inter_retry;
        }

        p->prefetch();
        fence();

        kx_ = p->key_lower_bound(key);

        void *snapshot_v;
        if (kx_.i >= 0) {
            snapshot_v = p->value(kx_.p);
            fence();
            if (p->key(kx_.p) <= key) {
                if (snapshot_v == p->value(kx_.p))
                    p = reinterpret_cast<leafnode *>(snapshot_v);
                else {
                    goto inter_retry;
                }
            }
        } else {
            p = p->leftmost();
        }
    }

    leafnode *l = reinterpret_cast<leafnode *> (p);
leaf_retry:
    next = l->advance_to_key(key, true);
    if (next != l) {
        //check for recovery
        if (l->trylock()) {
            if (next->trylock()){
                status = l->check_for_recovery(this, l, next, NULL, 0, NULL);
				if (!status)
					return false;
			}
            else
                l->unlock();
        }

        l = next;
        goto leaf_retry;
    }

    l->lock();
    next = l->advance_to_key(key, true);
    if (next != l) {
        l->unlock();
        l = next;
        goto leaf_retry;
    }

    if ((l->permute()).size() == 0 && l->highest_() != 0) {
        l->unlock();
        goto from_root;
    }

    l->prefetch();
    fence();

    kx_ = l->key_lower_bound_by(key);

	// Check if leaf_insert returns DUMMY_PTR. If so, put failed due to crash
	void* crash = l->leaf_insert(this, NULL, 0, NULL, key, value, kx_, true, true, NULL);
	if (crash == DUMMY_PTR)
		return false;

	if (!crash) {
		put(key, value);
	}

	return true;
}

bool masstree::put(char *key, uint64_t value)
{
restart:
    void *root = this->root_;
    key_indexed_position kx_;
    uint32_t depth = 0;
    leafnode *next;
	bool status;

    leafvalue *lv = make_leaf(key, strlen(key), value);

    leafnode *p = reinterpret_cast<leafnode *> (root);
from_root:
    while (p->level() != 0) {
inter_retry:
        next = p->advance_to_key(lv->fkey[depth], true);
        if (next != p) {
            // check for recovery
            if (p->trylock()) {
                if (next->trylock()){
                    status = p->check_for_recovery(this, p, next, root, depth, lv);
					if (!status)
						return false;
				}
                else
                    p->unlock();
            }
            p = next;
            goto inter_retry;
        }

        p->prefetch();
        fence();

        kx_ = p->key_lower_bound(lv->fkey[depth]);

        void *snapshot_v;
        if (kx_.i >= 0) {
            snapshot_v = p->value(kx_.p);
            fence();
            if (p->key(kx_.p) <= lv->fkey[depth]) {
                if (snapshot_v == p->value(kx_.p))
                    p = reinterpret_cast<leafnode *>(snapshot_v);
                else {
                    goto inter_retry;
                }
            }
        } else {
            p = p->leftmost();
        }
    }

    leafnode *l = reinterpret_cast<leafnode *> (p);
leaf_retry:
    next = l->advance_to_key(lv->fkey[depth], true);
    if (next != l) {
        //check for recovery
        if (l->trylock()) {
            if (next->trylock()){
                status = l->check_for_recovery(this, l, next, root, depth, lv);
				if (!status)
					return false;
			}
            else
                l->unlock();
        }

        l = next;
        goto leaf_retry;
    }

    l->lock();
    next = l->advance_to_key(lv->fkey[depth], true);
    if (next != l) {
        l->unlock();
        l = next;
        goto leaf_retry;
    }

    if ((l->permute()).size() == 0 && l->highest_() != 0) {
        l->unlock();
        goto restart;
    }

    l->prefetch();
    fence();

    kx_ = l->key_lower_bound_by(lv->fkey[depth]);
    if (kx_.p >= 0) {
        // i)   If there is additional layer, retry B+tree traversing from the next layer
        if (!IS_LV(l->value(kx_.p))) {
            p = reinterpret_cast<leafnode *> (l->value(kx_.p));
            root = l;
            depth++;
            l->unlock();
            goto from_root;
        // ii)  Atomically update value for the matching key
        } else if (IS_LV(l->value(kx_.p)) && (LV_PTR(l->value(kx_.p)))->key_len == lv->key_len &&
                memcmp(lv->fkey, (LV_PTR(l->value(kx_.p)))->fkey, lv->key_len) == 0) {
            (LV_PTR(l->value(kx_.p)))->value = *(uint64_t *)value;
            clflush((char *)&(LV_PTR(l->value(kx_.p)))->value, sizeof(void *), true);
            l->unlock();
        // iii) Allocate additional layers (B+tree's roots) up to
        //      the number of common prefixes (8bytes unit).
        //      Insert two keys to the leafnode in the last layer
        //      During these processes, this leafnode must be locked
        } else {
            l->make_new_layer(l, kx_, LV_PTR(l->value(kx_.p)), lv, ++depth);
            l->unlock();
        }
    } else {
		// Check if leaf_insert returns DUMMY_PTR. If so, put failed due to crash
		void* crash = l->leaf_insert(this, root, depth, lv, lv->fkey[depth], SET_LV(lv), kx_, true, true, NULL);
		if (crash == DUMMY_PTR)
			return false;
		
        if (!crash) {
            put(key, value);
        }
    }
	return true;
}

bool masstree::del(uint64_t key)
{
    void *root = this->root_;
    key_indexed_position kx_;
    uint32_t depth = 0;
    leafnode *next;
    void *snapshot_v;
	bool status;

    leafnode *p = reinterpret_cast<leafnode *> (root);
    while (p->level() != 0) {
inter_retry:
        next = p->advance_to_key(key, true);
        if (next != p) {
            // check for recovery
            if (p->trylock()) {
                if (next->trylock()){
                    status = p->check_for_recovery(this, p, next, NULL, 0, NULL);
					if (!status)
						return false;
				}
                else
                    p->unlock();
            }
            p = next;
            goto inter_retry;
        }

        p->prefetch();
        fence();

        kx_ = p->key_lower_bound(key);
        if (kx_.i >= 0) {
            snapshot_v = p->value(kx_.p);
            fence();
            if (p->key(kx_.p) <= key) {
                if (snapshot_v == p->value(kx_.p))
                    p = reinterpret_cast<leafnode *>(snapshot_v);
                else
                    goto inter_retry;
            }
        } else {
            p = p->leftmost();
        }
    }

    leafnode *l = reinterpret_cast<leafnode *> (p);
leaf_retry:
    next = l->advance_to_key(key, true);
    if (next != l) {
        //check for recovery
        if (l->trylock()) {
            if (next->trylock()){
                status = l->check_for_recovery(this, l, next, NULL, 0, NULL);
				if (!status)
					return false;
			}
            else
                l->unlock();
        }

        l = next;
        goto leaf_retry;
    }

    l->lock();
    next = l->advance_to_key(key, true);
    if (next != l) {
        l->unlock();
        l = next;
        goto leaf_retry;
    }

    l->prefetch();
    fence();

    kx_ = l->key_lower_bound_by(key);
    if (kx_.p < 0)
        return true;

	// Check if leaf_delete returns DUMMY_PTR. If so, put failed due to crash
	void* crash = l->leaf_delete(this, NULL, 0, NULL, key, kx_, true, true, NULL);
	if (crash == DUMMY_PTR)
		return false;

	if (!crash) {
		del(key);
	}

	return true;
}

bool masstree::del(char *key)
{
    void *root = this->root_;
    key_indexed_position kx_;
    uint32_t depth = 0;
    leafnode *next;
	bool status;

    leafvalue *lv = make_leaf(key, strlen(key), 0);

    leafnode *p = reinterpret_cast<leafnode *> (root);
from_root:
    while (p->level() != 0) {
inter_retry:
        next = p->advance_to_key(lv->fkey[depth], true);
        if (next != p) {
            // check for recovery
            if (p->trylock()) {
                if (next->trylock()){
                    status = p->check_for_recovery(this, p, next, root, depth, lv);
					if (!status)
						return false;
				}
                else
                    p->unlock();
            }
            p = next;
            goto inter_retry;
        }

        p->prefetch();
        fence();

        kx_ = p->key_lower_bound(lv->fkey[depth]);

        void *snapshot_v;
        if (kx_.i >= 0) {
            snapshot_v = p->value(kx_.p);
            fence();
            if (p->key(kx_.p) <= lv->fkey[depth]) {
                if (snapshot_v == p->value(kx_.p))
                    p = reinterpret_cast<leafnode *>(snapshot_v);
                else {
                    goto inter_retry;
                }
            }
        } else {
            p = p->leftmost();
        }
    }

    leafnode *l = reinterpret_cast<leafnode *> (p);
leaf_retry:
    next = l->advance_to_key(lv->fkey[depth], true);
    if (next != l) {
        //check for recovery
        if (l->trylock()) {
            if (next->trylock()){
                status = l->check_for_recovery(this, l, next, root, depth, lv);
				if (!status)
					return false;
			}
            else
                l->unlock();
        }

        l = next;
        goto leaf_retry;
    }

    l->lock();
    next = l->advance_to_key(lv->fkey[depth], true);
    if (next != l) {
        l->unlock();
        l = next;
        goto leaf_retry;
    }

    l->prefetch();
    fence();

    kx_ = l->key_lower_bound_by(lv->fkey[depth]);
    if (kx_.p >= 0) {
        // i)   If there is additional layer, retry B+tree traversing from the next layer
        if (!IS_LV(l->value(kx_.p))) {
            p = reinterpret_cast<leafnode *> (l->value(kx_.p));
            root = l;
            depth++;
            l->unlock();
            goto from_root;
        // ii)  Checking false-positive result and starting to delete it
        } else if (IS_LV(l->value(kx_.p)) && (LV_PTR(l->value(kx_.p)))->key_len == lv->key_len &&
                memcmp(lv->fkey, (LV_PTR(l->value(kx_.p)))->fkey, lv->key_len) == 0) {
            
	    	// Check if leaf_delete returns DUMMY_PTR. If so, put failed due to crash
    		void* crash = l->leaf_delete(this, root, depth, lv, lv->fkey[depth], kx_, true, true, NULL);
    		if (crash == DUMMY_PTR)
        		return false;

    		if (!crash) {
        		del(key);
    		}

        } else {
            l->unlock();
            return true;
        }
    } else {
	
        l->unlock();
        return true;
    }
	return true;
}

inline void leafnode::assign_initialize(int p, const uint64_t& key, void *value)
{
    entry[p].key = key;
    entry[p].value = value;
}

inline void leafnode::assign_initialize(int p, leafnode *x, int xp)
{
    entry[p].key = x->entry[xp].key;
    entry[p].value = x->entry[xp].value;
}

inline void leafnode::assign_initialize_for_layer(int p, const uint64_t& key)
{
    entry[p].key = key;
}

// Leaf sibling install
int leafnode::split_into(leafnode *nr, int p, const uint64_t& key, void *value, uint64_t& split_key)
{
    int width = this->permutation.size();
    int mid = width / 2 + 1;

    permuter perml = this->permutation;
    permuter pv = perml.value_from(mid - (p < mid));
    for (int x = mid; x <= width; ++x)
        if (x == p)
            nr->assign_initialize(x - mid, key, value);
        else {
            nr->assign_initialize(x - mid, this, pv & 15);
            pv >>= 4;
        }

    permuter permr = permuter::make_sorted(width + 1 - mid);
    if (p >= mid)
        permr.remove_to_back(p - mid);
    nr->permutation = permr.value();

    //leafnode::link_split(this, nr);
    nr->highest = nr->entry[0].key;
    nr->next = this->next;
    clflush((char *)nr, sizeof(leafnode), true);
    this->next = nr;
    clflush((char *)(&this->next), sizeof(uintptr_t), true);

    split_key = nr->highest;
    return p >= mid ? 1 + (mid == LEAF_WIDTH) : 0;
}

// Internal node sibling install
void leafnode::split_into_inter(leafnode *nr, int p, const uint64_t& key, void *value, uint64_t& split_key)
{
    int width = this->permutation.size();
    int mid = width / 2 + 1;

    permuter perml = this->permutation;
    permuter pv = perml.value_from(mid);
    for (int x = mid; x < width; ++x) {
        nr->assign_initialize(x - mid, this, pv & 15);
        pv >>= 4;
    }

    permuter permr = permuter::make_sorted(width - mid);
    nr->permutation = permr.value();

    //leafnode::link_split(this, nr);
    nr->leftmost_ptr = reinterpret_cast<leafnode *>(this->entry[perml[mid - 1]].value);
    nr->highest = this->entry[perml[mid - 1]].key;
    nr->next = this->next;
    clflush((char *)nr, sizeof(leafnode), true);
    this->next = nr;
    clflush((char *)(&this->next), sizeof(uintptr_t), true);

    split_key = nr->highest;
    //return p >= mid ? 1 + (mid == LEAF_WIDTH) : 0;
}

void leafnode::assign(int p, const uint64_t& key, void *value)
{
    entry[p].key = key;
    fence();
    entry[p].value = value;
}

void leafnode::assign_value(int p, void *value)
{
    entry[p].value = value;
}

void *leafnode::entry_addr(int p)
{
    return &entry[p];
}

void masstree::setNewRoot(void *new_root)
{
    this->root_ = new_root;
    clflush((char *)&this->root_, sizeof(void *), true);
}

leafnode *leafnode::correct_layer_root(void *root, leafvalue *lv, uint32_t depth, key_indexed_position &pkx_)
{
    leafnode *oldp;
    leafnode *p = reinterpret_cast<leafnode *> (root);

leaf_retry:
    p->lock();
    oldp = p->advance_to_key(lv->fkey[depth - 1], true);
    if (oldp != p) {
        p->unlock();
        p = oldp;
        goto leaf_retry;
    }

    p->prefetch();
    fence();

    pkx_ = p->key_lower_bound_by(lv->fkey[depth - 1]);
    if (pkx_.p < 0) {
        printf("[correct_layer_root] cannot find layer's root\n");
        printf("key = %lu, depth = %u\n", lv->fkey[depth - 1], depth);
        for (int i = 0; i < p->permutation.size(); i++) {
            printf("key[%d] = %lu\n", i, p->entry[p->permutation[i]].key);
        }
        exit(0);
    }

    root = p;
    return p;
}

leafnode *leafnode::search_for_leftsibling(void *root, uint64_t key, uint32_t level, leafnode *right)
{
    leafnode *p;
    key_indexed_position kx_;
    leafnode *next;

    p = reinterpret_cast<leafnode *> (root);
    while (p->level() > level) {
inter_retry:
        next = p->advance_to_key(key, true);
        if (next != p) {
            p = next;
            goto inter_retry;
        }

        p->prefetch();
        fence();

        kx_ = p->key_lower_bound(key);

        void *snapshot_v;
        if (kx_.i >= 0) {
            snapshot_v = p->value(kx_.p);
            fence();
            if (p->key(kx_.p) <= key) {
                if (snapshot_v == p->value(kx_.p))
                    p = reinterpret_cast<leafnode *>(snapshot_v);
                else {
                    goto inter_retry;
                }
            }
        } else {
            p = p->leftmost();
        }
    }

leaf_retry:
    if (p->trylock()) {
        next = p->advance_to_key(key, true);
        if (next != p) {
            p->unlock();
            p = next;
            goto leaf_retry;
        }
		//p->unlock();
    } else {
        if (p == right)
            return p;
        goto leaf_retry;
    }

    return p;
}

void *leafnode::leaf_insert(masstree *t, void *root, uint32_t depth, leafvalue *lv, uint64_t key,
        void *value, key_indexed_position &kx_, bool flush, bool with_lock, leafnode *invalid_sibling)
{
    void *ret;
	bool status;

    // permutation based insert
    if (this->permutation.size() < LEAF_WIDTH) {
        kx_.p = this->permutation.back();
        this->assign(kx_.p, key, value);
        clflush((char *)(&this->entry[kx_.p]), sizeof(kv), true);

        permuter cp = this->permutation.value();
        cp.insert_from_back(kx_.i);
        fence();
        this->permutation = cp.value();
        clflush((char *)(&this->permutation), sizeof(permuter), true);

        this->unlock();
        ret = this;
    } else {
        // overflow
        // leafnode split
		mt_cout << __func__ << " : Masstree split starts " << std::endl;
        leafnode *new_sibling = new leafnode(this->level_);
        new_sibling->lock();
        uint64_t split_key;
		
		mt_cout << __func__ << " : SPLIT STEP 1 : Sibling create " << std::endl;
        int split_type = this->split_into(new_sibling, kx_.i, key, value, split_key);

#ifdef MT_CRASH_AFTER_SPLIT_1
		if (simulateCrash){
			int shouldCrash = rand()%2;
			if(shouldCrash){
				std::cout << "\tInsertion  abruptly terminated : After split" << std::endl;
				lock_initialization();
				return DUMMY_PTR;
			}
		}
#endif

#ifdef CRASH_AFTER_SPLIT_1
		pid_t pid = fork();

		if (pid == 0) {
            // Crash state after sibling node is installed.
            lock_initialization();
			mt_cout << __func__ << " Child crashed after SPLIT STEP 1" << std::endl;
            return DUMMY_PTR;
        }

		else if (pid > 0){
        	int returnStatus;
        	waitpid(pid, &returnStatus, 0);
        	mt_cout << __func__ <<"Continuing in parent process to SPLIT STEP 2" << std::endl;
   		}
    	else {
        	mt_cout << __func__ << "Fork failed" << std::endl;
        	return NULL;
    	}
#endif

        leafnode *nl = reinterpret_cast<leafnode *> (this);
        leafnode *nr = reinterpret_cast<leafnode *> (new_sibling);

        permuter perml = nl->permutation;
        int width = perml.size();
        perml.set_size(width - nr->permutation.size());

        if (width != LEAF_WIDTH)
            perml.exchange(perml.size(), LEAF_WIDTH - 1);
        nl->version_++;
        fence();
		mt_cout << __func__ << " : SPLIT STEP 2 : Update left indirect table " << std::endl;
        nl->permutation = perml.value();
        clflush((char *)(&nl->permutation), sizeof(permuter), true);

#ifdef MT_CRASH_AFTER_SPLIT_2
        if (simulateCrash){
            int shouldCrash = rand()%2;
            if(shouldCrash){
                std::cout << "\tInsertion  abruptly terminated : After split 2" << std::endl;
                lock_initialization();
                return DUMMY_PTR;
            }
        }
#endif

#ifdef CRASH_AFTER_SPLIT_2
        pid_t pid2 = fork();

        if (pid2 == 0) {
            // Crash state after sibling node is installed.
            lock_initialization();
			mt_cout << __func__ << " Child crashed after SPLIT STEP 2" << std::endl;
            return DUMMY_PTR;
        }

        else if (pid2 > 0){
            int returnStatus;
            waitpid(pid2, &returnStatus, 0);
            mt_cout << __func__ <<"Continuing in parent process to SPLIT STEP 3" << std::endl;
        }
        else {
            mt_cout << __func__ << "Fork failed" << std::endl;
            return NULL;
        }
#endif

        if (depth > 0) {
            key_indexed_position pkx_;
            leafnode *p = correct_layer_root(root, lv, depth, pkx_);
            if (p->value(pkx_.p) == this) {
				
				mt_cout << __func__ << " : SPLIT STEP 3 : Update parent d>0" << std::endl;
                leafnode *new_root = new leafnode(this, split_key, new_sibling, level_ + 1);
                clflush((char *) new_root, sizeof(leafnode), true);
                p->entry[pkx_.p].value = new_root;
                clflush((char *) &p->entry[pkx_.p].value, sizeof(uintptr_t), true);
                p->unlock();
            } else {
				mt_cout << __func__ << " : SPLIT STEP 3 : Update parent d = 0 else" << std::endl;
                root = p;
                status = t->split(p->entry[pkx_.p].value, root, depth, lv, split_key, new_sibling, level_ + 1, NULL);
				if (!status)
					return DUMMY_PTR;
            }
        } else {
            if (t->root() == this) {
				mt_cout << __func__ << " : SPLIT STEP 3 : Update parent d = 0" << std::endl;
                leafnode *new_root = new leafnode(this, split_key, new_sibling, level_ + 1);
                clflush((char *) new_root, sizeof(leafnode), true);
                t->setNewRoot(new_root);
            } else {
				mt_cout << __func__ << " : SPLIT STEP 3 : Update parent d = 0 else" << std::endl;
                status = t->split(NULL, NULL, 0, NULL, split_key, new_sibling, level_ + 1, NULL);
				if (!status)
					return DUMMY_PTR;
            }
        }

        // permutation base final insertion
        if (split_type == 0) {
            kx_.p = perml.back();
            nl->assign(kx_.p, key, value);
            clflush((char *)(&nl->entry[kx_.p]), sizeof(kv), true);

            permuter cp = nl->permutation.value();
            cp.insert_from_back(kx_.i);
            fence();
            nl->permutation = cp.value();
            clflush((char *)(&nl->permutation), sizeof(permuter), true);
            ret = nl;
        } else {
            kx_.i = kx_.p = kx_.i - perml.size();

            permuter cp = nr->permutation.value();
            cp.insert_from_back(kx_.i);
            fence();
            nr->permutation = cp.value();
            clflush((char *)(&nr->permutation), sizeof(permuter), true);
            ret = nr;
        }

        nr->unlock();
        nl->unlock();
    }

    return ret;
}

void *leafnode::leaf_delete(masstree *t, void *root, uint32_t depth, leafvalue *lv, uint64_t key,
        key_indexed_position &kx_, bool flush, bool with_lock, leafnode *invalid_sibling)
{
    int merge_state;
    void *ret = NULL;

    // permutation based remove
    if (this->permutation.size() > LEAF_THRESHOLD) {
        permuter cp = this->permutation.value();
        cp.remove_to_back(kx_.i);
        fence();
        this->permutation = cp.value();
        clflush((char *)(&this->permutation), sizeof(permuter), true);

        this->unlock();
        ret = this;
    } else {
        // Underflow
        // Merge
        permuter cp;
        leafnode *nl, *nr;
        nr = reinterpret_cast<leafnode *> (this);

        if (depth > 0) {
            key_indexed_position pkx_;
            leafnode *p = correct_layer_root(root, lv, depth, pkx_);
            if (p->value(pkx_.p) == nr) {
                cp = nr->permutation.value();
                cp = cp.make_empty();
                fence();
                nr->permutation = cp.value();
                clflush((char *)(&nr->permutation), sizeof(permuter), true);
                p->unlock();
                nr->unlock();
                return nr;
            } else {
                nl = search_for_leftsibling(p->entry[pkx_.p].value, nr->highest ? nr->highest - 1 : nr->highest, nr->level_, nr);
                merge_state = t->merge(p->entry[pkx_.p].value, reinterpret_cast<void *> (p), depth, lv, nr->highest, nr->level_ + 1, NULL);
              
				// Crash in inner-delete, called by merge
				 if (merge_state == DUMMY_MERGE_RETURN)
					return DUMMY_PTR;
#ifdef MT_CRASH_AFTER_MERGE_1
        if (simulateCrash){
            int shouldCrash = rand()%2;
            if(shouldCrash){
                std::cout << "\tDeletion  abruptly terminated : After merge 1" << std::endl;
                lock_initialization();
                return DUMMY_PTR;
            }
        }
#endif	
 
#ifdef CRASH_AFTER_MERGE_1
        		pid_t pid = fork();

        		if (pid == 0) {
            		// Crash state after key removed from parent.
            		lock_initialization();
            		mt_cout << __func__ << " Child crashed after MERGE STEP 1" << std::endl;
            		return DUMMY_PTR;
        		}

        		else if (pid > 0){
            		int returnStatus;
            		waitpid(pid, &returnStatus, 0);
            		mt_cout << __func__ <<" Continuing in parent process to MERGE STEP 2" << std::endl;
        		}
        		else {
            		mt_cout << __func__ << " Fork failed" << std::endl;
            		return NULL;
        		}
#endif

				if (merge_state == 16) {
                    p = correct_layer_root(root, lv, depth, pkx_);
                    p->entry[pkx_.p].value = nr;
                    clflush((char *)&p->entry[pkx_.p].value, sizeof(void *), true);
                    p->unlock();
                }
            }
        } else {
            if (t->root() == nr) {
                cp = nr->permutation.value();
                cp = cp.make_empty();
                fence();
                nr->permutation = cp.value();
                clflush((char *)(&nr->permutation), sizeof(permuter), true);
                nr->unlock();
                return nr;
            } else {
                nl = search_for_leftsibling(t->root(), nr->highest ? nr->highest - 1 : nr->highest, nr->level_, nr);
                merge_state = t->merge(NULL, NULL, 0, NULL, nr->highest, nr->level_ + 1, NULL);

				// Crash in innere delete, called by merge
				if (merge_state == DUMMY_MERGE_RETURN)
					return DUMMY_PTR;

#ifdef MT_CRASH_AFTER_MERGE_1
        if (simulateCrash){
            int shouldCrash = rand()%2;
            if(shouldCrash){
                std::cout << "\tDeletion  abruptly terminated : After merge 1" << std::endl;
                lock_initialization();
                return DUMMY_PTR;
            }
        }
#endif


#ifdef CRASH_AFTER_MERGE_1
                pid_t pid = fork();
                
                if (pid == 0) {
                    // Crash state after key removed from parent.
                    lock_initialization();
                    mt_cout << __func__ << " Child crashed after MERGE STEP 1" << std::endl;
                    return DUMMY_PTR;
                }
        
                else if (pid > 0){
                    int returnStatus;
                    waitpid(pid, &returnStatus, 0);
                    mt_cout << __func__ <<" Continuing in parent process to MERGE STEP 2" << std::endl;
                }
                else {
                    mt_cout << __func__ << " Fork failed" << std::endl;
                    return NULL;
                }
#endif

                if (merge_state == 16)
                    t->setNewRoot(nr);
            }
        }

        // Final step for node reclamation
        // next pointer is changed, except for leftmost child
        if (merge_state >= 0 && merge_state < 16) {
            nl->next = nr->next;
            clflush((char *)(&nl->next), sizeof(leafnode *), true);
        }

        cp = nr->permutation.value();
        cp = cp.make_empty();
        nr->permutation = cp.value();
        clflush((char *)(&nr->permutation), sizeof(permuter), true);

        if (nl != nr) {
            nr->unlock();
            nl->unlock();
        } else {
            nl->unlock();
        }
        ret = nr;
    }

    assert(ret != NULL);
    return ret;
}

void *leafnode::inter_insert(masstree *t, void *root, uint32_t depth, leafvalue *lv, uint64_t key, void *value,
        key_indexed_position &kx_, bool flush, bool with_lock, leafnode *invalid_sibling, leafnode *child)
{	
    void *ret;
	bool status;

    // permutation based insert
    if (this->permutation.size() < LEAF_WIDTH) {
        kx_.p = this->permutation.back();
        this->assign(kx_.p, key, value);
        clflush((char *)(&this->entry[kx_.p]), sizeof(kv), true);

        permuter cp = this->permutation.value();
        cp.insert_from_back(kx_.i);
        fence();
        this->permutation = cp.value();
        clflush((char *)(&this->permutation), sizeof(permuter), true);

        if (child != NULL) {
            child->next->unlock();
            child->unlock();
        }

        this->unlock();
        ret = this;
    } else {
        // overflow
        // internode split
		mt_cout << __func__ << " Starting internal node split" << std::endl;
        leafnode *new_sibling = new leafnode(this->level_);
        new_sibling->lock();
        uint64_t split_key;
		mt_cout << __func__ << " : SPLIT STEP 1 : Sibling create" << std::endl;
        this->split_into_inter(new_sibling, kx_.i, key, value, split_key);

#ifdef MT_CRASH_AFTER_SPLIT_INNER_1
        if (simulateCrash){
            int shouldCrash = rand()%2;
            if(shouldCrash){
                std::cout << "\tInsertion  abruptly terminated : Inner split 1" << std::endl;
                lock_initialization();
                return DUMMY_PTR;
            }
        }
#endif


#ifdef CRASH_AFTER_SPLIT_INNER_1
        pid_t pid = fork();

        if (pid == 0) {
            // Crash state after sibling node is installed.
            lock_initialization();
            mt_cout << __func__ << " Child crashed after SPLIT INNER STEP 1" << std::endl;
            return DUMMY_PTR;
        }

        else if (pid > 0){
            int returnStatus;
            waitpid(pid, &returnStatus, 0);
            mt_cout << __func__ <<"Continuing in parent process to SPLIT INNER STEP 2" << std::endl;
        }
        else {
            mt_cout << __func__ << "Fork failed" << std::endl;
            return NULL;
        }
#endif

        leafnode *nl = reinterpret_cast<leafnode *> (this);
        leafnode *nr = reinterpret_cast<leafnode *> (new_sibling);

        permuter perml = nl->permutation;
        int width = perml.size();
        // Removing mid-1 entry
        perml.set_size(width - (nr->permutation.size() + 1));

        if (width != LEAF_WIDTH)
            perml.exchange(perml.size(), LEAF_WIDTH - 1);
        nl->version_++;
        fence();
		mt_cout << __func__ << " : SPLIT STEP 2 : Update left indirect table" << std::endl;
        nl->permutation = perml.value();
        clflush((char *)(&nl->permutation), sizeof(permuter), true);

#ifdef MT_CRASH_AFTER_SPLIT_INNER_2
        if (simulateCrash){
            int shouldCrash = rand()%2;
            if(shouldCrash){
                std::cout << "\tInsertion  abruptly terminated : Inner split 2" << std::endl;
                lock_initialization();
                return DUMMY_PTR;
            }
        }
#endif


#ifdef CRASH_AFTER_SPLIT_INNER_2
        pid_t pid2 = fork();

        if (pid2 == 0) {
            // Crash state after sibling node is installed.
            lock_initialization();
            mt_cout << __func__ << " Child crashed after SPLIT INNER STEP 2" << std::endl;
            return DUMMY_PTR;
        }

        else if (pid2 > 0){
            int returnStatus;
            waitpid(pid2, &returnStatus, 0);
            mt_cout << __func__ <<"Continuing in parent process to SPLIT INNER STEP 3" << std::endl;
        }
        else {
            mt_cout << __func__ << "Fork failed" << std::endl;
            return NULL;
        }
#endif


        if (key < split_key) {
            kx_.p = nl->permutation.back();
            nl->assign(kx_.p, key, value);
            clflush((char *)(&nl->entry[kx_.p]), sizeof(kv), true);

            permuter cp = nl->permutation.value();
            cp.insert_from_back(kx_.i);
            fence();
            nl->permutation = cp.value();
            clflush((char *)(&nl->permutation), sizeof(permuter), true);

            ret = nl;
        } else {
            kx_ = nr->key_lower_bound_by(key);
            kx_.p = nr->permutation.back();
            nr->assign(kx_.p, key, value);
            clflush((char *)(&nr->entry[kx_.p]), sizeof(kv), true);

            permuter cp = nr->permutation.value();
            cp.insert_from_back(kx_.i);
            fence();
            nr->permutation = cp.value();
            clflush((char *)(&nr->permutation), sizeof(permuter), true);

            ret = nr;
        }

        // lock coupling (hand-over-hand locking)
        if (child != NULL) {
            child->next->unlock();
            child->unlock();
        }

        if (depth > 0) {
            key_indexed_position pkx_;
            leafnode *p = correct_layer_root(root, lv, depth, pkx_);
            if (p->value(pkx_.p) == this) {
				mt_cout << __func__ << " : SPLIT STEP 3 : Update parent d>0" << std::endl;
                leafnode *new_root = new leafnode(this, split_key, new_sibling, level_ + 1);
                clflush((char *) new_root, sizeof(leafnode), true);
                p->entry[pkx_.p].value = new_root;
                clflush((char *) &p->entry[pkx_.p].value, sizeof(uintptr_t), true);
                p->unlock();

                this->next->unlock();
                this->unlock();
            } else {
                root = p;
                status = t->split(p->entry[pkx_.p].value, root, depth, lv, split_key, new_sibling, level_ + 1, this);
				if (!status)
					return DUMMY_PTR; 
            }
        } else {
            if (t->root() == this) {
				mt_cout << __func__ << " : SPLIT STEP 3 : Update parent d = 0" << std::endl;
                leafnode *new_root = new leafnode(this, split_key, new_sibling, level_ + 1);
                clflush((char *) new_root, sizeof(leafnode), true);
                t->setNewRoot(new_root);

                this->next->unlock();
                this->unlock();
            } else {
                status = t->split(NULL, NULL, 0, NULL, split_key, new_sibling, level_ + 1, this);
				if (!status)
					return DUMMY_PTR;
            }
        }
    }

    return ret;
}

int leafnode::inter_delete(masstree *t, void *root, uint32_t depth, leafvalue *lv, uint64_t key,
        key_indexed_position &kx_, bool flush, bool with_lock, leafnode *invalid_sibling, leafnode *child)
{
    int ret, merge_state;

    // permutation based remove
    if (this->permutation.size() >= LEAF_THRESHOLD) {
        permuter cp;
        if (kx_.i >= 0) {
            cp = this->permutation.value();
            cp.remove_to_back(kx_.i);
            fence();
            this->permutation = cp.value();
            clflush((char *)(&this->permutation), sizeof(permuter), true);
        }

        this->unlock();
    } else {
        // Underflow
        // Merge
        permuter cp;
        leafnode *nl, *nr;
        nr = reinterpret_cast<leafnode *> (this);

        if (depth > 0) {
            key_indexed_position pkx_;
            leafnode *p = correct_layer_root(root, lv, depth, pkx_);
            if (p->value(pkx_.p) == nr) {
                kx_.i = 16;
                p->unlock();
                nr->unlock();
                return (ret = kx_.i);
            } else {
                nl = search_for_leftsibling(p->entry[pkx_.p].value, nr->highest ? nr->highest - 1 : nr->highest, nr->level_, nr);
                merge_state = t->merge(p->entry[pkx_.p].value, root, depth, lv, nr->highest, nr->level_ + 1, nl);
				if (merge_state == DUMMY_MERGE_RETURN)
					return DUMMY_MERGE_RETURN;
            }
        } else {
            if (t->root() == nr) {
                kx_.i = 16;
                nr->unlock();
                return (ret = kx_.i);
            } else {
                nl = search_for_leftsibling(t->root(), nr->highest ? nr->highest - 1 : nr->highest, nr->level_, nr);
                merge_state = t->merge(NULL, NULL, 0, NULL, nr->highest, nr->level_ + 1, nl);
            	if (merge_state == DUMMY_MERGE_RETURN)	
					return DUMMY_MERGE_RETURN;
			}
        }

#ifdef MT_CRASH_AFTER_MERGE_INNER_1
        if (simulateCrash){
            int shouldCrash = rand()%2;
            if(shouldCrash){
                std::cout << "\tDeletion abruptly terminated : Inner merge 1" << std::endl;
                lock_initialization();
                return DUMMY_MERGE_RETURN;
            }
        }
#endif

#ifdef CRASH_AFTER_MERGE_INNER_1
		pid_t pid = fork();

		if (pid == 0) {
			// Crash state after key removed from parent.
			lock_initialization();
			mt_cout << __func__ << " Child crashed after INNER MERGE STEP 1 " << DUMMY_MERGE_RETURN << std::endl;
			return DUMMY_MERGE_RETURN;
		}

		else if (pid > 0){
			int returnStatus;
			waitpid(pid, &returnStatus, 0);
			mt_cout << __func__ <<" Continuing in parent process to INNER MERGE STEP 2" << std::endl;
		}
		else {
			mt_cout << __func__ << " Fork failed" << std::endl;
			return false;
		}
#endif

        // Final step for internal node reclamation
        if (merge_state >= 0 && merge_state < 16) {
            nl->next = nr->next;
            clflush((char *)(&nl->next), sizeof(leafnode *), true);
        } else if (merge_state == 16) {
            kx_.i = 16;
        }

        if (nl != nr) {
            nr->unlock();
            nl->unlock();
        } else {
            nl->unlock();
        }
    }

    return (ret = kx_.i);
}

bool masstree::split(void *left, void *root, uint32_t depth, leafvalue *lv,
        uint64_t key, void *right, uint32_t level, void *child)
{
    leafnode *p;
    key_indexed_position kx_;
    uint64_t oldv;
    leafnode *next;
	bool status;
	
    if (depth > 0) {
        if (level > reinterpret_cast<leafnode *>(left)->level())
            return true ;
        p = reinterpret_cast<leafnode *> (left);
        reinterpret_cast<leafnode *> (root)->unlock();
    } else {
        if (level > reinterpret_cast<leafnode *>(root_)->level())
            return true;
        p = reinterpret_cast<leafnode *> (root_);
    }

    while (p->level() > level) {
inter_retry:
        next = p->advance_to_key(key, true);
        if (next != p) {
            p = next;
            goto inter_retry;
        }

        p->prefetch();
        fence();

        kx_ = p->key_lower_bound(key);

        void *snapshot_v;
        if (kx_.i >= 0) {
            snapshot_v = p->value(kx_.p);
            fence();
            if (p->key(kx_.p) <= key) {
                if (snapshot_v == p->value(kx_.p))
                    p = reinterpret_cast<leafnode *>(snapshot_v);
                else {
                    goto inter_retry;
                }
            }
        } else {
            p = p->leftmost();
        }
    }

leaf_retry:
    p->lock();
    next = p->advance_to_key(key, true);
    if (next != p) {
        p->unlock();
        p = next;
        goto leaf_retry;
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound_by(key);
    if (kx_.p >= 0 || key == p->highest_()) {
        p->unlock();
        reinterpret_cast<leafnode *> (right)->unlock();
        reinterpret_cast<leafnode *> (child)->unlock();
        return true;
    }

	void* crash = p->inter_insert(this, root, depth, lv, key, right, kx_, true,
                true, NULL, reinterpret_cast<leafnode *> (child));
	if (crash == DUMMY_PTR)
		return false;

	if (!crash){
		status = split(left, root, depth, lv, key, right, level, child);
		if (!status)
			return false;
	}

	return true;
}

int masstree::merge(void *left, void *root, uint32_t depth, leafvalue *lv,
        uint64_t key, uint32_t level, void *child)
{
    leafnode *p;
    key_indexed_position kx_;
    uint64_t oldv;
    leafnode *next;
    void *snapshot_v;

    if (depth > 0) {
        //if (level > reinterpret_cast<leafnode *>(left)->level())
        //    return ;
        p = reinterpret_cast<leafnode *> (left);
        reinterpret_cast<leafnode *> (root)->unlock();
    } else {
        //if (level > reinterpret_cast<leafnode *>(this->root_)->level())
        //    return ;
        p = reinterpret_cast<leafnode *> (this->root_);
    }

    while (p->level() > level) {
inter_retry:
        next = p->advance_to_key(key, true);
        if (next != p) {
            p = next;
            goto inter_retry;
        }

        p->prefetch();
        fence();

        kx_ = p->key_lower_bound(key);
        if (kx_.i >= 0) {
            snapshot_v = p->value(kx_.p);
            fence();
            if (p->key(kx_.p) <= key) {
                if (snapshot_v == p->value(kx_.p))
                    p = reinterpret_cast<leafnode *>(snapshot_v);
                else {
                    goto inter_retry;
                }
            }
        } else {
            p = p->leftmost();
        }
    }

leaf_retry:
    p->lock();
    next = p->advance_to_key(key, true);
    if (next != p) {
        p->unlock();
        p = next;
        goto leaf_retry;
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound(key);

    return p->inter_delete(this, root, depth, lv, key, kx_, true,
            true, NULL, reinterpret_cast<leafnode *> (child));
}

void *masstree::get(uint64_t key)
{
    void *root = this->root_;
    key_indexed_position kx_;
    leafnode *next;

    leafnode *p = reinterpret_cast<leafnode *> (root);
    while (p->level() != 0) {
inter_retry:
        next = p->advance_to_key(key, false);
        if (next != p) {
            p = next;
            goto inter_retry;
        }

        p->prefetch();
        fence();

        kx_ = p->key_lower_bound(key);

        void *snapshot_v;
        if (kx_.i >= 0) {
            snapshot_v = p->value(kx_.p);
            fence();
            if (p->key(kx_.p) <= key) {
                if (snapshot_v == p->value(kx_.p))
                    p = reinterpret_cast<leafnode *>(snapshot_v);
                else {
                    goto inter_retry;
                }
            }
        } else {
            p = p->leftmost();
        }
    }

    leafnode *l = reinterpret_cast<leafnode *> (p);
leaf_retry:
    l->prefetch();
    fence();

    kx_ = l->key_lower_bound_by(key);

    void *snapshot_v = l->value(kx_.p);
    fence();
    if (kx_.p >= 0 && l->key(kx_.p) == key) {
        if (snapshot_v == l->value(kx_.p))
            return snapshot_v;
        else {
            l = l->advance_to_key(key, false);
            goto leaf_retry;
        }
    } else {
        next = l->advance_to_key(key, false);
        if (next != l) {
            l = next;
            goto leaf_retry;
        } else {
            return NULL;
            printf("should not enter here\n");
            printf("key = %lu, searched key = %lu, key index = %d\n", key, l->key(kx_.p), kx_.p);
            permuter cp = l->permute();
            for (int i = 0; i < cp.size(); i++) {
                printf("key = %lu\n", l->key(cp[i]));
            }

            if (l->next_()) {
                cp = l->next_()->permute();
                printf("next high key = %lu\n", l->next_()->highest_());
                for (int i = 0; i < cp.size(); i++) {
                    printf("next key = %lu\n", l->next_()->key(cp[i]));
                }
            }
            exit(0);
        }
    }
}

void *masstree::get(char *key)
{
    void *root = this->root_;
    key_indexed_position kx_;
    uint32_t depth = 0;
    leafnode *next;
    void *snapshot_v;

    leafvalue *lv = make_leaf(key, strlen(key), 0);

    leafnode *p = reinterpret_cast<leafnode *> (root);
from_root:
    while (p->level() != 0) {
inter_retry:
        next = p->advance_to_key(lv->fkey[depth], false);
        if (next != p) {
            p = next;
            goto inter_retry;
        }

        p->prefetch();
        fence();

        kx_ = p->key_lower_bound(lv->fkey[depth]);

        if (kx_.i >= 0) {
            snapshot_v = p->value(kx_.p);
            fence();
            if (p->key(kx_.p) <= lv->fkey[depth]) {
                if (snapshot_v == p->value(kx_.p))
                    p = reinterpret_cast<leafnode *>(snapshot_v);
                else {
                    goto inter_retry;
                }
            }
        } else {
            p = p->leftmost();
        }
    }

    leafnode *l = reinterpret_cast<leafnode *> (p);
leaf_retry:
    l->prefetch();
    fence();

    kx_ = l->key_lower_bound_by(lv->fkey[depth]);
    if (kx_.p >= 0) {
        // If there is additional layer, retry B+tree traversing from the next layer
        snapshot_v = l->value(kx_.p);
        if (!IS_LV(l->value(kx_.p))) {
            if (l->key(kx_.p) == lv->fkey[depth] && snapshot_v == l->value(kx_.p)) {
                p = reinterpret_cast<leafnode *> (snapshot_v);
                depth++;
                goto from_root;
            }
        } else {
            snapshot_v = &((LV_PTR(l->value(kx_.p)))->value);
            if (l->key(kx_.p) == lv->fkey[depth] && (LV_PTR(l->value(kx_.p)))->key_len == lv->key_len
                    && memcmp((LV_PTR(l->value(kx_.p)))->fkey, lv->fkey, lv->key_len) == 0) {
                if (snapshot_v == &((LV_PTR(l->value(kx_.p)))->value))
                    return snapshot_v;
            } else {
                return NULL;
            }
        }

        l = l->advance_to_key(lv->fkey[depth], false);
        goto leaf_retry;
    } else {
        next = l->advance_to_key(lv->fkey[depth], false);
        if (next != l) {
            l = next;
            goto leaf_retry;
        } else {
            return NULL;
            printf("should not enter here\n");
            printf("fkey = %s, key = %lu, searched key = %lu, key index = %d\n",
                    (char *)(lv->fkey), lv->fkey[depth], l->key(kx_.p), kx_.p);

            permuter cp = l->permute();
            for (int i = 0; i < cp.size(); i++) {
                printf("key = %lu\n", l->key(cp[i]));
                printf("fkey = %s\n", (char *)((LV_PTR(l->value(cp[i])))->fkey));
            }

            if (l->next_()) {
                cp = l->next_()->permute();
                for (int i = 0; i < cp.size(); i++) {
                    printf("next key = %lu\n", l->next_()->key(cp[i]));
                }
            }
            exit(0);
        }
    }
}

void leafnode::get_range(leafvalue * &lv, int num, int &count, leafvalue *buf[], leafnode *root, uint32_t depth)
{
    key_indexed_position kx_;
    leafnode *next;
    void *snapshot_v, *snapshot_n;
    permuter perm;
    int backup;

    leafnode *p = root;
from_root:
    while (p->level() != 0) {
inter_retry:
        next = p->advance_to_key(lv->fkey[depth], false);
        if (next != p) {
            p = next;
            goto inter_retry;
        }

        p->prefetch();
        fence();

        kx_ = p->key_lower_bound(lv->fkey[depth]);

        if (kx_.i >= 0) {
            snapshot_v = p->value(kx_.p);
            fence();
            if (p->key(kx_.p) <= lv->fkey[depth]) {
                if (snapshot_v == p->value(kx_.p))
                    p = reinterpret_cast<leafnode *>(snapshot_v);
                else {
                    goto inter_retry;
                }
            }
        } else {
            p = p->leftmost();
        }
    }

    count = 0;
    leafnode *l = reinterpret_cast<leafnode *> (p);
    while (count < num) {
leaf_retry:
        backup = count;
        snapshot_n = l->next_();
        perm = l->permute();
        l->prefetch();
        fence();

        for (int i = 0; i < perm.size() && count < num; i++) {
            // If there is additional layer, retry B+tree traversing from the next layer
            snapshot_v = l->value(perm[i]);
            fence();
            if (!IS_LV(l->value(perm[i]))) {
                if (l->key(perm[i]) > lv->fkey[depth] && snapshot_v == l->value(perm[i])) {
                    p = reinterpret_cast<leafnode *> (snapshot_v);
                    leafvalue *smallest = p->smallest_leaf(lv->key_len, lv->value);
                    p->get_range(smallest, num, count, buf, p, depth + 1);
                } else if (l->key(perm[i]) == lv->fkey[depth] && snapshot_v == l->value(perm[i])) {
                    p = reinterpret_cast<leafnode *> (snapshot_v);
                    p->get_range(lv, num, count, buf, p, depth + 1);
                }
            } else {
                snapshot_v = (LV_PTR(snapshot_v));
                if (l->key(perm[i]) > lv->fkey[depth]) {
                    if (snapshot_v == (LV_PTR(l->value(perm[i]))))
                        buf[count++] = reinterpret_cast<leafvalue *> (snapshot_v);
                    else {
                        count = backup;
                        goto leaf_retry;
                    }
                } else if (l->key(perm[i]) == lv->fkey[depth] && memcmp((LV_PTR(l->value(perm[i])))->fkey, lv->fkey, lv->key_len) >= 0) {
                    if (snapshot_v == (LV_PTR(l->value(perm[i]))))
                        buf[count++] = reinterpret_cast<leafvalue *> (snapshot_v);
                    else {
                        count = backup;
                        goto leaf_retry;
                    }
                }
            }
        }

        if (perm != l->permute() || l->next_() != snapshot_n) {
            count = backup;
            continue;
        } else {
            if (snapshot_n == NULL)
                break;
            else
                l = reinterpret_cast<leafnode *> (snapshot_n);
        }
    }
}

int masstree::scan(char *min, int num, leafvalue *buf[])
{
    void *root = this->root_;
    key_indexed_position kx_;
    uint32_t depth = 0;
    leafnode *next;
    void *snapshot_v, *snapshot_n;
    permuter perm;
    int count, backup;

    leafvalue *lv = make_leaf(min, strlen(min), 0);

    leafnode *p = reinterpret_cast<leafnode *> (root);
    while (p->level() != 0) {
inter_retry:
        next = p->advance_to_key(lv->fkey[depth], false);
        if (next != p) {
            p = next;
            goto inter_retry;
        }

        p->prefetch();
        fence();

        kx_ = p->key_lower_bound(lv->fkey[depth]);

        if (kx_.i >= 0) {
            snapshot_v = p->value(kx_.p);
            fence();
            if (p->key(kx_.p) <= lv->fkey[depth]) {
                if (snapshot_v == p->value(kx_.p))
                    p = reinterpret_cast<leafnode *>(snapshot_v);
                else {
                    goto inter_retry;
                }
            }
        } else {
            p = p->leftmost();
        }
    }

    count = 0;
    leafnode *l = reinterpret_cast<leafnode *> (p);
    while (count < num) {
leaf_retry:
        backup = count;
        snapshot_n = l->next_();
        perm = l->permute();
        l->prefetch();
        fence();

        for (int i = 0; i < perm.size() && count < num; i++) {
            // If there is additional layer, retry B+tree traversing from the next layer
            snapshot_v = l->value(perm[i]);
            mfence();
            if (!IS_LV(l->value(perm[i]))) {
                if (l->key(perm[i]) > lv->fkey[depth] && snapshot_v == l->value(perm[i])) {
                    p = reinterpret_cast<leafnode *> (snapshot_v);
                    leafvalue *smallest = p->smallest_leaf(lv->key_len, lv->value);
                    p->get_range(smallest, num, count, buf, p, depth + 1);
                } else if (l->key(perm[i]) == lv->fkey[depth] && snapshot_v == l->value(perm[i])) {
                    p = reinterpret_cast<leafnode *> (snapshot_v);
                    p->get_range(lv, num, count, buf, p, depth + 1);
                }
            } else {
                snapshot_v = (LV_PTR(snapshot_v));
                if (l->key(perm[i]) > lv->fkey[depth]) {
                    if (snapshot_v == (LV_PTR(l->value(perm[i]))))
                        buf[count++] = reinterpret_cast<leafvalue *> (snapshot_v);
                    else {
                        count = backup;
                        goto leaf_retry;
                    }
                } else if (l->key(perm[i]) == lv->fkey[depth] && memcmp((LV_PTR(l->value(perm[i])))->fkey, lv->fkey, lv->key_len) >= 0) {
                    if (snapshot_v == (LV_PTR(l->value(perm[i]))))
                        buf[count++] = reinterpret_cast<leafvalue *> (snapshot_v);
                    else {
                        count = backup;
                        goto leaf_retry;
                    }
                }
            }
        }

        if (perm != l->permute() || l->next_() != snapshot_n) {
            count = backup;
            continue;
        } else {
            if (snapshot_n == NULL)
                break;
            else
                l = reinterpret_cast<leafnode *> (snapshot_n);
        }
    }

    return count;
}

int masstree::scan(uint64_t min, int num, uint64_t *buf)
{
    void *root = this->root_;
    key_indexed_position kx_;
    uint32_t depth = 0;
    leafnode *next;
    void *snapshot_v;
    leafnode *snapshot_n;
    permuter perm;
    int count, backup;

    leafnode *p = reinterpret_cast<leafnode *> (root);
    while (p->level() != 0) {
inter_retry:
        next = p->advance_to_key(min, false);
        if (next != p) {
            p = next;
            goto inter_retry;
        }

        p->prefetch();
        fence();

        kx_ = p->key_lower_bound(min);

        if (kx_.i >= 0) {
            snapshot_v = p->value(kx_.p);
            fence();
            if (p->key(kx_.p) <= min) {
                if (snapshot_v == p->value(kx_.p))
                    p = reinterpret_cast<leafnode *>(snapshot_v);
                else {
                    goto inter_retry;
                }
            }
        } else {
            p = p->leftmost();
        }
    }

    count = 0;
    leafnode *l = reinterpret_cast<leafnode *> (p);
    while (count < num) {
leaf_retry:
        backup = count;
        snapshot_n = l->next_();
        perm = l->permute();
        l->prefetch();
        fence();

        for (int i = 0; i < perm.size() && count < num; i++) {
            snapshot_v = l->value(perm[i]);
            mfence();
            if (l->key(perm[i]) >= min) {
                if (snapshot_v == l->value(perm[i])) {
                    buf[count++] = (uint64_t)snapshot_v;
                } else {
                    count = backup;
                    goto leaf_retry;
                }
            }
        }

        if (perm != l->permute() || l->next_() != snapshot_n) {
            count = backup;
            continue;
        } else {
            if (snapshot_n == NULL)
                break;
            else
                l = snapshot_n;
        }
    }

    return count;
}
}
#endif
