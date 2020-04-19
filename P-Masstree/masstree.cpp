#include "masstree.h"
#include "Epoche.cpp"

using namespace MASS;

namespace masstree {

static constexpr uint64_t CACHE_LINE_SIZE = 64;

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
#ifdef CLFLUSH
        asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
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

masstree::masstree() {
    leafnode *init_root = new leafnode(0);
    root_ = init_root;
    clflush((char *)root_, sizeof(leafnode), true);
}

masstree::masstree (void *new_root) {
    root_ = new_root;
    clflush((char *)root_, sizeof(leafnode), true);      // 304 is the leafnode size of masstree
}

ThreadInfo masstree::getThreadInfo() {
    return ThreadInfo(this->epoche);
}

leafnode::leafnode(uint32_t level) : permutation(permuter::make_empty()) {
    level_ = level;
    obsolete = 0;
    wlock = new std::mutex();
    next = NULL;
    leftmost_ptr = NULL;
    highest = 0;
#ifdef LOCK_INIT
    lock_initializer.push_back(wlock);
#endif
}

leafnode::leafnode(void *left, uint64_t key, void *right, uint32_t level = 1) : permutation(permuter::make_empty()) {
    level_ = level;
    obsolete = 0;
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

void *leafnode::operator new(size_t size) {
    void *ret;
    posix_memalign(&ret, CACHE_LINE_SIZE, size);
    return ret;
}

void leafnode::operator delete(void *addr) {
    free(addr);
}

void leafnode::lock() {wlock->lock();}

void leafnode::unlock() {wlock->unlock();}

bool leafnode::trylock() {return wlock->try_lock();}

uint32_t leafnode::is_obsolete() {return obsolete;}

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

void leafnode::prefetch() const
{
    for (int i = 64; i < std::min((16 * LEAF_WIDTH) + 1, 4 * 64); i += 64)
        prefetch_((const char *) this + i);
}

leafvalue *masstree::make_leaf(char *key, size_t key_len, uint64_t value)
{
    void *aligned_alloc;
    size_t len = (key_len % sizeof(uint64_t)) == 0 ? key_len : (((key_len) / sizeof(uint64_t)) + 1) * sizeof(uint64_t);

    posix_memalign(&aligned_alloc, CACHE_LINE_SIZE, sizeof(leafvalue) + len + sizeof(uint64_t));
    leafvalue *lv = reinterpret_cast<leafvalue *> (aligned_alloc);
    memset(lv, 0, sizeof(leafvalue) + len + sizeof(uint64_t));

    lv->value = value;
    lv->key_len = key_len;          // key_len or len??
    memcpy(lv->fkey, key, key_len);

    for (int i = 0; i < (len / sizeof(uint64_t)); i++)
        lv->fkey[i] = __builtin_bswap64(lv->fkey[i]);

    if (value != 0)
        clflush((char *) lv, sizeof(leafvalue) + len + sizeof(uint64_t), true);
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

    nl->permutation = permuter::make_sorted(2);

    fence();
    if (twig_tail != l)
        twig_tail->entry[0].value = nl;
    twig_tail = nl;
    if (twig_head != l) {
        leafnode *iter = twig_head;
        mfence();
        for ( ; iter != twig_tail && iter != NULL; iter = reinterpret_cast <leafnode *>(iter->entry[0].value)) {
            clflush((char *)iter, sizeof(leafnode), false);
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
}

void leafnode::check_for_recovery(masstree *t, leafnode *left, leafnode *right, void *root, uint32_t depth, leafvalue *lv)
{
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
            t->split(p->entry[pkx_.p].value, root, depth, lv, right->highest, right, left->level() + 1, left);
        }
    } else {
        if (t->root() == left) {
            leafnode *new_root = new leafnode(left, right->highest, right, left->level() + 1);
            clflush((char *) new_root, sizeof(leafnode), true);
            t->setNewRoot(new_root);

            right->unlock();
            left->unlock();
        } else {
            t->split(NULL, NULL, 0, NULL, right->highest, right, left->level() + 1, left);
        }
    }
}

void masstree::put(uint64_t key, void *value, ThreadInfo &threadEpocheInfo)
{
    EpocheGuard epocheGuard(threadEpocheInfo);
    key_indexed_position kx_;
    leafnode *next = NULL, *p = NULL;

from_root:
    p = reinterpret_cast<leafnode *> (this->root_);
    while (p->level() != 0) {
inter_retry:
        next = p->advance_to_key(key, true);
        if (next != p) {
            // check for recovery
            if (p->trylock()) {
                if (next->trylock())
                    p->check_for_recovery(this, p, next, NULL, 0, NULL);
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
            if (next->trylock())
                l->check_for_recovery(this, l, next, NULL, 0, NULL);
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

    if (l->is_obsolete()) {
        l->unlock();
        goto from_root;
    }

    l->prefetch();
    fence();

    kx_ = l->key_lower_bound_by(key);
    if (kx_.p >= 0 && l->key(kx_.p) == key) {
        l->assign_value(kx_.p, value);
        l->unlock();
    } else {
        if (!(l->leaf_insert(this, NULL, 0, NULL, key, value, kx_, true, true, NULL))) {
            put(key, value, threadEpocheInfo);
        }
    }
}

void masstree::put(char *key, uint64_t value, ThreadInfo &threadEpocheInfo)
{
    EpocheGuard epocheGuard(threadEpocheInfo);
    void *root = NULL;
    key_indexed_position kx_;
    uint32_t depth;
    leafnode *next = NULL, *p = NULL;
    leafvalue *lv = make_leaf(key, strlen(key), value);

restart:
    root = this->root_;
    depth = 0;
    p = reinterpret_cast<leafnode *> (root);

from_root:
    while (p->level() != 0) {
inter_retry:
        next = p->advance_to_key(lv->fkey[depth], true);
        if (next != p) {
            // check for recovery
            if (p->trylock()) {
                if (next->trylock())
                    p->check_for_recovery(this, p, next, root, depth, lv);
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
            if (next->trylock())
                l->check_for_recovery(this, l, next, root, depth, lv);
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

    if (l->is_obsolete()) {
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
            (LV_PTR(l->value(kx_.p)))->value = value;
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
        if (!(l->leaf_insert(this, root, depth, lv, lv->fkey[depth], SET_LV(lv), kx_, true, true, NULL))) {
            put(key, value, threadEpocheInfo);
        }
    }
}

void masstree::del(uint64_t key, ThreadInfo &threadEpocheInfo)
{
    EpocheGuard epocheGuard(threadEpocheInfo);
    void *root = NULL;
    key_indexed_position kx_;
    leafnode *next;
    void *snapshot_v;

from_root:
    root = this->root_;
    leafnode *p = reinterpret_cast<leafnode *> (root);
    while (p->level() != 0) {
inter_retry:
        next = p->advance_to_key(key, true);
        if (next != p) {
            // check for recovery
            if (p->trylock()) {
                if (next->trylock())
                    p->check_for_recovery(this, p, next, NULL, 0, NULL);
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
            if (next->trylock())
                l->check_for_recovery(this, l, next, NULL, 0, NULL);
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
    if (kx_.p < 0) {
        l->unlock();
        return ;
    }

    if (!(l->leaf_delete(this, NULL, 0, NULL, key, kx_, true, true, NULL, threadEpocheInfo))) {
        del(key, threadEpocheInfo);
    }
}

void masstree::del(char *key, ThreadInfo &threadEpocheInfo)
{
    EpocheGuard epocheGuard(threadEpocheInfo);
    void *root = this->root_;
    key_indexed_position kx_;
    uint32_t depth = 0;
    leafnode *next;

    leafvalue *lv = make_leaf(key, strlen(key), 0);

    leafnode *p = reinterpret_cast<leafnode *> (root);
from_root:
    while (p->level() != 0) {
inter_retry:
        next = p->advance_to_key(lv->fkey[depth], true);
        if (next != p) {
            // check for recovery
            if (p->trylock()) {
                if (next->trylock())
                    p->check_for_recovery(this, p, next, root, depth, lv);
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
            if (next->trylock())
                l->check_for_recovery(this, l, next, root, depth, lv);
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
            if (!(l->leaf_delete(this, root, depth, lv, lv->fkey[depth], kx_, true, true, NULL, threadEpocheInfo))) {
                free(lv);
                del(key, threadEpocheInfo);
            }
        } else {
            l->unlock();
            free(lv);
            return ;
        }
    } else {
        l->unlock();
        free(lv);
        return ;
    }
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

int leafnode::split_into(leafnode *nr, int p, const uint64_t& key, void *value, uint64_t& split_key)
{
    int width = this->permutation.size();
    int mid = width / 2 + 1;

    permuter perml = this->permutation;
    permuter pv = perml.value_from(mid - (p < mid));
    for (int x = mid; x <= width; ++x) {
        if (x == p)
            nr->assign_initialize(x - mid, key, value);
        else {
            nr->assign_initialize(x - mid, this, pv & 15);
            pv >>= 4;
        }
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
    clflush((char *)&entry[p].value, sizeof(void *), true);
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

leafnode *leafnode::search_for_leftsibling(void **root, uint64_t key, uint32_t level, leafnode *right)
{
    leafnode *p;
    key_indexed_position kx_;
    leafnode *next;

from_root:
    p = reinterpret_cast<leafnode *> (*root);
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

        if (p->is_obsolete()) {
            p->unlock();
            goto from_root;
        }
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

        // TODO: Need to add an additional context here checking crash and running recovery
        // mechanism to avoid the duplicate split for the node that was split, but the
        // permutor was not updated to only reflect the half of the entries.
        // * Algorithm sketch:
        // i) compare the high key of right sibling with the highest key of left sibling.
        // ii) If the highest key of left sibling is larger than the high key of right sibling,
        //     1) invalidate the right sibling by atomically changing the next pointer of left
        //        sibling to the next pointer of right sibling. And, continue to the split process.
        //        (current implementation reflects this way, but will be changed to second method)
        //     2) replay the original split process from the third step that removes the half of
        //        the entries from the left sibling. (this would be more reasonable in terms of
        //        reusing the existing split mechanism)
        if (this->next != NULL && this->key(this->permutation[this->permutation.size() - 1]) > this->next->highest) {
            this->next = this->next->next;
            clflush((char *)&this->next, sizeof(leafnode *), true);
        }

        leafnode *new_sibling = new leafnode(this->level_);
        new_sibling->lock();
        uint64_t split_key;
        int split_type = this->split_into(new_sibling, kx_.i, key, value, split_key);

        leafnode *nl = reinterpret_cast<leafnode *> (this);
        leafnode *nr = reinterpret_cast<leafnode *> (new_sibling);

        permuter perml = nl->permutation;
        int width = perml.size();
        perml.set_size(width - nr->permutation.size());

        if (width != LEAF_WIDTH)
            perml.exchange(perml.size(), LEAF_WIDTH - 1);

        nl->permutation = perml.value();
        clflush((char *)(&nl->permutation), sizeof(permuter), true);

        if (depth > 0) {
            key_indexed_position pkx_;
            leafnode *p = correct_layer_root(root, lv, depth, pkx_);
            if (p->value(pkx_.p) == this) {
                leafnode *new_root = new leafnode(this, split_key, new_sibling, level_ + 1);
                clflush((char *) new_root, sizeof(leafnode), true);
                p->entry[pkx_.p].value = new_root;
                clflush((char *) &p->entry[pkx_.p].value, sizeof(uintptr_t), true);
                p->unlock();
            } else {
                root = p;
                t->split(p->entry[pkx_.p].value, root, depth, lv, split_key, new_sibling, level_ + 1, NULL);
            }
        } else {
            if (t->root() == this) {
                leafnode *new_root = new leafnode(this, split_key, new_sibling, level_ + 1);
                clflush((char *) new_root, sizeof(leafnode), true);
                t->setNewRoot(new_root);
            } else {
                t->split(NULL, NULL, 0, NULL, split_key, new_sibling, level_ + 1, NULL);
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
        key_indexed_position &kx_, bool flush, bool with_lock, leafnode *invalid_sibling, ThreadInfo &threadInfo)
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
        if (lv != NULL) threadInfo.getEpoche().markNodeForDeletion((LV_PTR(this->value(kx_.p))), threadInfo);
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
                nl = search_for_leftsibling(&p->entry[pkx_.p].value, nr->highest ? nr->highest - 1 : nr->highest, nr->level_, nr);
                merge_state = t->merge(p->entry[pkx_.p].value, reinterpret_cast<void *> (p), depth, lv, nr->highest, nr->level_ + 1, NULL, threadInfo);
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
                nl = search_for_leftsibling(t->root_dp(), nr->highest ? nr->highest - 1 : nr->highest, nr->level_, nr);
                merge_state = t->merge(NULL, NULL, 0, NULL, nr->highest, nr->level_ + 1, NULL, threadInfo);
                if (merge_state == 16)
                    t->setNewRoot(nr);
            }
        }

        // Final step for node reclamation
        // next pointer is changed, except for leftmost child
        if (merge_state >= 0 && merge_state < 16) {
            nl->next = nr->next;
            clflush((char *)(&nl->next), sizeof(leafnode *), true);
            nr->obsolete = 1;
            threadInfo.getEpoche().markNodeForDeletion(nr, threadInfo);
        }

        cp = nr->permutation.value();
        cp = cp.make_empty();
        nr->permutation = cp.value();
        clflush((char *)(&nr->permutation), sizeof(permuter), true);

        if (nl != nr) {
            nr->unlock();
            nl->unlock();
        } else {
            nr->unlock();
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

        // TODO: Need to add an additional context here checking crash and running recovery
        // mechanism to avoid the duplicate split for the node that was split, but the
        // permutor was not updated to only reflect the half of the entries.
        // * Algorithm sketch:
        // i) compare the high key of right sibling with the highest key of left sibling.
        // ii) If the highest key of left sibling is larger than the high key of right sibling,
        //     1) invalidate the right sibling by atomically changing the next pointer of left
        //        sibling to the next pointer of right sibling. And, continue to the split process.
        //        (current implementation reflects this way, but will be changed to second method)
        //     2) replay the original split process from the third step that removes the half of
        //        the entries from the left sibling. (this would be more reasonable in terms of
        //        reusing the existing split mechanism)
        if (this->next != NULL && this->key(this->permutation[this->permutation.size() - 1]) > this->next->highest) {
            this->next = this->next->next;
            clflush((char *)&this->next, sizeof(leafnode *), true);
        }

        leafnode *new_sibling = new leafnode(this->level_);
        new_sibling->lock();
        uint64_t split_key;
        this->split_into_inter(new_sibling, kx_.i, key, value, split_key);

        leafnode *nl = reinterpret_cast<leafnode *> (this);
        leafnode *nr = reinterpret_cast<leafnode *> (new_sibling);

        permuter perml = nl->permutation;
        int width = perml.size();
        // Removing mid-1 entry
        perml.set_size(width - (nr->permutation.size() + 1));

        if (width != LEAF_WIDTH)
            perml.exchange(perml.size(), LEAF_WIDTH - 1);

        nl->permutation = perml.value();
        clflush((char *)(&nl->permutation), sizeof(permuter), true);

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
                leafnode *new_root = new leafnode(this, split_key, new_sibling, level_ + 1);
                clflush((char *) new_root, sizeof(leafnode), true);
                p->entry[pkx_.p].value = new_root;
                clflush((char *) &p->entry[pkx_.p].value, sizeof(uintptr_t), true);
                p->unlock();

                this->next->unlock();
                this->unlock();
            } else {
                root = p;
                t->split(p->entry[pkx_.p].value, root, depth, lv, split_key, new_sibling, level_ + 1, this);
            }
        } else {
            if (t->root() == this) {
                leafnode *new_root = new leafnode(this, split_key, new_sibling, level_ + 1);
                clflush((char *) new_root, sizeof(leafnode), true);
                t->setNewRoot(new_root);

                this->next->unlock();
                this->unlock();
            } else {
                t->split(NULL, NULL, 0, NULL, split_key, new_sibling, level_ + 1, this);
            }
        }
    }

    return ret;
}

int leafnode::inter_delete(masstree *t, void *root, uint32_t depth, leafvalue *lv, uint64_t key,
        key_indexed_position &kx_, bool flush, bool with_lock, leafnode *invalid_sibling, leafnode *child, ThreadInfo &threadInfo)
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
                nr->obsolete = 1;
                threadInfo.getEpoche().markNodeForDeletion(nr, threadInfo);
                p->unlock();
                nr->unlock();
                return (ret = kx_.i);
            } else {
                nl = search_for_leftsibling(&p->entry[pkx_.p].value, nr->highest ? nr->highest - 1 : nr->highest, nr->level_, nr);
                merge_state = t->merge(p->entry[pkx_.p].value, root, depth, lv, nr->highest, nr->level_ + 1, nl, threadInfo);
            }
        } else {
            if (t->root() == nr) {
                kx_.i = 16;
                nr->obsolete = 1;
                threadInfo.getEpoche().markNodeForDeletion(nr, threadInfo);
                nr->unlock();
                return (ret = kx_.i);
            } else {
                nl = search_for_leftsibling(t->root_dp(), nr->highest ? nr->highest - 1 : nr->highest, nr->level_, nr);
                merge_state = t->merge(NULL, NULL, 0, NULL, nr->highest, nr->level_ + 1, nl, threadInfo);
            }
        }

        // Final step for internal node reclamation
        if (merge_state >= 0 && merge_state < 16) {
            nl->next = nr->next;
            clflush((char *)(&nl->next), sizeof(leafnode *), true);
            nr->obsolete = 1;
            threadInfo.getEpoche().markNodeForDeletion(nr, threadInfo);
        } else if (merge_state == 16) {
            kx_.i = 16;
            nr->obsolete = 1;
            threadInfo.getEpoche().markNodeForDeletion(nr, threadInfo);
        }

        if (nl != nr) {
            nr->unlock();
            nl->unlock();
        } else {
            nr->unlock();
        }
    }

    return (ret = kx_.i);
}

void masstree::split(void *left, void *root, uint32_t depth, leafvalue *lv,
        uint64_t key, void *right, uint32_t level, void *child)
{
    leafnode *p;
    key_indexed_position kx_;
    uint64_t oldv;
    leafnode *next;

    if (depth > 0) {
        if (level > reinterpret_cast<leafnode *>(left)->level())
            return ;
        p = reinterpret_cast<leafnode *> (left);
        reinterpret_cast<leafnode *> (root)->unlock();
    } else {
        if (level > reinterpret_cast<leafnode *>(root_)->level())
            return ;
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
        return;
    }

    if (!p->inter_insert(this, root, depth, lv, key, right, kx_, true,
                true, NULL, reinterpret_cast<leafnode *> (child))) {
        split(left, root, depth, lv, key, right, level, child);
    }
}

int masstree::merge(void *left, void *root, uint32_t depth, leafvalue *lv,
        uint64_t key, uint32_t level, void *child, ThreadInfo &threadInfo)
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
            true, NULL, reinterpret_cast<leafnode *> (child), threadInfo);
}

void *masstree::get(uint64_t key, ThreadInfo &threadEpocheInfo)
{
    EpocheGuard epocheGuard(threadEpocheInfo);
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

void *masstree::get(char *key, ThreadInfo &threadEpocheInfo)
{
    EpocheGuard epocheGuard(threadEpocheInfo);
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
                if (snapshot_v == &((LV_PTR(l->value(kx_.p)))->value)) {
                    free(lv);
                    return snapshot_v;
                }
            } else {
                free(lv);
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
            free(lv);
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

int masstree::scan(char *min, int num, leafvalue *buf[], ThreadInfo &threadEpocheInfo)
{
    EpocheGuard epocheGuard(threadEpocheInfo);
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

    free(lv);
    return count;
}

int masstree::scan(uint64_t min, int num, uint64_t *buf, ThreadInfo &threadEpocheInfo)
{
    EpocheGuard epocheGuard(threadEpocheInfo);
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
