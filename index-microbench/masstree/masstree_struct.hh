/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2014 President and Fellows of Harvard College
 * Copyright (c) 2012-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file
 * is legally binding.
 */
#ifndef MASSTREE_STRUCT_HH
#define MASSTREE_STRUCT_HH
#include "masstree.hh"
#include "nodeversion.hh"
#include "stringbag.hh"
#include "mtcounters.hh"
#include "timestamp.hh"
namespace Masstree {

template <typename P>
struct make_nodeversion {
    typedef typename mass::conditional<P::concurrent,
                                       nodeversion,
                                       singlethreaded_nodeversion>::type type;
};

template <typename P>
struct make_prefetcher {
    typedef typename mass::conditional<P::prefetch,
                                       value_prefetcher<typename P::value_type>,
                                       do_nothing>::type type;
};

template <typename P>
class node_base : public make_nodeversion<P>::type {
  public:
    static constexpr bool concurrent = P::concurrent;
    static constexpr int nikey = 1;
    typedef leaf<P> leaf_type;
    typedef internode<P> internode_type;
    typedef node_base<P> base_type;
    typedef typename P::value_type value_type;
    typedef leafvalue<P> leafvalue_type;
    typedef typename P::ikey_type ikey_type;
    typedef key<ikey_type> key_type;
    typedef typename make_nodeversion<P>::type nodeversion_type;
    typedef typename P::threadinfo_type threadinfo;

  //huanchen-static
  typedef leafvalue_static<P> leafvalue_static_type;
  //huanchen-static-multivalue
  typedef leafvalue_static_multivalue<P> leafvalue_static_multivalue_type;

    node_base(bool isleaf)
        : nodeversion_type(isleaf) {
    }

    int size() const {
        if (this->isleaf())
            return static_cast<const leaf_type*>(this)->size();
        else
            return static_cast<const internode_type*>(this)->size();
    }

    inline base_type* parent() const {
        // almost always an internode
        if (this->isleaf())
            return static_cast<const leaf_type*>(this)->parent_;
        else
            return static_cast<const internode_type*>(this)->parent_;
    }
    static inline base_type* parent_for_layer_root(base_type* higher_layer) {
        (void) higher_layer;
        return 0;
    }
    static inline bool parent_exists(base_type* p) {
        return p != 0;
    }
    inline bool has_parent() const {
        return parent_exists(parent());
    }
    inline internode_type* locked_parent(threadinfo& ti) const;
    inline void set_parent(base_type* p) {
        if (this->isleaf())
            static_cast<leaf_type*>(this)->parent_ = p;
        else
            static_cast<internode_type*>(this)->parent_ = p;
    }
    inline base_type* unsplit_ancestor() const {
        base_type* x = const_cast<base_type*>(this), *p;
        while (x->has_split() && (p = x->parent()))
            x = p;
        return x;
    }
    inline leaf_type* leftmost() const {
        base_type* x = unsplit_ancestor();
        while (!x->isleaf()) {
            internode_type* in = static_cast<internode_type*>(x);
            x = in->child_[0];
        }
	//return x;
        return reinterpret_cast<leaf_type*>(x); //huanchen-static
    }

    inline leaf_type* reach_leaf(const key_type& k, nodeversion_type& version,
                                 threadinfo& ti) const;

    void prefetch_full() const {
        for (int i = 0; i < std::min(16 * std::min(P::leaf_width, P::internode_width) + 1, 4 * 64); i += 64)
            ::prefetch((const char *) this + i);
    }

    void print(FILE* f, const char* prefix, int indent, int kdepth);
};

template <typename P>
class internode : public node_base<P> {
  public:
    static constexpr int width = P::internode_width;
    typedef typename node_base<P>::nodeversion_type nodeversion_type;
    typedef key<typename P::ikey_type> key_type;
    typedef typename P::ikey_type ikey_type;
    typedef typename key_bound<width, P::bound_method>::type bound_type;
    typedef typename P::threadinfo_type threadinfo;

    uint8_t nkeys_;
    ikey_type ikey0_[width];
    node_base<P>* child_[width + 1];
    node_base<P>* parent_;
    kvtimestamp_t created_at_[P::debug_level > 0];

    internode()
        : node_base<P>(false), nkeys_(0), parent_() {
    }

    static internode<P>* make(threadinfo& ti) {
        void* ptr = ti.pool_allocate(sizeof(internode<P>),
                                     memtag_masstree_internode);
        internode<P>* n = new(ptr) internode<P>;
        assert(n);
        if (P::debug_level > 0)
            n->created_at_[0] = ti.operation_timestamp();
        return n;
    }

    int size() const {
        return nkeys_;
    }

    key_type get_key(int p) const {
        return key_type(ikey0_[p]);
    }
    ikey_type ikey(int p) const {
        return ikey0_[p];
    }
    inline int stable_last_key_compare(const key_type& k, nodeversion_type v,
                                       threadinfo& ti) const;

    void prefetch() const {
        for (int i = 64; i < std::min(16 * width + 1, 4 * 64); i += 64)
            ::prefetch((const char *) this + i);
    }

    void print(FILE* f, const char* prefix, int indent, int kdepth);

    void deallocate(threadinfo& ti) {
        ti.pool_deallocate(this, sizeof(*this), memtag_masstree_internode);
    }
    void deallocate_rcu(threadinfo& ti) {
        ti.pool_deallocate_rcu(this, sizeof(*this), memtag_masstree_internode);
    }

  private:
    void assign(int p, ikey_type ikey, node_base<P>* child) {
        child->set_parent(this);
        child_[p + 1] = child;
        ikey0_[p] = ikey;
    }

    void shift_from(int p, const internode<P>* x, int xp, int n) {
        masstree_precondition(x != this);
        if (n) {
            memcpy(ikey0_ + p, x->ikey0_ + xp, sizeof(ikey0_[0]) * n);
            memcpy(child_ + p + 1, x->child_ + xp + 1, sizeof(child_[0]) * n);
        }
    }
    void shift_up(int p, int xp, int n) {
        memmove(ikey0_ + p, ikey0_ + xp, sizeof(ikey0_[0]) * n);
        for (node_base<P> **a = child_ + p + n, **b = child_ + xp + n; n; --a, --b, --n)
            *a = *b;
    }
    void shift_down(int p, int xp, int n) {
        memmove(ikey0_ + p, ikey0_ + xp, sizeof(ikey0_[0]) * n);
        for (node_base<P> **a = child_ + p + 1, **b = child_ + xp + 1; n; ++a, ++b, --n)
            *a = *b;
    }

    int split_into(internode<P>* nr, int p, ikey_type ka, node_base<P>* value,
                   ikey_type& split_ikey, int split_type);

    template <typename PP> friend class tcursor;
};

template <typename P>
class leafvalue {
  public:
    typedef typename P::value_type value_type;
    typedef typename make_prefetcher<P>::type prefetcher_type;

    leafvalue() {
    }
    leafvalue(value_type v) {
        u_.v = v;
    }
    leafvalue(node_base<P>* n) {
        u_.x = reinterpret_cast<uintptr_t>(n);
    }

    static leafvalue<P> make_empty() {
        return leafvalue<P>(value_type());
    }

    typedef bool (leafvalue<P>::*unspecified_bool_type)() const;
    operator unspecified_bool_type() const {
        return u_.x ? &leafvalue<P>::empty : 0;
    }
    bool empty() const {
        return !u_.x;
    }

    value_type value() const {
        return u_.v;
    }
    value_type& value() {
        return u_.v;
    }

    node_base<P>* layer() const {
        return reinterpret_cast<node_base<P>*>(u_.x);
    }

    void prefetch(int keylenx) const {
        if (!leaf<P>::keylenx_is_layer(keylenx))
            prefetcher_type()(u_.v);
        else
            u_.n->prefetch_full();
    }

  //huanchen-static
  uintptr_t get_value() {
    return u_.x;
  }

  void set_value(uintptr_t x) {
    u_.x = x;
  }

  private:
    union {
        node_base<P>* n;
        value_type v;
        uintptr_t x;
    } u_;
};

template <typename P>
class leaf : public node_base<P> {
  public:
    static constexpr int width = P::leaf_width;
    typedef typename node_base<P>::nodeversion_type nodeversion_type;
    typedef key<typename P::ikey_type> key_type;
    typedef typename node_base<P>::leafvalue_type leafvalue_type;
    typedef kpermuter<P::leaf_width> permuter_type;
    typedef typename P::ikey_type ikey_type;
    typedef typename key_bound<width, P::bound_method>::type bound_type;
    typedef typename P::threadinfo_type threadinfo;
    typedef stringbag<uint8_t> internal_ksuf_type;
    typedef stringbag<uint16_t> external_ksuf_type;
    static constexpr int layer_keylenx = sizeof(ikey_type) + 1 + 128;

    enum {
        modstate_insert = 0, modstate_remove = 1, modstate_deleted_layer = 2
    };

    int8_t extrasize64_;
    uint8_t modstate_;
    uint8_t keylenx_[width];
    typename permuter_type::storage_type permutation_;
    ikey_type ikey0_[width];
    leafvalue_type lv_[width];
    external_ksuf_type* ksuf_;
    union {
        leaf<P>* ptr;
        uintptr_t x;
    } next_;
    leaf<P>* prev_;
    node_base<P>* parent_;
    kvtimestamp_t node_ts_;
    kvtimestamp_t created_at_[P::debug_level > 0];
    internal_ksuf_type iksuf_[0];

    leaf(size_t sz, kvtimestamp_t node_ts)
        : node_base<P>(true), modstate_(modstate_insert),
          permutation_(permuter_type::make_empty()),
          ksuf_(), parent_(), node_ts_(node_ts), iksuf_{} {
        masstree_precondition(sz % 64 == 0 && sz / 64 < 128);
        extrasize64_ = (int(sz) >> 6) - ((int(sizeof(*this)) + 63) >> 6);
        if (extrasize64_ > 0)
            new((void *)&iksuf_[0]) internal_ksuf_type(width, sz - sizeof(*this));
    }

    static leaf<P>* make(int ksufsize, kvtimestamp_t node_ts, threadinfo& ti) {
        size_t sz = iceil(sizeof(leaf<P>) + std::min(ksufsize, 128), 64);
        void* ptr = ti.pool_allocate(sz, memtag_masstree_leaf);
        leaf<P>* n = new(ptr) leaf<P>(sz, node_ts);
        assert(n);
        if (P::debug_level > 0)
            n->created_at_[0] = ti.operation_timestamp();
        return n;
    }
    static leaf<P>* make_root(int ksufsize, leaf<P>* parent, threadinfo& ti) {
        leaf<P>* n = make(ksufsize, parent ? parent->node_ts_ : 0, ti);
        n->next_.ptr = n->prev_ = 0;
        n->parent_ = node_base<P>::parent_for_layer_root(parent);
        n->mark_root();
        return n;
    }

    static size_t min_allocated_size() {
        return (sizeof(leaf<P>) + 63) & ~size_t(63);
    }
    size_t allocated_size() const {
        int es = (extrasize64_ >= 0 ? extrasize64_ : -extrasize64_ - 1);
        return (sizeof(*this) + es * 64 + 63) & ~size_t(63);
    }
    int size() const {
        return permuter_type::size(permutation_);
    }
    permuter_type permutation() const {
        return permuter_type(permutation_);
    }
    typename nodeversion_type::value_type full_version_value() const {
        static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(permuter_type::size_bits), "not enough bits to add size to version");
        return (this->version_value() << permuter_type::size_bits) + size();
    }
    typename nodeversion_type::value_type full_unlocked_version_value() const {
        static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(permuter_type::size_bits), "not enough bits to add size to version");
        typename node_base<P>::nodeversion_type v(*this);
        if (v.locked())
            // subtlely, unlocked_version_value() is different than v.unlock(); v.version_value() because the latter will add a
            // split bit if we're doing a split. So we do the latter to get the fully correct version.
            v.unlock();
        return (v.version_value() << permuter_type::size_bits) + size();
    }

    using node_base<P>::has_changed;
    bool has_changed(nodeversion_type oldv,
                     typename permuter_type::storage_type oldperm) const {
        return this->has_changed(oldv) || oldperm != permutation_;
    }

    key_type get_key(int p) const {
        int kl = keylenx_[p];
        if (!keylenx_has_ksuf(kl))
            return key_type(ikey0_[p], kl);
        else
            return key_type(ikey0_[p], ksuf(p));
    }
    ikey_type ikey(int p) const {
        return ikey0_[p];
    }
    ikey_type ikey_bound() const {
        return ikey0_[0];
    }
    int ikeylen(int p) const {
        return keylenx_ikeylen(keylenx_[p]);
    }
    inline int stable_last_key_compare(const key_type& k, nodeversion_type v,
                                       threadinfo& ti) const;

    inline leaf<P>* advance_to_key(const key_type& k, nodeversion_type& version,
                                   threadinfo& ti) const;

    static int keylenx_ikeylen(int keylenx) {
        return keylenx & 31;
    }
    static bool keylenx_is_layer(int keylenx) {
        return keylenx > 63;
    }
    static bool keylenx_has_ksuf(int keylenx) {
        return keylenx == (int) sizeof(ikey_type) + 1;
    }

    bool is_layer(int p) const {
        return keylenx_is_layer(keylenx_[p]);
    }
    bool has_ksuf(int p) const {
        return keylenx_has_ksuf(keylenx_[p]);
    }
    Str ksuf(int p) const {
        masstree_precondition(has_ksuf(p));
        return ksuf_ ? ksuf_->get(p) : iksuf_[0].get(p);
    }
    bool ksuf_equals(int p, const key_type& ka) {
        // Precondition: keylenx_[p] == ka.ikeylen() && ikey0_[p] == ka.ikey()
        return ksuf_equals(p, ka, keylenx_[p]);
    }
    bool ksuf_equals(int p, const key_type& ka, int keylenx) {
        // Precondition: keylenx_[p] == ka.ikeylen() && ikey0_[p] == ka.ikey()
        return !keylenx_has_ksuf(keylenx)
            || (!ksuf_ && iksuf_[0].equals_sloppy(p, ka.suffix()))
            || (ksuf_ && ksuf_->equals_sloppy(p, ka.suffix()));
    }
    int ksuf_compare(int p, const key_type& ka) {
        if (!has_ksuf(p))
            return 0;
        else if (!ksuf_)
            return iksuf_[0].compare(p, ka.suffix());
        else
            return ksuf_->compare(p, ka.suffix());
    }

    size_t ksuf_used_capacity() const {
        if (ksuf_)
            return ksuf_->used_capacity();
        else if (extrasize64_ > 0)
            return iksuf_[0].used_capacity();
        else
            return 0;
    }
    size_t ksuf_capacity() const {
        if (ksuf_)
            return ksuf_->capacity();
        else if (extrasize64_ > 0)
            return iksuf_[0].capacity();
        else
            return 0;
    }
    bool ksuf_external() const {
        return ksuf_;
    }
    Str ksuf_storage(int p) const {
        if (ksuf_)
            return ksuf_->get(p);
        else if (extrasize64_ > 0)
            return iksuf_[0].get(p);
        else
            return Str();
    }

    bool deleted_layer() const {
        return modstate_ == modstate_deleted_layer;
    }

    void prefetch() const {
        for (int i = 64; i < std::min(16 * width + 1, 4 * 64); i += 64)
            ::prefetch((const char *) this + i);
        if (extrasize64_ > 0)
            ::prefetch((const char *) &iksuf_[0]);
        else if (extrasize64_ < 0) {
            ::prefetch((const char *) ksuf_);
            ::prefetch((const char *) ksuf_ + CACHE_LINE_SIZE);
        }
    }

    void print(FILE* f, const char* prefix, int indent, int kdepth);

    leaf<P>* safe_next() const {
        return reinterpret_cast<leaf<P>*>(next_.x & ~(uintptr_t) 1);
    }

    void deallocate(threadinfo& ti) {
        if (ksuf_)
            ti.deallocate(ksuf_, ksuf_->capacity(),
                          memtag_masstree_ksuffixes);
        if (extrasize64_ != 0)
            iksuf_[0].~stringbag();
        ti.pool_deallocate(this, allocated_size(), memtag_masstree_leaf);
    }
    void deallocate_rcu(threadinfo& ti) {
        if (ksuf_)
            ti.deallocate_rcu(ksuf_, ksuf_->capacity(),
                              memtag_masstree_ksuffixes);
        ti.pool_deallocate_rcu(this, allocated_size(), memtag_masstree_leaf);
    }

  private:
    inline void mark_deleted_layer() {
        modstate_ = modstate_deleted_layer;
    }

    inline void assign(int p, const key_type& ka, threadinfo& ti) {
        lv_[p] = leafvalue_type::make_empty();
        if (ka.has_suffix())
            assign_ksuf(p, ka.suffix(), false, ti);
        ikey0_[p] = ka.ikey();
        keylenx_[p] = ka.ikeylen();
    }
    inline void assign_initialize(int p, const key_type& ka, threadinfo& ti) {
        lv_[p] = leafvalue_type::make_empty();
        if (ka.has_suffix())
            assign_ksuf(p, ka.suffix(), true, ti);
        ikey0_[p] = ka.ikey();
        keylenx_[p] = ka.ikeylen();
    }
    inline void assign_initialize(int p, leaf<P>* x, int xp, threadinfo& ti) {
        lv_[p] = x->lv_[xp];
        if (x->has_ksuf(xp))
            assign_ksuf(p, x->ksuf(xp), true, ti);
        ikey0_[p] = x->ikey0_[xp];
        keylenx_[p] = x->keylenx_[xp];
    }
    inline void assign_initialize_for_layer(int p, const key_type& ka) {
        assert(ka.has_suffix());
        ikey0_[p] = ka.ikey();
        keylenx_[p] = layer_keylenx;
    }
    void assign_ksuf(int p, Str s, bool initializing, threadinfo& ti);

    inline ikey_type ikey_after_insert(const permuter_type& perm, int i,
                                       const key_type& ka, int ka_i) const;
    int split_into(leaf<P>* nr, int p, const key_type& ka, ikey_type& split_ikey,
                   threadinfo& ti);

    template <typename PP> friend class tcursor;
};


template <typename P>
void basic_table<P>::initialize(threadinfo& ti) {
    masstree_precondition(!root_);
    root_ = node_type::leaf_type::make_root(0, 0, ti);
    static_root_ = NULL;
}


/** @brief Return this node's parent in locked state.
    @pre this->locked()
    @post this->parent() == result && (!result || result->locked()) */
template <typename P>
internode<P>* node_base<P>::locked_parent(threadinfo& ti) const
{
    node_base<P>* p;
    masstree_precondition(!this->concurrent || this->locked());
    while (1) {
        p = this->parent();
        if (!node_base<P>::parent_exists(p))
            break;
        nodeversion_type pv = p->lock(*p, ti.lock_fence(tc_internode_lock));
        if (p == this->parent()) {
            masstree_invariant(!p->isleaf());
            break;
        }
        p->unlock(pv);
        relax_fence();
    }
    return static_cast<internode<P>*>(p);
}


/** @brief Return the result of key_compare(k, LAST KEY IN NODE).

    Reruns the comparison until a stable comparison is obtained. */
template <typename P>
inline int
internode<P>::stable_last_key_compare(const key_type& k, nodeversion_type v,
                                      threadinfo& ti) const
{
    while (1) {
        int cmp = key_compare(k, *this, size() - 1);
        if (likely(!this->has_changed(v)))
            return cmp;
        v = this->stable_annotated(ti.stable_fence());
    }
}

template <typename P>
inline int
leaf<P>::stable_last_key_compare(const key_type& k, nodeversion_type v,
                                 threadinfo& ti) const
{
    while (1) {
        typename leaf<P>::permuter_type perm(permutation_);
        int p = perm[perm.size() - 1];
        int cmp = key_compare(k, *this, p);
        if (likely(!this->has_changed(v)))
            return cmp;
        v = this->stable_annotated(ti.stable_fence());
    }
}


/** @brief Return the leaf in this tree layer responsible for @a ka.

    Returns a stable leaf. Sets @a version to the stable version. */
template <typename P>
inline leaf<P>* node_base<P>::reach_leaf(const key_type& ka,
                                         nodeversion_type& version,
                                         threadinfo& ti) const
{
    const node_base<P> *n[2];
    typename node_base<P>::nodeversion_type v[2];
    bool sense;

    // Get a non-stale root.
    // Detect staleness by checking whether n has ever split.
    // The true root has never split.
 retry:
    sense = false;
    n[sense] = this;
    while (1) {
        v[sense] = n[sense]->stable_annotated(ti.stable_fence());
        if (!v[sense].has_split())
            break;
        ti.mark(tc_root_retry);
        n[sense] = n[sense]->unsplit_ancestor();
    }

    // Loop over internal nodes.
    while (!v[sense].isleaf()) {
        const internode<P> *in = static_cast<const internode<P> *>(n[sense]);
        in->prefetch();
        int kp = internode<P>::bound_type::upper(ka, *in);
        n[!sense] = in->child_[kp];
        if (!n[!sense])
            goto retry;
        v[!sense] = n[!sense]->stable_annotated(ti.stable_fence());

        if (likely(!in->has_changed(v[sense]))) {
            sense = !sense;
            continue;
        }

        typename node_base<P>::nodeversion_type oldv = v[sense];
        v[sense] = in->stable_annotated(ti.stable_fence());
        if (oldv.has_split(v[sense])
            && in->stable_last_key_compare(ka, v[sense], ti) > 0) {
            ti.mark(tc_root_retry);
            goto retry;
        } else
            ti.mark(tc_internode_retry);
    }

    version = v[sense];
    return const_cast<leaf<P> *>(static_cast<const leaf<P> *>(n[sense]));
}

/** @brief Return the leaf at or after *this responsible for @a ka.
    @pre *this was responsible for @a ka at version @a v

    Checks whether *this has split since version @a v. If it has split, then
    advances through the leaves using the B^link-tree pointers and returns
    the relevant leaf, setting @a v to the stable version for that leaf. */
template <typename P>
leaf<P>* leaf<P>::advance_to_key(const key_type& ka, nodeversion_type& v,
                                 threadinfo& ti) const
{
    const leaf<P>* n = this;
    nodeversion_type oldv = v;
    v = n->stable_annotated(ti.stable_fence());
    if (v.has_split(oldv)
        && n->stable_last_key_compare(ka, v, ti) > 0) {
        leaf<P> *next;
        ti.mark(tc_leaf_walk);
        while (likely(!v.deleted()) && (next = n->safe_next())
               && compare(ka.ikey(), next->ikey_bound()) >= 0) {
            n = next;
            v = n->stable_annotated(ti.stable_fence());
        }
    }
    return const_cast<leaf<P>*>(n);
}


/** @brief Assign position @a p's keysuffix to @a s.

    This version of assign_ksuf() is called when @a s might not fit into
    the current keysuffix container. It may allocate a new container, copying
    suffixes over.

    The @a initializing parameter determines which suffixes are copied. If @a
    initializing is false, then this is an insertion into a live node. The
    live node's permutation indicates which keysuffixes are active, and only
    active suffixes are copied. If @a initializing is true, then this
    assignment is part of the initialization process for a new node. The
    permutation might not be set up yet. In this case, it is assumed that key
    positions [0,p) are ready: keysuffixes in that range are copied. In either
    case, the key at position p is NOT copied; it is assigned to @a s. */
template <typename P>
void leaf<P>::assign_ksuf(int p, Str s, bool initializing, threadinfo& ti) {
    if ((ksuf_ && ksuf_->assign(p, s))
        || (extrasize64_ > 0 && iksuf_[0].assign(p, s)))
        return;

    external_ksuf_type* oksuf = ksuf_;

    permuter_type perm(permutation_);
    int n = initializing ? p : perm.size();

    size_t csz = 0;
    for (int i = 0; i < n; ++i) {
        int mp = initializing ? i : perm[i];
        if (mp != p && has_ksuf(mp))
            csz += ksuf(mp).len;
    }

    size_t sz = iceil_log2(external_ksuf_type::safe_size(width, csz + s.len));
    if (oksuf)
        sz = std::max(sz, oksuf->capacity());

    void* ptr = ti.allocate(sz, memtag_masstree_ksuffixes);
    ti.stringbag_alloc += sz; //huanchen-stats
    external_ksuf_type* nksuf = new(ptr) external_ksuf_type(width, sz);
    for (int i = 0; i < n; ++i) {
        int mp = initializing ? i : perm[i];
        if (mp != p && has_ksuf(mp)) {
            bool ok = nksuf->assign(mp, ksuf(mp));
            assert(ok); (void) ok;
        }
    }
    bool ok = nksuf->assign(p, s);
    assert(ok); (void) ok;
    fence();

    // removed ksufs aren't copied to the new ksuf, but observers
    // might need them. We ensure that observers must retry by
    // ensuring that we are not currently in the remove state.
    // State transitions are accompanied by mark_insert() so observers
    // will retry.
    masstree_invariant(modstate_ != modstate_remove);

    ksuf_ = nksuf;
    fence();

    if (extrasize64_ >= 0)      // now the new ksuf_ installed, mark old dead
        extrasize64_ = -extrasize64_ - 1;

    if (oksuf)
        ti.deallocate_rcu(oksuf, oksuf->capacity(),
                          memtag_masstree_ksuffixes);

}

template <typename P>
inline basic_table<P>::basic_table()
    : root_(0) {
}

template <typename P>
inline node_base<P>* basic_table<P>::root() const {
    return root_;
}

template <typename P>
inline node_base<P>* basic_table<P>::fix_root() {
    node_base<P>* root = root_;
    if (unlikely(root->has_split())) {
        node_base<P>* old_root = root;
        root = root->unsplit_ancestor();
        (void) cmpxchg(&root_, old_root, root);
    }
    return root;
}

//huanchen-static
template <typename P>
inline void basic_table<P>::set_static_root(node_type *staticRoot) {
  static_root_ = staticRoot;
}

template <typename P>
inline node_base<P>* basic_table<P>::static_root() const {
  return static_root_;
}

//huanchen-static
//**********************************************************************************
// leafvalue_static
//**********************************************************************************
template <typename P>
class leafvalue_static {
  public:
    leafvalue_static() {
    }
    leafvalue_static(const char *nv) {
      for (int i = 0; i < 8; i++)
	u_.v[i] = nv[i];
    }
    leafvalue_static(node_base<P>* n) {
      u_.x = reinterpret_cast<uintptr_t>(n);
    }

    bool empty() const {
      return !u_.x;
    }

    const char* value() const {
      //std::cout << "struct: " << (void*)u_.v << "\t" << u_.v << "\n";
      return (const char*)u_.v;
    }

    node_base<P>* layer() const {
      return reinterpret_cast<node_base<P>*>(u_.x);
    }

    uintptr_t get_value() {
      return u_.x;
    }

    void set_value(uintptr_t x) {
      u_.x = x;
    }

  private:
    union {
      char v[8];
      uintptr_t x;
    } u_;
};


//huanchen-static
//**********************************************************************************
// massnode
//**********************************************************************************
template <typename P>
class massnode : public node_base<P> {
public:
  typedef typename P::ikey_type ikey_type;
  typedef key<typename P::ikey_type> key_type;
  typedef typename node_base<P>::leafvalue_type leafvalue_type;
  typedef typename node_base<P>::leafvalue_static_type leafvalue_static_type;
  typedef typename P::threadinfo_type threadinfo;

  uint32_t nkeys_;
  uint32_t size_;
  uint8_t hasKsuf_;

  massnode (uint32_t nkeys, uint32_t size, uint8_t hasKsuf)
    :node_base<P>(false), nkeys_(nkeys), size_(size), hasKsuf_(hasKsuf) {

  }

  static massnode<P> *make (size_t ksufSize, bool has_ksuf, uint32_t nkeys, threadinfo& ti) {
    size_t sz;
    if (has_ksuf) {
      sz = sizeof(massnode<P>) 
	+ sizeof(uint8_t) * nkeys
	+ sizeof(ikey_type) * nkeys
	+ sizeof(leafvalue_static_type) * nkeys
	+ sizeof(uint32_t) * (nkeys + 1)
	+ ksufSize;
    }
    else {
      sz = sizeof(massnode<P>) 
	+ sizeof(uint8_t) * nkeys
	+ sizeof(ikey_type) * nkeys
	+ sizeof(leafvalue_static_type) * nkeys;
    }
    void *ptr = ti.allocate(sz, memtag_masstree_leaf);
    massnode<P>* n;
    if (has_ksuf)
      n = new(ptr) massnode<P>(nkeys, sz, (uint8_t)1);
    else
      n = new(ptr) massnode<P>(nkeys, sz, (uint8_t)0);
    return n;
  }

  massnode<P> *resize (size_t sz, threadinfo &ti) {
    return (massnode<P>*)ti.reallocate((void*)(this), size_, sz);
  }

  uint8_t *get_keylenx() {
    return (uint8_t*)((char*)this + sizeof(massnode<P>));
  }


  ikey_type *get_ikey0() {
    return (ikey_type*)((char*)this + sizeof(massnode<P>) 
			+ sizeof(uint8_t) * nkeys_);
  }

  leafvalue_static_type *get_lv() {
    return (leafvalue_static_type*)((char*)this + sizeof(massnode<P>) 
			     + sizeof(uint8_t) * nkeys_
			     + sizeof(ikey_type) * nkeys_);
  }

  //suffix========================================================
  uint32_t *get_ksuf_pos_offset() {
    return (uint32_t*)((char*)this + sizeof(massnode<P>) 
		       + sizeof(uint8_t) * nkeys_
		       + sizeof(ikey_type) * nkeys_
		       + sizeof(leafvalue_static_type) * nkeys_);
  }

  char *get_ksuf() {
    if (hasKsuf_ == 1) {
      return (char*)((char*)this + sizeof(massnode<P>) 
		     + sizeof(uint8_t) * nkeys_
		     + sizeof(ikey_type) * nkeys_
		     + sizeof(leafvalue_static_type) * nkeys_
		     + sizeof(uint32_t) * (nkeys_ + 1));
    }
    else {
      return (char*)((char*)this + sizeof(massnode<P>) 
		     + sizeof(uint8_t) * nkeys_
		     + sizeof(ikey_type) * nkeys_
		     + sizeof(leafvalue_static_type) * nkeys_);
    }
  }

  //=============================================================

  int subtree_size() {
    if (this ==NULL)
      return 0;
    std::vector<massnode<P>*> node_trace;
    node_trace.push_back(this);
    int cur_pos = 0;
    int subtree_size = 0;
    while (cur_pos < node_trace.size()) {
      massnode<P>* cur_node = node_trace[cur_pos];
      subtree_size += cur_node->allocated_size();
      for (int i = 0; i < cur_node->size(); i++)
	if (keylenx_is_layer(cur_node->ikeylen(i)))
	  node_trace.push_back(static_cast<massnode<P>*>(cur_node->lv(i).layer()));
      cur_pos++;
    }
    return subtree_size;
  }

  size_t allocated_size() const {
    return size_;
  }

  uint32_t size() const {
    return nkeys_;
  }

  void set_allocated_size(size_t sz) {
    size_ = sz;
  }

  void set_size(uint32_t nkeys) {
    nkeys_ = nkeys;
  }

  bool has_ksuf() {
    return (hasKsuf_ == 1);
  }

  void set_has_ksuf(uint8_t hasKsuf) {
    hasKsuf_ = hasKsuf;
  }

  uint8_t ikeylen(int p) {
    return get_keylenx()[p];
  }

  void set_ikeylen(int p, uint8_t keylen) {
    get_keylenx()[p] = keylen;
  }

  ikey_type ikey(int p) {
    return get_ikey0()[p];
  }

  void set_ikey(int p, ikey_type ikey) {
    get_ikey0()[p] = ikey;
  }

  leafvalue_static_type lv(int p) {
    return get_lv()[p];
  }

  void set_lv(int p, leafvalue_static_type lv) {
    get_lv()[p] = lv;
  }

  key_type get_key(int p) {
    int kl = get_keylenx()[p];
    if (!keylenx_has_ksuf(kl))
      return key_type(get_ikey0()[p], kl);
    else
      return key_type(get_ikey0()[p], ksuf(p));
  }

  static bool keylenx_is_layer(int keylenx) {
    return keylenx > 63;
  }

  void invalidate(int p) {
    set_ikeylen(p, (uint8_t)0);
  }

  bool isValid(int p) {
    return (ikeylen(p) != 0);
  }

  //suffix================================================================================
  uint32_t ksuf_offset(int p) {
    if (hasKsuf_ == 1)
      return get_ksuf_pos_offset()[p];
    else
      return 0;
  }

  void set_ksuf_offset(int p, uint32_t offset) {
    if (hasKsuf_ == 1)
      get_ksuf_pos_offset()[p] = offset;
  }

  uint32_t ksuflen(int p) {
    if (hasKsuf_ == 1)
      return get_ksuf_pos_offset()[p+1] - get_ksuf_pos_offset()[p];
    else
      return 0;
  }

  char *ksufpos(int p) {
    if (hasKsuf_ == 1)
      return (char*)(get_ksuf() + get_ksuf_pos_offset()[p]);
    else
      return (char*)(get_ksuf());
  }

  bool has_ksuf(int p) {
    if (hasKsuf_ == 1)
      return get_ksuf_pos_offset()[p] != 0;
    else
      return false;
  }

  Str ksuf(int p) {
    return Str(ksufpos(p), ksuflen(p));
  }

  static int keylenx_ikeylen(int keylenx) {
    return keylenx & 31;
  }

  static uint8_t keylenx_ikeylen(uint8_t keylenx) {
    return keylenx & (uint8_t)31;
  }

  static bool keylenx_has_ksuf(int keylenx) {
    return keylenx == (int)sizeof(ikey_type) + 1;
  }

  bool ksuf_equals(int p, const key_type &ka, int keylenx) {
    return !keylenx_has_ksuf(keylenx) || equals_sloppy(p, ka);
  }

  bool equals_sloppy(int p, const key_type &ka) {
    Str kp_str = ksuf(p);
    if (kp_str.len != ka.suffix().len)
      return false;
    return string_slice<uintptr_t>::equals_sloppy(kp_str.s, ka.suffix().s, ka.suffix().len);
  }
  //========================================================================================

  void deallocate(threadinfo &ti) {
    ti.deallocate(this, size_, memtag_masstree_leaf);
  }

  void prefetch(int m) {
    ::prefetch((const char*)get_ikey0() + sizeof(ikey_type) * m);
  }

private:
  template <typename PP> friend class tcursor;

};



//huanchen-static-multivalue
//**********************************************************************************
// static_multivalue_header
//**********************************************************************************
struct static_multivalue_header {
  uint32_t pos_offset;
  uint32_t len;
};

//huanchen-static-multivalue
//**********************************************************************************
// leafvalue_static_multivalue
//**********************************************************************************
template <typename P>
class leafvalue_static_multivalue {
  public:
    leafvalue_static_multivalue() {
    }
    leafvalue_static_multivalue(static_multivalue_header nv) {
      u_.v.pos_offset = nv.pos_offset;
      u_.v.len = nv.len;
    }
    leafvalue_static_multivalue(uint32_t pos_offset, uint32_t len) {
      u_.v.pos_offset = pos_offset;
      u_.v.len = len;
    }
    leafvalue_static_multivalue(node_base<P>* n) {
      u_.x = reinterpret_cast<uintptr_t>(n);
    }

    bool empty() const {
      return !u_.x;
    }

    static_multivalue_header value() const {
      return u_.v;
    }

    uint32_t value_pos_offset() const {
      return u_.v.pos_offset;
    }

    uint32_t value_len() const {
      return u_.v.len;
    }

    node_base<P>* layer() const {
      return reinterpret_cast<node_base<P>*>(u_.x);
    }

    uintptr_t get_value() {
      return u_.x;
    }

    void set_value(uintptr_t x) {
      u_.x = x;
    }

  private:
    union {
      static_multivalue_header v;
      uintptr_t x;
    } u_;
};



//huanchen-static-multivalue
//**********************************************************************************
// massnode_multivalue
//**********************************************************************************
template <typename P>
class massnode_multivalue : public node_base<P> {
public:
  typedef typename P::ikey_type ikey_type;
  typedef key<typename P::ikey_type> key_type;
  typedef typename node_base<P>::leafvalue_type leafvalue_type;
  typedef typename node_base<P>::leafvalue_static_multivalue_type leafvalue_static_multivalue_type;
  typedef typename P::threadinfo_type threadinfo;

  uint32_t nkeys_;
  uint32_t size_;
  uint32_t vsize_;

  massnode_multivalue (uint32_t nkeys, uint32_t size, uint32_t valueSize)
    :node_base<P>(false), nkeys_(nkeys), size_(size), vsize_(valueSize) {

  }

  static massnode_multivalue<P> *make (size_t ksufSize, uint32_t valueSize, uint32_t nkeys, threadinfo& ti) {
    size_t sz = sizeof(massnode_multivalue<P>) 
      + sizeof(uint8_t) * nkeys
      + sizeof(ikey_type) * nkeys
      + sizeof(leafvalue_static_multivalue_type) * nkeys
      + (size_t)valueSize
      + sizeof(uint32_t) * (nkeys + 1)
      + ksufSize;
    void *ptr = ti.allocate(sz, memtag_masstree_leaf);
    massnode_multivalue<P> *n = new(ptr) massnode_multivalue<P>(nkeys, sz, valueSize);
    return n;
  }

  massnode_multivalue<P> *resize (size_t sz, threadinfo &ti) {
    return (massnode_multivalue<P>*)ti.reallocate((void*)(this), size_, sz);
  }

  uint8_t *get_keylenx() {
    return (uint8_t*)((char*)this + sizeof(massnode_multivalue<P>));
  }

  ikey_type *get_ikey0() {
    return (ikey_type*)((char*)this + sizeof(massnode_multivalue<P>) 
			+ sizeof(uint8_t) * nkeys_);
  }

  leafvalue_static_multivalue_type *get_lv() {
    return (leafvalue_static_multivalue_type*)((char*)this + sizeof(massnode_multivalue<P>) 
					       + sizeof(uint8_t) * nkeys_
					       + sizeof(ikey_type) * nkeys_);
  }

  char *get_value() {
    return (char*)((char*)this + sizeof(massnode_multivalue<P>) 
		   + sizeof(uint8_t) * nkeys_
		   + sizeof(ikey_type) * nkeys_
		   + sizeof(leafvalue_static_multivalue_type) * nkeys_);
  }

  //suffix========================================================
  uint32_t *get_ksuf_pos_offset() {
    return (uint32_t*)((char*)this + sizeof(massnode_multivalue<P>) 
		       + sizeof(uint8_t) * nkeys_
		       + sizeof(ikey_type) * nkeys_
		       + sizeof(leafvalue_static_multivalue_type) * nkeys_
		       + vsize_);
  }

  char *get_ksuf() {
    return (char*)((char*)this + sizeof(massnode_multivalue<P>) 
		   + sizeof(uint8_t) * nkeys_
		   + sizeof(ikey_type) * nkeys_
		   + sizeof(leafvalue_static_multivalue_type) * nkeys_
		   + vsize_
		   + sizeof(uint32_t) * (nkeys_ + 1));
  }
  //=============================================================

  void printSMT() {
    if (this == NULL)
      return;
    std::vector<massnode_multivalue<P>*> node_trace;
    node_trace.push_back(this);
    int cur_pos = 0;
    std::cout << "###############\n";
    while (cur_pos < node_trace.size()) {
      massnode_multivalue<P>* cur_node = node_trace[cur_pos];
      std::cout << "node # = " << cur_pos << "\n";
      std::cout << "nkeys = " << cur_node->size() << "\n";
      std::cout << "size = " << cur_node->allocated_size() << "\n";
      std::cout << "value size = " << cur_node->value_size() << "\n";
      int num_child = 0;
      for (int i = 0; i < cur_node->size(); i++) {
	if (keylenx_is_layer(cur_node->ikeylen(i))) {
	  node_trace.push_back(static_cast<massnode_multivalue<P>*>(cur_node->lv(i).layer()));
	  num_child++;
	}
      }
      //std::cout << "num_child = " << num_child << "\n";

      cur_pos++;
    }
    std::cout << "###############\n";
  }

  int subtree_size() {
    if (this ==NULL)
      return 0;
    std::vector<massnode_multivalue<P>*> node_trace;
    node_trace.push_back(this);
    int cur_pos = 0;
    int subtree_size = 0;
    while (cur_pos < node_trace.size()) {
      massnode_multivalue<P>* cur_node = node_trace[cur_pos];
      subtree_size += cur_node->allocated_size();
      for (int i = 0; i < cur_node->size(); i++) {
	if (keylenx_is_layer(cur_node->ikeylen(i)))
	  node_trace.push_back(static_cast<massnode_multivalue<P>*>(cur_node->lv(i).layer()));
      }
      cur_pos++;
    }
    return subtree_size;
  }

  int subtree_value_size() {
    if (this ==NULL)
      return 0;
    std::vector<massnode_multivalue<P>*> node_trace;
    node_trace.push_back(this);
    int cur_pos = 0;
    int subtree_value_size = 0;
    while (cur_pos < node_trace.size()) {
      massnode_multivalue<P>* cur_node = node_trace[cur_pos];
      subtree_value_size += cur_node->value_size();
      for (int i = 0; i < cur_node->size(); i++) {
	if (keylenx_is_layer(cur_node->ikeylen(i)))
	  node_trace.push_back(static_cast<massnode_multivalue<P>*>(cur_node->lv(i).layer()));
      }
      cur_pos++;
    }
    return subtree_value_size;
  }

  size_t allocated_size() const {
    return size_;
  }

  uint32_t size() const {
    return nkeys_;
  }

  uint32_t value_size() const {
    return vsize_;
  }

  void set_allocated_size(size_t sz) {
    size_ = sz;
  }

  void set_size(uint32_t nkeys) {
    nkeys_ = nkeys;
  }

  void set_value_size(uint32_t vsz) {
    vsize_ = vsz;
  }

  uint8_t ikeylen(int p) {
    return get_keylenx()[p];
  }

  void set_ikeylen(int p, uint8_t keylen) {
    get_keylenx()[p] = keylen;
  }

  ikey_type ikey(int p) {
    return get_ikey0()[p];
  }

  void set_ikey(int p, ikey_type ikey) {
    get_ikey0()[p] = ikey;
  }

  leafvalue_static_multivalue_type lv(int p) {
    return get_lv()[p];
  }

  void set_lv(int p, leafvalue_static_multivalue_type lv) {
    get_lv()[p] = lv;
  }

  uint32_t valuelen(int p) {
    return get_lv()[p].value_len();
  }

  char *valuepos(int p) {
    return (char*)(get_value() + get_lv()[p].value_pos_offset());
  }

  Str value(int p) {
    return Str(valuepos(p), valuelen(p));
  }

  void printValue(int p) {
    std::cout << "value(" << p << ") = ";
    for (int i = 0; i < valuelen(p); i++)
      std::cout << (int)(valuepos(p)[i]) << " ";
    std::cout << "\n";
  }

  key_type get_key(int p) {
    int kl = get_keylenx()[p];
    if (!keylenx_has_ksuf(kl))
      return key_type(get_ikey0()[p], kl);
    else
      return key_type(get_ikey0()[p], ksuf(p));
  }

  static bool keylenx_is_layer(int keylenx) {
    return keylenx > 63;
  }

  void invalidate(int p) {
    set_ikeylen(p, (uint8_t)0);
  }

  bool isValid(int p) {
    return (ikeylen(p) != 0);
  }

  //suffix================================================================================
  uint32_t ksuf_offset(int p) {
    return get_ksuf_pos_offset()[p];
  }

  void set_ksuf_offset(int p, uint32_t offset) {
    get_ksuf_pos_offset()[p] = offset;
  }

  uint32_t ksuflen(int p) {
    return get_ksuf_pos_offset()[p+1] - get_ksuf_pos_offset()[p];
  }

  char *ksufpos(int p) {
    return (char*)(get_ksuf() + get_ksuf_pos_offset()[p]);
  }

  bool has_ksuf(int p) {
    return get_ksuf_pos_offset()[p] != 0;
  }

  Str ksuf(int p) {
    return Str(ksufpos(p), ksuflen(p));
  }

  static int keylenx_ikeylen(int keylenx) {
    return keylenx & 31;
  }

  static uint8_t keylenx_ikeylen(uint8_t keylenx) {
    return keylenx & (uint8_t)31;
  }

  static bool keylenx_has_ksuf(int keylenx) {
    return keylenx == (int)sizeof(ikey_type) + 1;
  }

  bool ksuf_equals(int p, const key_type &ka, int keylenx) {
    return !keylenx_has_ksuf(keylenx) || equals_sloppy(p, ka);
  }

  bool equals_sloppy(int p, const key_type &ka) {
    Str kp_str = ksuf(p);
    if (kp_str.len != ka.suffix().len)
      return false;
    return string_slice<uintptr_t>::equals_sloppy(kp_str.s, ka.suffix().s, ka.suffix().len);
  }
  //========================================================================================

  void deallocate(threadinfo &ti) {
    ti.deallocate(this, size_, memtag_masstree_leaf);
  }

  void prefetch(int m) {
    ::prefetch((const char*)get_ikey0() + sizeof(ikey_type) * m);
  }

private:
  template <typename PP> friend class tcursor;

};



//huanchen-static-dynamicvalue
//**********************************************************************************
// massnode_dynamicvalue
//**********************************************************************************
template <typename P>
class massnode_dynamicvalue : public node_base<P> {
public:
  typedef typename P::ikey_type ikey_type;
  typedef key<typename P::ikey_type> key_type;
  typedef typename node_base<P>::leafvalue_type leafvalue_type;
  typedef typename P::threadinfo_type threadinfo;

  uint32_t nkeys_;
  uint32_t size_;

  massnode_dynamicvalue (uint32_t nkeys, uint32_t size)
    :node_base<P>(false), nkeys_(nkeys), size_(size) {

  }

  static massnode_dynamicvalue<P> *make (size_t ksufSize, uint32_t nkeys, threadinfo& ti) {
    size_t sz = sizeof(massnode_dynamicvalue<P>) 
      + sizeof(uint8_t) * nkeys
      + sizeof(ikey_type) * nkeys
      + sizeof(leafvalue_type) * nkeys
      + sizeof(uint32_t) * (nkeys + 1)
      + ksufSize;
    void *ptr = ti.allocate(sz, memtag_masstree_leaf);
    massnode_dynamicvalue<P> *n = new(ptr) massnode_dynamicvalue<P>(nkeys, sz);
    return n;
  }

  massnode_dynamicvalue<P> *resize (size_t sz, threadinfo &ti) {
    return (massnode_dynamicvalue<P>*)ti.reallocate((void*)(this), size_, sz);
  }

  uint8_t *get_keylenx() {
    return (uint8_t*)((char*)this + sizeof(massnode_dynamicvalue<P>));
  }

  ikey_type *get_ikey0() {
    return (ikey_type*)((char*)this + sizeof(massnode_dynamicvalue<P>) 
			+ sizeof(uint8_t) * nkeys_);
  }

  leafvalue_type *get_lv() {
    return (leafvalue_type*)((char*)this + sizeof(massnode_dynamicvalue<P>) 
			     + sizeof(uint8_t) * nkeys_
			     + sizeof(ikey_type) * nkeys_);
  }

  //suffix========================================================
  uint32_t *get_ksuf_pos_offset() {
    return (uint32_t*)((char*)this + sizeof(massnode_dynamicvalue<P>) 
		       + sizeof(uint8_t) * nkeys_
		       + sizeof(ikey_type) * nkeys_
		       + sizeof(leafvalue_type) * nkeys_);
  }

  char *get_ksuf() {
    return (char*)((char*)this + sizeof(massnode_dynamicvalue<P>) 
		   + sizeof(uint8_t) * nkeys_
		   + sizeof(ikey_type) * nkeys_
		   + sizeof(leafvalue_type) * nkeys_
		   + sizeof(uint32_t) * (nkeys_ + 1));
  }
  //=============================================================

  void printSMT() {
    if (this == NULL)
      return;
    std::vector<massnode_dynamicvalue<P>*> node_trace;
    node_trace.push_back(this);
    int cur_pos = 0;
    std::cout << "###############\n";
    while (cur_pos < node_trace.size()) {
      massnode_dynamicvalue<P>* cur_node = node_trace[cur_pos];
      std::cout << "node # = " << cur_pos << "\n";
      std::cout << "nkeys = " << cur_node->size() << "\n";
      std::cout << "size = " << cur_node->allocated_size() << "\n";
      int num_child = 0;
      for (int i = 0; i < cur_node->size(); i++) {
	if (keylenx_is_layer(cur_node->ikeylen(i))) {
	  node_trace.push_back(static_cast<massnode_dynamicvalue<P>*>(cur_node->lv(i).layer()));
	  num_child++;
	}
      }
      //std::cout << "num_child = " << num_child << "\n";

      cur_pos++;
    }
    std::cout << "###############\n";
  }


  int subtree_size() {
    if (this ==NULL)
      return 0;
    std::vector<massnode_dynamicvalue<P>*> node_trace;
    node_trace.push_back(this);
    int cur_pos = 0;
    int subtree_size = 0;
    while (cur_pos < node_trace.size()) {
      massnode_dynamicvalue<P>* cur_node = node_trace[cur_pos];
      subtree_size += cur_node->allocated_size();
      for (int i = 0; i < cur_node->size(); i++) {
	if (keylenx_is_layer(cur_node->ikeylen(i)))
	  node_trace.push_back(static_cast<massnode_dynamicvalue<P>*>(cur_node->lv(i).layer()));
      }
      cur_pos++;
    }
    return subtree_size;
  }

  size_t allocated_size() const {
    return size_;
  }

  uint32_t size() const {
    return nkeys_;
  }

  void set_allocated_size(size_t sz) {
    size_ = sz;
  }

  void set_size(uint32_t nkeys) {
    nkeys_ = nkeys;
  }

  uint8_t ikeylen(int p) {
    return get_keylenx()[p];
  }

  void set_ikeylen(int p, uint8_t keylen) {
    get_keylenx()[p] = keylen;
  }

  ikey_type ikey(int p) {
    return get_ikey0()[p];
  }

  void set_ikey(int p, ikey_type ikey) {
    get_ikey0()[p] = ikey;
  }

  leafvalue_type lv(int p) {
    return get_lv()[p];
  }

  void set_lv(int p, leafvalue_type lv) {
    get_lv()[p] = lv;
  }

  key_type get_key(int p) {
    int kl = get_keylenx()[p];
    if (!keylenx_has_ksuf(kl))
      return key_type(get_ikey0()[p], kl);
    else
      return key_type(get_ikey0()[p], ksuf(p));
  }

  static bool keylenx_is_layer(int keylenx) {
    return keylenx > 63;
  }

  void invalidate(int p) {
    set_ikeylen(p, (uint8_t)0);
  }

  bool isValid(int p) {
    return (ikeylen(p) != 0);
  }

  //suffix================================================================================
  uint32_t ksuf_offset(int p) {
    return get_ksuf_pos_offset()[p];
  }

  void set_ksuf_offset(int p, uint32_t offset) {
    get_ksuf_pos_offset()[p] = offset;
  }

  uint32_t ksuflen(int p) {
    return get_ksuf_pos_offset()[p+1] - get_ksuf_pos_offset()[p];
  }

  char *ksufpos(int p) {
    return (char*)(get_ksuf() + get_ksuf_pos_offset()[p]);
  }

  bool has_ksuf(int p) {
    return get_ksuf_pos_offset()[p] != 0;
  }

  Str ksuf(int p) {
    return Str(ksufpos(p), ksuflen(p));
  }

  static int keylenx_ikeylen(int keylenx) {
    return keylenx & 31;
  }

  static uint8_t keylenx_ikeylen(uint8_t keylenx) {
    return keylenx & (uint8_t)31;
  }

  static bool keylenx_has_ksuf(int keylenx) {
    return keylenx == (int)sizeof(ikey_type) + 1;
  }

  bool ksuf_equals(int p, const key_type &ka, int keylenx) {
    return !keylenx_has_ksuf(keylenx) || equals_sloppy(p, ka);
  }

  bool equals_sloppy(int p, const key_type &ka) {
    Str kp_str = ksuf(p);
    if (kp_str.len != ka.suffix().len)
      return false;
    return string_slice<uintptr_t>::equals_sloppy(kp_str.s, ka.suffix().s, ka.suffix().len);
  }
  //========================================================================================

  void deallocate(threadinfo &ti) {
    /*
    for (int i = 0; i < nkeys_; i++) {
      if (isValid(i))
	lv(i).value()->deallocate_rcu(ti);
    }
    */
    ti.deallocate(this, size_, memtag_masstree_leaf);
  }

  void prefetch(int m) {
    ::prefetch((const char*)get_ikey0() + sizeof(ikey_type) * m);
  }

private:
  template <typename PP> friend class tcursor;

};

} // namespace Masstree
#endif
