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
#ifndef MASSTREE_GET_HH
#define MASSTREE_GET_HH 1
#include "masstree_tcursor.hh"
#include "masstree_key.hh"

//huanchen-static
#include <deque>
#include <iostream>
#include <string.h>

namespace Masstree {

template <typename P>
inline int unlocked_tcursor<P>::lower_bound_binary() const
{
    int l = 0, r = perm_.size();
    while (l < r) {
        int m = (l + r) >> 1;
        int mp = perm_[m];
        int cmp = key_compare(ka_, *n_, mp);
        if (cmp < 0)
            r = m;
        else if (cmp == 0)
            return mp;
        else
            l = m + 1;
    }
    return -1;
}

template <typename P>
inline int unlocked_tcursor<P>::lower_bound_linear() const
{
    int l = 0, r = perm_.size();
    while (l < r) {
        int lp = perm_[l];
        int cmp = key_compare(ka_, *n_, lp);
        if (cmp < 0)
            break;
        else if (cmp == 0)
            return lp;
        else
            ++l;
    }
    return -1;
}

template <typename P>
bool unlocked_tcursor<P>::find_unlocked(threadinfo& ti)
{
    bool ksuf_match = false;
    int kp, keylenx = 0;
    node_base<P>* root = const_cast<node_base<P>*>(root_);

 retry:
    n_ = root->reach_leaf(ka_, v_, ti);

 forward:
    if (v_.deleted())
        goto retry;

    n_->prefetch();
    perm_ = n_->permutation();
    if (leaf<P>::bound_type::is_binary)
        kp = lower_bound_binary();
    else
        kp = lower_bound_linear();
    if (kp >= 0) {
        keylenx = n_->keylenx_[kp];
        fence();                // see note in check_leaf_insert()
        lv_ = n_->lv_[kp];
        lv_.prefetch(keylenx);
        ksuf_match = n_->ksuf_equals(kp, ka_, keylenx);
    }
    if (n_->has_changed(v_)) {
        ti.mark(threadcounter(tc_stable_leaf_insert + n_->simple_has_split(v_)));
        n_ = n_->advance_to_key(ka_, v_, ti);
        goto forward;
    }

    if (kp < 0)
        return false;
    else if (n_->keylenx_is_layer(keylenx)) {
        ka_.shift();
        root = lv_.layer();
        goto retry;
    } else
        return ksuf_match;
}

template <typename P>
inline bool basic_table<P>::get(Str key, value_type &value,
                                threadinfo& ti) const
{
    unlocked_tcursor<P> lp(*this, key);
    bool found = lp.find_unlocked(ti);
    if (found)
        value = lp.value();
    return found;
}

template <typename P>
inline node_base<P>* tcursor<P>::get_leaf_locked(node_type* root,
                                                 nodeversion_type& v,
                                                 threadinfo& ti)
{
    nodeversion_type oldv = v;
    typename permuter_type::storage_type old_perm;
    leaf_type *next;

    n_->prefetch();

    if (!ka_.has_suffix())
        v = n_->lock(oldv, ti.lock_fence(tc_leaf_lock));
    else {
        // First, look up without locking.
        // The goal is to avoid dirtying cache lines on upper layers of a long
        // key walk. But we do lock if the next layer has split.
        old_perm = n_->permutation_;
        ki_ = leaf_type::bound_type::lower_with_position(ka_, *n_, kp_);
        if (kp_ >= 0 && n_->is_layer(kp_)) {
            fence();
            leafvalue_type entry(n_->lv_[kp_]);
            entry.layer()->prefetch_full();
            fence();
            if (likely(!v.deleted())
                && !n_->has_changed(oldv, old_perm)
                && !entry.layer()->has_split()) {
                ka_.shift();
                return entry.layer();
            }
        }

        // Otherwise lock.
        v = n_->lock(oldv, ti.lock_fence(tc_leaf_lock));

        // Maybe the old position works.
        if (likely(!v.deleted()) && !n_->has_changed(oldv, old_perm)) {
        found:
            if (kp_ >= 0 && n_->is_layer(kp_)) {
                root = n_->lv_[kp_].layer();
                if (root->has_split())
                    n_->lv_[kp_] = root = root->unsplit_ancestor();
                n_->unlock(v);
                ka_.shift();
                return root;
            } else
                return 0;
        }
    }


    // Walk along leaves.
    while (1) {
        if (unlikely(v.deleted())) {
            n_->unlock(v);
            return root;
        }
        ki_ = leaf_type::bound_type::lower_with_position(ka_, *n_, kp_);
        if (kp_ >= 0) {
            n_->lv_[kp_].prefetch(n_->keylenx_[kp_]);
            goto found;
        } else if (likely(ki_ != n_->size())
                   || likely(!v.has_split(oldv))
                   || !(next = n_->safe_next())
                   || compare(ka_.ikey(), next->ikey_bound()) < 0)
            goto found;
        n_->unlock(v);
        ti.mark(tc_leaf_retry);
        ti.mark(tc_leaf_walk);
        do {
            n_ = next;
            oldv = n_->stable();
        } while (!unlikely(oldv.deleted())
                 && (next = n_->safe_next())
                 && compare(ka_.ikey(), next->ikey_bound()) >= 0);
        n_->prefetch();
        v = n_->lock(oldv, ti.lock_fence(tc_leaf_lock));
    }
}

template <typename P>
inline node_base<P>* tcursor<P>::check_leaf_locked(node_type* root,
                                                   nodeversion_type v,
                                                   threadinfo& ti)
{
    if (node_type* next_root = get_leaf_locked(root, v, ti))
        return next_root;
    if (kp_ >= 0) {
        if (!n_->ksuf_equals(kp_, ka_))
            kp_ = -1;
    } else if (ki_ == 0 && unlikely(n_->deleted_layer())) {
        n_->unlock();
        return reset_retry();
    }
    return 0;
}

template <typename P>
bool tcursor<P>::find_locked(threadinfo& ti)
{
    nodeversion_type v;
    node_type* root = root_;
    while (1) {
        n_ = root->reach_leaf(ka_, v, ti);
        original_n_ = n_;
        original_v_ = n_->full_unlocked_version_value();

        root = check_leaf_locked(root, v, ti);
        if (!root) {
            state_ = kp_ >= 0;
            return kp_ >= 0;
        }
    }
}

//huanchen-static
//**********************************************************************************
// buildStatic
//**********************************************************************************
template <typename P>
massnode<P> *unlocked_tcursor<P>::buildStatic(threadinfo &ti) {
  typedef typename P::ikey_type ikey_type;

  std::vector<uint8_t> ikeylen_list;
  std::vector<ikey_type> ikey_list;
  std::deque<leafvalue<P>> trienode_list;
  std::vector<leafvalue<P>> lv_list;
  std::vector<massnode<P>*> massnode_list;

  //suffix=====================================
  std::vector<bool> has_ksuf_list;
  std::vector<Str> ksuf_list;
  size_t ksufSize = 0;
  //===========================================

  int kp = 0;
  unsigned int massID = 1;
  int nkeys = 0;
  node_base<P> *root = const_cast <node_base<P>*> (root_);
  leaf<P> *next;

 nextTrieNode:
  n_ = root -> leftmost();
 nextLeafNode:
  // extract info from a B-tree
  perm_ = n_ -> permutation();
  nkeys += perm_.size();
  for (int i = 0; i < perm_.size(); i++) {
    kp = perm_[i];
    ikeylen_list.push_back(n_->keylenx_[kp]); // ikeylen array
    ikey_list.push_back(n_->ikey0_[kp]); // ikey array
    lv_list.push_back(n_ -> lv_[kp]); // lv array

    if (n_->keylenx_is_layer(n_->keylenx_[kp])) {
      trienode_list.push_back(n_->lv_[kp]); // trienode BFS queue
    }

    //suffix=========================================================
    if (n_->has_ksuf(kp)) {      
      ksufSize += n_->ksuf(kp).len;
      has_ksuf_list.push_back(true);
      ksuf_list.push_back(n_->ksuf(kp)); // key_suffix array
    }
    else {
      has_ksuf_list.push_back(false);
      ksuf_list.push_back(Str()); // key_suffix array
    }
    //===============================================================
  }

  next = n_->safe_next();
  if (next) {
    n_ = next;
    goto nextLeafNode;
  }

  massnode<P> *newNode = massnode<P>::make(ksufSize, true, nkeys, ti);
  massnode_list.push_back(newNode);
  char *ksuf_curpos = newNode->get_ksuf();
  char *ksuf_startpos = newNode->get_ksuf();

  // turning a B-tree into a massnode
  for (int i = 0; i < nkeys; i++) {
    newNode->set_ikeylen(i, ikeylen_list[i]);
    newNode->set_ikey(i, ikey_list[i]);

    if (leaf<P>::keylenx_is_layer(newNode->ikeylen(i))) {
      newNode->get_lv()[i].set_value((uintptr_t)massID);
      massID++;
    }
    else {
      newNode->set_lv(i, leafvalue_static<P>(lv_list[i].value()->col(0).s));
      lv_list[i].value()->deallocate_rcu(ti);
    }

    newNode->set_ksuf_offset(i, (uint32_t)(ksuf_curpos - ksuf_startpos));

    //suffix==================================================================
    if (has_ksuf_list[i]) {
      memcpy(ksuf_curpos, ksuf_list[i].s, ksuf_list[i].len);
      ksuf_curpos += ksuf_list[i].len;
    }

    //========================================================================
  }
  newNode->set_ksuf_offset(nkeys, (uint32_t)(ksuf_curpos - ksuf_startpos));

  // reset holders
  ikeylen_list.clear();
  ikey_list.clear();
  lv_list.clear();

  //suffix============================
  has_ksuf_list.clear();
  ksuf_list.clear();
  ksufSize = 0;
  //==================================

  nkeys = 0;

  // next trienode
  if (!trienode_list.empty()) {
    root = trienode_list.front().layer();
    trienode_list.pop_front();
    goto nextTrieNode;
  }

  for (unsigned int i = 0; i < massnode_list.size(); i++)
    for (unsigned int j = 0; j < massnode_list[i]->nkeys_; j++)
      if (leaf<P>::keylenx_is_layer(massnode_list[i]->ikeylen(j)))
	massnode_list[i]->set_lv(j, leafvalue_static<P>(massnode_list[massnode_list[i]->get_lv()[j].get_value()])); // link the massnode into a trie

  return massnode_list[0];
}

template <typename P>
massnode<P> *unlocked_tcursor<P>::buildStatic_quick(int nkeys, threadinfo &ti) {
  std::deque<leafvalue<P>> trienode_list;
  std::vector<massnode<P>*> massnode_list;

  int kp = 0;
  unsigned int massID = 1;
  node_base<P> *root = const_cast <node_base<P>*> (root_);
  leaf<P> *next;
  int cur_pos;

 nextTrieNode:
  massnode<P> *newNode = massnode<P>::make(0, false, nkeys, ti);
  massnode_list.push_back(newNode);
  cur_pos = 0;
  n_ = root -> leftmost();
 nextLeafNode:
  // extract info from a B-tree
  perm_ = n_ -> permutation();
  for (int i = 0; i < perm_.size(); i++) {
    kp = perm_[i];
    newNode->set_ikeylen(cur_pos, n_->keylenx_[kp]);
    newNode->set_ikey(cur_pos, n_->ikey0_[kp]);

    if (leaf<P>::keylenx_is_layer(newNode->ikeylen(cur_pos))) {
      newNode->get_lv()[cur_pos].set_value((uintptr_t)massID);
      massID++;
    }
    else {
      newNode->set_lv(cur_pos, leafvalue_static<P>(n_->lv_[kp].value()->col(0).s));
      n_->lv_[kp].value()->deallocate_rcu(ti);
    }

    //newNode->set_ksuf_offset(cur_pos, 0);

    if (n_->keylenx_is_layer(n_->keylenx_[kp])) {
      trienode_list.push_back(n_->lv_[kp]); // trienode BFS queue
    }
    cur_pos++;
  }

  next = n_->safe_next();
  if (next) {
    n_ = next;
    goto nextLeafNode;
  }

  //newNode->set_ksuf_offset(cur_pos, 0);

  // next trienode
  if (!trienode_list.empty()) {
    root = trienode_list.front().layer();
    trienode_list.pop_front();
    goto nextTrieNode;
  }

  for (unsigned int i = 0; i < massnode_list.size(); i++)
    for (unsigned int j = 0; j < massnode_list[i]->nkeys_; j++)
      if (leaf<P>::keylenx_is_layer(massnode_list[i]->ikeylen(j)))
	massnode_list[i]->set_lv(j, leafvalue_static<P>(massnode_list[massnode_list[i]->get_lv()[j].get_value()])); // link the massnode into a trie

  return massnode_list[0];
}

//huanchen-static-multivalue
//**********************************************************************************
// buildStaticMultivalue
//**********************************************************************************
template <typename P>
massnode_multivalue<P> *unlocked_tcursor<P>::buildStaticMultivalue(threadinfo &ti) {
  typedef typename P::ikey_type ikey_type;

  std::vector<uint8_t> ikeylen_list;
  std::vector<ikey_type> ikey_list;
  std::deque<leafvalue<P>> trienode_list;
  std::vector<leafvalue<P>> lv_list;
  std::vector<massnode_multivalue<P>*> massnode_list;

  //suffix=====================================
  std::vector<bool> has_ksuf_list;
  std::vector<Str> ksuf_list;
  size_t ksufSize = 0;
  //===========================================

  int kp = 0;
  unsigned int massID = 1;
  int nkeys = 0;
  node_base<P> *root = const_cast <node_base<P>*> (root_);
  leaf<P> *next;

  uint32_t valueSize = 0;

 nextTrieNode:
  n_ = root -> leftmost();
 nextLeafNode:
  // extract info from a B-tree
  perm_ = n_ -> permutation();
  nkeys += perm_.size();
  for (int i = 0; i < perm_.size(); i++) {
    kp = perm_[i];
    ikeylen_list.push_back(n_->keylenx_[kp]); // ikeylen array
    ikey_list.push_back(n_->ikey0_[kp]); // ikey array
    lv_list.push_back(n_->lv_[kp]); // lv array

    if (n_->keylenx_is_layer(n_->keylenx_[kp]))
      trienode_list.push_back(n_->lv_[kp]); // trienode BFS queue
    else
      valueSize += (n_->lv_[kp].value()->col(0).len);

    //suffix=========================================================
    if (n_->has_ksuf(kp)) {      
      ksufSize += n_->ksuf(kp).len;
      has_ksuf_list.push_back(true);
      ksuf_list.push_back(n_->ksuf(kp)); // key_suffix array
    }
    else {
      has_ksuf_list.push_back(false);
      ksuf_list.push_back(Str()); // key_suffix array
    }
    //===============================================================
  }

  next = n_->safe_next();
  if (next) {
    n_ = next;
    goto nextLeafNode;
  }

  massnode_multivalue<P> *newNode = massnode_multivalue<P>::make(ksufSize, valueSize, nkeys, ti);
  massnode_list.push_back(newNode);
  char *ksuf_curpos = newNode->get_ksuf();
  char *ksuf_startpos = newNode->get_ksuf();
  char *value_curpos = newNode->get_value();
  char *value_startpos = newNode->get_value();

  // turning a B-tree into a massnode_multivalue
  for (int i = 0; i < nkeys; i++) {
    newNode->set_ikeylen(i, ikeylen_list[i]);
    newNode->set_ikey(i, ikey_list[i]);

    if (leaf<P>::keylenx_is_layer(newNode->ikeylen(i))) {
      newNode->get_lv()[i].set_value((uintptr_t)massID);
      massID++;
    }
    else {
      newNode->set_lv(i, leafvalue_static_multivalue<P>((uint32_t)(value_curpos - value_startpos), 
							(uint32_t)(lv_list[i].value()->col(0).len)));
      memcpy(value_curpos, lv_list[i].value()->col(0).s, lv_list[i].value()->col(0).len);
      value_curpos += lv_list[i].value()->col(0).len;
    }

    newNode->set_ksuf_offset(i, (uint32_t)(ksuf_curpos - ksuf_startpos));
    //suffix==================================================================
    if (has_ksuf_list[i]) {
      memcpy(ksuf_curpos, ksuf_list[i].s, ksuf_list[i].len);
      ksuf_curpos += ksuf_list[i].len;
    }
    //========================================================================
  }
  newNode->set_ksuf_offset(nkeys, (uint32_t)(ksuf_curpos - ksuf_startpos));

  // reset holders
  ikeylen_list.clear();
  ikey_list.clear();
  lv_list.clear();

  //suffix============================
  has_ksuf_list.clear();
  ksuf_list.clear();
  ksufSize = 0;
  //==================================

  valueSize = 0; //this cost 3 hours

  nkeys = 0;

  // next trienode
  if (!trienode_list.empty()) {
    root = trienode_list.front().layer();
    trienode_list.pop_front();
    goto nextTrieNode;
  }

  for (unsigned int i = 0; i < massnode_list.size(); i++)
    for (unsigned int j = 0; j < massnode_list[i]->nkeys_; j++)
      if (leaf<P>::keylenx_is_layer(massnode_list[i]->ikeylen(j)))
	massnode_list[i]->set_lv(j, leafvalue_static_multivalue<P>(massnode_list[massnode_list[i]->get_lv()[j].get_value()])); // link the massnode into a trie

  return massnode_list[0];
}



//huanchen-static-multivalue
//**********************************************************************************
// buildStaticDynamicvalue
//**********************************************************************************
template <typename P>
massnode_dynamicvalue<P> *unlocked_tcursor<P>::buildStaticDynamicvalue(threadinfo &ti) {
  typedef typename P::ikey_type ikey_type;

  std::vector<uint8_t> ikeylen_list;
  std::vector<ikey_type> ikey_list;
  std::deque<leafvalue<P>> trienode_list;
  std::vector<leafvalue<P>> lv_list;
  std::vector<massnode_dynamicvalue<P>*> massnode_list;

  //suffix=====================================
  std::vector<bool> has_ksuf_list;
  std::vector<Str> ksuf_list;
  size_t ksufSize = 0;
  //===========================================

  int kp = 0;
  unsigned int massID = 1;
  int nkeys = 0;
  node_base<P> *root = const_cast <node_base<P>*> (root_);
  leaf<P> *next;


 nextTrieNode:
  n_ = root -> leftmost();
 nextLeafNode:
  // extract info from a B-tree
  perm_ = n_ -> permutation();
  nkeys += perm_.size();
  for (int i = 0; i < perm_.size(); i++) {
    kp = perm_[i];
    ikeylen_list.push_back(n_->keylenx_[kp]); // ikeylen array
    ikey_list.push_back(n_->ikey0_[kp]); // ikey array
    lv_list.push_back(n_->lv_[kp]); // lv array

    if (n_->keylenx_is_layer(n_->keylenx_[kp]))
      trienode_list.push_back(n_->lv_[kp]); // trienode BFS queue

    //suffix=========================================================
    if (n_->has_ksuf(kp)) {      
      ksufSize += n_->ksuf(kp).len;
      has_ksuf_list.push_back(true);
      ksuf_list.push_back(n_->ksuf(kp)); // key_suffix array
    }
    else {
      has_ksuf_list.push_back(false);
      ksuf_list.push_back(Str()); // key_suffix array
    }
    //===============================================================
  }

  next = n_->safe_next();
  if (next) {
    n_ = next;
    goto nextLeafNode;
  }

  massnode_dynamicvalue<P> *newNode = massnode_dynamicvalue<P>::make(ksufSize, nkeys, ti);
  massnode_list.push_back(newNode);
  char *ksuf_curpos = newNode->get_ksuf();
  char *ksuf_startpos = newNode->get_ksuf();

  // turning a B-tree into a massnode_multivalue
  for (int i = 0; i < nkeys; i++) {
    newNode->set_ikeylen(i, ikeylen_list[i]);
    newNode->set_ikey(i, ikey_list[i]);

    if (leaf<P>::keylenx_is_layer(newNode->ikeylen(i))) {
      newNode->get_lv()[i].set_value((uintptr_t)massID);
      massID++;
    }
    else {
      newNode->set_lv(i, lv_list[i]);
    }

    newNode->set_ksuf_offset(i, (uint32_t)(ksuf_curpos - ksuf_startpos));
    //suffix==================================================================
    if (has_ksuf_list[i]) {
      memcpy(ksuf_curpos, ksuf_list[i].s, ksuf_list[i].len);
      ksuf_curpos += ksuf_list[i].len;
    }
    //========================================================================
  }
  newNode->set_ksuf_offset(nkeys, (uint32_t)(ksuf_curpos - ksuf_startpos));

  // reset holders
  ikeylen_list.clear();
  ikey_list.clear();
  lv_list.clear();

  //suffix============================
  has_ksuf_list.clear();
  ksuf_list.clear();
  ksufSize = 0;
  //==================================

  nkeys = 0;

  // next trienode
  if (!trienode_list.empty()) {
    root = trienode_list.front().layer();
    trienode_list.pop_front();
    goto nextTrieNode;
  }

  for (unsigned int i = 0; i < massnode_list.size(); i++)
    for (unsigned int j = 0; j < massnode_list[i]->nkeys_; j++)
      if (leaf<P>::keylenx_is_layer(massnode_list[i]->ikeylen(j)))
	massnode_list[i]->set_lv(j, leafvalue<P>(massnode_list[massnode_list[i]->get_lv()[j].get_value()])); // link the massnode into a trie

  return massnode_list[0];
}



//huanchen-static
//**********************************************************************************
// stcursor::find
//**********************************************************************************
template <typename P>
bool stcursor<P>::find() {
  if (!root_)
    return false;
  bool ksuf_match = false; //suffix
  int kp, keylenx = 0;
  n_ = static_cast<massnode<P>*>(root_);

 nextNode:
  kp = lower_bound_binary();
  if (kp < 0)
    return false;
  keylenx = n_->ikeylen(kp);
  lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode<P>*>(lv_->layer());
    goto nextNode;
  }
  else {
    if (!n_->isValid(kp))
      return false;
    ksuf_match = n_->ksuf_equals(kp, ka_, keylenx); //suffix
    return ksuf_match; //suffix
  }
}

//huanchen-static-multivalue
//**********************************************************************************
// stcursor_multivalue::find
//**********************************************************************************
template <typename P>
bool stcursor_multivalue<P>::find() {
  if (!root_)
    return false;
  bool ksuf_match = false; //suffix
  int kp, keylenx = 0;
  n_ = static_cast<massnode_multivalue<P>*>(root_);

 nextNode:
  kp = lower_bound_binary();
  if (kp < 0)
    return false;
  keylenx = n_->ikeylen(kp);
  lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode_multivalue<P>*>(lv_->layer());
    goto nextNode;
  }
  else {
    if (!n_->isValid(kp))
      return false;
    ksuf_match = n_->ksuf_equals(kp, ka_, keylenx); //suffix
    return ksuf_match; //suffix
  }
}


//huanchen-static-dynamicvalue
//**********************************************************************************
// stcursor_dynamicvalue::find
//**********************************************************************************
template <typename P>
bool stcursor_dynamicvalue<P>::find() {
  if (!root_)
    return false;
  bool ksuf_match = false; //suffix
  int kp, keylenx = 0;
  n_ = static_cast<massnode_dynamicvalue<P>*>(root_);

 nextNode:
  kp = lower_bound_binary();
  if (kp < 0)
    return false;
  keylenx = n_->ikeylen(kp);
  lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode_dynamicvalue<P>*>(lv_->layer());
    goto nextNode;
  }
  else {
    if (!n_->isValid(kp))
      return false;
    ksuf_match = n_->ksuf_equals(kp, ka_, keylenx); //suffix
    return ksuf_match; //suffix
  }
}


//huanchen-static
//**********************************************************************************
// stcursor::remove
//**********************************************************************************
template <typename P>
bool stcursor<P>::remove() {
  if (!root_)
    return false;
  bool ksuf_match = false; //suffix
  int kp, keylenx = 0;
  n_ = static_cast<massnode<P>*>(root_);

 nextNode:
  kp = lower_bound_binary();
  if (kp < 0)
    return false;
  keylenx = n_->ikeylen(kp);
  lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode<P>*>(lv_->layer());
    goto nextNode;
  }
  else {
    if (!n_->isValid(kp))
      return false;
    ksuf_match = n_->ksuf_equals(kp, ka_, keylenx); //suffix
    if (!ksuf_match) //suffix
      return false;
    n_->invalidate(kp);
    //std::cout << "remove_kp = " << kp << "\n";
    return true;
  }
}


//**********************************************************************************
// stcursor::update
//**********************************************************************************
template <typename P>
bool stcursor<P>::update(const char *nv) {
  if (!root_)
    return false;
  bool ksuf_match = false; //suffix
  int kp, keylenx = 0;
  n_ = static_cast<massnode<P>*>(root_);

 nextNode:
  kp = lower_bound_binary();
  if (kp < 0)
    return false;
  keylenx = n_->ikeylen(kp);
  lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode<P>*>(lv_->layer());
    goto nextNode;
  }
  else {
    if (!n_->isValid(kp))
      return false;
    ksuf_match = n_->ksuf_equals(kp, ka_, keylenx); //suffix
    if (!ksuf_match) //suffix
      return false;
    n_->set_lv(kp, leafvalue_static<P>(nv));
    //std::cout << "remove_kp = " << kp << "\n";
    return true;
  }
}


//huanchen-static-multivalue
//**********************************************************************************
// stcursor_multivalue::remove
//**********************************************************************************
template <typename P>
bool stcursor_multivalue<P>::remove() {
  if (!root_)
    return false;
  bool ksuf_match = false; //suffix
  int kp, keylenx = 0;
  n_ = static_cast<massnode_multivalue<P>*>(root_);

 nextNode:
  kp = lower_bound_binary();
  if (kp < 0)
    return false;
  keylenx = n_->ikeylen(kp);
  lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode_multivalue<P>*>(lv_->layer());
    goto nextNode;
  }
  else {
    if (!n_->isValid(kp))
      return false;
    ksuf_match = n_->ksuf_equals(kp, ka_, keylenx); //suffix
    if (!ksuf_match) //suffix
      return false;
    n_->invalidate(kp);
    return true;
  }
}


//huanchen-static-multivalue
//**********************************************************************************
// stcursor_multivalue::remove
//**********************************************************************************
template <typename P>
bool stcursor_dynamicvalue<P>::remove(threadinfo &ti) {
  if (!root_)
    return false;
  bool ksuf_match = false; //suffix
  int kp, keylenx = 0;
  n_ = static_cast<massnode_dynamicvalue<P>*>(root_);

 nextNode:
  kp = lower_bound_binary();
  if (kp < 0)
    return false;
  keylenx = n_->ikeylen(kp);
  lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode_dynamicvalue<P>*>(lv_->layer());
    goto nextNode;
  }
  else {
    if (!n_->isValid(kp))
      return false;
    ksuf_match = n_->ksuf_equals(kp, ka_, keylenx); //suffix
    if (!ksuf_match) //suffix
      return false;
    n_->invalidate(kp);
    lv_->value()->deallocate_rcu(ti);
    return true;
  }
}


//huanchen-static
//**********************************************************************************
// stcursor::destroy
//**********************************************************************************
template <typename P>
void stcursor<P>::destroy(threadinfo &ti) {
  std::vector<massnode<P>*> massnode_list;
  unsigned int bfs_cursor = 0;
  int keylenx = 0;
  //traverse the tree, get all the massnodes
  if (root_)
    massnode_list.push_back(static_cast<massnode<P>*>(root_));
  while (bfs_cursor < massnode_list.size()) {
    n_ = massnode_list[bfs_cursor];
    for (unsigned int i = 0; i < n_->size(); i++) {
      keylenx = n_->ikeylen(i);
      lv_ = &(n_->get_lv()[i]);
      if (n_->keylenx_is_layer(keylenx))
	massnode_list.push_back(static_cast<massnode<P>*>(lv_->layer()));
    }
    bfs_cursor++;
  }
  //free
  for (unsigned int i = 0; i < massnode_list.size(); i++)
    massnode_list[i]->deallocate(ti);
}


//huanchen-static-multivalue
//**********************************************************************************
// stcursor_multivalue::destroy
//**********************************************************************************
template <typename P>
void stcursor_multivalue<P>::destroy(threadinfo &ti) {
  std::vector<massnode_multivalue<P>*> massnode_list;
  unsigned int bfs_cursor = 0;
  int keylenx = 0;
  //traverse the tree, get all the massnodes
  if (root_)
    massnode_list.push_back(static_cast<massnode_multivalue<P>*>(root_));
  while (bfs_cursor < massnode_list.size()) {
    n_ = massnode_list[bfs_cursor];
    for (unsigned int i = 0; i < n_->size(); i++) {
      keylenx = n_->ikeylen(i);
      lv_ = &(n_->get_lv()[i]);
      if (n_->keylenx_is_layer(keylenx))
	massnode_list.push_back(static_cast<massnode_multivalue<P>*>(lv_->layer()));
    }
    bfs_cursor++;
  }
  //free
  for (unsigned int i = 0; i < massnode_list.size(); i++)
    massnode_list[i]->deallocate(ti);
}


//huanchen-static-dynamicvalue
//**********************************************************************************
// stcursor_dynamicvalue::destroy
//**********************************************************************************
template <typename P>
void stcursor_dynamicvalue<P>::destroy(threadinfo &ti) {
  std::vector<massnode_dynamicvalue<P>*> massnode_list;
  unsigned int bfs_cursor = 0;
  int keylenx = 0;
  //traverse the tree, get all the massnodes
  if (root_)
    massnode_list.push_back(static_cast<massnode_dynamicvalue<P>*>(root_));
  while (bfs_cursor < massnode_list.size()) {
    n_ = massnode_list[bfs_cursor];
    for (unsigned int i = 0; i < n_->size(); i++) {
      keylenx = n_->ikeylen(i);
      lv_ = &(n_->get_lv()[i]);
      if (n_->keylenx_is_layer(keylenx))
	massnode_list.push_back(static_cast<massnode_dynamicvalue<P>*>(lv_->layer()));
    }
    bfs_cursor++;
  }
  //free
  for (unsigned int i = 0; i < massnode_list.size(); i++)
    massnode_list[i]->deallocate(ti);
}


//huanchen-static
//**********************************************************************************
// stcursor::lower_bound_binary
//**********************************************************************************
template <typename P>
inline int stcursor<P>::lower_bound_binary() const {
  int l = 0;
  int r = n_->nkeys_;
  while (l < r) {
    int m = (l + r) >> 1;

    n_ -> prefetch((l + m)/2);
    n_ -> prefetch((m + r)/2);
    int cmp = key_compare(ka_, *n_, m);
    if (cmp < 0)
      r = m;
    else if (cmp == 0)
      return m;
    else
      l = m + 1;
  }
  return -1;
}


//huanchen-static-multivalue
//**********************************************************************************
// stcursor_multivalue::lower_bound_binary
//**********************************************************************************
template <typename P>
inline int stcursor_multivalue<P>::lower_bound_binary() const {
  int l = 0;
  int r = n_->nkeys_;
  while (l < r) {
    int m = (l + r) >> 1;

    n_ -> prefetch((l + m)/2);
    n_ -> prefetch((m + r)/2);
    int cmp = key_compare(ka_, *n_, m);
    if (cmp < 0)
      r = m;
    else if (cmp == 0)
      return m;
    else
      l = m + 1;
  }
  return -1;
}


//huanchen-static-dynamicvalue
//**********************************************************************************
// stcursor_dynamicvalue::lower_bound_binary
//**********************************************************************************
template <typename P>
inline int stcursor_dynamicvalue<P>::lower_bound_binary() const {
  int l = 0;
  int r = n_->nkeys_;
  while (l < r) {
    int m = (l + r) >> 1;

    n_ -> prefetch((l + m)/2);
    n_ -> prefetch((m + r)/2);
    int cmp = key_compare(ka_, *n_, m);
    if (cmp < 0)
      r = m;
    else if (cmp == 0)
      return m;
    else
      l = m + 1;
  }
  return -1;
}


//huanchen-static-scan
//**********************************************************************************
// stcursor_scan::find_upper_bound_or_equal
//**********************************************************************************
template <typename P>
bool stcursor_scan<P>::find_upper_bound_or_equal() {
  if (!root_)
    return false;
  int kp, keylenx = 0;
  n_ = static_cast<massnode<P>*>(root_);
 nextNode:
  kp = upper_bound_or_equal_binary();
  //out of bound, go back to parent
  if (kp >= n_->nkeys_)
    return next_item_from_next_node(kp);

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    if (kp >= n_->nkeys_)
      return next_item_from_next_node(kp);
  }

  cur_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode<P>*>(cur_lv_->layer());
    if (!isExact_)
      return find_leftmost();
    goto nextNode;
  }
  if (isExact_)
    if (memcmp(ka_.suffix().s, n_->ksuf(kp).s, ka_.suffix_length()) > 0)
      return next_item(kp);

  cur_key_suffix_ = n_->ksuf(kp);
  return true;
}


//huanchen-static-scan-multivalue
//**********************************************************************************
// stcursor_scan_multivalue::find_upper_bound_or_equal
//**********************************************************************************
template <typename P>
bool stcursor_scan_multivalue<P>::find_upper_bound_or_equal() {
  if (!root_)
    return false;
  int kp, keylenx = 0;
  n_ = static_cast<massnode_multivalue<P>*>(root_);
 nextNode:
  kp = upper_bound_or_equal_binary();
  //out of bound, go back to parent
  if (kp >= n_->nkeys_)
    return next_item_from_next_node(kp);

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    if (kp >= n_->nkeys_)
      return next_item_from_next_node(kp);
  }

  cur_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode_multivalue<P>*>(cur_lv_->layer());
    if (!isExact_)
      return find_leftmost();
    goto nextNode;
  }
  if (isExact_)
    if (memcmp(ka_.suffix().s, n_->ksuf(kp).s, ka_.suffix_length()) > 0)
      return next_item(kp);

  cur_key_suffix_ = n_->ksuf(kp);
  return true;
}


//huanchen-static-scan-dynamicvalue
//**********************************************************************************
// stcursor_scan_dynamicvalue::find_upper_bound_or_equal
//**********************************************************************************
template <typename P>
bool stcursor_scan_dynamicvalue<P>::find_upper_bound_or_equal() {
  if (!root_)
    return false;
  int kp, keylenx = 0;
  n_ = static_cast<massnode_dynamicvalue<P>*>(root_);
 nextNode:
  kp = upper_bound_or_equal_binary();
  //out of bound, go back to parent
  if (kp >= n_->nkeys_)
    return next_item_from_next_node(kp);

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    if (kp >= n_->nkeys_)
      return next_item_from_next_node(kp);
  }

  cur_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode_dynamicvalue<P>*>(cur_lv_->layer());
    if (!isExact_)
      return find_leftmost();
    goto nextNode;
  }
  if (isExact_)
    if (memcmp(ka_.suffix().s, n_->ksuf(kp).s, ka_.suffix_length()) > 0)
      return next_item(kp);

  cur_key_suffix_ = n_->ksuf(kp);
  return true;
}


//huanchen-static-scan
//**********************************************************************************
// stcursor_scan::find_upper_bound
//**********************************************************************************
template <typename P>
bool stcursor_scan<P>::find_upper_bound() {
  if (!root_)
    return false;
  int kp, keylenx = 0;
  n_ = static_cast<massnode<P>*>(root_);
 nextNode:
  kp = upper_bound_or_equal_binary();
  //out of bound, go back to parent
  if (kp >= n_->nkeys_)
    return next_item_from_next_node(kp);

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    if (kp >= n_->nkeys_)
      return next_item_from_next_node(kp);
  }

  cur_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode<P>*>(cur_lv_->layer());
    if (!isExact_)
      return find_leftmost();
    goto nextNode;
  }

  if (!isExact_) {
    cur_key_suffix_ = n_->ksuf(kp);
    return true;
  }

  return next_item(kp);
}


//huanchen-static-scan-multivalue
//**********************************************************************************
// stcursor_scan_multivalue::find_upper_bound
//**********************************************************************************
template <typename P>
bool stcursor_scan_multivalue<P>::find_upper_bound() {
  if (!root_)
    return false;
  int kp, keylenx = 0;
  n_ = static_cast<massnode_multivalue<P>*>(root_);
 nextNode:
  kp = upper_bound_or_equal_binary();
  //out of bound, go back to parent
  if (kp >= n_->nkeys_)
    return next_item_from_next_node(kp);

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    if (kp >= n_->nkeys_)
      return next_item_from_next_node(kp);
  }

  cur_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode_multivalue<P>*>(cur_lv_->layer());
    if (!isExact_)
      return find_leftmost();
    goto nextNode;
  }

  if (!isExact_) {
    cur_key_suffix_ = n_->ksuf(kp);
    return true;
  }

  return next_item(kp);
}


//huanchen-static-scan-dynamicvalue
//**********************************************************************************
// stcursor_scan_dynamicvalue::find_upper_bound
//**********************************************************************************
template <typename P>
bool stcursor_scan_dynamicvalue<P>::find_upper_bound() {
  if (!root_)
    return false;
  int kp, keylenx = 0;
  n_ = static_cast<massnode_dynamicvalue<P>*>(root_);
 nextNode:
  kp = upper_bound_or_equal_binary();
  //out of bound, go back to parent
  if (kp >= n_->nkeys_)
    return next_item_from_next_node(kp);

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    if (kp >= n_->nkeys_)
      return next_item_from_next_node(kp);
  }

  cur_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode_dynamicvalue<P>*>(cur_lv_->layer());
    if (!isExact_)
      return find_leftmost();
    goto nextNode;
  }

  if (!isExact_) {
    cur_key_suffix_ = n_->ksuf(kp);
    return true;
  }

  return next_item(kp);
}


//huanchen-static-scan
//**********************************************************************************
// stcursor_scan::find_leftmost
//**********************************************************************************
template <typename P>
bool stcursor_scan<P>::find_leftmost() {
  if (!n_)
    return false;
  int keylenx = 0;
 nextNode:
  cur_key_prefix_.push_back(n_->ikey(0));
  keylenx = n_->ikeylen(0);
  cur_lv_ = &(n_->get_lv()[0]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode<P>*>(cur_lv_->layer());
    goto nextNode;
  }
  cur_key_suffix_ = n_->ksuf(0);
  return true;
}

//huanchen-static-multivalue
//**********************************************************************************
// stcursor_scan_multivalue::find_leftmost
//**********************************************************************************
template <typename P>
bool stcursor_scan_multivalue<P>::find_leftmost() {
  if (!n_)
    return false;
  int keylenx = 0;
 nextNode:
  cur_key_prefix_.push_back(n_->ikey(0));
  keylenx = n_->ikeylen(0);
  cur_lv_ = &(n_->get_lv()[0]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode_multivalue<P>*>(cur_lv_->layer());
    goto nextNode;
  }
  cur_key_suffix_ = n_->ksuf(0);
  return true;
}

//huanchen-static-dynamicvalue
//**********************************************************************************
// stcursor_scan_dynamicvalue::find_leftmost
//**********************************************************************************
template <typename P>
bool stcursor_scan_dynamicvalue<P>::find_leftmost() {
  if (!n_)
    return false;
  int keylenx = 0;
 nextNode:
  cur_key_prefix_.push_back(n_->ikey(0));
  keylenx = n_->ikeylen(0);
  cur_lv_ = &(n_->get_lv()[0]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode_dynamicvalue<P>*>(cur_lv_->layer());
    goto nextNode;
  }
  cur_key_suffix_ = n_->ksuf(0);
  return true;
}



//huanchen-static-scan
//**********************************************************************************
// stcursor_scan::next_item
//**********************************************************************************
template <typename P>
inline bool stcursor_scan<P>::next_item(int kp) {
  int keylenx = 0;
  kp++;
  //out of bound, go back to parent
  if (kp >= n_->nkeys_)
    return next_item_from_next_node(kp);
      
  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    if (kp >= n_->nkeys_)
      return next_item_from_next_node(kp);
  }
  cur_key_prefix_.pop_back();
  posTrace_[posTrace_.size() - 1]++;
  cur_key_prefix_.push_back(n_->ikey(kp));

  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
      
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode<P>*>(next_lv_->layer());
    return find_leftmost();
  }

  cur_key_suffix_ = n_->ksuf(kp);
  return true;
}

//huanchen-static-scan-multivalue
//**********************************************************************************
// stcursor_scan_multivalue::next_item
//**********************************************************************************
template <typename P>
inline bool stcursor_scan_multivalue<P>::next_item(int kp) {
  int keylenx = 0;
  kp++;
  //out of bound, go back to parent
  if (kp >= n_->nkeys_)
    return next_item_from_next_node(kp);
      
  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    if (kp >= n_->nkeys_)
      return next_item_from_next_node(kp);
  }
  cur_key_prefix_.pop_back();
  posTrace_[posTrace_.size() - 1]++;
  cur_key_prefix_.push_back(n_->ikey(kp));

  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
      
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode_multivalue<P>*>(next_lv_->layer());
    return find_leftmost();
  }

  cur_key_suffix_ = n_->ksuf(kp);
  return true;
}

//huanchen-static-scan-dynamicvalue
//**********************************************************************************
// stcursor_scan_dynamicvalue::next_item
//**********************************************************************************
template <typename P>
inline bool stcursor_scan_dynamicvalue<P>::next_item(int kp) {
  int keylenx = 0;
  kp++;
  //out of bound, go back to parent
  if (kp >= n_->nkeys_)
    return next_item_from_next_node(kp);
      
  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    if (kp >= n_->nkeys_)
      return next_item_from_next_node(kp);
  }
  cur_key_prefix_.pop_back();
  posTrace_[posTrace_.size() - 1]++;
  cur_key_prefix_.push_back(n_->ikey(kp));

  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
      
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode_dynamicvalue<P>*>(next_lv_->layer());
    return find_leftmost();
  }

  cur_key_suffix_ = n_->ksuf(kp);
  return true;
}



//huanchen-static-scan
//**********************************************************************************
// stcursor_scan::next_item_from_next_node
//**********************************************************************************
template <typename P>
inline bool stcursor_scan<P>::next_item_from_next_node(int kp) {
  int keylenx = 0;
  int stack_size = nodeTrace_.size();
  if (stack_size == 0)
    return false;
  n_ = nodeTrace_[stack_size - 1];
  kp = posTrace_[stack_size - 1] + 1;
  nodeTrace_.pop_back();
  posTrace_.pop_back();
  cur_key_prefix_.pop_back();
  while (kp >= n_->nkeys_) {
    stack_size = nodeTrace_.size();
    if (stack_size == 0)
      return false;
    n_ = nodeTrace_[stack_size - 1];
    kp = posTrace_[stack_size - 1] + 1;
    nodeTrace_.pop_back();
    posTrace_.pop_back();
    cur_key_prefix_.pop_back();
  }

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    while (kp >= n_->nkeys_) {
      stack_size = nodeTrace_.size();
      if (stack_size == 0)
	return false;
      n_ = nodeTrace_[stack_size - 1];
      kp = posTrace_[stack_size - 1] + 1;
      nodeTrace_.pop_back();
      posTrace_.pop_back();
      cur_key_prefix_.pop_back();
    }
  }

  cur_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode<P>*>(cur_lv_->layer());
    return find_leftmost();
  }
  cur_key_suffix_ = n_->ksuf(kp);
  return true;
}

//huanchen-static-scan-multivalue
//**********************************************************************************
// stcursor_scan_multivalue::next_item_from_next_node
//**********************************************************************************
template <typename P>
inline bool stcursor_scan_multivalue<P>::next_item_from_next_node(int kp) {
  int keylenx = 0;
  int stack_size = nodeTrace_.size();
  if (stack_size == 0)
    return false;
  n_ = nodeTrace_[stack_size - 1];
  kp = posTrace_[stack_size - 1] + 1;
  nodeTrace_.pop_back();
  posTrace_.pop_back();
  cur_key_prefix_.pop_back();
  while (kp >= n_->nkeys_) {
    stack_size = nodeTrace_.size();
    if (stack_size == 0)
      return false;
    n_ = nodeTrace_[stack_size - 1];
    kp = posTrace_[stack_size - 1] + 1;
    nodeTrace_.pop_back();
    posTrace_.pop_back();
    cur_key_prefix_.pop_back();
  }

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    while (kp >= n_->nkeys_) {
      stack_size = nodeTrace_.size();
      if (stack_size == 0)
	return false;
      n_ = nodeTrace_[stack_size - 1];
      kp = posTrace_[stack_size - 1] + 1;
      nodeTrace_.pop_back();
      posTrace_.pop_back();
      cur_key_prefix_.pop_back();
    }
  }

  cur_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode_multivalue<P>*>(cur_lv_->layer());
    return find_leftmost();
  }
  cur_key_suffix_ = n_->ksuf(kp);
  return true;
}


//huanchen-static-scan-dynamicvalue
//**********************************************************************************
// stcursor_scan_dynamicvalue::next_item_from_next_node
//**********************************************************************************
template <typename P>
inline bool stcursor_scan_dynamicvalue<P>::next_item_from_next_node(int kp) {
  int keylenx = 0;
  int stack_size = nodeTrace_.size();
  if (stack_size == 0)
    return false;
  n_ = nodeTrace_[stack_size - 1];
  kp = posTrace_[stack_size - 1] + 1;
  nodeTrace_.pop_back();
  posTrace_.pop_back();
  cur_key_prefix_.pop_back();
  while (kp >= n_->nkeys_) {
    stack_size = nodeTrace_.size();
    if (stack_size == 0)
      return false;
    n_ = nodeTrace_[stack_size - 1];
    kp = posTrace_[stack_size - 1] + 1;
    nodeTrace_.pop_back();
    posTrace_.pop_back();
    cur_key_prefix_.pop_back();
  }

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    while (kp >= n_->nkeys_) {
      stack_size = nodeTrace_.size();
      if (stack_size == 0)
	return false;
      n_ = nodeTrace_[stack_size - 1];
      kp = posTrace_[stack_size - 1] + 1;
      nodeTrace_.pop_back();
      posTrace_.pop_back();
      cur_key_prefix_.pop_back();
    }
  }

  cur_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode_dynamicvalue<P>*>(cur_lv_->layer());
    return find_leftmost();
  }
  cur_key_suffix_ = n_->ksuf(kp);
  return true;
}




//huanchen-static-scan
//**********************************************************************************
// stcursor_scan::find_next
//**********************************************************************************
template <typename P>
bool stcursor_scan<P>::find_next() {
  if (!root_)
    return false;
  int kp, keylenx = 0;
  n_ = static_cast<massnode<P>*>(root_);
 nextNode:
  kp = lower_bound_binary();
  if (kp < 0)
    return false;

  cur_key_prefix_.push_back(n_->ikey(kp));
  next_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  
  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode<P>*>(cur_lv_->layer());
    goto nextNode;
  }
  if (!n_->isValid(kp))
    return false;
  cur_key_suffix_ = n_->ksuf(kp);

  return next_item_next(kp);
}

//huanchen-static-scan-multivalue
//**********************************************************************************
// stcursor_scan_multivalue::find_next
//**********************************************************************************
template <typename P>
bool stcursor_scan_multivalue<P>::find_next() {
  if (!root_)
    return false;
  int kp, keylenx = 0;
  n_ = static_cast<massnode_multivalue<P>*>(root_);
 nextNode:
  kp = lower_bound_binary();
  if (kp < 0)
    return false;

  cur_key_prefix_.push_back(n_->ikey(kp));
  next_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  
  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode_multivalue<P>*>(cur_lv_->layer());
    goto nextNode;
  }
  if (!n_->isValid(kp))
    return false;
  cur_key_suffix_ = n_->ksuf(kp);

  return next_item_next(kp);
}


//huanchen-static-scan-dynamicvalue
//**********************************************************************************
// stcursor_scan_dynamicvalue::find_next
//**********************************************************************************
template <typename P>
bool stcursor_scan_dynamicvalue<P>::find_next() {
  if (!root_)
    return false;
  int kp, keylenx = 0;
  n_ = static_cast<massnode_dynamicvalue<P>*>(root_);
 nextNode:
  kp = lower_bound_binary();
  if (kp < 0)
    return false;

  cur_key_prefix_.push_back(n_->ikey(kp));
  next_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  
  keylenx = n_->ikeylen(kp);
  cur_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    ka_.shift();
    n_ = static_cast<massnode_dynamicvalue<P>*>(cur_lv_->layer());
    goto nextNode;
  }
  if (!n_->isValid(kp))
    return false;
  cur_key_suffix_ = n_->ksuf(kp);

  return next_item_next(kp);
}




//huanchen-static-scan
//**********************************************************************************
// stcursor_scan::find_next_leftmost
//**********************************************************************************
template <typename P>
bool stcursor_scan<P>::find_next_leftmost() {
  if (!n_)
    return false;
  int keylenx = 0;
 nextNode:
  next_key_prefix_.push_back(n_->ikey(0));
  keylenx = n_->ikeylen(0);
  next_lv_ = &(n_->get_lv()[0]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode<P>*>(next_lv_->layer());
    goto nextNode;
  }
  next_key_suffix_ = n_->ksuf(0);
  return true;
}

//huanchen-static-scan-multivalue
//**********************************************************************************
// stcursor_scan_multivalue::find_next_leftmost
//**********************************************************************************
template <typename P>
bool stcursor_scan_multivalue<P>::find_next_leftmost() {
  if (!n_)
    return false;
  int keylenx = 0;
 nextNode:
  next_key_prefix_.push_back(n_->ikey(0));
  keylenx = n_->ikeylen(0);
  next_lv_ = &(n_->get_lv()[0]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode_multivalue<P>*>(next_lv_->layer());
    goto nextNode;
  }
  next_key_suffix_ = n_->ksuf(0);
  return true;
}

//huanchen-static-scan-dynamicvalue
//**********************************************************************************
// stcursor_scan_dynamicvalue::find_next_leftmost
//**********************************************************************************
template <typename P>
bool stcursor_scan_dynamicvalue<P>::find_next_leftmost() {
  if (!n_)
    return false;
  int keylenx = 0;
 nextNode:
  next_key_prefix_.push_back(n_->ikey(0));
  keylenx = n_->ikeylen(0);
  next_lv_ = &(n_->get_lv()[0]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode_dynamicvalue<P>*>(next_lv_->layer());
    goto nextNode;
  }
  next_key_suffix_ = n_->ksuf(0);
  return true;
}



//huanchen-static-scan
//**********************************************************************************
// stcursor_scan::next_item_next
//**********************************************************************************
template <typename P>
inline bool stcursor_scan<P>::next_item_next(int kp) {
  int keylenx = 0;
  kp++;
  next_key_prefix_.pop_back();
  //out of bound, go back to parent
  if (kp >= n_->nkeys_)
    return next_item_from_next_node_next(kp);

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    if (kp >= n_->nkeys_)
      return next_item_from_next_node_next(kp);
  }

  posTrace_[posTrace_.size() - 1]++;
  next_key_prefix_.push_back(n_->ikey(kp));
  keylenx = n_->ikeylen(kp);
  next_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode<P>*>(next_lv_->layer());
    return find_next_leftmost();
  }
  next_key_suffix_ = n_->ksuf(kp);
  return true;
}

//huanchen-static-scan-multivalue
//**********************************************************************************
// stcursor_scan_multivalue::next_item_next
//**********************************************************************************
template <typename P>
inline bool stcursor_scan_multivalue<P>::next_item_next(int kp) {
  int keylenx = 0;
  kp++;
  next_key_prefix_.pop_back();
  //out of bound, go back to parent
  if (kp >= n_->nkeys_)
    return next_item_from_next_node_next(kp);

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    if (kp >= n_->nkeys_)
      return next_item_from_next_node_next(kp);
  }

  posTrace_[posTrace_.size() - 1]++;
  next_key_prefix_.push_back(n_->ikey(kp));
  keylenx = n_->ikeylen(kp);
  next_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode_multivalue<P>*>(next_lv_->layer());
    return find_next_leftmost();
  }
  next_key_suffix_ = n_->ksuf(kp);

  return true;
}

//huanchen-static-scan-dynamicvalue
//**********************************************************************************
// stcursor_scan_dynamicvalue::next_item_next
//**********************************************************************************
template <typename P>
inline bool stcursor_scan_dynamicvalue<P>::next_item_next(int kp) {
  int keylenx = 0;
  kp++;
  next_key_prefix_.pop_back();
  //out of bound, go back to parent
  if (kp >= n_->nkeys_)
    return next_item_from_next_node_next(kp);

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    if (kp >= n_->nkeys_)
      return next_item_from_next_node_next(kp);
  }

  posTrace_[posTrace_.size() - 1]++;
  next_key_prefix_.push_back(n_->ikey(kp));
  keylenx = n_->ikeylen(kp);
  next_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode_dynamicvalue<P>*>(next_lv_->layer());
    return find_next_leftmost();
  }
  next_key_suffix_ = n_->ksuf(kp);

  return true;
}



//huanchen-static-scan
//**********************************************************************************
// stcursor_scan::next_item_from_next_node_next
//**********************************************************************************
template <typename P>
bool stcursor_scan<P>::next_item_from_next_node_next(int kp) {
  int keylenx = 0;
  int stack_size = nodeTrace_.size();
  if (stack_size == 0)
    return false;
  n_ = nodeTrace_[stack_size - 1];
  kp = posTrace_[stack_size - 1] + 1;
  nodeTrace_.pop_back();
  posTrace_.pop_back();
  next_key_prefix_.pop_back();
  while (kp >= n_->nkeys_) {
    stack_size = nodeTrace_.size();
    if (stack_size == 0)
      return false;
    n_ = nodeTrace_[stack_size - 1];
    kp = posTrace_[stack_size - 1] + 1;
    nodeTrace_.pop_back();
    posTrace_.pop_back();
    next_key_prefix_.pop_back();
  }

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    while (kp >= n_->nkeys_) {
      stack_size = nodeTrace_.size();
      if (stack_size == 0)
	return false;
      n_ = nodeTrace_[stack_size - 1];
      kp = posTrace_[stack_size - 1] + 1;
      nodeTrace_.pop_back();
      posTrace_.pop_back();
      next_key_prefix_.pop_back();
    }
  }

  next_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  keylenx = n_->ikeylen(kp);
  next_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode<P>*>(next_lv_->layer());
    return find_next_leftmost();
  }
  next_key_suffix_ = n_->ksuf(kp);
  return true;
}

//huanchen-static-scan-multivalue
//**********************************************************************************
// stcursor_scan_multivalue::next_item_from_next_node_next
//**********************************************************************************
template <typename P>
bool stcursor_scan_multivalue<P>::next_item_from_next_node_next(int kp) {
  int keylenx = 0;
  int stack_size = nodeTrace_.size();
  if (stack_size == 0)
    return false;
  n_ = nodeTrace_[stack_size - 1];
  kp = posTrace_[stack_size - 1] + 1;
  nodeTrace_.pop_back();
  posTrace_.pop_back();
  next_key_prefix_.pop_back();
  while (kp >= n_->nkeys_) {
    stack_size = nodeTrace_.size();
    if (stack_size == 0)
      return false;
    n_ = nodeTrace_[stack_size - 1];
    kp = posTrace_[stack_size - 1] + 1;
    nodeTrace_.pop_back();
    posTrace_.pop_back();
    next_key_prefix_.pop_back();
  }

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    while (kp >= n_->nkeys_) {
      stack_size = nodeTrace_.size();
      if (stack_size == 0)
	return false;
      n_ = nodeTrace_[stack_size - 1];
      kp = posTrace_[stack_size - 1] + 1;
      nodeTrace_.pop_back();
      posTrace_.pop_back();
      next_key_prefix_.pop_back();
    }
  }

  next_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  keylenx = n_->ikeylen(kp);
  next_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode_multivalue<P>*>(next_lv_->layer());
    return find_next_leftmost();
  }
  next_key_suffix_ = n_->ksuf(kp);

  return true;
}

//huanchen-static-scan-dynamicvalue
//**********************************************************************************
// stcursor_scan_dynamicvalue::next_item_from_next_node_next
//**********************************************************************************
template <typename P>
bool stcursor_scan_dynamicvalue<P>::next_item_from_next_node_next(int kp) {
  int keylenx = 0;
  int stack_size = nodeTrace_.size();
  if (stack_size == 0)
    return false;
  n_ = nodeTrace_[stack_size - 1];
  kp = posTrace_[stack_size - 1] + 1;
  nodeTrace_.pop_back();
  posTrace_.pop_back();
  next_key_prefix_.pop_back();
  while (kp >= n_->nkeys_) {
    stack_size = nodeTrace_.size();
    if (stack_size == 0)
      return false;
    n_ = nodeTrace_[stack_size - 1];
    kp = posTrace_[stack_size - 1] + 1;
    nodeTrace_.pop_back();
    posTrace_.pop_back();
    next_key_prefix_.pop_back();
  }

  //find the next valid kp
  while (!n_->isValid(kp)) {
    kp++;
    while (kp >= n_->nkeys_) {
      stack_size = nodeTrace_.size();
      if (stack_size == 0)
	return false;
      n_ = nodeTrace_[stack_size - 1];
      kp = posTrace_[stack_size - 1] + 1;
      nodeTrace_.pop_back();
      posTrace_.pop_back();
      next_key_prefix_.pop_back();
    }
  }

  next_key_prefix_.push_back(n_->ikey(kp));
  nodeTrace_.push_back(n_);
  posTrace_.push_back(kp);
  keylenx = n_->ikeylen(kp);
  next_lv_ = &(n_->get_lv()[kp]);
  if (n_->keylenx_is_layer(keylenx)) {
    n_ = static_cast<massnode_dynamicvalue<P>*>(next_lv_->layer());
    return find_next_leftmost();
  }
  next_key_suffix_ = n_->ksuf(kp);

  return true;
}



//huanchen-static-scan
//**********************************************************************************
// stcursor_scan::lower_bound_binary
//**********************************************************************************
template <typename P>
inline int stcursor_scan<P>::lower_bound_binary() const {
  int l = 0;
  int r = n_->nkeys_;
  while (l < r) {
    int m = (l + r) >> 1;
    int cmp = key_compare(ka_, *n_, m);
    if (cmp < 0)
      r = m;
    else if (cmp == 0)
      return m;
    else
      l = m + 1;
  }
  return -1;
}

//huanchen-static-scan-multivalue
//**********************************************************************************
// stcursor_scan_multivalue::lower_bound_binary
//**********************************************************************************
template <typename P>
inline int stcursor_scan_multivalue<P>::lower_bound_binary() const {
  int l = 0;
  int r = n_->nkeys_;
  while (l < r) {
    int m = (l + r) >> 1;
    int cmp = key_compare(ka_, *n_, m);
    if (cmp < 0)
      r = m;
    else if (cmp == 0)
      return m;
    else
      l = m + 1;
  }
  return -1;
}

//huanchen-static-scan-dynamicvalue
//**********************************************************************************
// stcursor_scan_dynamicvalue::lower_bound_binary
//**********************************************************************************
template <typename P>
inline int stcursor_scan_dynamicvalue<P>::lower_bound_binary() const {
  int l = 0;
  int r = n_->nkeys_;
  while (l < r) {
    int m = (l + r) >> 1;
    int cmp = key_compare(ka_, *n_, m);
    if (cmp < 0)
      r = m;
    else if (cmp == 0)
      return m;
    else
      l = m + 1;
  }
  return -1;
}


//huanchen-static-scan
//**********************************************************************************
// stcursor_scan::upper_bound_or_equal_binary
//**********************************************************************************
template <typename P>
inline int stcursor_scan<P>::upper_bound_or_equal_binary() {
  isExact_ = true;
  int l = 0;
  int r = n_->nkeys_;
  while (l < r) {
    int m = (l + r) >> 1;
    int cmp = key_compare(ka_, *n_, m);
    if (cmp < 0)
      r = m;
    else if (cmp == 0)
      return m;
    else
      l = m + 1;
  }
  isExact_ = false;
  return r;
}

//huanchen-static-scan-multivalue
//**********************************************************************************
// stcursor_scan_multivalue::upper_bound_or_equal_binary
//**********************************************************************************
template <typename P>
inline int stcursor_scan_multivalue<P>::upper_bound_or_equal_binary() {
  isExact_ = true;
  int l = 0;
  int r = n_->nkeys_;
  while (l < r) {
    int m = (l + r) >> 1;
    int cmp = key_compare(ka_, *n_, m);
    if (cmp < 0)
      r = m;
    else if (cmp == 0)
      return m;
    else
      l = m + 1;
  }
  isExact_ = false;
  return r;
}

//huanchen-static-scan-dynamicvalue
//**********************************************************************************
// stcursor_scan_dynamicvalue::upper_bound_or_equal_binary
//**********************************************************************************
template <typename P>
inline int stcursor_scan_dynamicvalue<P>::upper_bound_or_equal_binary() {
  isExact_ = true;
  int l = 0;
  int r = n_->nkeys_;
  while (l < r) {
    int m = (l + r) >> 1;
    int cmp = key_compare(ka_, *n_, m);
    if (cmp < 0)
      r = m;
    else if (cmp == 0)
      return m;
    else
      l = m + 1;
  }
  isExact_ = false;
  return r;
}



//huanchen-static-merge
//**********************************************************************************
// stcursor_merge::merge_nodes
//**********************************************************************************
template <typename P>
bool stcursor_merge<P>::merge_nodes(merge_task t, threadinfo &ti, threadinfo &ti_merge) {
  //std::cout << "merge_nodes start\n";
  /*
  if ((t.m == NULL) || (t.n == NULL)) {
    std::cout << "ERROR: merge_node, node m or n is NULL!!!\n";
    return false;
  }

  m_ = t.m;
  n_ = t.n;
  */

  if (t.m == NULL) {
    std::cout << "ERROR: merge_node, node m is NULL!!!\n";
    return false;
  }
  m_ = t.m;

  if (t.n == NULL) {
    root_ = m_;
    return true;
  }
  else
    n_ = t.n;

  //calculate size & num_keys of m, n and the tmp new node
  int m_size = m_->allocated_size();
  int n_size = n_->allocated_size();
  int new_max_size; 
  int m_nkeys = m_->size();
  int n_nkeys = n_->size();
  int new_max_nkeys = m_nkeys + n_nkeys;

  bool hasKsuf = (m_->has_ksuf() || n_->has_ksuf());

  if (hasKsuf)
    new_max_size = m_size + n_size - sizeof(massnode<P>) - sizeof(uint32_t);
  else
    new_max_size = m_size + n_size - sizeof(massnode<P>);

  //resize(expand) n
  n_ = n_->resize((size_t)new_max_size, ti);
  //n_->set_allocated_size((size_t)new_max_size);

  //calculate the start position offsets of each array in m, n and tmp new
  int new_ikeylen_startpos = sizeof(massnode<P>);
  int n_ikeylen_startpos = sizeof(massnode<P>);
  int m_ikeylen_startpos = sizeof(massnode<P>);
  int new_ikeylen_len = (int)(sizeof(uint8_t) * new_max_nkeys);
  int n_ikeylen_len = (int)(sizeof(uint8_t) * n_nkeys);
  int m_ikeylen_len = (int)(sizeof(uint8_t) * m_nkeys);

  int new_ikey_startpos = new_ikeylen_startpos + new_ikeylen_len;
  int n_ikey_startpos = n_ikeylen_startpos + n_ikeylen_len;
  int m_ikey_startpos = m_ikeylen_startpos + m_ikeylen_len;
  int new_ikey_len = (int)(sizeof(ikey_type) * new_max_nkeys);
  int n_ikey_len = (int)(sizeof(ikey_type) * n_nkeys);
  int m_ikey_len = (int)(sizeof(ikey_type) * m_nkeys);

  int new_lv_startpos = new_ikey_startpos + new_ikey_len;
  int n_lv_startpos = n_ikey_startpos + n_ikey_len;
  int m_lv_startpos = m_ikey_startpos + m_ikey_len;
  int new_lv_len = (int)(sizeof(leafvalue_static<P>) * new_max_nkeys);
  int n_lv_len = (int)(sizeof(leafvalue_static<P>) * n_nkeys);
  int m_lv_len = (int)(sizeof(leafvalue_static<P>) * m_nkeys);

  int new_ksuf_offset_startpos = new_lv_startpos + new_lv_len;
  int n_ksuf_offset_startpos = n_lv_startpos + n_lv_len;
  int m_ksuf_offset_startpos = m_lv_startpos + m_lv_len;
  int new_ksuf_offset_len = (int)(sizeof(uint32_t) * (new_max_nkeys + 1));
  int n_ksuf_offset_len = (int)(sizeof(uint32_t) * (n_nkeys + 1));
  int m_ksuf_offset_len = (int)(sizeof(uint32_t) * (m_nkeys + 1));

  int new_ksuf_startpos = new_ksuf_offset_startpos + new_ksuf_offset_len;
  int n_ksuf_startpos = n_ksuf_offset_startpos + n_ksuf_offset_len;
  int m_ksuf_startpos = m_ksuf_offset_startpos + m_ksuf_offset_len;
  int new_ksuf_len = new_max_size - new_ksuf_startpos;
  int n_ksuf_len = n_size - n_ksuf_startpos;
  int m_ksuf_len = m_size - m_ksuf_startpos;

  //calculate the start position offset of moved arrays in n
  int new_n_ikeylen_startpos = new_ikeylen_startpos + new_ikeylen_len - n_ikeylen_len;
  int new_n_ikey_startpos = new_ikey_startpos + new_ikey_len - n_ikey_len;
  int new_n_lv_startpos = new_lv_startpos + new_lv_len - n_lv_len;
  int new_n_ksuf_offset_startpos = new_ksuf_offset_startpos + new_ksuf_offset_len - n_ksuf_offset_len;
  int new_n_ksuf_startpos = new_ksuf_startpos + new_ksuf_len - n_ksuf_len;

  //move the arrays in n and prepare for merging
  uint8_t* m_ikeylen = (uint8_t*)((char*)m_ + m_ikeylen_startpos);
  ikey_type* m_ikey = (ikey_type*)((char*)m_ + m_ikey_startpos);
  leafvalue_static<P>* m_lv = (leafvalue_static<P>*)((char*)m_ + m_lv_startpos);
  uint32_t* m_ksuf_offset = (uint32_t*)((char*)m_ + m_ksuf_offset_startpos);
  char* m_ksuf = (char*)((char*)m_ + m_ksuf_startpos);

  uint8_t* n_ikeylen = (uint8_t*)((char*)n_ + new_n_ikeylen_startpos);
  ikey_type* n_ikey = (ikey_type*)((char*)n_ + new_n_ikey_startpos);
  leafvalue_static<P>* n_lv = (leafvalue_static<P>*)((char*)n_ + new_n_lv_startpos);
  uint32_t* n_ksuf_offset = (uint32_t*)((char*)n_ + new_n_ksuf_offset_startpos);
  char* n_ksuf = (char*)((char*)n_ + new_n_ksuf_startpos);

  uint8_t* new_n_ikeylen = (uint8_t*)((char*)n_ + new_ikeylen_startpos);
  ikey_type* new_n_ikey = (ikey_type*)((char*)n_ + new_ikey_startpos);
  leafvalue_static<P>* new_n_lv = (leafvalue_static<P>*)((char*)n_ + new_lv_startpos);
  uint32_t* new_n_ksuf_offset = (uint32_t*)((char*)n_ + new_ksuf_offset_startpos);
  char* new_n_ksuf = (char*)((char*)n_ + new_ksuf_startpos);

  if ((n_->ikey(n_nkeys - 1)) < m_ikey[0]) {
    //std::cout << "merge array\n";
    if (hasKsuf) {
      memmove((void*)(new_n_ksuf), (void*)((char*)n_ + n_ksuf_startpos), n_ksuf_len);
      memmove((void*)(new_n_ksuf_offset), (void*)((char*)n_ + n_ksuf_offset_startpos), n_ksuf_offset_len);
    }
    memmove((void*)(new_n_lv), (void*)((char*)n_ + n_lv_startpos), n_lv_len);
    memmove((void*)(new_n_ikey), (void*)((char*)n_ + n_ikey_startpos), n_ikey_len);

    if (hasKsuf) {
      for (int i = 0; i <= m_nkeys; i++)
	m_ksuf_offset[i] += new_n_ksuf_offset[n_nkeys];
    }
    
    memcpy((void*)((char*)new_n_ikeylen + n_ikeylen_len), (void*)(m_ikeylen), m_ikeylen_len);
    memcpy((void*)((char*)new_n_ikey + n_ikey_len), (void*)(m_ikey), m_ikey_len);
    memcpy((void*)((char*)new_n_lv + n_lv_len), (void*)(m_lv), m_lv_len);

    if (hasKsuf) {
      memcpy((void*)((char*)new_n_ksuf_offset + n_ksuf_offset_len - sizeof(uint32_t)), (void*)(m_ksuf_offset), m_ksuf_offset_len);
      memcpy((void*)((char*)new_n_ksuf + n_ksuf_len), (void*)(m_ksuf), m_ksuf_len);
    }
    //delete m
    m_->deallocate(ti_merge);

    n_->set_size((uint32_t)new_max_nkeys);
    n_->set_allocated_size((size_t)new_max_size);
    if (hasKsuf)
      n_->set_has_ksuf((uint8_t)1);
    else
      n_->set_has_ksuf((uint8_t)0);
    
    if (t.parent_node)
      t.parent_node->set_lv(t.parent_node_pos, leafvalue_static<P>(static_cast<node_base<P>*>(n_)));
    else
      root_ = n_;
    
    //std::cout << "merge_node success\n";
    return true;
  }

  if (hasKsuf) {
    memmove((void*)(n_ksuf), (void*)((char*)n_ + n_ksuf_startpos), n_ksuf_len);
    memmove((void*)(n_ksuf_offset), (void*)((char*)n_ + n_ksuf_offset_startpos), n_ksuf_offset_len);
  }
  memmove((void*)(n_lv), (void*)((char*)n_ + n_lv_startpos), n_lv_len);
  memmove((void*)(n_ikey), (void*)((char*)n_ + n_ikey_startpos), n_ikey_len);
  memmove((void*)(n_ikeylen), (void*)((char*)n_ + n_ikeylen_startpos), n_ikeylen_len);

  //merge
  //---------------------------------------------------------------------------------
  //std::cout << "proceeded to merge\n";
  int m_pos = 0;
  int n_pos = 0;
  int new_n_pos = 0;

  char* new_n_ksuf_pos = new_n_ksuf;
  char* m_ksuf_pos = m_ksuf;
  char* n_ksuf_pos = n_ksuf;

  int copy_ksuf_len = 0;

  int start_task_pos = task_.size();
  /*
  if (n_ikey[n_nkeys-1] < m_ikey[0]) {
    //std::cout << "merge array\n";
    for (int i = 0; i <= m_nkeys; i++)
      m_ksuf_offset[i] += n_ksuf_offset[n_nkeys];

    memmove((void*)(new_n_ikeylen), (void*)(n_ikeylen), n_ikeylen_len);
    memcpy((void*)((char*)new_n_ikeylen + n_ikeylen_len), (void*)(m_ikeylen), m_ikeylen_len);
    memmove((void*)(new_n_ikey), (void*)(n_ikey), n_ikey_len);
    memcpy((void*)((char*)new_n_ikey + n_ikey_len), (void*)(m_ikey), m_ikey_len);
    memmove((void*)(new_n_lv), (void*)(n_lv), n_lv_len);
    memcpy((void*)((char*)new_n_lv + n_lv_len), (void*)(m_lv), m_lv_len);
    memmove((void*)(new_n_ksuf_offset), (void*)(n_ksuf_offset), n_ksuf_offset_len - sizeof(uint32_t));
    memcpy((void*)((char*)new_n_ksuf_offset + n_ksuf_offset_len - sizeof(uint32_t)), (void*)(m_ksuf_offset), m_ksuf_offset_len);
    memmove((void*)(new_n_ksuf), (void*)(n_ksuf), n_ksuf_len);
    memcpy((void*)((char*)new_n_ksuf + n_ksuf_len), (void*)(m_ksuf), m_ksuf_len);

    //delete m
    m_->deallocate(ti_merge);

    n_->set_size((uint32_t)new_max_nkeys);
    n_->set_allocated_size((size_t)new_max_size);
    
    if (t.parent_node)
      t.parent_node->set_lv(t.parent_node_pos, leafvalue_static<P>(static_cast<node_base<P>*>(n_)));
    else
      root_ = n_;
    
    //std::cout << "merge_node success\n";
    return true;
  }
  */
  while ((m_pos < m_nkeys) && (n_pos < n_nkeys)) {
    //if deleted
    if (n_ikeylen[n_pos] == 0) {
      //std::cout << "item deleted; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
      if (hasKsuf)
	n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
      n_pos++;
    }
    else {
      uint8_t m_ikey_length = m_->keylenx_ikeylen(m_ikeylen[m_pos]);
      uint8_t n_ikey_length = n_->keylenx_ikeylen(n_ikeylen[n_pos]);
      if (m_ikey[m_pos] < n_ikey[n_pos]
	  || ((m_ikey[m_pos] == n_ikey[n_pos]) && (m_ikey_length < n_ikey_length))) { 
	//std::cout << "item_m inserted; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	//move an item from m to the new array
	new_n_ikeylen[new_n_pos] = m_ikeylen[m_pos];
	new_n_ikey[new_n_pos] = m_ikey[m_pos];
	new_n_lv[new_n_pos] = m_lv[m_pos];

	if (hasKsuf) {
	  copy_ksuf_len = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
	  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);
	  
	  if (copy_ksuf_len < 0) {
	    std::cout << "ERROR: merge_node1, COPY_LENGTH < 0!!!\n";
	    return false;
	  }
	  
	  if (copy_ksuf_len > 0)
	    memcpy(new_n_ksuf_pos, m_ksuf_pos, copy_ksuf_len);
	}

	new_n_pos++;
	m_pos++;
	if (hasKsuf) {
	  new_n_ksuf_pos += copy_ksuf_len;
	  m_ksuf_pos += copy_ksuf_len;
	}
      }
      else if (m_ikey[m_pos] > n_ikey[n_pos]
	       || ((m_ikey[m_pos] == n_ikey[n_pos]) && (m_ikey_length > n_ikey_length))) { 
	//std::cout << "item_n inserted, m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	//move an item from n to the new array
	new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	new_n_ikey[new_n_pos] = n_ikey[n_pos];
	new_n_lv[new_n_pos] = n_lv[n_pos];

	if (hasKsuf) {
	  copy_ksuf_len = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);
	  
	  if (copy_ksuf_len < 0) {
	    std::cout << "ERROR: merge_node2, COPY_LENGTH < 0!!!\n";
	    return false;
	  }
	  
	  if (copy_ksuf_len > 0)
	    memmove(new_n_ksuf_pos, n_ksuf_pos, copy_ksuf_len);
	}

	new_n_pos++;
	n_pos++;
	if (hasKsuf) {
	  new_n_ksuf_pos += copy_ksuf_len;
	  n_ksuf_pos += copy_ksuf_len;
	}
      }
      else { //same keyslice, same ikey length
	if (m_->keylenx_is_layer(m_ikeylen[m_pos]) && n_->keylenx_is_layer(n_ikeylen[n_pos])) {
	  //std::cout << "both layers; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	  //if m_pos is layer AND n_pos is layer
	  new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	  new_n_ikey[new_n_pos] = n_ikey[n_pos];
	  //lv TBD
	  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	  merge_task tt;
	  tt.task = 0; //merge node m to n
	  tt.parent_node = n_;
	  tt.parent_node_pos = new_n_pos;
	  tt.m = static_cast<massnode<P>*>(m_lv[m_pos].layer());
	  tt.n = static_cast<massnode<P>*>(n_lv[n_pos].layer());
	  task_.push_back(tt);
	}
	else if (n_->keylenx_is_layer(n_ikeylen[n_pos])) {
	  //std::cout << "n layer; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	  //if n_pos is layer
	  new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	  new_n_ikey[new_n_pos] = n_ikey[n_pos];
	  //lv TBD
	  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	  merge_task tt;
	  tt.task = 1; //merge item_m to n
	  tt.parent_node = n_;
	  tt.parent_node_pos = new_n_pos;
	  tt.n = static_cast<massnode<P>*>(n_lv[n_pos].layer());

	  tt.lv_m = m_lv[m_pos];
	  if (hasKsuf)
	    tt.ksuf_len_m = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
	  //assign next layer ikeylen
	  tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	  //make next layer ikey
	  tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(m_ksuf_pos, tt.ksuf_len_m);
	    
	  //make next layer suffix
	  if (tt.ksuf_len_m > sizeof(ikey_type)) {
	    tt.ksuf_len_m -= sizeof(ikey_type);
	    tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	    memcpy((void*)tt.ksuf_m, (const void*)(m_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_m);
	  }
	  else {
	    tt.ksuf_m = 0;
	    tt.ksuf_len_m = 0;
	  }

	  task_.push_back(tt);
	  m_ksuf_pos += (m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos]);
	}
	else if (m_->keylenx_is_layer(m_ikeylen[m_pos])) {
	  //std::cout << "m layer; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	  //if m_pos is layer
	  new_n_ikeylen[new_n_pos] = m_ikeylen[m_pos];
	  new_n_ikey[new_n_pos] = m_ikey[m_pos];
	  //lv TBD
	  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	  merge_task tt;
	  tt.task = 1; //merge item m to n
	  tt.parent_node = n_;
	  tt.parent_node_pos = new_n_pos;
	  tt.n = static_cast<massnode<P>*>(m_lv[m_pos].layer());

	  tt.lv_m = n_lv[n_pos];
	  tt.ksuf_len_m = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	  //assign next layer ikeylen
	  tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	  //make next layer ikey
	  tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(n_ksuf_pos, tt.ksuf_len_m);
	    
	  //make next layer suffix
	  if (tt.ksuf_len_m > sizeof(ikey_type)) {
	    tt.ksuf_len_m -= sizeof(ikey_type);
	    tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	    memcpy((void*)tt.ksuf_m, (const void*)(n_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_m);
	  }
	  else {
	    tt.ksuf_m = 0;
	    tt.ksuf_len_m = 0;
	  }

	  task_.push_back(tt);
	  n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
	}
	else {
	  //std::cout << "both NOT layer; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	  //if neither m_pos nor n_pos is layer
	  int ksuflen_m = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
	  int ksuflen_n = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	  if ((ksuflen_m == 0) && (ksuflen_n == 0)) {
	    new_n_ikeylen[new_n_pos] = m_ikeylen[m_pos];
	    new_n_ikey[new_n_pos] = m_ikey[m_pos];
	    new_n_lv[new_n_pos] = m_lv[m_pos];
	    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);
	  }
	  else if ((ksuflen_m == ksuflen_n) 
		   && (strncmp(m_ksuf_pos, n_ksuf_pos, ksuflen_m) == 0)) {
	    new_n_ikeylen[new_n_pos] = m_ikeylen[m_pos];
	    new_n_ikey[new_n_pos] = m_ikey[m_pos];
	    new_n_lv[new_n_pos] = m_lv[m_pos];

	    copy_ksuf_len = ksuflen_m;
	    memcpy(new_n_ksuf_pos, m_ksuf_pos, copy_ksuf_len);

	    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	    new_n_ksuf_pos += copy_ksuf_len;
	    m_ksuf_pos += copy_ksuf_len;
	    n_ksuf_pos += copy_ksuf_len;
	  }
	  else {
	    new_n_ikeylen[new_n_pos] = (uint8_t)(n_ikeylen[n_pos] + (uint8_t)64);
	    new_n_ikey[new_n_pos] = n_ikey[n_pos];
	    //lv TBD
	    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	    merge_task tt;
	    tt.task = 2; //create a new node to include m and n
	    tt.parent_node = n_;
	    tt.parent_node_pos = new_n_pos;

	    tt.lv_m = m_lv[m_pos];
	    tt.ksuf_len_m = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
	    //assign m next layer ikeylen
	    tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	    //make m next layer ikey
	    tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(m_ksuf_pos, tt.ksuf_len_m);
	    
	    //make m next layer suffix
	    if (tt.ksuf_len_m > sizeof(ikey_type)) {
	      tt.ksuf_len_m -= sizeof(ikey_type);
	      tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	      memcpy((void*)tt.ksuf_m, (const void*)(m_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_m);
	    }
	    else {
	      tt.ksuf_m = 0;
	      tt.ksuf_len_m = 0;
	    }


	    tt.lv_n = n_lv[n_pos];
	    tt.ksuf_len_n = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	    //assign n next layer ikeylen
	    tt.ikeylen_n = convert_to_ikeylen(tt.ksuf_len_n);
	    //make n next layer ikey
	    tt.ikey_n = string_slice<ikey_type>::make_comparable_sloppy(n_ksuf_pos, tt.ksuf_len_n);
	    
	    //make n next layer suffix
	    if (tt.ksuf_len_n > sizeof(ikey_type)) {
	      tt.ksuf_len_n -= sizeof(ikey_type);
	      tt.ksuf_n = (char*)malloc(tt.ksuf_len_n + 1);
	      memcpy((void*)tt.ksuf_n, (const void*)(n_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_n);
	    }
	    else {
	      tt.ksuf_n = 0;
	      tt.ksuf_len_n = 0;
	    }

	    task_.push_back(tt);
	    m_ksuf_pos += (m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos]);
	    n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
	  }
	}

	new_n_pos++;
	m_pos++;
	n_pos++;
      } //same keyslice, same ikey length else end
    } //else end
  } //while end

  //if m has leftovers, move them to the new node
  while (m_pos < m_nkeys) {
    //std::cout << "m leftovers; m_pos = " << m_pos <<"\n";
    new_n_ikeylen[new_n_pos] = m_ikeylen[m_pos];
    new_n_ikey[new_n_pos] = m_ikey[m_pos];
    new_n_lv[new_n_pos] = m_lv[m_pos];
    
    if (hasKsuf) {
      copy_ksuf_len = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
      new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);
      
      if (copy_ksuf_len < 0) {
	std::cout << "ERROR: merge_node3, COPY_LENGTH < 0!!!\n";
	return false;
      }
      
      if (copy_ksuf_len > 0)
	memcpy(new_n_ksuf_pos, m_ksuf_pos, copy_ksuf_len);
    }

    new_n_pos++;
    m_pos++;
    if (hasKsuf) {
      new_n_ksuf_pos += copy_ksuf_len;
      m_ksuf_pos += copy_ksuf_len;    
    }
  }

  //if n has leftovers, shift them to the new positions
  while (n_pos < n_nkeys) {
    //std::cout << "n leftovers; n_pos = " << n_pos << "\n";
    new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
    new_n_ikey[new_n_pos] = n_ikey[n_pos];
    new_n_lv[new_n_pos] = n_lv[n_pos];

    if (hasKsuf) {
      copy_ksuf_len = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
      new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);
      
      if (copy_ksuf_len < 0) {
	std::cout << "ERROR: merge_node4, COPY_LENGTH < 0!!!\n";
	return false;
      }
      
      if (copy_ksuf_len > 0)
	memmove(new_n_ksuf_pos, n_ksuf_pos, copy_ksuf_len);
    }

    new_n_pos++;
    n_pos++;
    if (hasKsuf) {
      new_n_ksuf_pos += copy_ksuf_len;
      n_ksuf_pos += copy_ksuf_len;
    }
  }

  //fill in the last ksuf_offset position
  if (hasKsuf)
    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

  //delete m
  m_->deallocate(ti_merge);

  //compact n if needed
  int new_nkeys = new_n_pos;
  int new_ikeylen_size = (int)(sizeof(uint8_t) * new_nkeys);
  int new_ikey_size = (int)(sizeof(ikey_type) * new_nkeys);
  int new_lv_size = (int)(sizeof(leafvalue_static<P>) * new_nkeys);
  int new_ksuf_offset_size = 0;
  int new_ksuf_size = 0;
  int new_size;

  if (hasKsuf) {
    new_ksuf_offset_size = (int)(sizeof(uint32_t) * (new_nkeys + 1));
    new_ksuf_size = new_n_ksuf_offset[new_n_pos];

    new_size = (int)(sizeof(massnode<P>)
		     + new_ikeylen_size
		     + new_ikey_size
		     + new_lv_size
		     + new_ksuf_offset_size
		     + new_ksuf_size);
  }
  else {
    new_size = (int)(sizeof(massnode<P>)
		     + new_ikeylen_size
		     + new_ikey_size
		     + new_lv_size);
  }

  if (new_nkeys > new_max_nkeys) {
    std::cout << "ERROR: new_nkeys > new_max_nkeys!!!\n";
    return false;
  }

  if (new_nkeys < new_max_nkeys) {
    //uint8_t* final_n_ikeylen = (uint8_t*)((char*)n_ + sizeof(massnode<P>));
    ikey_type* final_n_ikey = (ikey_type*)((char*)n_ + sizeof(massnode<P>)
					   + new_ikeylen_size);
    leafvalue_static<P>* final_n_lv = (leafvalue_static<P>*)((char*)n_ + sizeof(massnode<P>)
							     + new_ikeylen_size
							     + new_ikey_size);
    uint32_t* final_n_ksuf_offset = (uint32_t*)((char*)n_ + sizeof(massnode<P>)
						+ new_ikeylen_size
						+ new_ikey_size
						+ new_lv_size);
    char* final_n_ksuf = (char*)((char*)n_ + sizeof(massnode<P>)
				 + new_ikeylen_size
				 + new_ikey_size
				 + new_lv_size
				 + new_ksuf_offset_size);
  
    memmove((void*)final_n_ikey, (const void*)new_n_ikey, new_ikey_size);
    memmove((void*)final_n_lv, (const void*)new_n_lv, new_lv_size);
    memmove((void*)final_n_ksuf_offset, (const void*)new_n_ksuf_offset, new_ksuf_offset_size);
    memmove((void*)final_n_ksuf, (const void*)new_n_ksuf, new_ksuf_size);

    //resize(shrink) n
    n_->set_allocated_size((size_t)new_max_size);
    n_ = n_->resize((size_t)new_size, ti);

    //resize may change the address, update the parent nodes addr in task_
    for (unsigned int i = start_task_pos; i < task_.size(); i++)
      task_[i].parent_node = n_;
  }

  n_->set_size((uint32_t)new_nkeys);
  n_->set_allocated_size((size_t)new_size);
  if (hasKsuf)
    n_->set_has_ksuf((uint8_t)1);
  else
    n_->set_has_ksuf((uint8_t)0);

  if (t.parent_node)
    t.parent_node->set_lv(t.parent_node_pos, leafvalue_static<P>(static_cast<node_base<P>*>(n_)));
  else
    root_ = n_;

  //std::cout << "merge_node success\n";
  return true;
}

//huanchen-static-merge
//**********************************************************************************
// stcursor_merge::add_item_to_node
//**********************************************************************************
template <typename P>
bool stcursor_merge<P>::add_item_to_node(merge_task t, threadinfo &ti) {
  //std::cout << "add_item_to_node\n";
  if (t.n == NULL) {
    std::cout << "ERROR: add_item_to_node, node n is NULL!!!\n";
    return false;
  }

  n_ = t.n;

  int m_size = (int)(sizeof(uint8_t)
		     + sizeof(ikey_type)
		     + sizeof(leafvalue_static<P>)
		     + sizeof(uint32_t)
		     + t.ksuf_len_m);
  int n_size = n_->allocated_size();
  int new_max_size = m_size + n_size;
  int n_nkeys = n_->size();
  int new_max_nkeys = n_nkeys + 1;

  //resize(expand) n
  n_ = n_->resize((size_t)new_max_size, ti);
  n_->set_allocated_size((size_t)new_max_size);

  //calculate the start position offsets of each array in n and tmp new
  int new_ikeylen_startpos = sizeof(massnode<P>);
  int n_ikeylen_startpos = sizeof(massnode<P>);
  int new_ikeylen_len = (int)(sizeof(uint8_t) * new_max_nkeys);
  int n_ikeylen_len = (int)(sizeof(uint8_t) * n_nkeys);

  int new_ikey_startpos = new_ikeylen_startpos + new_ikeylen_len;
  int n_ikey_startpos = n_ikeylen_startpos + n_ikeylen_len;
  int new_ikey_len = (int)(sizeof(ikey_type) * new_max_nkeys);
  int n_ikey_len = (int)(sizeof(ikey_type) * n_nkeys);

  int new_lv_startpos = new_ikey_startpos + new_ikey_len;
  int n_lv_startpos = n_ikey_startpos + n_ikey_len;
  int new_lv_len = (int)(sizeof(leafvalue_static<P>) * new_max_nkeys);
  int n_lv_len = (int)(sizeof(leafvalue_static<P>) * n_nkeys);

  int new_ksuf_offset_startpos = new_lv_startpos + new_lv_len;
  int n_ksuf_offset_startpos = n_lv_startpos + n_lv_len;
  int new_ksuf_offset_len = (int)(sizeof(uint32_t) * (new_max_nkeys + 1));
  int n_ksuf_offset_len = (int)(sizeof(uint32_t) * (n_nkeys + 1));

  int new_ksuf_startpos = new_ksuf_offset_startpos + new_ksuf_offset_len;
  int n_ksuf_startpos = n_ksuf_offset_startpos + n_ksuf_offset_len;
  int new_ksuf_len = new_max_size - new_ksuf_startpos;
  int n_ksuf_len = n_size - n_ksuf_startpos;

  //calculate the start position offset of moved arrays in n
  int new_n_ikeylen_startpos = new_ikeylen_startpos + new_ikeylen_len - n_ikeylen_len;
  int new_n_ikey_startpos = new_ikey_startpos + new_ikey_len - n_ikey_len;
  int new_n_lv_startpos = new_lv_startpos + new_lv_len - n_lv_len;
  int new_n_ksuf_offset_startpos = new_ksuf_offset_startpos + new_ksuf_offset_len - n_ksuf_offset_len;
  int new_n_ksuf_startpos = new_ksuf_startpos + new_ksuf_len - n_ksuf_len;

  //move the arrays in n and prepare for merging
  uint8_t* n_ikeylen = (uint8_t*)((char*)n_ + new_n_ikeylen_startpos);
  ikey_type* n_ikey = (ikey_type*)((char*)n_ + new_n_ikey_startpos);
  leafvalue_static<P>* n_lv = (leafvalue_static<P>*)((char*)n_ + new_n_lv_startpos);
  uint32_t* n_ksuf_offset = (uint32_t*)((char*)n_ + new_n_ksuf_offset_startpos);
  char* n_ksuf = (char*)((char*)n_ + new_n_ksuf_startpos);

  uint8_t* new_n_ikeylen = (uint8_t*)((char*)n_ + new_ikeylen_startpos);
  ikey_type* new_n_ikey = (ikey_type*)((char*)n_ + new_ikey_startpos);
  leafvalue_static<P>* new_n_lv = (leafvalue_static<P>*)((char*)n_ + new_lv_startpos);
  uint32_t* new_n_ksuf_offset = (uint32_t*)((char*)n_ + new_ksuf_offset_startpos);
  char* new_n_ksuf = (char*)((char*)n_ + new_ksuf_startpos);

  memmove((void*)(n_ksuf), (void*)((char*)n_ + n_ksuf_startpos), n_ksuf_len);
  memmove((void*)(n_ksuf_offset), (void*)((char*)n_ + n_ksuf_offset_startpos), n_ksuf_offset_len);
  memmove((void*)(n_lv), (void*)((char*)n_ + n_lv_startpos), n_lv_len);
  memmove((void*)(n_ikey), (void*)((char*)n_ + n_ikey_startpos), n_ikey_len);
  memmove((void*)(n_ikeylen), (void*)((char*)n_ + n_ikeylen_startpos), n_ikeylen_len);

  //merge
  //---------------------------------------------------------------------------------
  bool m_inserted = false;
  int n_pos = 0;
  int new_n_pos = 0;
  int new_nkeys = new_max_nkeys;

  char* new_n_ksuf_pos = new_n_ksuf;
  char* n_ksuf_pos = n_ksuf;

  int copy_ksuf_len = 0;

  while (!m_inserted && (n_pos < n_nkeys)) {
    //if deleted
    if (n_ikeylen[n_pos] == 0) {
      //std::cout << "item deleted; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
      n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
      n_pos++;
      new_nkeys--;
    }
    else {
      uint8_t m_ikey_length = m_->keylenx_ikeylen(t.ikeylen_m);
      uint8_t n_ikey_length = n_->keylenx_ikeylen(n_ikeylen[n_pos]);
      if (t.ikey_m < n_ikey[n_pos]
	  || ((t.ikey_m == n_ikey[n_pos]) && (m_ikey_length < n_ikey_length))) { 
	//std::cout << "item_m inserted; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	//move item m to the new array
	new_n_ikeylen[new_n_pos] = t.ikeylen_m;
	new_n_ikey[new_n_pos] = t.ikey_m;
	new_n_lv[new_n_pos] = t.lv_m;

	copy_ksuf_len = t.ksuf_len_m;
	new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	if (copy_ksuf_len < 0) {
	  std::cout << "ERROR: add_item_to_node1, COPY_LENGTH < 0!!!\n";
	  return false;
	}

	if (copy_ksuf_len > 0)
	  memcpy(new_n_ksuf_pos, t.ksuf_m, copy_ksuf_len);

	new_n_pos++;
	m_inserted = true;
	new_n_ksuf_pos += copy_ksuf_len;
      }
      else if (t.ikey_m > n_ikey[n_pos]
	       || ((t.ikey_m == n_ikey[n_pos]) && (m_ikey_length > n_ikey_length))) { 
	//std::cout << "item_n inserted, m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	//move an item from n to the new array
	new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	new_n_ikey[new_n_pos] = n_ikey[n_pos];
	new_n_lv[new_n_pos] = n_lv[n_pos];

	copy_ksuf_len = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	if (copy_ksuf_len < 0) {
	  std::cout << "ERROR: add_item_to_node2, COPY_LENGTH < 0!!!\n";
	  return false;
	}

	if (copy_ksuf_len > 0)
	  memmove(new_n_ksuf_pos, n_ksuf_pos, copy_ksuf_len);

	new_n_pos++;
	n_pos++;
	new_n_ksuf_pos += copy_ksuf_len;
	n_ksuf_pos += copy_ksuf_len;
      }
      else { //same keyslice, same ikey length
	if (n_->keylenx_is_layer(n_ikeylen[n_pos])) {
	  //std::cout << "n layer; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	  //if n_pos is layer
	  new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	  new_n_ikey[new_n_pos] = n_ikey[n_pos];
	  //lv TBD
	  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	  merge_task tt;
	  tt.task = 1; //merge item m to n
	  tt.parent_node = n_;
	  tt.parent_node_pos = new_n_pos;
	  tt.n = static_cast<massnode<P>*>(n_lv[n_pos].layer());

	  tt.lv_m = t.lv_m;
	  tt.ksuf_len_m = t.ksuf_len_m;
	  //assign next layer ikeylen
	  tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	  //make next layer ikey
	  tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(t.ksuf_m, tt.ksuf_len_m);

	  //make next layer suffix
	  //tt.ksuf_len_m -= sizeof(ikey_type);
	  if (tt.ksuf_len_m > sizeof(ikey_type)) {
	    tt.ksuf_len_m -= sizeof(ikey_type);
	    tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	    memcpy((void*)tt.ksuf_m, (const void*)(t.ksuf_m + sizeof(ikey_type)), tt.ksuf_len_m);
	  }
	  else {
	    tt.ksuf_m = 0;
	    tt.ksuf_len_m = 0;
	  }

	  task_.push_back(tt);
	}
	else {
	  //std::cout << "both NOT layer; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	  int ksuflen_m = t.ksuf_len_m;
	  int ksuflen_n = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	  if ((ksuflen_m == 0) && (ksuflen_n == 0)) {
	    new_n_ikeylen[new_n_pos] = t.ikeylen_m;
	    new_n_ikey[new_n_pos] = t.ikey_m;
	    new_n_lv[new_n_pos] = t.lv_m;
	    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);
	  }
	  else if ((ksuflen_m == ksuflen_n) 
		   && (strncmp(t.ksuf_m, n_ksuf_pos, ksuflen_m) == 0)) {
	    new_n_ikeylen[new_n_pos] = t.ikeylen_m;
	    new_n_ikey[new_n_pos] = t.ikey_m;
	    new_n_lv[new_n_pos] = t.lv_m;

	    copy_ksuf_len = ksuflen_m;
	    memcpy(new_n_ksuf_pos, t.ksuf_m, copy_ksuf_len);

	    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	    new_n_ksuf_pos += copy_ksuf_len;
	    n_ksuf_pos += copy_ksuf_len;
	  }
	  else {
	    //if n_pos is NOT layer
	    new_n_ikeylen[new_n_pos] = (uint8_t)(n_ikeylen[n_pos] + (uint8_t)64);
	    new_n_ikey[new_n_pos] = n_ikey[n_pos];
	    //lv TBD
	    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	    merge_task tt;
	    tt.task = 2; //create a new node to include m and n
	    tt.parent_node = n_;
	    tt.parent_node_pos = new_n_pos;

	    tt.lv_m = t.lv_m;
	    tt.ksuf_len_m = t.ksuf_len_m;
	    //assign m next layer ikeylen
	    tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	    //make m next layer ikey
	    tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(t.ksuf_m, tt.ksuf_len_m);
	    
	    //make m next layer suffix
	    if (tt.ksuf_len_m > sizeof(ikey_type)) {
	      tt.ksuf_len_m -= sizeof(ikey_type);
	      tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	      memcpy((void*)tt.ksuf_m, (const void*)(t.ksuf_m + sizeof(ikey_type)), tt.ksuf_len_m);
	    }
	    else {
	      tt.ksuf_m = 0;
	      tt.ksuf_len_m = 0;
	    }


	    tt.lv_n = n_lv[n_pos];
	    tt.ksuf_len_n = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	    //assign n next layer ikeylen
	    tt.ikeylen_n = convert_to_ikeylen(tt.ksuf_len_n);
	    //make n next layer ikey
	    tt.ikey_n = string_slice<ikey_type>::make_comparable_sloppy(n_ksuf_pos, tt.ksuf_len_n);
	    
	    //make n next layer suffix
	    if (tt.ksuf_len_n > sizeof(ikey_type)) {
	      tt.ksuf_len_n -= sizeof(ikey_type);
	      tt.ksuf_n = (char*)malloc(tt.ksuf_len_n + 1);
	      memcpy((void*)tt.ksuf_n, (const void*)(n_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_n);
	    }
	    else {
	      tt.ksuf_n = 0;
	      tt.ksuf_len_n = 0;
	    }

	    task_.push_back(tt);
	    n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
	  }
	}

	new_nkeys--;
	new_n_pos++;
	m_inserted = true;
	n_pos++;
      } //same keyslice, same key length else end
    } //else end
  } //while end


  //if item m has not been inserted, insert it now
  if (!m_inserted) {
    //std::cout << "m leftovers; m_inserted = " << m_inserted <<"\n";
    new_n_ikeylen[new_n_pos] = t.ikeylen_m;
    new_n_ikey[new_n_pos] = t.ikey_m;
    new_n_lv[new_n_pos] = t.lv_m;
    
    copy_ksuf_len = t.ksuf_len_m;
    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

    if (copy_ksuf_len < 0) {
      std::cout << "ERROR: add_item_to_node3, COPY_LENGTH < 0!!!\n";
      return false;
    }

    if (copy_ksuf_len > 0)
      memcpy(new_n_ksuf_pos, t.ksuf_m, copy_ksuf_len);
    
    new_n_pos++;
    m_inserted = true;
    new_n_ksuf_pos += copy_ksuf_len;
  }

  //if n has leftovers, shift them to the new positions
  while (n_pos < n_nkeys) {
    //std::cout << "n leftovers; n_pos = " << n_pos << "\n";
    new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
    new_n_ikey[new_n_pos] = n_ikey[n_pos];
    new_n_lv[new_n_pos] = n_lv[n_pos];

    copy_ksuf_len = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

    if (copy_ksuf_len < 0) {
      std::cout << "ERROR: add_item_to_node4, COPY_LENGTH < 0!!!\n";
      return false;
    }

    if (copy_ksuf_len > 0)
      memmove(new_n_ksuf_pos, n_ksuf_pos, copy_ksuf_len);

    new_n_pos++;
    n_pos++;
    new_n_ksuf_pos += copy_ksuf_len;
    n_ksuf_pos += copy_ksuf_len;
  }

  //fill in the last ksuf_offset position
  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

  //delete m
  if (t.ksuf_len_m != 0)
    free(t.ksuf_m);

  //compact n if needed
  int new_ikeylen_size = (int)(sizeof(uint8_t) * new_nkeys);
  int new_ikey_size = (int)(sizeof(ikey_type) * new_nkeys);
  int new_lv_size = (int)(sizeof(leafvalue_static<P>) * new_nkeys);
  int new_ksuf_offset_size = (int)(sizeof(uint32_t) * (new_nkeys + 1));
  int new_ksuf_size = new_n_ksuf_offset[new_n_pos];

  int new_size = (int)(sizeof(massnode<P>)
		       + new_ikeylen_size
		       + new_ikey_size
		       + new_lv_size
		       + new_ksuf_offset_size
		       + new_ksuf_size);

  if (new_nkeys < new_max_nkeys) {
    //uint8_t* final_n_ikeylen = (uint8_t*)((char*)n_ + sizeof(massnode<P>));
    ikey_type* final_n_ikey = (ikey_type*)((char*)n_ + sizeof(massnode<P>)
					   + new_ikeylen_size);
    leafvalue_static<P>* final_n_lv = (leafvalue_static<P>*)((char*)n_ + sizeof(massnode<P>)
							     + new_ikeylen_size
							     + new_ikey_size);
    uint32_t* final_n_ksuf_offset = (uint32_t*)((char*)n_ + sizeof(massnode<P>)
						+ new_ikeylen_size
						+ new_ikey_size
						+ new_lv_size);
    char* final_n_ksuf = (char*)((char*)n_ + sizeof(massnode<P>)
				 + new_ikeylen_size
				 + new_ikey_size
				 + new_lv_size
				 + new_ksuf_offset_size);


    memmove((void*)final_n_ikey, (const void*)new_n_ikey, new_ikey_size);
    memmove((void*)final_n_lv, (const void*)new_n_lv, new_lv_size);
    memmove((void*)final_n_ksuf_offset, (const void*)new_n_ksuf_offset, new_ksuf_offset_size);
    memmove((void*)final_n_ksuf, (const void*)new_n_ksuf, new_ksuf_size);

    //resize(shrink) n
    n_ = n_->resize((size_t)new_size, ti);

    //resize may change the address, update the parent nodes addr in task_
    task_[task_.size() - 1].parent_node = n_;
  }

  n_->set_size((uint32_t)new_nkeys);
  n_->set_allocated_size((uint32_t)new_size);

  if (t.parent_node == NULL) {
    std::cout << "ERROR: add_item_to_node, parent_node is NULL!!!\n";
    return false;
  }

  t.parent_node->set_lv(t.parent_node_pos, leafvalue_static<P>(static_cast<node_base<P>*>(n_)));

  return true;
}

//huanchen-static-merge
//**********************************************************************************
// stcursor_merge::create_node
//**********************************************************************************
template <typename P>
bool stcursor_merge<P>::create_node(merge_task t, threadinfo &ti) {
  //std::cout << "create_node\n";
  size_t ksufSize = 0;
  uint32_t nkeys = 0;
  
  if ((t.ikey_m == t.ikey_n) && (t.ikeylen_m == t.ikeylen_n)) {
    ksufSize = 0;
    nkeys = 1;
    n_ = massnode<P>::make(ksufSize, true, nkeys, ti);
    n_->set_ikeylen(0, (uint8_t)(t.ikeylen_n + (uint8_t)64));
    n_->set_ikey(0, t.ikey_n);
    //lv TBD
    n_->set_ksuf_offset(0, (uint32_t)0);
    n_->set_ksuf_offset(1, (uint32_t)0);

    merge_task tt;
    tt.task = 2; //create a new node to include m and n
    tt.parent_node = n_;
    tt.parent_node_pos = 0;

    tt.lv_m = t.lv_m;
    tt.ksuf_len_m = t.ksuf_len_m;
    //assign m next layer ikeylen
    tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
    //make m next layer ikey
    tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(t.ksuf_m, tt.ksuf_len_m);

    //make m next layer suffix
    //tt.ksuf_len_m -= sizeof(ikey_type);
    if (tt.ksuf_len_m > sizeof(ikey_type)) {
      tt.ksuf_len_m -= sizeof(ikey_type);
      tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
      memcpy((void*)tt.ksuf_m, (const void*)(t.ksuf_m + sizeof(ikey_type)), tt.ksuf_len_m);
    }
    else {
      tt.ksuf_m = 0;
      tt.ksuf_len_m = 0;
    }

    tt.lv_n = t.lv_n;
    tt.ksuf_len_n = t.ksuf_len_n;
    //assign n next layer ikeylen
    tt.ikeylen_n = convert_to_ikeylen(tt.ksuf_len_n);
    //make n next layer ikey
    tt.ikey_n = string_slice<ikey_type>::make_comparable_sloppy(t.ksuf_n, tt.ksuf_len_n);

    //make n next layer suffix
    if (tt.ksuf_len_n > sizeof(ikey_type)) {
      tt.ksuf_len_n -= sizeof(ikey_type);
      tt.ksuf_n = (char*)malloc(tt.ksuf_len_n + 1);
      memcpy((void*)tt.ksuf_n, (const void*)(t.ksuf_n + sizeof(ikey_type)), tt.ksuf_len_n);
    }
    else {
      tt.ksuf_n = 0;
      tt.ksuf_len_n = 0;
    }

    task_.push_back(tt);
  }
  else {
    ksufSize = t.ksuf_len_m + t.ksuf_len_n;
    nkeys = 2;
    n_ = massnode<P>::make(ksufSize, true, nkeys, ti);

    if ((t.ikey_m < t.ikey_n)
	|| ((t.ikey_m == t.ikey_n) && (t.ikeylen_m < t.ikeylen_n))) {
      n_->set_ikeylen(0, t.ikeylen_m);
      n_->set_ikey(0, t.ikey_m);
      n_->set_lv(0, t.lv_m);
      n_->set_ksuf_offset(0, (uint32_t)0);
      n_->set_ksuf_offset(1, (uint32_t)t.ksuf_len_m);

      n_->set_ikeylen(1, t.ikeylen_n);
      n_->set_ikey(1, t.ikey_n);
      n_->set_lv(1, t.lv_n);
      n_->set_ksuf_offset(2, (uint32_t)(t.ksuf_len_m + t.ksuf_len_n));
      /*
      if ((t.ksuf_len_m < 0) || (t.ksuf_len_n) < 0) {
	std::cout << "ERROR: ksuf copy length < 0!!!\n";
	return false;
      }
      */
      if (t.ksuf_len_m > 0)
	memcpy((void*)(n_->ksufpos(0)), (const void*)t.ksuf_m, t.ksuf_len_m);
      if (t.ksuf_len_n > 0)
	memcpy((void*)(n_->ksufpos(1)), (const void*)t.ksuf_n, t.ksuf_len_n);
    }
    else {
      n_->set_ikeylen(0, t.ikeylen_n);
      n_->set_ikey(0, t.ikey_n);
      n_->set_lv(0, t.lv_n);
      n_->set_ksuf_offset(0, (uint32_t)0);
      n_->set_ksuf_offset(1, (uint32_t)t.ksuf_len_n);

      n_->set_ikeylen(1, t.ikeylen_m);
      n_->set_ikey(1, t.ikey_m);
      n_->set_lv(1, t.lv_m);
      n_->set_ksuf_offset(2, (uint32_t)(t.ksuf_len_n + t.ksuf_len_m));
      /*
      if ((t.ksuf_len_m < 0) || (t.ksuf_len_n) < 0) {
	std::cout << "ERROR: ksuf copy length < 0!!!\n";
	return false;
      }
      */
      if (t.ksuf_len_n > 0)
	memcpy((void*)(n_->ksufpos(0)), (const void*)t.ksuf_n, t.ksuf_len_n);
      if (t.ksuf_len_m > 0)
	memcpy((void*)(n_->ksufpos(1)), (const void*)t.ksuf_m, t.ksuf_len_m);
    }
  }

  //delete m
  if (t.ksuf_len_m != 0)
    free(t.ksuf_m);
  //delete n
  if (t.ksuf_len_n != 0)
    free(t.ksuf_n);

  if (t.parent_node == NULL) {
    std::cout << "ERROR: create_node, parent_node is NULL!!!\n";
    return false;
  }

  t.parent_node->set_lv(t.parent_node_pos, leafvalue_static<P>(static_cast<node_base<P>*>(n_)));

  return true;
}

//huanchen-static-merge
//**********************************************************************************
// stcursor_merge::merge
//**********************************************************************************
template <typename P>
bool stcursor_merge<P>::merge(threadinfo &ti, threadinfo &ti_merge) {
  merge_task t;
  bool merge_success = true;

  t.task = 0; //merge m to n
  t.parent_node = NULL;
  t.m = static_cast<massnode<P>*>(merge_root_);
  t.n = static_cast<massnode<P>*>(root_);
  task_.push_back(t);

  unsigned int cur_pos = 0;
  while (cur_pos < task_.size()) {
    //if (task_.size() % 10000 == 0)
    //std::cout << "1 task_.size() = " << task_.size() << "\n";
    if (task_[cur_pos].task == 0)
      merge_success = merge_nodes(task_[cur_pos], ti, ti_merge);
    else if (task_[cur_pos].task == 1)
      merge_success = add_item_to_node(task_[cur_pos], ti);
    else if (task_[cur_pos].task == 2)
      merge_success = create_node(task_[cur_pos], ti);
    else
      return false;
    cur_pos++;
    if (!merge_success) {
      std::cout << "MERGE FAIL!!!\n";
      return false;
    }
  }
  //std::cout << "merge success-----------------------------------------\n";
  return merge_success;
}

//huanchen-static-merge
//**********************************************************************************
// stcursor_merge::convert_to_ikeylen
//**********************************************************************************
template <typename P>
inline uint8_t stcursor_merge<P>::convert_to_ikeylen(uint32_t len) {
  uint8_t ikeylen = (uint8_t)0;
  //if (len >= 8)
  //ikeylen = (uint8_t)8; 
  if (len > 8)
    ikeylen = (uint8_t)9;
  else if (len == 8)
    ikeylen = (uint8_t)8;
  else if (len == 7)
    ikeylen = (uint8_t)7;
  else if (len == 6)
    ikeylen = (uint8_t)6;
  else if (len == 5)
    ikeylen = (uint8_t)5;
  else if (len == 4)
    ikeylen = (uint8_t)4;
  else if (len == 3)
    ikeylen = (uint8_t)3;
  else if (len == 2)
    ikeylen = (uint8_t)2;
  else if (len == 1)
    ikeylen = (uint8_t)1;
  else
    return ikeylen;
  return ikeylen;
}


//huanchen-static-merge-multivalue
//**********************************************************************************
// stcursor_merge_multivalue::merge_nodes
//**********************************************************************************
template <typename P>
bool stcursor_merge_multivalue<P>::merge_nodes(merge_task_multivalue t, threadinfo &ti, threadinfo &ti_merge) {
  //std::cout << "merge_nodes(multivalue) start$$$$$$$$$$$\n";
  //std::cout << "alloc1 = " << ti.alloc << "\n";
  /*
  if ((t.m == NULL) || (t.n == NULL)) {
    std::cout << "ERROR: merge_node(multivalue), node m or n is NULL!!!\n";
    return false;
  }

  m_ = t.m;
  n_ = t.n;
  */

  if (t.m == NULL) {
    std::cout << "ERROR: merge_node, node m is NULL!!!\n";
    return false;
  }
  m_ = t.m;

  if (t.n == NULL) {
    root_ = m_;
    return true;
  }
  else
    n_ = t.n;

  //calculate size & num_keys of m, n and the tmp new node
  int m_size = m_->allocated_size();
  int n_size = n_->allocated_size();
  int new_max_size = m_size + n_size - sizeof(massnode_multivalue<P>) - sizeof(uint32_t);
  int m_nkeys = m_->size();
  int n_nkeys = n_->size();
  int new_max_nkeys = m_nkeys + n_nkeys;
  //values
  int m_value_size = m_->value_size();
  int n_value_size = n_->value_size();
  int new_max_value_size = m_value_size + n_value_size;

  //resize(expand) n
  n_ = n_->resize((size_t)new_max_size, ti);
  //n_->set_allocated_size((size_t)new_max_size);
  //std::cout << "alloc2 = " << ti.alloc << "\n";

  //calculate the start position offsets of each array in m, n and tmp new
  int new_ikeylen_startpos = sizeof(massnode_multivalue<P>);
  int n_ikeylen_startpos = sizeof(massnode_multivalue<P>);
  int m_ikeylen_startpos = sizeof(massnode_multivalue<P>);
  int new_ikeylen_len = (int)(sizeof(uint8_t) * new_max_nkeys);
  int n_ikeylen_len = (int)(sizeof(uint8_t) * n_nkeys);
  int m_ikeylen_len = (int)(sizeof(uint8_t) * m_nkeys);

  int new_ikey_startpos = new_ikeylen_startpos + new_ikeylen_len;
  int n_ikey_startpos = n_ikeylen_startpos + n_ikeylen_len;
  int m_ikey_startpos = m_ikeylen_startpos + m_ikeylen_len;
  int new_ikey_len = (int)(sizeof(ikey_type) * new_max_nkeys);
  int n_ikey_len = (int)(sizeof(ikey_type) * n_nkeys);
  int m_ikey_len = (int)(sizeof(ikey_type) * m_nkeys);

  int new_lv_startpos = new_ikey_startpos + new_ikey_len;
  int n_lv_startpos = n_ikey_startpos + n_ikey_len;
  int m_lv_startpos = m_ikey_startpos + m_ikey_len;
  int new_lv_len = (int)(sizeof(leafvalue_static_multivalue<P>) * new_max_nkeys);
  int n_lv_len = (int)(sizeof(leafvalue_static_multivalue<P>) * n_nkeys);
  int m_lv_len = (int)(sizeof(leafvalue_static_multivalue<P>) * m_nkeys);

  //values
  int new_value_startpos = new_lv_startpos + new_lv_len;
  int n_value_startpos = n_lv_startpos + n_lv_len;
  int m_value_startpos = m_lv_startpos + m_lv_len;
  int new_value_len = (int)(new_max_value_size);
  int n_value_len = (int)(n_value_size);
  int m_value_len = (int)(m_value_size);

  int new_ksuf_offset_startpos = new_value_startpos + new_value_len;
  int n_ksuf_offset_startpos = n_value_startpos + n_value_len;
  int m_ksuf_offset_startpos = m_value_startpos + m_value_len;
  int new_ksuf_offset_len = (int)(sizeof(uint32_t) * (new_max_nkeys + 1));
  int n_ksuf_offset_len = (int)(sizeof(uint32_t) * (n_nkeys + 1));
  int m_ksuf_offset_len = (int)(sizeof(uint32_t) * (m_nkeys + 1));

  int new_ksuf_startpos = new_ksuf_offset_startpos + new_ksuf_offset_len;
  int n_ksuf_startpos = n_ksuf_offset_startpos + n_ksuf_offset_len;
  int m_ksuf_startpos = m_ksuf_offset_startpos + m_ksuf_offset_len;
  int new_ksuf_len = new_max_size - new_ksuf_startpos;
  int n_ksuf_len = n_size - n_ksuf_startpos;
  int m_ksuf_len = m_size - m_ksuf_startpos;

  //calculate the start position offset of moved arrays in n
  int new_n_ikeylen_startpos = new_ikeylen_startpos + new_ikeylen_len - n_ikeylen_len;
  int new_n_ikey_startpos = new_ikey_startpos + new_ikey_len - n_ikey_len;
  int new_n_lv_startpos = new_lv_startpos + new_lv_len - n_lv_len;
  int new_n_value_startpos = new_value_startpos + new_value_len - n_value_len; //values
  int new_n_ksuf_offset_startpos = new_ksuf_offset_startpos + new_ksuf_offset_len - n_ksuf_offset_len;
  int new_n_ksuf_startpos = new_ksuf_startpos + new_ksuf_len - n_ksuf_len;

  //move the arrays in n and prepare for merging
  uint8_t* m_ikeylen = (uint8_t*)((char*)m_ + m_ikeylen_startpos);
  ikey_type* m_ikey = (ikey_type*)((char*)m_ + m_ikey_startpos);
  leafvalue_static_multivalue<P>* m_lv = (leafvalue_static_multivalue<P>*)((char*)m_ + m_lv_startpos);
  char* m_value = (char*)((char*)m_ + m_value_startpos); //values
  uint32_t* m_ksuf_offset = (uint32_t*)((char*)m_ + m_ksuf_offset_startpos);
  char* m_ksuf = (char*)((char*)m_ + m_ksuf_startpos);

  uint8_t* n_ikeylen = (uint8_t*)((char*)n_ + new_n_ikeylen_startpos);
  ikey_type* n_ikey = (ikey_type*)((char*)n_ + new_n_ikey_startpos);
  leafvalue_static_multivalue<P>* n_lv = (leafvalue_static_multivalue<P>*)((char*)n_ + new_n_lv_startpos);
  char* n_value = (char*)((char*)n_ + new_n_value_startpos); //values
  uint32_t* n_ksuf_offset = (uint32_t*)((char*)n_ + new_n_ksuf_offset_startpos);
  char* n_ksuf = (char*)((char*)n_ + new_n_ksuf_startpos);

  uint8_t* new_n_ikeylen = (uint8_t*)((char*)n_ + new_ikeylen_startpos);
  ikey_type* new_n_ikey = (ikey_type*)((char*)n_ + new_ikey_startpos);
  leafvalue_static_multivalue<P>* new_n_lv = (leafvalue_static_multivalue<P>*)((char*)n_ + new_lv_startpos);
  char* new_n_value = (char*)((char*)n_ + new_value_startpos); //values
  uint32_t* new_n_ksuf_offset = (uint32_t*)((char*)n_ + new_ksuf_offset_startpos);
  char* new_n_ksuf = (char*)((char*)n_ + new_ksuf_startpos);

  memmove((void*)(n_ksuf), (void*)((char*)n_ + n_ksuf_startpos), n_ksuf_len);
  memmove((void*)(n_ksuf_offset), (void*)((char*)n_ + n_ksuf_offset_startpos), n_ksuf_offset_len);
  memmove((void*)(n_value), (void*)((char*)n_ + n_value_startpos), n_value_len); //values
  memmove((void*)(n_lv), (void*)((char*)n_ + n_lv_startpos), n_lv_len);
  memmove((void*)(n_ikey), (void*)((char*)n_ + n_ikey_startpos), n_ikey_len);
  memmove((void*)(n_ikeylen), (void*)((char*)n_ + n_ikeylen_startpos), n_ikeylen_len);

  //merge
  //---------------------------------------------------------------------------------
  int m_pos = 0;
  int n_pos = 0;
  int new_n_pos = 0;

  char* new_n_ksuf_pos = new_n_ksuf;
  char* m_ksuf_pos = m_ksuf;
  char* n_ksuf_pos = n_ksuf;

  int copy_ksuf_len = 0;

  //values
  char* new_n_value_pos = new_n_value;
  char* m_value_pos = m_value;
  char* n_value_pos = n_value;

  int copy_value_len = 0;

  int start_task_pos = task_.size();

  while ((m_pos < m_nkeys) && (n_pos < n_nkeys)) {
    //if deleted
    if (n_ikeylen[n_pos] == 0) {
      //std::cout << "item deleted; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
      n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
      n_value_pos += n_lv[n_pos].value_len(); //values
      n_pos++;
    }
    else {
      uint8_t m_ikey_length = m_->keylenx_ikeylen(m_ikeylen[m_pos]);
      uint8_t n_ikey_length = n_->keylenx_ikeylen(n_ikeylen[n_pos]);
      if (m_ikey[m_pos] < n_ikey[n_pos]
	  || ((m_ikey[m_pos] == n_ikey[n_pos]) && (m_ikey_length < n_ikey_length))) { 
	//std::cout << "item_m inserted; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	//move an item from m to the new array
	new_n_ikeylen[new_n_pos] = m_ikeylen[m_pos];
	new_n_ikey[new_n_pos] = m_ikey[m_pos];

	//values
	//-----------------------------------------------------------------------
	if (m_->keylenx_is_layer(m_ikeylen[m_pos])) {
	  copy_value_len = 0;
	  new_n_lv[new_n_pos] = m_lv[m_pos];
	}
	else {
	  copy_value_len = m_lv[m_pos].value_len();
	  new_n_lv[new_n_pos] = leafvalue_static_multivalue<P>((uint32_t)(new_n_value_pos - new_n_value), (uint32_t)(m_lv[m_pos].value_len())); //values
	}

	if (copy_value_len < 0) {
	  std::cout << "ERROR: merge_node1(multivalue), COPY_VALUE_LENGTH < 0!!!\n";
	  return false;
	}

	if (copy_value_len > 0)
	  memcpy(new_n_value_pos, m_value_pos, copy_value_len);
	//-----------------------------------------------------------------------

	copy_ksuf_len = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
	new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	if (copy_ksuf_len < 0) {
	  std::cout << "ERROR: merge_node1(multivalue), COPY_LENGTH < 0!!!\n";
	  return false;
	}

	if (copy_ksuf_len > 0)
	  memcpy(new_n_ksuf_pos, m_ksuf_pos, copy_ksuf_len);

	new_n_pos++;
	m_pos++;
	new_n_ksuf_pos += copy_ksuf_len;
	m_ksuf_pos += copy_ksuf_len;
	//values
	new_n_value_pos += copy_value_len;
	m_value_pos += copy_value_len;
      }
      else if (m_ikey[m_pos] > n_ikey[n_pos]
	       || ((m_ikey[m_pos] == n_ikey[n_pos]) && (m_ikey_length > n_ikey_length))) { 
	//std::cout << "item_n inserted, m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	//move an item from n to the new array
	new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	new_n_ikey[new_n_pos] = n_ikey[n_pos];

	//values
	//-----------------------------------------------------------------------
	if (n_->keylenx_is_layer(n_ikeylen[n_pos])) {
	  copy_value_len = 0;
	  new_n_lv[new_n_pos] = n_lv[n_pos];
	}
	else {
	  copy_value_len = n_lv[n_pos].value_len();
	  new_n_lv[new_n_pos] = leafvalue_static_multivalue<P>((uint32_t)(new_n_value_pos - new_n_value), (uint32_t)(n_lv[n_pos].value_len())); //values
	}

	if (copy_value_len < 0) {
	  std::cout << "ERROR: merge_node2(multivalue), COPY_VALUE_LENGTH < 0!!!\n";
	  return false;
	}

	if (copy_value_len > 0)
	  memmove(new_n_value_pos, n_value_pos, copy_value_len);
	//-----------------------------------------------------------------------

	copy_ksuf_len = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	if (copy_ksuf_len < 0) {
	  std::cout << "n_ksuf_offset[" << (n_pos + 1) << "] = " << n_ksuf_offset[n_pos + 1] << "\n";
	  std::cout << "n_ksuf_offset[" << n_pos << "] = " << n_ksuf_offset[n_pos] << "\n";
	  std::cout << "ERROR: merge_node2(multivalue), COPY_LENGTH < 0!!!\n";
	  return false;
	}

	if (copy_ksuf_len > 0)
	  memmove(new_n_ksuf_pos, n_ksuf_pos, copy_ksuf_len);

	new_n_pos++;
	n_pos++;
	new_n_ksuf_pos += copy_ksuf_len;
	n_ksuf_pos += copy_ksuf_len;
	//values
	new_n_value_pos += copy_value_len;
	n_value_pos += copy_value_len;
      }
      else { //same keyslice, same ikey length
	if (m_->keylenx_is_layer(m_ikeylen[m_pos]) && n_->keylenx_is_layer(n_ikeylen[n_pos])) {
	  //std::cout << "both layers; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	  //if m_pos is layer AND n_pos is layer
	  new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	  new_n_ikey[new_n_pos] = n_ikey[n_pos];
	  //lv TBD
	  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	  merge_task_multivalue tt;
	  tt.task = 0; //merge node m to n
	  tt.parent_node = n_;
	  tt.parent_node_pos = new_n_pos;
	  tt.m = static_cast<massnode_multivalue<P>*>(m_lv[m_pos].layer());
	  tt.n = static_cast<massnode_multivalue<P>*>(n_lv[n_pos].layer());

	  task_.push_back(tt);
	}
	else if (n_->keylenx_is_layer(n_ikeylen[n_pos])) {
	  //std::cout << "n layer; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	  //if n_pos is layer
	  new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	  new_n_ikey[new_n_pos] = n_ikey[n_pos];
	  //lv TBD
	  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	  merge_task_multivalue tt;
	  tt.task = 1; //merge item_m to n
	  tt.parent_node = n_;
	  tt.parent_node_pos = new_n_pos;
	  tt.n = static_cast<massnode_multivalue<P>*>(n_lv[n_pos].layer());

	  tt.lv_m = m_lv[m_pos];
	  //values
	  copy_value_len = m_lv[m_pos].value_len();
	  tt.value_m = (char*)malloc(copy_value_len + 1);
	  memcpy((void*)tt.value_m, (const void*)(m_value_pos), copy_value_len);

	  tt.ksuf_len_m = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
	  //assign next layer ikeylen
	  tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	  //make next layer ikey
	  tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(m_ksuf_pos, tt.ksuf_len_m);
	    
	  //make next layer suffix
	  if (tt.ksuf_len_m > sizeof(ikey_type)) {
	    tt.ksuf_len_m -= sizeof(ikey_type);
	    tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	    memcpy((void*)tt.ksuf_m, (const void*)(m_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_m);
	  }
	  else {
	    tt.ksuf_m = 0;
	    tt.ksuf_len_m = 0;
	  }

	  task_.push_back(tt);
	  m_ksuf_pos += (m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos]);
	  m_value_pos += copy_value_len; //values
	}
	else if (m_->keylenx_is_layer(m_ikeylen[m_pos])) {
	  //std::cout << "m layer; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	  //if m_pos is layer
	  new_n_ikeylen[new_n_pos] = m_ikeylen[m_pos];
	  new_n_ikey[new_n_pos] = m_ikey[m_pos];
	  //lv TBD
	  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	  merge_task_multivalue tt;
	  tt.task = 1; //merge item m to n
	  tt.parent_node = n_;
	  tt.parent_node_pos = new_n_pos;
	  tt.n = static_cast<massnode_multivalue<P>*>(m_lv[m_pos].layer());

	  tt.lv_m = n_lv[n_pos];
	  //values
	  copy_value_len = n_lv[n_pos].value_len();
	  tt.value_m = (char*)malloc(copy_value_len + 1);
	  memcpy((void*)tt.value_m, (const void*)(n_value_pos), copy_value_len);

	  tt.ksuf_len_m = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	  //assign next layer ikeylen
	  tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	  //make next layer ikey
	  tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(n_ksuf_pos, tt.ksuf_len_m);
	    
	  //make next layer suffix
	  if (tt.ksuf_len_m > sizeof(ikey_type)) {
	    tt.ksuf_len_m -= sizeof(ikey_type);
	    tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	    memcpy((void*)tt.ksuf_m, (const void*)(n_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_m);
	  }
	  else {
	    tt.ksuf_m = 0;
	    tt.ksuf_len_m = 0;
	  }

	  task_.push_back(tt);
	  n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
	  n_value_pos += copy_value_len; //values
	}
	else {
	  //std::cout << "both NOT layer; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	  //if neither m_pos nor n_pos is layer
	  //values
	  int ksuflen_m = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
	  int ksuflen_n = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	  if ((ksuflen_m == 0) && (ksuflen_n == 0)) {
	    //std::cout << "both NOT layer 1; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	    new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	    new_n_ikey[new_n_pos] = n_ikey[n_pos];
	    //new_n_lv[new_n_pos] = n_lv[n_pos];
	    new_n_lv[new_n_pos] = leafvalue_static_multivalue<P>((uint32_t)(new_n_value_pos - new_n_value), (uint32_t)(m_lv[m_pos].value_len() + n_lv[n_pos].value_len())); //values

	    copy_value_len = m_lv[m_pos].value_len();
	    memcpy(new_n_value_pos, m_value_pos, copy_value_len);
	    new_n_value_pos += copy_value_len;
	    m_value_pos += copy_value_len;

	    copy_value_len = n_lv[n_pos].value_len();
	    memmove(new_n_value_pos, n_value_pos, copy_value_len);
	    new_n_value_pos += copy_value_len;
	    n_value_pos += copy_value_len;

	    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);
	  }
	  else if ((ksuflen_m == ksuflen_n) 
		   && (strncmp(m_ksuf_pos, n_ksuf_pos, ksuflen_m) == 0)) {
	    //std::cout << "both NOT layer 2; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	    new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	    new_n_ikey[new_n_pos] = n_ikey[n_pos];
	    new_n_lv[new_n_pos] = leafvalue_static_multivalue<P>((uint32_t)(new_n_value_pos - new_n_value), (uint32_t)(m_lv[m_pos].value_len() + n_lv[n_pos].value_len())); //values

	    copy_value_len = m_lv[m_pos].value_len();
	    memcpy(new_n_value_pos, m_value_pos, copy_value_len);
	    new_n_value_pos += copy_value_len;
	    m_value_pos += copy_value_len;

	    copy_value_len = n_lv[n_pos].value_len();
	    memmove(new_n_value_pos, n_value_pos, copy_value_len);
	    new_n_value_pos += copy_value_len;
	    n_value_pos += copy_value_len;

	    copy_ksuf_len = ksuflen_m;
	    memcpy(new_n_ksuf_pos, m_ksuf_pos, copy_ksuf_len);

	    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	    new_n_ksuf_pos += copy_ksuf_len;
	    m_ksuf_pos += copy_ksuf_len;
	    n_ksuf_pos += copy_ksuf_len;
	  }
	  else {
	    //std::cout << "both NOT layer 3; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	    new_n_ikeylen[new_n_pos] = (uint8_t)(n_ikeylen[n_pos] + (uint8_t)64);
	    new_n_ikey[new_n_pos] = n_ikey[n_pos];
	    //lv TBD
	    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	    merge_task_multivalue tt;
	    tt.task = 2; //create a new node to include m and n
	    tt.parent_node = n_;
	    tt.parent_node_pos = new_n_pos;

	    tt.lv_m = m_lv[m_pos];
	    //values
	    copy_value_len = m_lv[m_pos].value_len();
	    tt.value_m = (char*)malloc(copy_value_len + 1);
	    memcpy((void*)tt.value_m, (const void*)(m_value_pos), copy_value_len);
	    m_value_pos += copy_value_len;

	    tt.ksuf_len_m = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
	    //assign m next layer ikeylen
	    tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	    //make m next layer ikey
	    tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(m_ksuf_pos, tt.ksuf_len_m);
	    
	    //make m next layer suffix
	    if (tt.ksuf_len_m > sizeof(ikey_type)) {
	      tt.ksuf_len_m -= sizeof(ikey_type);
	      tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	      memcpy((void*)tt.ksuf_m, (const void*)(m_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_m);
	    }
	    else {
	      tt.ksuf_m = 0;
	      tt.ksuf_len_m = 0;
	    }


	    tt.lv_n = n_lv[n_pos];
	    //values
	    copy_value_len = n_lv[n_pos].value_len();
	    tt.value_n = (char*)malloc(copy_value_len + 1);
	    memcpy((void*)tt.value_n, (const void*)(n_value_pos), copy_value_len);
	    n_value_pos += copy_value_len;

	    tt.ksuf_len_n = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	    //assign n next layer ikeylen
	    tt.ikeylen_n = convert_to_ikeylen(tt.ksuf_len_n);
	    //make n next layer ikey
	    tt.ikey_n = string_slice<ikey_type>::make_comparable_sloppy(n_ksuf_pos, tt.ksuf_len_n);
	    
	    //make n next layer suffix
	    if (tt.ksuf_len_n > sizeof(ikey_type)) {
	      tt.ksuf_len_n -= sizeof(ikey_type);
	      tt.ksuf_n = (char*)malloc(tt.ksuf_len_n + 1);
	      memcpy((void*)tt.ksuf_n, (const void*)(n_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_n);
	    }
	    else {
	      tt.ksuf_n = 0;
	      tt.ksuf_len_n = 0;
	    }

	    task_.push_back(tt);
	    m_ksuf_pos += (m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos]);
	    n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
	  }
	}

	new_n_pos++;
	m_pos++;
	n_pos++;
      } //same keyslice, same ikey length else end
    } //else end
  } //while end

  //if m has leftovers, move them to the new node
  while (m_pos < m_nkeys) {
    //std::cout << "m leftovers; m_pos = " << m_pos <<"\n";
    new_n_ikeylen[new_n_pos] = m_ikeylen[m_pos];
    new_n_ikey[new_n_pos] = m_ikey[m_pos];

    //values
    //-----------------------------------------------------------------------
    if (m_->keylenx_is_layer(m_ikeylen[m_pos])) {
      copy_value_len = 0;
      new_n_lv[new_n_pos] = m_lv[m_pos];
    }
    else {
      copy_value_len = m_lv[m_pos].value_len();
      new_n_lv[new_n_pos] = leafvalue_static_multivalue<P>((uint32_t)(new_n_value_pos - new_n_value), (uint32_t)(m_lv[m_pos].value_len())); //values
    }

    if (copy_value_len < 0) {
      std::cout << "ERROR: merge_node6(multivalue), COPY_VALUE_LENGTH < 0!!!\n";
      return false;
    }

    if (copy_value_len > 0)
      memcpy(new_n_value_pos, m_value_pos, copy_value_len);
    //-----------------------------------------------------------------------
    
    copy_ksuf_len = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

    if (copy_ksuf_len < 0) {
      std::cout << "ERROR: merge_node3(multivalue), COPY_LENGTH < 0!!!\n";
      std::cout << "m_ksuf_offset[" << (m_pos + 1) << "] = " << m_ksuf_offset[m_pos + 1] << "\n";
      std::cout << "m_ksuf_offset[" << m_pos << "] = " << m_ksuf_offset[m_pos] << "\n";
      return false;
    }

    if (copy_ksuf_len > 0)
      memcpy(new_n_ksuf_pos, m_ksuf_pos, copy_ksuf_len);
    
    //new_nkeys++;
    new_n_pos++;
    m_pos++;
    new_n_ksuf_pos += copy_ksuf_len;
    m_ksuf_pos += copy_ksuf_len;
    //values
    new_n_value_pos += copy_value_len;
    m_value_pos += copy_value_len;
  }

  //if n has leftovers, shift them to the new positions
  while (n_pos < n_nkeys) {
    //std::cout << "n leftovers; n_pos = " << n_pos << "\n";
    new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
    new_n_ikey[new_n_pos] = n_ikey[n_pos];

    //values
    //-----------------------------------------------------------------------
    if (n_->keylenx_is_layer(n_ikeylen[n_pos])) {
      copy_value_len = 0;
      new_n_lv[new_n_pos] = n_lv[n_pos];
    }
    else {
      copy_value_len = n_lv[n_pos].value_len();
      new_n_lv[new_n_pos] = leafvalue_static_multivalue<P>((uint32_t)(new_n_value_pos - new_n_value), (uint32_t)(n_lv[n_pos].value_len())); //values
    }

    if (copy_value_len < 0) {
      std::cout << "ERROR: merge_node7(multivalue), COPY_VALUE_LENGTH < 0!!!\n";
      return false;
    }

    if (copy_value_len > 0)
      memmove(new_n_value_pos, n_value_pos, copy_value_len);
    //-----------------------------------------------------------------------

    copy_ksuf_len = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);
    if (copy_ksuf_len < 0) {
      std::cout << "ERROR: merge_node4(multivalue), COPY_LENGTH < 0!!!\n";
      return false;
    }

    if (copy_ksuf_len > 0)
      memmove(new_n_ksuf_pos, n_ksuf_pos, copy_ksuf_len);

    new_n_pos++;
    n_pos++;
    new_n_ksuf_pos += copy_ksuf_len;
    n_ksuf_pos += copy_ksuf_len;
    //values
    new_n_value_pos += copy_value_len;
    n_value_pos += copy_value_len;
  }

  //fill in the last ksuf_offset position
  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

  //delete m
  m_->deallocate(ti_merge);

  //compact n if needed
  int new_nkeys = new_n_pos;
  int new_ikeylen_size = (int)(sizeof(uint8_t) * new_nkeys);
  int new_ikey_size = (int)(sizeof(ikey_type) * new_nkeys);
  int new_lv_size = (int)(sizeof(leafvalue_static_multivalue<P>) * new_nkeys);
  int new_value_size = (int)(new_n_value_pos - new_n_value); //values
  int new_ksuf_offset_size = (int)(sizeof(uint32_t) * (new_nkeys + 1));
  int new_ksuf_size = new_n_ksuf_offset[new_n_pos];

  int new_size = (int)(sizeof(massnode_multivalue<P>)
		       + new_ikeylen_size
		       + new_ikey_size
		       + new_lv_size
		       + new_value_size //values
		       + new_ksuf_offset_size
		       + new_ksuf_size);

  if (new_nkeys > new_max_nkeys) {
    std::cout << "ERROR: new_nkeys > new_max_nkeys!!!\n";
    return false;
  }

  if (new_nkeys < new_max_nkeys) {
    uint8_t* final_n_ikeylen = (uint8_t*)((char*)n_ + sizeof(massnode_multivalue<P>));
    ikey_type* final_n_ikey = (ikey_type*)((char*)n_ + sizeof(massnode_multivalue<P>)
					   + new_ikeylen_size);
    leafvalue_static_multivalue<P>* final_n_lv 
      = (leafvalue_static_multivalue<P>*)((char*)n_ + sizeof(massnode_multivalue<P>)
					  + new_ikeylen_size
					  + new_ikey_size);
    //values
    char* final_n_value = (char*)((char*)n_ + sizeof(massnode_multivalue<P>)
				  + new_ikeylen_size
				  + new_ikey_size
				  + new_lv_size);
    uint32_t* final_n_ksuf_offset = (uint32_t*)((char*)n_ + sizeof(massnode_multivalue<P>)
						+ new_ikeylen_size
						+ new_ikey_size
						+ new_lv_size
						+ new_value_size); //values
    char* final_n_ksuf = (char*)((char*)n_ + sizeof(massnode_multivalue<P>)
				 + new_ikeylen_size
				 + new_ikey_size
				 + new_lv_size
				 + new_value_size //values
				 + new_ksuf_offset_size);
  
    memmove((void*)final_n_ikey, (const void*)new_n_ikey, new_ikey_size);
    memmove((void*)final_n_lv, (const void*)new_n_lv, new_lv_size);
    memmove((void*)final_n_value, (const void*)new_n_value, new_value_size); //values
    memmove((void*)final_n_ksuf_offset, (const void*)new_n_ksuf_offset, new_ksuf_offset_size);
    memmove((void*)final_n_ksuf, (const void*)new_n_ksuf, new_ksuf_size);

    //resize(shrink) n
    n_->set_allocated_size((size_t)new_max_size);
    n_ = n_->resize((size_t)new_size, ti);
    //std::cout << "alloc3 = " << ti.alloc << "\n";

    //resize may change the address, update the parent nodes addr in task_
    for (int i = start_task_pos; i < task_.size(); i++)
      task_[i].parent_node = n_;
  }

  n_->set_size((uint32_t)new_nkeys);
  n_->set_allocated_size((size_t)new_size);
  n_->set_value_size((uint32_t)new_value_size);

  if (t.parent_node)
    t.parent_node->set_lv(t.parent_node_pos, leafvalue_static_multivalue<P>(static_cast<node_base<P>*>(n_)));
  else
    root_ = n_;
  //std::cout << "alloc4 = " << ti.alloc << "\n";
  return true;
}


//huanchen-static-merge-multivalue
//**********************************************************************************
// stcursor_merge_multivalue::add_item_to_node
//**********************************************************************************
template <typename P>
bool stcursor_merge_multivalue<P>::add_item_to_node(merge_task_multivalue t, threadinfo &ti) {
  //std::cout << "add_item_to_node(multivalue) start$$$$$$$$$$\n";
  if (t.n == NULL) {
    std::cout << "ERROR: add_item_to_node(multivalue), node n is NULL!!!\n";
    return false;
  }

  n_ = t.n;

  int m_size = (int)(sizeof(uint8_t)
		     + sizeof(ikey_type)
		     + sizeof(leafvalue_static_multivalue<P>)
		     + t.lv_m.value_len() //values
		     + sizeof(uint32_t)
		     + t.ksuf_len_m);
  int n_size = n_->allocated_size();
  int new_max_size = m_size + n_size;
  int n_nkeys = n_->size();
  int new_max_nkeys = n_nkeys + 1;

  //values
  int m_value_size = t.lv_m.value_len();
  int n_value_size = n_->value_size();
  int new_max_value_size = m_value_size + n_value_size;

  //resize(expand) n
  n_ = n_->resize((size_t)new_max_size, ti);
  n_->set_allocated_size((size_t)new_max_size);

  //calculate the start position offsets of each array in n and tmp new
  int new_ikeylen_startpos = sizeof(massnode_multivalue<P>);
  int n_ikeylen_startpos = sizeof(massnode_multivalue<P>);
  int new_ikeylen_len = (int)(sizeof(uint8_t) * new_max_nkeys);
  int n_ikeylen_len = (int)(sizeof(uint8_t) * n_nkeys);

  int new_ikey_startpos = new_ikeylen_startpos + new_ikeylen_len;
  int n_ikey_startpos = n_ikeylen_startpos + n_ikeylen_len;
  int new_ikey_len = (int)(sizeof(ikey_type) * new_max_nkeys);
  int n_ikey_len = (int)(sizeof(ikey_type) * n_nkeys);

  int new_lv_startpos = new_ikey_startpos + new_ikey_len;
  int n_lv_startpos = n_ikey_startpos + n_ikey_len;
  int new_lv_len = (int)(sizeof(leafvalue_static_multivalue<P>) * new_max_nkeys);
  int n_lv_len = (int)(sizeof(leafvalue_static_multivalue<P>) * n_nkeys);

  //values
  int new_value_startpos = new_lv_startpos + new_lv_len;
  int n_value_startpos = n_lv_startpos + n_lv_len;
  int new_value_len = (int)(new_max_value_size);
  int n_value_len = (int)(n_value_size);

  int new_ksuf_offset_startpos = new_value_startpos + new_value_len;
  int n_ksuf_offset_startpos = n_value_startpos + n_value_len;
  int new_ksuf_offset_len = (int)(sizeof(uint32_t) * (new_max_nkeys + 1));
  int n_ksuf_offset_len = (int)(sizeof(uint32_t) * (n_nkeys + 1));

  int new_ksuf_startpos = new_ksuf_offset_startpos + new_ksuf_offset_len;
  int n_ksuf_startpos = n_ksuf_offset_startpos + n_ksuf_offset_len;
  int new_ksuf_len = new_max_size - new_ksuf_startpos;
  int n_ksuf_len = n_size - n_ksuf_startpos;

  //calculate the start position offset of moved arrays in n
  int new_n_ikeylen_startpos = new_ikeylen_startpos + new_ikeylen_len - n_ikeylen_len;
  int new_n_ikey_startpos = new_ikey_startpos + new_ikey_len - n_ikey_len;
  int new_n_lv_startpos = new_lv_startpos + new_lv_len - n_lv_len;
  int new_n_value_startpos = new_value_startpos + new_value_len - n_value_len; //values
  int new_n_ksuf_offset_startpos = new_ksuf_offset_startpos + new_ksuf_offset_len - n_ksuf_offset_len;
  int new_n_ksuf_startpos = new_ksuf_startpos + new_ksuf_len - n_ksuf_len;

  //move the arrays in n and prepare for merging
  uint8_t* n_ikeylen = (uint8_t*)((char*)n_ + new_n_ikeylen_startpos);
  ikey_type* n_ikey = (ikey_type*)((char*)n_ + new_n_ikey_startpos);
  leafvalue_static_multivalue<P>* n_lv = (leafvalue_static_multivalue<P>*)((char*)n_ + new_n_lv_startpos);
  char* n_value = (char*)((char*)n_ + new_n_value_startpos); //values
  uint32_t* n_ksuf_offset = (uint32_t*)((char*)n_ + new_n_ksuf_offset_startpos);
  char* n_ksuf = (char*)((char*)n_ + new_n_ksuf_startpos);

  uint8_t* new_n_ikeylen = (uint8_t*)((char*)n_ + new_ikeylen_startpos);
  ikey_type* new_n_ikey = (ikey_type*)((char*)n_ + new_ikey_startpos);
  leafvalue_static_multivalue<P>* new_n_lv = (leafvalue_static_multivalue<P>*)((char*)n_ + new_lv_startpos);
  char* new_n_value = (char*)((char*)n_ + new_value_startpos); //values
  uint32_t* new_n_ksuf_offset = (uint32_t*)((char*)n_ + new_ksuf_offset_startpos);
  char* new_n_ksuf = (char*)((char*)n_ + new_ksuf_startpos);

  memmove((void*)(n_ksuf), (void*)((char*)n_ + n_ksuf_startpos), n_ksuf_len);
  memmove((void*)(n_ksuf_offset), (void*)((char*)n_ + n_ksuf_offset_startpos), n_ksuf_offset_len);
  memmove((void*)(n_value), (void*)((char*)n_ + n_value_startpos), n_value_len); //values
  memmove((void*)(n_lv), (void*)((char*)n_ + n_lv_startpos), n_lv_len);
  memmove((void*)(n_ikey), (void*)((char*)n_ + n_ikey_startpos), n_ikey_len);
  memmove((void*)(n_ikeylen), (void*)((char*)n_ + n_ikeylen_startpos), n_ikeylen_len);

  //merge
  //---------------------------------------------------------------------------------
  bool m_inserted = false;
  int n_pos = 0;
  int new_n_pos = 0;
  int new_nkeys = new_max_nkeys;

  char* new_n_ksuf_pos = new_n_ksuf;
  char* n_ksuf_pos = n_ksuf;

  int copy_ksuf_len = 0;

  //values
  char* new_n_value_pos = new_n_value;
  char* n_value_pos = n_value;

  int copy_value_len = 0;

  int start_task_pos = task_.size();

  while (!m_inserted && (n_pos < n_nkeys)) {
    //if deleted
    if (n_ikeylen[n_pos] == 0) {
      //std::cout << "item deleted; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
      n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
      n_value_pos += n_lv[n_pos].value_len(); //values
      n_pos++;
      new_nkeys--;
    }
    else {
      uint8_t m_ikey_length = m_->keylenx_ikeylen(t.ikeylen_m);
      uint8_t n_ikey_length = n_->keylenx_ikeylen(n_ikeylen[n_pos]);
      if (t.ikey_m < n_ikey[n_pos]
	  || ((t.ikey_m == n_ikey[n_pos]) && (m_ikey_length < n_ikey_length))) { 
	//std::cout << "item_m inserted; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	//move item m to the new array
	new_n_ikeylen[new_n_pos] = t.ikeylen_m;
	new_n_ikey[new_n_pos] = t.ikey_m;
	new_n_lv[new_n_pos] = leafvalue_static_multivalue<P>((uint32_t)(new_n_value_pos - new_n_value), (uint32_t)(t.lv_m.value_len())); //values

	//values
	//-----------------------------------------------------------------------
	copy_value_len = t.lv_m.value_len();

	if (copy_value_len > 0)
	  memcpy(new_n_value_pos, t.value_m, copy_value_len);
	//-----------------------------------------------------------------------

	copy_ksuf_len = t.ksuf_len_m;
	new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);
	if (copy_ksuf_len < 0) {
	  std::cout << "ERROR: add_item_to_node1(multivalue), COPY_LENGTH < 0!!!\n";
	  return false;
	}

	if (copy_ksuf_len > 0)
	  memcpy(new_n_ksuf_pos, t.ksuf_m, copy_ksuf_len);

	new_n_pos++;
	m_inserted = true;
	new_n_ksuf_pos += copy_ksuf_len;
	//values
	new_n_value_pos += copy_value_len;
      }
      else if (t.ikey_m > n_ikey[n_pos]
	       || ((t.ikey_m == n_ikey[n_pos]) && (m_ikey_length > n_ikey_length))) { 
	//std::cout << "item_n inserted, m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	//move an item from n to the new array
	new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	new_n_ikey[new_n_pos] = n_ikey[n_pos];

	//values
	//-----------------------------------------------------------------------
	if (n_->keylenx_is_layer(n_ikeylen[n_pos])) {
	  copy_value_len = 0;
	  new_n_lv[new_n_pos] = n_lv[n_pos];
	}
	else {
	  copy_value_len = n_lv[n_pos].value_len();
	  new_n_lv[new_n_pos] = leafvalue_static_multivalue<P>((uint32_t)(new_n_value_pos - new_n_value), (uint32_t)(n_lv[n_pos].value_len())); //values
	}

	if (copy_value_len < 0) {
	  std::cout << "ERROR: add_item_to_node2(multivalue), COPY_VALUE_LENGTH < 0!!!\n";
	  return false;
	}

	if (copy_value_len > 0)
	  memmove(new_n_value_pos, n_value_pos, copy_value_len);
	//-----------------------------------------------------------------------

	copy_ksuf_len = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	if (copy_ksuf_len < 0) {
	  std::cout << "n_ksuf_offset[" << (n_pos + 1) << "] = " << n_ksuf_offset[n_pos + 1] << "\n";
	  std::cout << "n_ksuf_offset[" << n_pos << "] = " << n_ksuf_offset[n_pos] << "\n";
	  std::cout << "ERROR: add_item_to_node2(multivalue), COPY_LENGTH < 0!!!\n";
	  return false;
	}

	if (copy_ksuf_len > 0)
	  memmove(new_n_ksuf_pos, n_ksuf_pos, copy_ksuf_len);

	new_n_pos++;
	n_pos++;
	new_n_ksuf_pos += copy_ksuf_len;
	n_ksuf_pos += copy_ksuf_len;
	//values
	new_n_value_pos += copy_value_len;
	n_value_pos += copy_value_len;
      }
      else { //same keyslice, same ikey length
	if (n_->keylenx_is_layer(n_ikeylen[n_pos])) {
	  //std::cout << "n layer; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	  //if n_pos is layer
	  new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	  new_n_ikey[new_n_pos] = n_ikey[n_pos];
	  //lv TBD
	  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	  merge_task_multivalue tt;
	  tt.task = 1; //merge item m to n
	  tt.parent_node = n_;
	  tt.parent_node_pos = new_n_pos;
	  tt.n = static_cast<massnode_multivalue<P>*>(n_lv[n_pos].layer());

	  tt.lv_m = t.lv_m;
	  //values
	  copy_value_len = t.lv_m.value_len();
	  tt.value_m = (char*)malloc(copy_value_len + 1);
	  memcpy((void*)tt.value_m, (const void*)(t.value_m), copy_value_len);

	  tt.ksuf_len_m = t.ksuf_len_m;
	  //assign next layer ikeylen
	  tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	  //make next layer ikey
	  tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(t.ksuf_m, tt.ksuf_len_m);

	  //make next layer suffix
	  if (tt.ksuf_len_m > sizeof(ikey_type)) {
	    tt.ksuf_len_m -= sizeof(ikey_type);
	    tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	    memcpy((void*)tt.ksuf_m, (const void*)(t.ksuf_m + sizeof(ikey_type)), tt.ksuf_len_m);
	  }
	  else {
	    tt.ksuf_m = 0;
	    tt.ksuf_len_m = 0;
	  }

	  task_.push_back(tt);
	}
	else {
	  //std::cout << "both NOT layer; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	  //if n_pos is NOT layer
	  //values
	  int ksuflen_m = t.ksuf_len_m;
	  int ksuflen_n = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	  if ((ksuflen_m == 0) && (ksuflen_n == 0)) {
	    //std::cout << "both NOT layer 1; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	    new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	    new_n_ikey[new_n_pos] = n_ikey[n_pos];
	    new_n_lv[new_n_pos] = leafvalue_static_multivalue<P>((uint32_t)(new_n_value_pos - new_n_value), (uint32_t)(t.lv_m.value_len() + n_lv[n_pos].value_len())); //values

	    copy_value_len = t.lv_m.value_len();
	    memcpy(new_n_value_pos, t.value_m, copy_value_len);
	    new_n_value_pos += copy_value_len;

	    copy_value_len = n_lv[n_pos].value_len();
	    memmove(new_n_value_pos, n_value_pos, copy_value_len);
	    new_n_value_pos += copy_value_len;
	    n_value_pos += copy_value_len;

	    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);
	  }
	  else if ((ksuflen_m == ksuflen_n) 
		   && (strncmp(t.ksuf_m, n_ksuf_pos, ksuflen_m) == 0)) {
	    //std::cout << "both NOT layer 2; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	    new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	    new_n_ikey[new_n_pos] = n_ikey[n_pos];
	    new_n_lv[new_n_pos] = leafvalue_static_multivalue<P>((uint32_t)(new_n_value_pos - new_n_value), (uint32_t)(t.lv_m.value_len() + n_lv[n_pos].value_len())); //values

	    copy_value_len = t.lv_m.value_len();
	    memcpy(new_n_value_pos, t.value_m, copy_value_len);
	    new_n_value_pos += copy_value_len;

	    copy_value_len = n_lv[n_pos].value_len();
	    memmove(new_n_value_pos, n_value_pos, copy_value_len);
	    new_n_value_pos += copy_value_len;
	    n_value_pos += copy_value_len;

	    copy_ksuf_len = ksuflen_m;
	    memmove(new_n_ksuf_pos, n_ksuf_pos, copy_ksuf_len);

	    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	    new_n_ksuf_pos += copy_ksuf_len;
	    n_ksuf_pos += copy_ksuf_len;
	  }
	  else {
	    //std::cout << "both NOT layer 3; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	    new_n_ikeylen[new_n_pos] = (uint8_t)(n_ikeylen[n_pos] + (uint8_t)64);
	    new_n_ikey[new_n_pos] = n_ikey[n_pos];
	    //lv TBD
	    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	    merge_task_multivalue tt;
	    tt.task = 2; //create a new node to include m and n
	    tt.parent_node = n_;
	    tt.parent_node_pos = new_n_pos;

	    tt.lv_m = t.lv_m;
	    //values
	    copy_value_len = t.lv_m.value_len();
	    tt.value_m = (char*)malloc(copy_value_len + 1);
	    memcpy((void*)tt.value_m, (const void*)(t.value_m), copy_value_len);

	    tt.ksuf_len_m = t.ksuf_len_m;
	    //assign m next layer ikeylen
	    tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	    //make m next layer ikey
	    tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(t.ksuf_m, tt.ksuf_len_m);
	    
	    //make m next layer suffix
	    if (tt.ksuf_len_m > sizeof(ikey_type)) {
	      tt.ksuf_len_m -= sizeof(ikey_type);
	      tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	      memcpy((void*)tt.ksuf_m, (const void*)(t.ksuf_m + sizeof(ikey_type)), tt.ksuf_len_m);
	    }
	    else {
	      tt.ksuf_m = 0;
	      tt.ksuf_len_m = 0;
	    }


	    tt.lv_n = n_lv[n_pos];
	    //values
	    copy_value_len = n_lv[n_pos].value_len();
	    tt.value_n = (char*)malloc(copy_value_len + 1);
	    memcpy((void*)tt.value_n, (const void*)(n_value_pos), copy_value_len);
	    n_value_pos += copy_value_len;

	    tt.ksuf_len_n = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	    //assign n next layer ikeylen
	    tt.ikeylen_n = convert_to_ikeylen(tt.ksuf_len_n);
	    //make n next layer ikey
	    tt.ikey_n = string_slice<ikey_type>::make_comparable_sloppy(n_ksuf_pos, tt.ksuf_len_n);
	    
	    //make n next layer suffix
	    if (tt.ksuf_len_n > sizeof(ikey_type)) {
	      tt.ksuf_len_n -= sizeof(ikey_type);
	      tt.ksuf_n = (char*)malloc(tt.ksuf_len_n + 1);
	      memcpy((void*)tt.ksuf_n, (const void*)(n_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_n);
	    }
	    else {
	      tt.ksuf_n = 0;
	      tt.ksuf_len_n = 0;
	    }

	    task_.push_back(tt);
	    n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
	  }
	}

	new_nkeys--;
	new_n_pos++;
	m_inserted = true;
	n_pos++;
      } //same keyslice, same key length else end
    } //else end
  } //while end

  //if item m has not been inserted, insert it now
  if (!m_inserted) {
    //std::cout << "m leftovers; m_inserted = " << m_inserted <<"\n";
    new_n_ikeylen[new_n_pos] = t.ikeylen_m;
    new_n_ikey[new_n_pos] = t.ikey_m;

    //values
    //-----------------------------------------------------------------------
    if (m_->keylenx_is_layer(t.ikeylen_m)) {
      copy_value_len = 0;
      new_n_lv[new_n_pos] = t.lv_m;
    }
    else {
      copy_value_len = t.lv_m.value_len();
      new_n_lv[new_n_pos] = leafvalue_static_multivalue<P>((uint32_t)(new_n_value_pos - new_n_value), (uint32_t)(t.lv_m.value_len())); //values
    }

    if (copy_value_len < 0) {
      std::cout << "ERROR: merge_node6(multivalue), COPY_VALUE_LENGTH < 0!!!\n";
      return false;
    }

    if (copy_value_len > 0)
      memcpy(new_n_value_pos, t.value_m, copy_value_len);
    //-----------------------------------------------------------------------
    
    copy_ksuf_len = t.ksuf_len_m;
    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

    if (copy_ksuf_len < 0) {
      std::cout << "ERROR: add_item_to_node3(multivalue), COPY_LENGTH < 0!!!\n";
      return false;
    }

    if (copy_ksuf_len > 0)
      memcpy(new_n_ksuf_pos, t.ksuf_m, copy_ksuf_len);
    
    new_n_pos++;
    m_inserted = true;
    new_n_ksuf_pos += copy_ksuf_len;
    //values
    new_n_value_pos += copy_value_len;
  }

  //if n has leftovers, shift them to the new positions
  while (n_pos < n_nkeys) {
    //std::cout << "n leftovers; n_pos = " << n_pos << "\n";
    new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
    new_n_ikey[new_n_pos] = n_ikey[n_pos];

    //values
    //-----------------------------------------------------------------------
    if (n_->keylenx_is_layer(n_ikeylen[n_pos])) {
      copy_value_len = 0;
      new_n_lv[new_n_pos] = n_lv[n_pos];
    }
    else {
      copy_value_len = n_lv[n_pos].value_len();
      new_n_lv[new_n_pos] = leafvalue_static_multivalue<P>((uint32_t)(new_n_value_pos - new_n_value), (uint32_t)(n_lv[n_pos].value_len())); //values
    }

    if (copy_value_len < 0) {
      std::cout << "ERROR: merge_node7(multivalue), COPY_VALUE_LENGTH < 0!!!\n";
      return false;
    }

    if (copy_value_len > 0)
      memmove(new_n_value_pos, n_value_pos, copy_value_len);
    //-----------------------------------------------------------------------

    copy_ksuf_len = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

    if (copy_ksuf_len < 0) {
      std::cout << "ERROR: add_item_to_node4(multivalue), COPY_LENGTH < 0!!!\n";
      return false;
    }

    if (copy_ksuf_len > 0)
      memmove(new_n_ksuf_pos, n_ksuf_pos, copy_ksuf_len);

    new_n_pos++;
    n_pos++;
    new_n_ksuf_pos += copy_ksuf_len;
    n_ksuf_pos += copy_ksuf_len;
    //values
    new_n_value_pos += copy_value_len;
    n_value_pos += copy_value_len;
  }

  //fill in the last ksuf_offset position
  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

  //delete m
  if (t.ksuf_len_m != 0)
    free(t.ksuf_m);
  free(t.value_m);

  //compact n if needed
  int new_ikeylen_size = (int)(sizeof(uint8_t) * new_nkeys);
  int new_ikey_size = (int)(sizeof(ikey_type) * new_nkeys);
  int new_lv_size = (int)(sizeof(leafvalue_static_multivalue<P>) * new_nkeys);
  int new_value_size = (int)(new_n_value_pos - new_n_value); //values
  int new_ksuf_offset_size = (int)(sizeof(uint32_t) * (new_nkeys + 1));
  int new_ksuf_size = new_n_ksuf_offset[new_n_pos];

  int new_size = (int)(sizeof(massnode_multivalue<P>)
		       + new_ikeylen_size
		       + new_ikey_size
		       + new_lv_size
		       + new_value_size //values
		       + new_ksuf_offset_size
		       + new_ksuf_size);

  if (new_nkeys < new_max_nkeys) {
    uint8_t* final_n_ikeylen = (uint8_t*)((char*)n_ + sizeof(massnode_multivalue<P>));
    ikey_type* final_n_ikey = (ikey_type*)((char*)n_ + sizeof(massnode_multivalue<P>)
					   + new_ikeylen_size);
    leafvalue_static_multivalue<P>* final_n_lv 
      = (leafvalue_static_multivalue<P>*)((char*)n_ + sizeof(massnode_multivalue<P>)
					  + new_ikeylen_size
					  + new_ikey_size);
    //values
    char* final_n_value = (char*)((char*)n_ + sizeof(massnode_multivalue<P>)
				  + new_ikeylen_size
				  + new_ikey_size
				  + new_lv_size);
    uint32_t* final_n_ksuf_offset = (uint32_t*)((char*)n_ + sizeof(massnode_multivalue<P>)
						+ new_ikeylen_size
						+ new_ikey_size
						+ new_lv_size
						+ new_value_size); //values
    char* final_n_ksuf = (char*)((char*)n_ + sizeof(massnode_multivalue<P>)
				 + new_ikeylen_size
				 + new_ikey_size
				 + new_lv_size
				 + new_value_size //values
				 + new_ksuf_offset_size);


    memmove((void*)final_n_ikey, (const void*)new_n_ikey, new_ikey_size);
    memmove((void*)final_n_lv, (const void*)new_n_lv, new_lv_size);
    memmove((void*)final_n_value, (const void*)new_n_value, new_value_size); //values
    memmove((void*)final_n_ksuf_offset, (const void*)new_n_ksuf_offset, new_ksuf_offset_size);
    memmove((void*)final_n_ksuf, (const void*)new_n_ksuf, new_ksuf_size);

    //resize(shrink) n
    n_ = n_->resize((size_t)new_size, ti);

    //resize may change the address, update the parent nodes addr in task_
    if (start_task_pos < task_.size()) //this costs 6 hours
      task_[task_.size() - 1].parent_node = n_;
  }

  n_->set_size((uint32_t)new_nkeys);
  n_->set_allocated_size((uint32_t)new_size);
  n_->set_value_size((uint32_t)new_value_size);

  if (t.parent_node == NULL) {
    std::cout << "ERROR: add_item_to_node(multivalue), parent_node is NULL!!!\n";
    return false;
  }

  t.parent_node->set_lv(t.parent_node_pos, leafvalue_static_multivalue<P>(static_cast<node_base<P>*>(n_)));

  return true;
}

//huanchen-static-merge-multivalue
//**********************************************************************************
// stcursor_merge_multivalue::create_node
//**********************************************************************************
template <typename P>
bool stcursor_merge_multivalue<P>::create_node(merge_task_multivalue t, threadinfo &ti) {
  //std::cout << "create_node(multivalue) start$$$$$$$$$$\n";

  size_t ksufSize = 0;
  uint32_t valueSize = 0; //values
  uint32_t nkeys = 0;

  if ((t.ikey_m == t.ikey_n) && (t.ikeylen_m == t.ikeylen_n)) {
    ksufSize = 0;
    valueSize = 0;
    nkeys = 1;
    n_ = massnode_multivalue<P>::make(ksufSize, valueSize, nkeys, ti); //values
    n_->set_ikeylen(0, (uint8_t)(t.ikeylen_n + (uint8_t)64));
    n_->set_ikey(0, t.ikey_n);
    //lv TBD
    n_->set_ksuf_offset(0, (uint32_t)0);
    n_->set_ksuf_offset(1, (uint32_t)0);

    merge_task_multivalue tt;
    tt.task = 2; //create a new node to include m and n
    tt.parent_node = n_;
    tt.parent_node_pos = 0;

    tt.lv_m = t.lv_m;
    //values
    tt.value_m = (char*)malloc(t.lv_m.value_len() + 1);
    memcpy((void*)tt.value_m, (const void*)(t.value_m), t.lv_m.value_len());

    tt.ksuf_len_m = t.ksuf_len_m;
    //assign m next layer ikeylen
    tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
    //make m next layer ikey
    tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(t.ksuf_m, tt.ksuf_len_m);

    //make m next layer suffix
    if (tt.ksuf_len_m > sizeof(ikey_type)) {
      tt.ksuf_len_m -= sizeof(ikey_type);
      tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
      memcpy((void*)tt.ksuf_m, (const void*)(t.ksuf_m + sizeof(ikey_type)), tt.ksuf_len_m);
    }
    else {
      tt.ksuf_m = 0;
      tt.ksuf_len_m = 0;
    }

    tt.lv_n = t.lv_n;
    //values
    tt.value_n = (char*)malloc(t.lv_n.value_len() + 1);
    memcpy((void*)tt.value_n, (const void*)(t.value_n), t.lv_n.value_len());

    tt.ksuf_len_n = t.ksuf_len_n;
    //assign m next layer ikeylen
    tt.ikeylen_n = convert_to_ikeylen(tt.ksuf_len_n);
    //make m next layer ikey
    tt.ikey_n = string_slice<ikey_type>::make_comparable_sloppy(t.ksuf_n, tt.ksuf_len_n);

    //make m next layer suffix
    if (tt.ksuf_len_n > sizeof(ikey_type)) {
      tt.ksuf_len_n -= sizeof(ikey_type);
      tt.ksuf_n = (char*)malloc(tt.ksuf_len_n + 1);
      memcpy((void*)tt.ksuf_n, (const void*)(t.ksuf_n + sizeof(ikey_type)), tt.ksuf_len_n);
    }
    else {
      tt.ksuf_n = 0;
      tt.ksuf_len_n = 0;
    }

    task_.push_back(tt);
  }
  else {
    ksufSize = t.ksuf_len_m + t.ksuf_len_n;
    valueSize = (t.lv_m.value_len() + t.lv_n.value_len());
    nkeys = 2;
    n_ = massnode_multivalue<P>::make(ksufSize, valueSize, nkeys, ti);

    if ((t.ikey_m < t.ikey_n)
	|| ((t.ikey_m == t.ikey_n) && (t.ikeylen_m < t.ikeylen_n))) {
      n_->set_ikeylen(0, t.ikeylen_m);
      n_->set_ikey(0, t.ikey_m);
      n_->set_lv(0, leafvalue_static_multivalue<P>((uint32_t)0, (uint32_t)(t.lv_m.value_len()))); //values
      n_->set_ksuf_offset(0, (uint32_t)0);
      n_->set_ksuf_offset(1, (uint32_t)t.ksuf_len_m);

      n_->set_ikeylen(1, t.ikeylen_n);
      n_->set_ikey(1, t.ikey_n);
      n_->set_lv(1, leafvalue_static_multivalue<P>((uint32_t)(t.lv_m.value_len()), (uint32_t)(t.lv_n.value_len()))); //values

      n_->set_ksuf_offset(2, (uint32_t)(t.ksuf_len_m + t.ksuf_len_n));

      //values
      if (t.lv_m.value_len() > 0)
	memcpy((void*)(n_->valuepos(0)), (const void*)t.value_m, t.lv_m.value_len());
      if (t.lv_n.value_len() > 0)
	memcpy((void*)(n_->valuepos(1)), (const void*)t.value_n, t.lv_n.value_len());

      if ((t.ksuf_len_m < 0) || (t.ksuf_len_n) < 0) {
	std::cout << "ERROR: (multivalue) ksuf copy length < 0!!!\n";
	return false;
      }

      if (t.ksuf_len_m > 0)
	memcpy((void*)(n_->ksufpos(0)), (const void*)t.ksuf_m, t.ksuf_len_m);
      if (t.ksuf_len_n > 0)
	memcpy((void*)(n_->ksufpos(1)), (const void*)t.ksuf_n, t.ksuf_len_n);
    }
    else {
      n_->set_ikeylen(0, t.ikeylen_n);
      n_->set_ikey(0, t.ikey_n);
      n_->set_lv(0, leafvalue_static_multivalue<P>((uint32_t)0, (uint32_t)(t.lv_n.value_len()))); //values
      n_->set_ksuf_offset(0, (uint32_t)0);
      n_->set_ksuf_offset(1, (uint32_t)t.ksuf_len_n);

      n_->set_ikeylen(1, t.ikeylen_m);
      n_->set_ikey(1, t.ikey_m);
      n_->set_lv(1, leafvalue_static_multivalue<P>((uint32_t)(t.lv_n.value_len()), (uint32_t)(t.lv_m.value_len()))); //values
      n_->set_ksuf_offset(2, (uint32_t)(t.ksuf_len_n + t.ksuf_len_m));

      //values
      if (t.lv_n.value_len() > 0)
	memcpy((void*)(n_->valuepos(0)), (const void*)t.value_n, t.lv_n.value_len());
      if (t.lv_m.value_len() > 0)
	memcpy((void*)(n_->valuepos(1)), (const void*)t.value_m, t.lv_m.value_len());

      if ((t.ksuf_len_m < 0) || (t.ksuf_len_n) < 0) {
	std::cout << "ERROR: (multivalue) ksuf copy length < 0!!!\n";
	return false;
      }

      if (t.ksuf_len_n > 0)
	memcpy((void*)(n_->ksufpos(0)), (const void*)t.ksuf_n, t.ksuf_len_n);
      if (t.ksuf_len_m > 0)
	memcpy((void*)(n_->ksufpos(1)), (const void*)t.ksuf_m, t.ksuf_len_m);
    }
  }

  //delete m
  if (t.ksuf_len_m != 0)
    free(t.ksuf_m);
  free(t.value_m);
  //delete n
  if (t.ksuf_len_n != 0)
    free(t.ksuf_n);
  free(t.value_n);

  if (t.parent_node == NULL) {
    std::cout << "ERROR: create_node(multivalue), parent_node is NULL!!!\n";
    return false;
  }

  t.parent_node->set_lv(t.parent_node_pos, leafvalue_static_multivalue<P>(static_cast<node_base<P>*>(n_)));

  return true;
}

//huanchen-static-merge-multivalue
//**********************************************************************************
// stcursor_merge_multivalue::merge
//**********************************************************************************
template <typename P>
bool stcursor_merge_multivalue<P>::merge(threadinfo &ti, threadinfo &ti_merge) {
  merge_task_multivalue t;
  bool merge_success = true;

  //std::cout << "MERGE START\n";
  t.task = 0; //merge m to n
  t.parent_node = NULL;
  t.m = static_cast<massnode_multivalue<P>*>(merge_root_);
  t.n = static_cast<massnode_multivalue<P>*>(root_);
  task_.push_back(t);

  int cur_pos = 0;
  while (cur_pos < task_.size()) {
    if (task_.size() % 10000 == 0)
      std::cout << "task_.size() = " << task_.size() << "\n";
    if (task_[cur_pos].task == 0)
      merge_success = merge_nodes(task_[cur_pos], ti, ti_merge);
    else if (task_[cur_pos].task == 1)
      merge_success = add_item_to_node(task_[cur_pos], ti);
    else if (task_[cur_pos].task == 2)
      merge_success = create_node(task_[cur_pos], ti);
    else
      return false;
    cur_pos++;
    if (!merge_success) {
      std::cout << "MERGE FAIL!!!\n";
      return false;
    }
  }

  //std::cout << "merge(multivalue) success-----------------------------------------\n";
  return merge_success;
}

//huanchen-static-merge-multivalue
//**********************************************************************************
// stcursor_merge_multivalue::convert_to_ikeylen
//**********************************************************************************
template <typename P>
inline uint8_t stcursor_merge_multivalue<P>::convert_to_ikeylen(uint32_t len) {
  uint8_t ikeylen = (uint8_t)0;
  //if (len >= 8)
  //ikeylen = (uint8_t)8; 
  if (len > 8)
    ikeylen = (uint8_t)9;
  else if (len == 8)
    ikeylen = (uint8_t)8;
  else if (len == 7)
    ikeylen = (uint8_t)7;
  else if (len == 6)
    ikeylen = (uint8_t)6;
  else if (len == 5)
    ikeylen = (uint8_t)5;
  else if (len == 4)
    ikeylen = (uint8_t)4;
  else if (len == 3)
    ikeylen = (uint8_t)3;
  else if (len == 2)
    ikeylen = (uint8_t)2;
  else if (len == 1)
    ikeylen = (uint8_t)1;
  else
    return ikeylen;
  return ikeylen;
}




//huanchen-static-merge-dynamicvalue
//**********************************************************************************
// stcursor_merge_dynamicvalue::merge_nodes
//**********************************************************************************
template <typename P>
bool stcursor_merge_dynamicvalue<P>::merge_nodes(merge_task_dynamicvalue t, threadinfo &ti, threadinfo &ti_merge) {
  //std::cout << "merge_nodes(dynamicvalue) start$$$$$$$$$$$\n";
  //std::cout << "alloc1 = " << ti.alloc << "\n";
  /*
  if ((t.m == NULL) || (t.n == NULL)) {
    std::cout << "ERROR: merge_node(dynamicvalue), node m or n is NULL!!!\n";
    return false;
  }

  m_ = t.m;
  n_ = t.n;
  */

  if (t.m == NULL) {
    std::cout << "ERROR: merge_node, node m is NULL!!!\n";
    return false;
  }
  m_ = t.m;

  if (t.n == NULL) {
    root_ = m_;
    return true;
  }
  else
    n_ = t.n;

  //calculate size & num_keys of m, n and the tmp new node
  int m_size = m_->allocated_size();
  int n_size = n_->allocated_size();
  int new_max_size = m_size + n_size - sizeof(massnode_dynamicvalue<P>) - sizeof(uint32_t);
  int m_nkeys = m_->size();
  int n_nkeys = n_->size();
  int new_max_nkeys = m_nkeys + n_nkeys;

  //resize(expand) n
  n_ = n_->resize((size_t)new_max_size, ti);
  //n_->set_allocated_size((size_t)new_max_size);
  //std::cout << "alloc2 = " << ti.alloc << "\n";

  //calculate the start position offsets of each array in m, n and tmp new
  int new_ikeylen_startpos = sizeof(massnode_dynamicvalue<P>);
  int n_ikeylen_startpos = sizeof(massnode_dynamicvalue<P>);
  int m_ikeylen_startpos = sizeof(massnode_dynamicvalue<P>);
  int new_ikeylen_len = (int)(sizeof(uint8_t) * new_max_nkeys);
  int n_ikeylen_len = (int)(sizeof(uint8_t) * n_nkeys);
  int m_ikeylen_len = (int)(sizeof(uint8_t) * m_nkeys);

  int new_ikey_startpos = new_ikeylen_startpos + new_ikeylen_len;
  int n_ikey_startpos = n_ikeylen_startpos + n_ikeylen_len;
  int m_ikey_startpos = m_ikeylen_startpos + m_ikeylen_len;
  int new_ikey_len = (int)(sizeof(ikey_type) * new_max_nkeys);
  int n_ikey_len = (int)(sizeof(ikey_type) * n_nkeys);
  int m_ikey_len = (int)(sizeof(ikey_type) * m_nkeys);

  int new_lv_startpos = new_ikey_startpos + new_ikey_len;
  int n_lv_startpos = n_ikey_startpos + n_ikey_len;
  int m_lv_startpos = m_ikey_startpos + m_ikey_len;
  int new_lv_len = (int)(sizeof(leafvalue<P>) * new_max_nkeys);
  int n_lv_len = (int)(sizeof(leafvalue<P>) * n_nkeys);
  int m_lv_len = (int)(sizeof(leafvalue<P>) * m_nkeys);

  int new_ksuf_offset_startpos = new_lv_startpos + new_lv_len;
  int n_ksuf_offset_startpos = n_lv_startpos + n_lv_len;
  int m_ksuf_offset_startpos = m_lv_startpos + m_lv_len;
  int new_ksuf_offset_len = (int)(sizeof(uint32_t) * (new_max_nkeys + 1));
  int n_ksuf_offset_len = (int)(sizeof(uint32_t) * (n_nkeys + 1));
  int m_ksuf_offset_len = (int)(sizeof(uint32_t) * (m_nkeys + 1));

  int new_ksuf_startpos = new_ksuf_offset_startpos + new_ksuf_offset_len;
  int n_ksuf_startpos = n_ksuf_offset_startpos + n_ksuf_offset_len;
  int m_ksuf_startpos = m_ksuf_offset_startpos + m_ksuf_offset_len;
  int new_ksuf_len = new_max_size - new_ksuf_startpos;
  int n_ksuf_len = n_size - n_ksuf_startpos;
  int m_ksuf_len = m_size - m_ksuf_startpos;

  //calculate the start position offset of moved arrays in n
  int new_n_ikeylen_startpos = new_ikeylen_startpos + new_ikeylen_len - n_ikeylen_len;
  int new_n_ikey_startpos = new_ikey_startpos + new_ikey_len - n_ikey_len;
  int new_n_lv_startpos = new_lv_startpos + new_lv_len - n_lv_len;
  int new_n_ksuf_offset_startpos = new_ksuf_offset_startpos + new_ksuf_offset_len - n_ksuf_offset_len;
  int new_n_ksuf_startpos = new_ksuf_startpos + new_ksuf_len - n_ksuf_len;

  //move the arrays in n and prepare for merging
  uint8_t* m_ikeylen = (uint8_t*)((char*)m_ + m_ikeylen_startpos);
  ikey_type* m_ikey = (ikey_type*)((char*)m_ + m_ikey_startpos);
  leafvalue<P>* m_lv = (leafvalue<P>*)((char*)m_ + m_lv_startpos);
  uint32_t* m_ksuf_offset = (uint32_t*)((char*)m_ + m_ksuf_offset_startpos);
  char* m_ksuf = (char*)((char*)m_ + m_ksuf_startpos);

  uint8_t* n_ikeylen = (uint8_t*)((char*)n_ + new_n_ikeylen_startpos);
  ikey_type* n_ikey = (ikey_type*)((char*)n_ + new_n_ikey_startpos);
  leafvalue<P>* n_lv = (leafvalue<P>*)((char*)n_ + new_n_lv_startpos);
  uint32_t* n_ksuf_offset = (uint32_t*)((char*)n_ + new_n_ksuf_offset_startpos);
  char* n_ksuf = (char*)((char*)n_ + new_n_ksuf_startpos);

  uint8_t* new_n_ikeylen = (uint8_t*)((char*)n_ + new_ikeylen_startpos);
  ikey_type* new_n_ikey = (ikey_type*)((char*)n_ + new_ikey_startpos);
  leafvalue<P>* new_n_lv = (leafvalue<P>*)((char*)n_ + new_lv_startpos);
  uint32_t* new_n_ksuf_offset = (uint32_t*)((char*)n_ + new_ksuf_offset_startpos);
  char* new_n_ksuf = (char*)((char*)n_ + new_ksuf_startpos);

  memmove((void*)(n_ksuf), (void*)((char*)n_ + n_ksuf_startpos), n_ksuf_len);
  memmove((void*)(n_ksuf_offset), (void*)((char*)n_ + n_ksuf_offset_startpos), n_ksuf_offset_len);
  memmove((void*)(n_lv), (void*)((char*)n_ + n_lv_startpos), n_lv_len);
  memmove((void*)(n_ikey), (void*)((char*)n_ + n_ikey_startpos), n_ikey_len);
  memmove((void*)(n_ikeylen), (void*)((char*)n_ + n_ikeylen_startpos), n_ikeylen_len);

  //merge
  //---------------------------------------------------------------------------------
  int m_pos = 0;
  int n_pos = 0;
  int new_n_pos = 0;

  char* new_n_ksuf_pos = new_n_ksuf;
  char* m_ksuf_pos = m_ksuf;
  char* n_ksuf_pos = n_ksuf;

  int copy_ksuf_len = 0;

  int start_task_pos = task_.size();

  while ((m_pos < m_nkeys) && (n_pos < n_nkeys)) {
    //if deleted
    if (n_ikeylen[n_pos] == 0) {
      //std::cout << "item deleted; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
      n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
      n_pos++;
    }
    else {
      uint8_t m_ikey_length = m_->keylenx_ikeylen(m_ikeylen[m_pos]);
      uint8_t n_ikey_length = n_->keylenx_ikeylen(n_ikeylen[n_pos]);
      if (m_ikey[m_pos] < n_ikey[n_pos]
	  || ((m_ikey[m_pos] == n_ikey[n_pos]) && (m_ikey_length < n_ikey_length))) { 
	//std::cout << "item_m inserted; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	//move an item from m to the new array
	new_n_ikeylen[new_n_pos] = m_ikeylen[m_pos];
	new_n_ikey[new_n_pos] = m_ikey[m_pos];
	new_n_lv[new_n_pos] = m_lv[m_pos];

	copy_ksuf_len = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
	new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	if (copy_ksuf_len < 0) {
	  std::cout << "ERROR: merge_node1(dynamicvalue), COPY_LENGTH < 0!!!\n";
	  return false;
	}

	if (copy_ksuf_len > 0)
	  memcpy(new_n_ksuf_pos, m_ksuf_pos, copy_ksuf_len);

	new_n_pos++;
	m_pos++;
	new_n_ksuf_pos += copy_ksuf_len;
	m_ksuf_pos += copy_ksuf_len;
      }
      else if (m_ikey[m_pos] > n_ikey[n_pos]
	       || ((m_ikey[m_pos] == n_ikey[n_pos]) && (m_ikey_length > n_ikey_length))) { 
	//std::cout << "item_n inserted, m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	//move an item from n to the new array
	new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	new_n_ikey[new_n_pos] = n_ikey[n_pos];
	new_n_lv[new_n_pos] = n_lv[n_pos];

	copy_ksuf_len = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	if (copy_ksuf_len < 0) {
	  std::cout << "n_ksuf_offset[" << (n_pos + 1) << "] = " << n_ksuf_offset[n_pos + 1] << "\n";
	  std::cout << "n_ksuf_offset[" << n_pos << "] = " << n_ksuf_offset[n_pos] << "\n";
	  std::cout << "ERROR: merge_node2(dynamicvalue), COPY_LENGTH < 0!!!\n";
	  return false;
	}

	if (copy_ksuf_len > 0)
	  memmove(new_n_ksuf_pos, n_ksuf_pos, copy_ksuf_len);

	new_n_pos++;
	n_pos++;
	new_n_ksuf_pos += copy_ksuf_len;
	n_ksuf_pos += copy_ksuf_len;
      }
      else { //same keyslice, same ikey length
	if (m_->keylenx_is_layer(m_ikeylen[m_pos]) && n_->keylenx_is_layer(n_ikeylen[n_pos])) {
	  //std::cout << "both layers; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	  //if m_pos is layer AND n_pos is layer
	  new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	  new_n_ikey[new_n_pos] = n_ikey[n_pos];
	  //lv TBD
	  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	  merge_task_dynamicvalue tt;
	  tt.task = 0; //merge node m to n
	  tt.parent_node = n_;
	  tt.parent_node_pos = new_n_pos;
	  tt.m = static_cast<massnode_dynamicvalue<P>*>(m_lv[m_pos].layer());
	  tt.n = static_cast<massnode_dynamicvalue<P>*>(n_lv[n_pos].layer());

	  task_.push_back(tt);
	}
	else if (n_->keylenx_is_layer(n_ikeylen[n_pos])) {
	  //std::cout << "n layer; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	  //if n_pos is layer
	  new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	  new_n_ikey[new_n_pos] = n_ikey[n_pos];
	  //lv TBD
	  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	  merge_task_dynamicvalue tt;
	  tt.task = 1; //merge item_m to n
	  tt.parent_node = n_;
	  tt.parent_node_pos = new_n_pos;
	  tt.n = static_cast<massnode_dynamicvalue<P>*>(n_lv[n_pos].layer());

	  tt.lv_m = m_lv[m_pos];

	  tt.ksuf_len_m = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
	  //assign next layer ikeylen
	  tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	  //make next layer ikey
	  tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(m_ksuf_pos, tt.ksuf_len_m);
	    
	  //make next layer suffix
	  if (tt.ksuf_len_m > sizeof(ikey_type)) {
	    tt.ksuf_len_m -= sizeof(ikey_type);
	    tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	    memcpy((void*)tt.ksuf_m, (const void*)(m_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_m);
	  }
	  else {
	    tt.ksuf_m = 0;
	    tt.ksuf_len_m = 0;
	  }

	  task_.push_back(tt);
	  m_ksuf_pos += (m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos]);
	}
	else if (m_->keylenx_is_layer(m_ikeylen[m_pos])) {
	  //std::cout << "m layer; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	  //if m_pos is layer
	  new_n_ikeylen[new_n_pos] = m_ikeylen[m_pos];
	  new_n_ikey[new_n_pos] = m_ikey[m_pos];
	  //lv TBD
	  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	  merge_task_dynamicvalue tt;
	  tt.task = 1; //merge item m to n
	  tt.parent_node = n_;
	  tt.parent_node_pos = new_n_pos;
	  tt.n = static_cast<massnode_dynamicvalue<P>*>(m_lv[m_pos].layer());

	  tt.lv_m = n_lv[n_pos];

	  tt.ksuf_len_m = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	  //assign next layer ikeylen
	  tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	  //make next layer ikey
	  tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(n_ksuf_pos, tt.ksuf_len_m);
	    
	  //make next layer suffix
	  if (tt.ksuf_len_m > sizeof(ikey_type)) {
	    tt.ksuf_len_m -= sizeof(ikey_type);
	    tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	    memcpy((void*)tt.ksuf_m, (const void*)(n_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_m);
	  }
	  else {
	    tt.ksuf_m = 0;
	    tt.ksuf_len_m = 0;
	  }

	  task_.push_back(tt);
	  n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
	}
	else {
	  //std::cout << "both NOT layer; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	  //if neither m_pos nor n_pos is layer
	  //values
	  int ksuflen_m = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
	  int ksuflen_n = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	  if ((ksuflen_m == 0) && (ksuflen_n == 0)) {
	    std::cout << "Error1: same key!\n";
	    return false;
	  }
	  else if ((ksuflen_m == ksuflen_n) 
		   && (strncmp(m_ksuf_pos, n_ksuf_pos, ksuflen_m) == 0)) {
	    std::cout << "Error2: same key!\n";
	    return false;
	  }
	  else {
	    //std::cout << "both NOT layer 3; m_pos = " << m_pos << ", n_pos = " << n_pos << "\n";
	    new_n_ikeylen[new_n_pos] = (uint8_t)(n_ikeylen[n_pos] + (uint8_t)64);
	    new_n_ikey[new_n_pos] = n_ikey[n_pos];
	    //lv TBD
	    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	    merge_task_dynamicvalue tt;
	    tt.task = 2; //create a new node to include m and n
	    tt.parent_node = n_;
	    tt.parent_node_pos = new_n_pos;

	    tt.lv_m = m_lv[m_pos];

	    tt.ksuf_len_m = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
	    //assign m next layer ikeylen
	    tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	    //make m next layer ikey
	    tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(m_ksuf_pos, tt.ksuf_len_m);
	    
	    //make m next layer suffix
	    if (tt.ksuf_len_m > sizeof(ikey_type)) {
	      tt.ksuf_len_m -= sizeof(ikey_type);
	      tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	      memcpy((void*)tt.ksuf_m, (const void*)(m_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_m);
	    }
	    else {
	      tt.ksuf_m = 0;
	      tt.ksuf_len_m = 0;
	    }


	    tt.lv_n = n_lv[n_pos];
	    tt.ksuf_len_n = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	    //assign n next layer ikeylen
	    tt.ikeylen_n = convert_to_ikeylen(tt.ksuf_len_n);
	    //make n next layer ikey
	    tt.ikey_n = string_slice<ikey_type>::make_comparable_sloppy(n_ksuf_pos, tt.ksuf_len_n);
	    
	    //make n next layer suffix
	    if (tt.ksuf_len_n > sizeof(ikey_type)) {
	      tt.ksuf_len_n -= sizeof(ikey_type);
	      tt.ksuf_n = (char*)malloc(tt.ksuf_len_n + 1);
	      memcpy((void*)tt.ksuf_n, (const void*)(n_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_n);
	    }
	    else {
	      tt.ksuf_n = 0;
	      tt.ksuf_len_n = 0;
	    }

	    task_.push_back(tt);
	    m_ksuf_pos += (m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos]);
	    n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
	  }
	}

	new_n_pos++;
	m_pos++;
	n_pos++;
      } //same keyslice, same ikey length else end
    } //else end
  } //while end

  //if m has leftovers, move them to the new node
  while (m_pos < m_nkeys) {
    //std::cout << "m leftovers; m_pos = " << m_pos <<"\n";
    new_n_ikeylen[new_n_pos] = m_ikeylen[m_pos];
    new_n_ikey[new_n_pos] = m_ikey[m_pos];
    new_n_lv[new_n_pos] = m_lv[m_pos];

    copy_ksuf_len = m_ksuf_offset[m_pos + 1] - m_ksuf_offset[m_pos];
    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

    if (copy_ksuf_len < 0) {
      std::cout << "ERROR: merge_node3(dynamicvalue), COPY_LENGTH < 0!!!\n";
      std::cout << "m_ksuf_offset[" << (m_pos + 1) << "] = " << m_ksuf_offset[m_pos + 1] << "\n";
      std::cout << "m_ksuf_offset[" << m_pos << "] = " << m_ksuf_offset[m_pos] << "\n";
      return false;
    }

    if (copy_ksuf_len > 0)
      memcpy(new_n_ksuf_pos, m_ksuf_pos, copy_ksuf_len);
    
    //new_nkeys++;
    new_n_pos++;
    m_pos++;
    new_n_ksuf_pos += copy_ksuf_len;
    m_ksuf_pos += copy_ksuf_len;
  }

  //if n has leftovers, shift them to the new positions
  while (n_pos < n_nkeys) {
    //std::cout << "n leftovers; n_pos = " << n_pos << "\n";
    new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
    new_n_ikey[new_n_pos] = n_ikey[n_pos];
    new_n_lv[new_n_pos] = n_lv[n_pos];

    copy_ksuf_len = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);
    if (copy_ksuf_len < 0) {
      std::cout << "ERROR: merge_node4(dynamicvalue), COPY_LENGTH < 0!!!\n";
      return false;
    }

    if (copy_ksuf_len > 0)
      memmove(new_n_ksuf_pos, n_ksuf_pos, copy_ksuf_len);

    new_n_pos++;
    n_pos++;
    new_n_ksuf_pos += copy_ksuf_len;
    n_ksuf_pos += copy_ksuf_len;
  }

  //fill in the last ksuf_offset position
  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

  //delete m
  m_->deallocate(ti_merge);

  //compact n if needed
  int new_nkeys = new_n_pos;
  int new_ikeylen_size = (int)(sizeof(uint8_t) * new_nkeys);
  int new_ikey_size = (int)(sizeof(ikey_type) * new_nkeys);
  int new_lv_size = (int)(sizeof(leafvalue<P>) * new_nkeys);
  int new_ksuf_offset_size = (int)(sizeof(uint32_t) * (new_nkeys + 1));
  int new_ksuf_size = new_n_ksuf_offset[new_n_pos];

  int new_size = (int)(sizeof(massnode_dynamicvalue<P>)
		       + new_ikeylen_size
		       + new_ikey_size
		       + new_lv_size
		       + new_ksuf_offset_size
		       + new_ksuf_size);

  if (new_nkeys > new_max_nkeys) {
    std::cout << "ERROR: new_nkeys > new_max_nkeys!!!\n";
    return false;
  }

  if (new_nkeys < new_max_nkeys) {
    uint8_t* final_n_ikeylen = (uint8_t*)((char*)n_ + sizeof(massnode_dynamicvalue<P>));
    ikey_type* final_n_ikey = (ikey_type*)((char*)n_ + sizeof(massnode_dynamicvalue<P>)
					   + new_ikeylen_size);
    leafvalue<P>* final_n_lv 
      = (leafvalue<P>*)((char*)n_ + sizeof(massnode_dynamicvalue<P>)
					  + new_ikeylen_size
					  + new_ikey_size);
    uint32_t* final_n_ksuf_offset = (uint32_t*)((char*)n_ + sizeof(massnode_dynamicvalue<P>)
						+ new_ikeylen_size
						+ new_ikey_size
						+ new_lv_size);
    char* final_n_ksuf = (char*)((char*)n_ + sizeof(massnode_dynamicvalue<P>)
				 + new_ikeylen_size
				 + new_ikey_size
				 + new_lv_size
				 + new_ksuf_offset_size);
  
    memmove((void*)final_n_ikey, (const void*)new_n_ikey, new_ikey_size);
    memmove((void*)final_n_lv, (const void*)new_n_lv, new_lv_size);
    memmove((void*)final_n_ksuf_offset, (const void*)new_n_ksuf_offset, new_ksuf_offset_size);
    memmove((void*)final_n_ksuf, (const void*)new_n_ksuf, new_ksuf_size);

    //resize(shrink) n
    n_->set_allocated_size((size_t)new_max_size);
    n_ = n_->resize((size_t)new_size, ti);
    //std::cout << "alloc3 = " << ti.alloc << "\n";

    //resize may change the address, update the parent nodes addr in task_
    for (int i = start_task_pos; i < task_.size(); i++)
      task_[i].parent_node = n_;
  }

  n_->set_size((uint32_t)new_nkeys);
  n_->set_allocated_size((size_t)new_size);

  if (t.parent_node)
    t.parent_node->set_lv(t.parent_node_pos, leafvalue<P>(static_cast<node_base<P>*>(n_)));
  else
    root_ = n_;
  //std::cout << "alloc4 = " << ti.alloc << "\n";
  return true;
}


//huanchen-static-merge-dynamicvalue
//**********************************************************************************
// stcursor_merge_dynamicvalue::add_item_to_node
//**********************************************************************************
template <typename P>
bool stcursor_merge_dynamicvalue<P>::add_item_to_node(merge_task_dynamicvalue t, threadinfo &ti) {
  //std::cout << "add_item_to_node(dynamicvalue) start$$$$$$$$$$\n";
  if (t.n == NULL) {
    std::cout << "ERROR: add_item_to_node(dynamicvalue), node n is NULL!!!\n";
    return false;
  }

  n_ = t.n;

  int m_size = (int)(sizeof(uint8_t)
		     + sizeof(ikey_type)
		     + sizeof(leafvalue<P>)
		     + sizeof(uint32_t)
		     + t.ksuf_len_m);
  int n_size = n_->allocated_size();
  int new_max_size = m_size + n_size;
  int n_nkeys = n_->size();
  int new_max_nkeys = n_nkeys + 1;

  //resize(expand) n
  n_ = n_->resize((size_t)new_max_size, ti);
  n_->set_allocated_size((size_t)new_max_size);

  //calculate the start position offsets of each array in n and tmp new
  int new_ikeylen_startpos = sizeof(massnode_dynamicvalue<P>);
  int n_ikeylen_startpos = sizeof(massnode_dynamicvalue<P>);
  int new_ikeylen_len = (int)(sizeof(uint8_t) * new_max_nkeys);
  int n_ikeylen_len = (int)(sizeof(uint8_t) * n_nkeys);

  int new_ikey_startpos = new_ikeylen_startpos + new_ikeylen_len;
  int n_ikey_startpos = n_ikeylen_startpos + n_ikeylen_len;
  int new_ikey_len = (int)(sizeof(ikey_type) * new_max_nkeys);
  int n_ikey_len = (int)(sizeof(ikey_type) * n_nkeys);

  int new_lv_startpos = new_ikey_startpos + new_ikey_len;
  int n_lv_startpos = n_ikey_startpos + n_ikey_len;
  int new_lv_len = (int)(sizeof(leafvalue<P>) * new_max_nkeys);
  int n_lv_len = (int)(sizeof(leafvalue<P>) * n_nkeys);

  int new_ksuf_offset_startpos = new_lv_startpos + new_lv_len;
  int n_ksuf_offset_startpos = n_lv_startpos + n_lv_len;
  int new_ksuf_offset_len = (int)(sizeof(uint32_t) * (new_max_nkeys + 1));
  int n_ksuf_offset_len = (int)(sizeof(uint32_t) * (n_nkeys + 1));

  int new_ksuf_startpos = new_ksuf_offset_startpos + new_ksuf_offset_len;
  int n_ksuf_startpos = n_ksuf_offset_startpos + n_ksuf_offset_len;
  int new_ksuf_len = new_max_size - new_ksuf_startpos;
  int n_ksuf_len = n_size - n_ksuf_startpos;

  //calculate the start position offset of moved arrays in n
  int new_n_ikeylen_startpos = new_ikeylen_startpos + new_ikeylen_len - n_ikeylen_len;
  int new_n_ikey_startpos = new_ikey_startpos + new_ikey_len - n_ikey_len;
  int new_n_lv_startpos = new_lv_startpos + new_lv_len - n_lv_len;
  int new_n_ksuf_offset_startpos = new_ksuf_offset_startpos + new_ksuf_offset_len - n_ksuf_offset_len;
  int new_n_ksuf_startpos = new_ksuf_startpos + new_ksuf_len - n_ksuf_len;

  //move the arrays in n and prepare for merging
  uint8_t* n_ikeylen = (uint8_t*)((char*)n_ + new_n_ikeylen_startpos);
  ikey_type* n_ikey = (ikey_type*)((char*)n_ + new_n_ikey_startpos);
  leafvalue<P>* n_lv = (leafvalue<P>*)((char*)n_ + new_n_lv_startpos);
  uint32_t* n_ksuf_offset = (uint32_t*)((char*)n_ + new_n_ksuf_offset_startpos);
  char* n_ksuf = (char*)((char*)n_ + new_n_ksuf_startpos);

  uint8_t* new_n_ikeylen = (uint8_t*)((char*)n_ + new_ikeylen_startpos);
  ikey_type* new_n_ikey = (ikey_type*)((char*)n_ + new_ikey_startpos);
  leafvalue<P>* new_n_lv = (leafvalue<P>*)((char*)n_ + new_lv_startpos);
  uint32_t* new_n_ksuf_offset = (uint32_t*)((char*)n_ + new_ksuf_offset_startpos);
  char* new_n_ksuf = (char*)((char*)n_ + new_ksuf_startpos);

  memmove((void*)(n_ksuf), (void*)((char*)n_ + n_ksuf_startpos), n_ksuf_len);
  memmove((void*)(n_ksuf_offset), (void*)((char*)n_ + n_ksuf_offset_startpos), n_ksuf_offset_len);
  memmove((void*)(n_lv), (void*)((char*)n_ + n_lv_startpos), n_lv_len);
  memmove((void*)(n_ikey), (void*)((char*)n_ + n_ikey_startpos), n_ikey_len);
  memmove((void*)(n_ikeylen), (void*)((char*)n_ + n_ikeylen_startpos), n_ikeylen_len);

  //merge
  //---------------------------------------------------------------------------------
  bool m_inserted = false;
  int n_pos = 0;
  int new_n_pos = 0;
  int new_nkeys = new_max_nkeys;

  char* new_n_ksuf_pos = new_n_ksuf;
  char* n_ksuf_pos = n_ksuf;

  int copy_ksuf_len = 0;

  int start_task_pos = task_.size();

  while (!m_inserted && (n_pos < n_nkeys)) {
    //if deleted
    if (n_ikeylen[n_pos] == 0) {
      //std::cout << "item deleted; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
      n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
      n_pos++;
      new_nkeys--;
    }
    else {
      uint8_t m_ikey_length = m_->keylenx_ikeylen(t.ikeylen_m);
      uint8_t n_ikey_length = n_->keylenx_ikeylen(n_ikeylen[n_pos]);
      if (t.ikey_m < n_ikey[n_pos]
	  || ((t.ikey_m == n_ikey[n_pos]) && (m_ikey_length < n_ikey_length))) { 
	//std::cout << "item_m inserted; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	//move item m to the new array
	new_n_ikeylen[new_n_pos] = t.ikeylen_m;
	new_n_ikey[new_n_pos] = t.ikey_m;
	new_n_lv[new_n_pos] = t.lv_m;

	copy_ksuf_len = t.ksuf_len_m;
	new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);
	if (copy_ksuf_len < 0) {
	  std::cout << "ERROR: add_item_to_node1(dynamicvalue), COPY_LENGTH < 0!!!\n";
	  return false;
	}

	if (copy_ksuf_len > 0)
	  memcpy(new_n_ksuf_pos, t.ksuf_m, copy_ksuf_len);

	new_n_pos++;
	m_inserted = true;
	new_n_ksuf_pos += copy_ksuf_len;
      }
      else if (t.ikey_m > n_ikey[n_pos]
	       || ((t.ikey_m == n_ikey[n_pos]) && (m_ikey_length > n_ikey_length))) { 
	//std::cout << "item_n inserted, m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	//move an item from n to the new array
	new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	new_n_ikey[new_n_pos] = n_ikey[n_pos];
	new_n_lv[new_n_pos] = n_lv[n_pos];

	copy_ksuf_len = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	if (copy_ksuf_len < 0) {
	  std::cout << "n_ksuf_offset[" << (n_pos + 1) << "] = " << n_ksuf_offset[n_pos + 1] << "\n";
	  std::cout << "n_ksuf_offset[" << n_pos << "] = " << n_ksuf_offset[n_pos] << "\n";
	  std::cout << "ERROR: add_item_to_node2(dynamicvalue), COPY_LENGTH < 0!!!\n";
	  return false;
	}

	if (copy_ksuf_len > 0)
	  memmove(new_n_ksuf_pos, n_ksuf_pos, copy_ksuf_len);

	new_n_pos++;
	n_pos++;
	new_n_ksuf_pos += copy_ksuf_len;
	n_ksuf_pos += copy_ksuf_len;
      }
      else { //same keyslice, same ikey length
	if (n_->keylenx_is_layer(n_ikeylen[n_pos])) {
	  //std::cout << "n layer; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	  //if n_pos is layer
	  new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
	  new_n_ikey[new_n_pos] = n_ikey[n_pos];
	  //lv TBD
	  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	  merge_task_dynamicvalue tt;
	  tt.task = 1; //merge item m to n
	  tt.parent_node = n_;
	  tt.parent_node_pos = new_n_pos;
	  tt.n = static_cast<massnode_dynamicvalue<P>*>(n_lv[n_pos].layer());

	  tt.lv_m = t.lv_m;

	  tt.ksuf_len_m = t.ksuf_len_m;
	  //assign next layer ikeylen
	  tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	  //make next layer ikey
	  tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(t.ksuf_m, tt.ksuf_len_m);

	  //make next layer suffix
	  if (tt.ksuf_len_m > sizeof(ikey_type)) {
	    tt.ksuf_len_m -= sizeof(ikey_type);
	    tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	    memcpy((void*)tt.ksuf_m, (const void*)(t.ksuf_m + sizeof(ikey_type)), tt.ksuf_len_m);
	  }
	  else {
	    tt.ksuf_m = 0;
	    tt.ksuf_len_m = 0;
	  }

	  task_.push_back(tt);
	}
	else {
	  //std::cout << "both NOT layer; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	  //if n_pos is NOT layer
	  //values
	  int ksuflen_m = t.ksuf_len_m;
	  int ksuflen_n = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	  if ((ksuflen_m == 0) && (ksuflen_n == 0)) {
	    std::cout << "Error3: same key!\n";
	    return false;
	  }
	  else if ((ksuflen_m == ksuflen_n) 
		   && (strncmp(t.ksuf_m, n_ksuf_pos, ksuflen_m) == 0)) {
	    std::cout << "Error4: same key!\n";
	    return false;
	  }
	  else {
	    //std::cout << "both NOT layer 3; m_inserted = " << m_inserted << ", n_pos = " << n_pos << "\n";
	    new_n_ikeylen[new_n_pos] = (uint8_t)(n_ikeylen[n_pos] + (uint8_t)64);
	    new_n_ikey[new_n_pos] = n_ikey[n_pos];
	    //lv TBD
	    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

	    merge_task_dynamicvalue tt;
	    tt.task = 2; //create a new node to include m and n
	    tt.parent_node = n_;
	    tt.parent_node_pos = new_n_pos;

	    tt.lv_m = t.lv_m;

	    tt.ksuf_len_m = t.ksuf_len_m;
	    //assign m next layer ikeylen
	    tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
	    //make m next layer ikey
	    tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(t.ksuf_m, tt.ksuf_len_m);
	    
	    //make m next layer suffix
	    if (tt.ksuf_len_m > sizeof(ikey_type)) {
	      tt.ksuf_len_m -= sizeof(ikey_type);
	      tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
	      memcpy((void*)tt.ksuf_m, (const void*)(t.ksuf_m + sizeof(ikey_type)), tt.ksuf_len_m);
	    }
	    else {
	      tt.ksuf_m = 0;
	      tt.ksuf_len_m = 0;
	    }


	    tt.lv_n = n_lv[n_pos];
	    tt.ksuf_len_n = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
	    //assign n next layer ikeylen
	    tt.ikeylen_n = convert_to_ikeylen(tt.ksuf_len_n);
	    //make n next layer ikey
	    tt.ikey_n = string_slice<ikey_type>::make_comparable_sloppy(n_ksuf_pos, tt.ksuf_len_n);
	    
	    //make n next layer suffix
	    if (tt.ksuf_len_n > sizeof(ikey_type)) {
	      tt.ksuf_len_n -= sizeof(ikey_type);
	      tt.ksuf_n = (char*)malloc(tt.ksuf_len_n + 1);
	      memcpy((void*)tt.ksuf_n, (const void*)(n_ksuf_pos + sizeof(ikey_type)), tt.ksuf_len_n);
	    }
	    else {
	      tt.ksuf_n = 0;
	      tt.ksuf_len_n = 0;
	    }

	    task_.push_back(tt);
	    n_ksuf_pos += (n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos]);
	  }
	}

	new_nkeys--;
	new_n_pos++;
	m_inserted = true;
	n_pos++;
      } //same keyslice, same key length else end
    } //else end
  } //while end

  //if item m has not been inserted, insert it now
  if (!m_inserted) {
    //std::cout << "m leftovers; m_inserted = " << m_inserted <<"\n";
    new_n_ikeylen[new_n_pos] = t.ikeylen_m;
    new_n_ikey[new_n_pos] = t.ikey_m;
    new_n_lv[new_n_pos] = t.lv_m;

    copy_ksuf_len = t.ksuf_len_m;
    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

    if (copy_ksuf_len < 0) {
      std::cout << "ERROR: add_item_to_node3(dynamicvalue), COPY_LENGTH < 0!!!\n";
      return false;
    }

    if (copy_ksuf_len > 0)
      memcpy(new_n_ksuf_pos, t.ksuf_m, copy_ksuf_len);
    
    new_n_pos++;
    m_inserted = true;
    new_n_ksuf_pos += copy_ksuf_len;
  }

  //if n has leftovers, shift them to the new positions
  while (n_pos < n_nkeys) {
    //std::cout << "n leftovers; n_pos = " << n_pos << "\n";
    new_n_ikeylen[new_n_pos] = n_ikeylen[n_pos];
    new_n_ikey[new_n_pos] = n_ikey[n_pos];
    new_n_lv[new_n_pos] = n_lv[n_pos];

    copy_ksuf_len = n_ksuf_offset[n_pos + 1] - n_ksuf_offset[n_pos];
    new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

    if (copy_ksuf_len < 0) {
      std::cout << "ERROR: add_item_to_node4(dynamicvalue), COPY_LENGTH < 0!!!\n";
      return false;
    }

    if (copy_ksuf_len > 0)
      memmove(new_n_ksuf_pos, n_ksuf_pos, copy_ksuf_len);

    new_n_pos++;
    n_pos++;
    new_n_ksuf_pos += copy_ksuf_len;
    n_ksuf_pos += copy_ksuf_len;
  }

  //fill in the last ksuf_offset position
  new_n_ksuf_offset[new_n_pos] = (uint32_t)(new_n_ksuf_pos - new_n_ksuf);

  //delete m
  if (t.ksuf_len_m != 0)
    free(t.ksuf_m);

  //compact n if needed
  int new_ikeylen_size = (int)(sizeof(uint8_t) * new_nkeys);
  int new_ikey_size = (int)(sizeof(ikey_type) * new_nkeys);
  int new_lv_size = (int)(sizeof(leafvalue<P>) * new_nkeys);
  int new_ksuf_offset_size = (int)(sizeof(uint32_t) * (new_nkeys + 1));
  int new_ksuf_size = new_n_ksuf_offset[new_n_pos];

  int new_size = (int)(sizeof(massnode_dynamicvalue<P>)
		       + new_ikeylen_size
		       + new_ikey_size
		       + new_lv_size
		       + new_ksuf_offset_size
		       + new_ksuf_size);

  if (new_nkeys < new_max_nkeys) {
    uint8_t* final_n_ikeylen = (uint8_t*)((char*)n_ + sizeof(massnode_dynamicvalue<P>));
    ikey_type* final_n_ikey = (ikey_type*)((char*)n_ + sizeof(massnode_dynamicvalue<P>)
					   + new_ikeylen_size);
    leafvalue<P>* final_n_lv 
      = (leafvalue<P>*)((char*)n_ + sizeof(massnode_dynamicvalue<P>)
					  + new_ikeylen_size
					  + new_ikey_size);
    uint32_t* final_n_ksuf_offset = (uint32_t*)((char*)n_ + sizeof(massnode_dynamicvalue<P>)
						+ new_ikeylen_size
						+ new_ikey_size
						+ new_lv_size);
    char* final_n_ksuf = (char*)((char*)n_ + sizeof(massnode_dynamicvalue<P>)
				 + new_ikeylen_size
				 + new_ikey_size
				 + new_lv_size
				 + new_ksuf_offset_size);


    memmove((void*)final_n_ikey, (const void*)new_n_ikey, new_ikey_size);
    memmove((void*)final_n_lv, (const void*)new_n_lv, new_lv_size);
    memmove((void*)final_n_ksuf_offset, (const void*)new_n_ksuf_offset, new_ksuf_offset_size);
    memmove((void*)final_n_ksuf, (const void*)new_n_ksuf, new_ksuf_size);

    //resize(shrink) n
    n_ = n_->resize((size_t)new_size, ti);

    //resize may change the address, update the parent nodes addr in task_
    if (start_task_pos < task_.size()) //this costs 6 hours
      task_[task_.size() - 1].parent_node = n_;
  }

  n_->set_size((uint32_t)new_nkeys);
  n_->set_allocated_size((uint32_t)new_size);

  if (t.parent_node == NULL) {
    std::cout << "ERROR: add_item_to_node(dynamicvalue), parent_node is NULL!!!\n";
    return false;
  }

  t.parent_node->set_lv(t.parent_node_pos, leafvalue<P>(static_cast<node_base<P>*>(n_)));

  return true;
}

//huanchen-static-merge-dynamicvalue
//**********************************************************************************
// stcursor_merge_dynamicvalue::create_node
//**********************************************************************************
template <typename P>
bool stcursor_merge_dynamicvalue<P>::create_node(merge_task_dynamicvalue t, threadinfo &ti) {
  //std::cout << "create_node(dynamicvalue) start$$$$$$$$$$\n";

  size_t ksufSize = 0;
  uint32_t nkeys = 0;

  if ((t.ikey_m == t.ikey_n) && (t.ikeylen_m == t.ikeylen_n)) {
    ksufSize = 0;
    nkeys = 1;
    n_ = massnode_dynamicvalue<P>::make(ksufSize, nkeys, ti); //values
    n_->set_ikeylen(0, (uint8_t)(t.ikeylen_n + (uint8_t)64));
    n_->set_ikey(0, t.ikey_n);
    //lv TBD
    n_->set_ksuf_offset(0, (uint32_t)0);
    n_->set_ksuf_offset(1, (uint32_t)0);

    merge_task_dynamicvalue tt;
    tt.task = 2; //create a new node to include m and n
    tt.parent_node = n_;
    tt.parent_node_pos = 0;

    tt.lv_m = t.lv_m;
    tt.ksuf_len_m = t.ksuf_len_m;
    //assign m next layer ikeylen
    tt.ikeylen_m = convert_to_ikeylen(tt.ksuf_len_m);
    //make m next layer ikey
    tt.ikey_m = string_slice<ikey_type>::make_comparable_sloppy(t.ksuf_m, tt.ksuf_len_m);

    //make m next layer suffix
    if (tt.ksuf_len_m > sizeof(ikey_type)) {
      tt.ksuf_len_m -= sizeof(ikey_type);
      tt.ksuf_m = (char*)malloc(tt.ksuf_len_m + 1);
      memcpy((void*)tt.ksuf_m, (const void*)(t.ksuf_m + sizeof(ikey_type)), tt.ksuf_len_m);
    }
    else {
      tt.ksuf_m = 0;
      tt.ksuf_len_m = 0;
    }

    tt.lv_n = t.lv_n;
    tt.ksuf_len_n = t.ksuf_len_n;
    //assign m next layer ikeylen
    tt.ikeylen_n = convert_to_ikeylen(tt.ksuf_len_n);
    //make m next layer ikey
    tt.ikey_n = string_slice<ikey_type>::make_comparable_sloppy(t.ksuf_n, tt.ksuf_len_n);

    //make m next layer suffix
    if (tt.ksuf_len_n > sizeof(ikey_type)) {
      tt.ksuf_len_n -= sizeof(ikey_type);
      tt.ksuf_n = (char*)malloc(tt.ksuf_len_n + 1);
      memcpy((void*)tt.ksuf_n, (const void*)(t.ksuf_n + sizeof(ikey_type)), tt.ksuf_len_n);
    }
    else {
      tt.ksuf_n = 0;
      tt.ksuf_len_n = 0;
    }

    task_.push_back(tt);
  }
  else {
    ksufSize = t.ksuf_len_m + t.ksuf_len_n;
    nkeys = 2;
    n_ = massnode_dynamicvalue<P>::make(ksufSize, nkeys, ti);

    if ((t.ikey_m < t.ikey_n)
	|| ((t.ikey_m == t.ikey_n) && (t.ikeylen_m < t.ikeylen_n))) {
      n_->set_ikeylen(0, t.ikeylen_m);
      n_->set_ikey(0, t.ikey_m);
      n_->set_lv(0, t.lv_m);
      n_->set_ksuf_offset(0, (uint32_t)0);
      n_->set_ksuf_offset(1, (uint32_t)t.ksuf_len_m);

      n_->set_ikeylen(1, t.ikeylen_n);
      n_->set_ikey(1, t.ikey_n);
      n_->set_lv(1, t.lv_n);

      n_->set_ksuf_offset(2, (uint32_t)(t.ksuf_len_m + t.ksuf_len_n));

      if ((t.ksuf_len_m < 0) || (t.ksuf_len_n) < 0) {
	std::cout << "ERROR: (dynamicvalue) ksuf copy length < 0!!!\n";
	return false;
      }

      if (t.ksuf_len_m > 0)
	memcpy((void*)(n_->ksufpos(0)), (const void*)t.ksuf_m, t.ksuf_len_m);
      if (t.ksuf_len_n > 0)
	memcpy((void*)(n_->ksufpos(1)), (const void*)t.ksuf_n, t.ksuf_len_n);
    }
    else {
      n_->set_ikeylen(0, t.ikeylen_n);
      n_->set_ikey(0, t.ikey_n);
      n_->set_lv(0, t.lv_n); //values
      n_->set_ksuf_offset(0, (uint32_t)0);
      n_->set_ksuf_offset(1, (uint32_t)t.ksuf_len_n);

      n_->set_ikeylen(1, t.ikeylen_m);
      n_->set_ikey(1, t.ikey_m);
      n_->set_lv(1, t.lv_m); //values
      n_->set_ksuf_offset(2, (uint32_t)(t.ksuf_len_n + t.ksuf_len_m));

      if ((t.ksuf_len_m < 0) || (t.ksuf_len_n) < 0) {
	std::cout << "ERROR: (dynamicvalue) ksuf copy length < 0!!!\n";
	return false;
      }

      if (t.ksuf_len_n > 0)
	memcpy((void*)(n_->ksufpos(0)), (const void*)t.ksuf_n, t.ksuf_len_n);
      if (t.ksuf_len_m > 0)
	memcpy((void*)(n_->ksufpos(1)), (const void*)t.ksuf_m, t.ksuf_len_m);
    }
  }

  //delete m
  if (t.ksuf_len_m != 0)
    free(t.ksuf_m);

  //delete n
  if (t.ksuf_len_n != 0)
    free(t.ksuf_n);

  if (t.parent_node == NULL) {
    std::cout << "ERROR: create_node(dynamicvalue), parent_node is NULL!!!\n";
    return false;
  }

  t.parent_node->set_lv(t.parent_node_pos, leafvalue<P>(static_cast<node_base<P>*>(n_)));

  return true;
}

//huanchen-static-merge-dynamicvalue
//**********************************************************************************
// stcursor_merge_dynamicvalue::merge
//**********************************************************************************
template <typename P>
bool stcursor_merge_dynamicvalue<P>::merge(threadinfo &ti, threadinfo &ti_merge) {
  merge_task_dynamicvalue t;
  bool merge_success = true;

  //std::cout << "MERGE START\n";
  t.task = 0; //merge m to n
  t.parent_node = NULL;
  t.m = static_cast<massnode_dynamicvalue<P>*>(merge_root_);
  t.n = static_cast<massnode_dynamicvalue<P>*>(root_);
  task_.push_back(t);

  int cur_pos = 0;
  while (cur_pos < task_.size()) {
    //if (task_.size() % 10000 == 0)
    //std::cout << "task_.size() = " << task_.size() << "\n";
    if (task_[cur_pos].task == 0)
      merge_success = merge_nodes(task_[cur_pos], ti, ti_merge);
    else if (task_[cur_pos].task == 1)
      merge_success = add_item_to_node(task_[cur_pos], ti);
    else if (task_[cur_pos].task == 2)
      merge_success = create_node(task_[cur_pos], ti);
    else
      return false;
    cur_pos++;
    if (!merge_success) {
      std::cout << "MERGE FAIL!!!\n";
      return false;
    }
  }

  //std::cout << "merge(dynamicvalue) success-----------------------------------------\n";
  return merge_success;
}

//huanchen-static-merge-dynamicvalue
//**********************************************************************************
// stcursor_merge_dynamicvalue::convert_to_ikeylen
//**********************************************************************************
template <typename P>
inline uint8_t stcursor_merge_dynamicvalue<P>::convert_to_ikeylen(uint32_t len) {
  uint8_t ikeylen = (uint8_t)0;
  //if (len >= 8)
  //ikeylen = (uint8_t)8; 
  if (len > 8)
    ikeylen = (uint8_t)9;
  else if (len == 8)
    ikeylen = (uint8_t)8;
  else if (len == 7)
    ikeylen = (uint8_t)7;
  else if (len == 6)
    ikeylen = (uint8_t)6;
  else if (len == 5)
    ikeylen = (uint8_t)5;
  else if (len == 4)
    ikeylen = (uint8_t)4;
  else if (len == 3)
    ikeylen = (uint8_t)3;
  else if (len == 2)
    ikeylen = (uint8_t)2;
  else if (len == 1)
    ikeylen = (uint8_t)1;
  else
    return ikeylen;
  return ikeylen;
}





//huanchen-stats
template <typename P>
void unlocked_tcursor<P>::stats(threadinfo &ti, std::vector<uint32_t> &nkeys_stats) {
  //typedef typename P::ikey_type ikey_type;

  leaf<P> *next;
  node_base<P> *root = const_cast<node_base<P>*> (root_);
  std::deque<leafvalue<P>> trienode_list;

  int kp = 0;

  for (int i = 0; i < 16; i++)
    nkeys_stats.push_back(0);

 nextTrieNode:
  n_ = reinterpret_cast<leaf<P>*>(root -> leftmost());
 nextLeafNode:
  perm_ = n_ -> permutation();
  nkeys_stats[perm_.size()]++;
  for (int i = 0; i < perm_.size(); i++) {
    kp = perm_[i];
    if (n_->keylenx_is_layer(n_->keylenx_[kp])) {
      trienode_list.push_back(n_->lv_[kp]);
    }
  }

  next = n_->safe_next();
  if (next) {
    n_ = next;
    goto nextLeafNode;
  }

  if (!trienode_list.empty()) {
    root = trienode_list.front().layer();
    trienode_list.pop_front();
    goto nextTrieNode;
  }
  
}

template <typename P>
int stcursor<P>::tree_size() {
  massnode<P>* root = static_cast<massnode<P>*>(root_);
  return root->subtree_size();
}

template <typename P>
int stcursor_multivalue<P>::tree_size() {
  massnode_multivalue<P>* root = static_cast<massnode_multivalue<P>*>(root_);
  //root->printSMT();
  return root->subtree_size();
}

template <typename P>
int stcursor_multivalue<P>::tree_value_size() {
  massnode_multivalue<P>* root = static_cast<massnode_multivalue<P>*>(root_);
  return root->subtree_value_size();
}

} // namespace Masstree
#endif
