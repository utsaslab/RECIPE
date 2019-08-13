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
#ifndef MASSTREE_TCURSOR_HH
#define MASSTREE_TCURSOR_HH 1
#include "local_vector.hh"
#include "masstree_key.hh"
#include "masstree_struct.hh"
namespace Masstree {
template <typename P> struct gc_layer_rcu_callback;

//huanchen-static
//**********************************************************************************
// stcursor
//**********************************************************************************
template <typename P>
class stcursor {
public:
  typedef typename P::value_type value_type;
  typedef key<typename P::ikey_type> key_type;
  typedef typename P::threadinfo_type threadinfo;

  inline stcursor(const basic_table<P>& table)
    : root_(table.static_root()) {
  }
  inline stcursor(const basic_table<P>& table, Str str)
    : ka_(str),
      root_(table.static_root()) {
  }
  inline stcursor(basic_table<P>& table, Str str)
    : ka_(str),
      root_(table.static_root()) {
  }
  inline stcursor(const basic_table<P>& table,
		 const char* s, int len)
    : ka_(s, len),
      root_(table.static_root()) {
  }
  inline stcursor(basic_table<P>& table,
		 const char* s, int len)
    : ka_(s, len),
      root_(table.static_root()) {
  }
  inline stcursor(const basic_table<P>& table,
		 const unsigned char* s, int len)
    : ka_(reinterpret_cast<const char*>(s), len),
      root_(table.static_root()) {
  }
  inline stcursor(basic_table<P>& table,
		 const unsigned char* s, int len)
    : ka_(reinterpret_cast<const char*>(s), len),
      root_(table.static_root()) {
  }
  
  bool find();
  bool remove();
  bool update(const char *nv);

  void destroy(threadinfo &ti);

  int tree_size();
  
  inline const char* value() const{
    return (const char*)lv_->value();
  }
  inline massnode<P>* node() const {
    return n_;
  }

private:
  key_type ka_;
  leafvalue_static<P>* lv_;
  massnode<P>* n_;
  node_base<P>* root_;
  
  inline int lower_bound_binary() const;
};


//huanchen-static-multivalue
//**********************************************************************************
// stcursor_multivalue
//**********************************************************************************
template <typename P>
class stcursor_multivalue {
public:
  typedef typename P::value_type value_type;
  typedef key<typename P::ikey_type> key_type;
  typedef typename P::threadinfo_type threadinfo;

  inline stcursor_multivalue(const basic_table<P>& table)
    : root_(table.static_root()) {
  }
  inline stcursor_multivalue(const basic_table<P>& table, Str str)
    : ka_(str),
      root_(table.static_root()) {
  }
  inline stcursor_multivalue(basic_table<P>& table, Str str)
    : ka_(str),
      root_(table.static_root()) {
  }
  inline stcursor_multivalue(const basic_table<P>& table,
		 const char* s, int len)
    : ka_(s, len),
      root_(table.static_root()) {
  }
  inline stcursor_multivalue(basic_table<P>& table,
		 const char* s, int len)
    : ka_(s, len),
      root_(table.static_root()) {
  }
  inline stcursor_multivalue(const basic_table<P>& table,
		 const unsigned char* s, int len)
    : ka_(reinterpret_cast<const char*>(s), len),
      root_(table.static_root()) {
  }
  inline stcursor_multivalue(basic_table<P>& table,
		 const unsigned char* s, int len)
    : ka_(reinterpret_cast<const char*>(s), len),
      root_(table.static_root()) {
  }
  
  bool find();
  bool remove();

  void destroy(threadinfo &ti);

  int tree_size();
  int tree_value_size();
  
  inline const char* value() const {
    return (const char*)(n_->get_value() + lv_->value_pos_offset());
  }
  /*
  inline const char* value_ptr() const {
    return (const char*)(n_->get_value() + lv_->value_pos_offset());
  }
  */
  inline char* value_ptr() const {
    return (char*)(n_->get_value() + lv_->value_pos_offset());
  }
  inline int value_len() const {
    return lv_->value_len();
  }
  inline massnode_multivalue<P>* node() const {
    return n_;
  }

private:
  key_type ka_;
  leafvalue_static_multivalue<P>* lv_;
  massnode_multivalue<P>* n_;
  node_base<P>* root_;
  
  inline int lower_bound_binary() const;
};


//huanchen-static-dynamicvalue
//**********************************************************************************
// stcursor_dynamicvalue
//**********************************************************************************
template <typename P>
class stcursor_dynamicvalue {
public:
  typedef typename P::value_type value_type;
  typedef key<typename P::ikey_type> key_type;
  typedef typename P::threadinfo_type threadinfo;

  inline stcursor_dynamicvalue(const basic_table<P>& table)
    : root_(table.static_root()) {
  }
  inline stcursor_dynamicvalue(const basic_table<P>& table, Str str)
    : ka_(str),
      root_(table.static_root()) {
  }
  inline stcursor_dynamicvalue(basic_table<P>& table, Str str)
    : ka_(str),
      root_(table.static_root()) {
  }
  inline stcursor_dynamicvalue(const basic_table<P>& table,
		 const char* s, int len)
    : ka_(s, len),
      root_(table.static_root()) {
  }
  inline stcursor_dynamicvalue(basic_table<P>& table,
		 const char* s, int len)
    : ka_(s, len),
      root_(table.static_root()) {
  }
  inline stcursor_dynamicvalue(const basic_table<P>& table,
		 const unsigned char* s, int len)
    : ka_(reinterpret_cast<const char*>(s), len),
      root_(table.static_root()) {
  }
  inline stcursor_dynamicvalue(basic_table<P>& table,
		 const unsigned char* s, int len)
    : ka_(reinterpret_cast<const char*>(s), len),
      root_(table.static_root()) {
  }
  
  bool find();
  bool remove(threadinfo &ti);

  void destroy(threadinfo &ti);

  //int tree_size();
  
  inline value_type &value() const {
    return lv_->value();
  }

  inline massnode_dynamicvalue<P>* node() const {
    return n_;
  }

private:
  key_type ka_;
  leafvalue<P>* lv_;
  massnode_dynamicvalue<P>* n_;
  node_base<P>* root_;
  
  inline int lower_bound_binary() const;
};


//huanchen-static
//**********************************************************************************
// stcursor_scan
//**********************************************************************************
template <typename P>
class stcursor_scan {
public:
  typedef typename P::value_type value_type;
  typedef typename P::ikey_type ikey_type;
  typedef key<typename P::ikey_type> key_type;
  typedef typename P::threadinfo_type threadinfo;

  inline stcursor_scan(const basic_table<P>& table)
    : root_(table.static_root()) {
  }
  inline stcursor_scan(const basic_table<P>& table, Str str)
    : ka_(str),
      root_(table.static_root()) {
  }
  inline stcursor_scan(basic_table<P>& table, Str str)
    : ka_(str),
      root_(table.static_root()) {
  }
  inline stcursor_scan(const basic_table<P>& table,
		 const char* s, int len)
    : ka_(s, len),
      root_(table.static_root()) {
  }
  inline stcursor_scan(basic_table<P>& table,
		 const char* s, int len)
    : ka_(s, len),
      root_(table.static_root()) {
  }
  inline stcursor_scan(const basic_table<P>& table,
		 const unsigned char* s, int len)
    : ka_(reinterpret_cast<const char*>(s), len),
      root_(table.static_root()) {
  }
  inline stcursor_scan(basic_table<P>& table,
		 const unsigned char* s, int len)
    : ka_(reinterpret_cast<const char*>(s), len),
      root_(table.static_root()) {
  }
  
  bool find_upper_bound_or_equal();
  bool find_upper_bound();
  bool find_next();

  inline const char* cur_value() const{
    return (const char*)cur_lv_->value();
  }
  inline const char* next_value() const{
    return (const char*)next_lv_->value();
  }
  inline massnode<P>* node() const {
    return n_;
  }
  inline char* cur_key() {
    int size = cur_key_prefix_.size();
    int len = sizeof(ikey_type) * size + cur_key_suffix_.len;
    char *retKey = (char*)malloc(len + 1);
    for (int i = 0; i < size; i++)
      for (int j = 0; j < sizeof(ikey_type); j++)
	retKey[i* sizeof(ikey_type) + j] 
	  = (reinterpret_cast<char*>(&(cur_key_prefix_[i])))[sizeof(ikey_type) - 1 - j];

    memcpy(retKey + (size * sizeof(ikey_type)), cur_key_suffix_.s, cur_key_suffix_.len);
    return retKey;
  }
  inline char* next_key() {
    int size = next_key_prefix_.size();
    int len = sizeof(ikey_type) * size + next_key_suffix_.len;
    char *retKey = (char*)malloc(len + 1);
    for (int i = 0; i < size; i++)
      for (int j = 0; j < sizeof(ikey_type); j++)
	retKey[i* sizeof(ikey_type) + j] 
	  = (reinterpret_cast<char*>(&(next_key_prefix_[i])))[sizeof(ikey_type) - 1 - j];

    memcpy(retKey + (size * sizeof(ikey_type)), next_key_suffix_.s, next_key_suffix_.len);
    return retKey;
  }
  inline int cur_keylen() const {
    return (sizeof(ikey_type) * cur_key_prefix_.size() + cur_key_suffix_.len);
  }
  inline int next_keylen() const {
    return (sizeof(ikey_type) * next_key_prefix_.size() + next_key_suffix_.len);
  }

private:
  key_type ka_;
  massnode<P>* n_;
  node_base<P>* root_;

  std::vector<massnode<P>*> nodeTrace_;
  std::vector<int> posTrace_;
  std::vector<ikey_type> cur_key_prefix_;
  Str cur_key_suffix_;
  leafvalue_static<P>* cur_lv_;
  std::vector<ikey_type> next_key_prefix_;
  Str next_key_suffix_;
  leafvalue_static<P>* next_lv_;

  bool isExact_;
  
  inline int lower_bound_binary() const;
  inline int upper_bound_or_equal_binary();
  inline bool find_next_leftmost();
  inline bool find_leftmost();
  inline bool next_item(int kp);
  inline bool next_item_next(int kp);
  inline bool next_item_from_next_node(int kp);
  inline bool next_item_from_next_node_next(int kp);
};


//huanchen-static-multivalue
//**********************************************************************************
// stcursor_scan_multivalue
//**********************************************************************************
template <typename P>
class stcursor_scan_multivalue {
public:
  typedef typename P::value_type value_type;
  typedef typename P::ikey_type ikey_type;
  typedef key<typename P::ikey_type> key_type;
  typedef typename P::threadinfo_type threadinfo;

  inline stcursor_scan_multivalue(const basic_table<P>& table)
    : root_(table.static_root()) {
  }
  inline stcursor_scan_multivalue(const basic_table<P>& table, Str str)
    : ka_(str),
      root_(table.static_root()) {
  }
  inline stcursor_scan_multivalue(basic_table<P>& table, Str str)
    : ka_(str),
      root_(table.static_root()) {
  }
  inline stcursor_scan_multivalue(const basic_table<P>& table,
		 const char* s, int len)
    : ka_(s, len),
      root_(table.static_root()) {
  }
  inline stcursor_scan_multivalue(basic_table<P>& table,
		 const char* s, int len)
    : ka_(s, len),
      root_(table.static_root()) {
  }
  inline stcursor_scan_multivalue(const basic_table<P>& table,
		 const unsigned char* s, int len)
    : ka_(reinterpret_cast<const char*>(s), len),
      root_(table.static_root()) {
  }
  inline stcursor_scan_multivalue(basic_table<P>& table,
		 const unsigned char* s, int len)
    : ka_(reinterpret_cast<const char*>(s), len),
      root_(table.static_root()) {
  }
  
  bool find_upper_bound_or_equal();
  bool find_upper_bound();
  bool find_next();

  inline const char* cur_value() const {
    return (const char*)(n_->get_value() + cur_lv_->value_pos_offset());
  }
  inline const char* cur_value_ptr() const {
    return (const char*)(n_->get_value() + cur_lv_->value_pos_offset());
  }
  inline int cur_value_len() const {
    return cur_lv_->value_len();
  }

  inline const char* next_value() const {
    return (const char*)(n_->get_value() + next_lv_->value_pos_offset());
  }
  inline const char* next_value_ptr() const {
    return (const char*)(n_->get_value() + next_lv_->value_pos_offset());
  }
  inline int next_value_len() const {
    return next_lv_->value_len();
  }

  inline massnode_multivalue<P>* node() const {
    return n_;
  }
  inline char* cur_key() {
    int size = cur_key_prefix_.size();
    int len = sizeof(ikey_type) * size + cur_key_suffix_.len;
    char *retKey = (char*)malloc(len + 1);
    for (int i = 0; i < size; i++)
      for (int j = 0; j < sizeof(ikey_type); j++)
	retKey[i* sizeof(ikey_type) + j] 
	  = (reinterpret_cast<char*>(&(cur_key_prefix_[i])))[sizeof(ikey_type) - 1 - j];

    memcpy(retKey + (size * sizeof(ikey_type)), cur_key_suffix_.s, cur_key_suffix_.len);
    return retKey;
  }
  inline char* next_key() {
    int size = next_key_prefix_.size();
    int len = sizeof(ikey_type) * size + next_key_suffix_.len;
    char *retKey = (char*)malloc(len + 1);
    for (int i = 0; i < size; i++)
      for (int j = 0; j < sizeof(ikey_type); j++)
	retKey[i* sizeof(ikey_type) + j] 
	  = (reinterpret_cast<char*>(&(next_key_prefix_[i])))[sizeof(ikey_type) - 1 - j];

    memcpy(retKey + (size * sizeof(ikey_type)), next_key_suffix_.s, next_key_suffix_.len);
    return retKey;
  }
  inline int cur_keylen() const {
    return (sizeof(ikey_type) * cur_key_prefix_.size() + cur_key_suffix_.len);
  }
  inline int next_keylen() const {
    return (sizeof(ikey_type) * next_key_prefix_.size() + next_key_suffix_.len);
  }

private:
  key_type ka_;
  massnode_multivalue<P>* n_;
  node_base<P>* root_;

  std::vector<massnode_multivalue<P>*> nodeTrace_;
  std::vector<int> posTrace_;
  std::vector<ikey_type> cur_key_prefix_;
  Str cur_key_suffix_;
  leafvalue_static_multivalue<P>* cur_lv_;
  std::vector<ikey_type> next_key_prefix_;
  Str next_key_suffix_;
  leafvalue_static_multivalue<P>* next_lv_;

  bool isExact_;
  
  inline int lower_bound_binary() const;
  inline int upper_bound_or_equal_binary();
  inline bool find_next_leftmost();
  inline bool find_leftmost();
  inline bool next_item(int kp);
  inline bool next_item_next(int kp);
  inline bool next_item_from_next_node(int kp);
  inline bool next_item_from_next_node_next(int kp);
};



//huanchen-static-dynamicvalue
//**********************************************************************************
// stcursor_scan_dynamicvalue
//**********************************************************************************
template <typename P>
class stcursor_scan_dynamicvalue {
public:
  typedef typename P::value_type value_type;
  typedef typename P::ikey_type ikey_type;
  typedef key<typename P::ikey_type> key_type;
  typedef typename P::threadinfo_type threadinfo;

  inline stcursor_scan_dynamicvalue(const basic_table<P>& table)
    : root_(table.static_root()) {
  }
  inline stcursor_scan_dynamicvalue(const basic_table<P>& table, Str str)
    : ka_(str),
      root_(table.static_root()) {
  }
  inline stcursor_scan_dynamicvalue(basic_table<P>& table, Str str)
    : ka_(str),
      root_(table.static_root()) {
  }
  inline stcursor_scan_dynamicvalue(const basic_table<P>& table,
		 const char* s, int len)
    : ka_(s, len),
      root_(table.static_root()) {
  }
  inline stcursor_scan_dynamicvalue(basic_table<P>& table,
		 const char* s, int len)
    : ka_(s, len),
      root_(table.static_root()) {
  }
  inline stcursor_scan_dynamicvalue(const basic_table<P>& table,
		 const unsigned char* s, int len)
    : ka_(reinterpret_cast<const char*>(s), len),
      root_(table.static_root()) {
  }
  inline stcursor_scan_dynamicvalue(basic_table<P>& table,
		 const unsigned char* s, int len)
    : ka_(reinterpret_cast<const char*>(s), len),
      root_(table.static_root()) {
  }
  
  bool find_upper_bound_or_equal();
  bool find_upper_bound();
  bool find_next();

  inline value_type cur_value() {
    return cur_lv_->value();
  }
  inline value_type next_value() {
    return next_lv_->value();
  }

  inline massnode_dynamicvalue<P>* node() const {
    return n_;
  }
  inline char* cur_key() {
    int size = cur_key_prefix_.size();
    int len = sizeof(ikey_type) * size + cur_key_suffix_.len;
    char *retKey = (char*)malloc(len + 1);
    for (int i = 0; i < size; i++)
      for (int j = 0; j < sizeof(ikey_type); j++)
	retKey[i* sizeof(ikey_type) + j] 
	  = (reinterpret_cast<char*>(&(cur_key_prefix_[i])))[sizeof(ikey_type) - 1 - j];

    memcpy(retKey + (size * sizeof(ikey_type)), cur_key_suffix_.s, cur_key_suffix_.len);
    return retKey;
  }
  inline char* next_key() {
    int size = next_key_prefix_.size();
    int len = sizeof(ikey_type) * size + next_key_suffix_.len;
    char *retKey = (char*)malloc(len + 1);
    for (int i = 0; i < size; i++)
      for (int j = 0; j < sizeof(ikey_type); j++)
	retKey[i* sizeof(ikey_type) + j] 
	  = (reinterpret_cast<char*>(&(next_key_prefix_[i])))[sizeof(ikey_type) - 1 - j];

    memcpy(retKey + (size * sizeof(ikey_type)), next_key_suffix_.s, next_key_suffix_.len);
    return retKey;
  }
  inline int cur_keylen() const {
    return (sizeof(ikey_type) * cur_key_prefix_.size() + cur_key_suffix_.len);
  }
  inline int next_keylen() const {
    return (sizeof(ikey_type) * next_key_prefix_.size() + next_key_suffix_.len);
  }

private:
  key_type ka_;
  massnode_dynamicvalue<P>* n_;
  node_base<P>* root_;

  std::vector<massnode_dynamicvalue<P>*> nodeTrace_;
  std::vector<int> posTrace_;
  std::vector<ikey_type> cur_key_prefix_;
  Str cur_key_suffix_;
  leafvalue<P>* cur_lv_;
  std::vector<ikey_type> next_key_prefix_;
  Str next_key_suffix_;
  leafvalue<P>* next_lv_;

  bool isExact_;
  
  inline int lower_bound_binary() const;
  inline int upper_bound_or_equal_binary();
  inline bool find_next_leftmost();
  inline bool find_leftmost();
  inline bool next_item(int kp);
  inline bool next_item_next(int kp);
  inline bool next_item_from_next_node(int kp);
  inline bool next_item_from_next_node_next(int kp);
};



//huanchen-static-merge
//**********************************************************************************
// stcursor_merge
//**********************************************************************************
template <typename P>
class stcursor_merge {
public:
  typedef typename P::value_type value_type;
  typedef typename P::ikey_type ikey_type;
  typedef key<typename P::ikey_type> key_type;
  typedef typename P::threadinfo_type threadinfo;

  //task 0: merge m to n
  //task 1: merge item_m to n
  //task 2: create a new massnode to include item_m & item_n
  struct merge_task {
    uint8_t task;
    massnode<P>* parent_node;
    uint32_t parent_node_pos;
    massnode<P>* m;
    massnode<P>* n;
    uint8_t ikeylen_m;
    uint8_t ikeylen_n;
    ikey_type ikey_m;
    ikey_type ikey_n;
    leafvalue_static<P> lv_m;
    leafvalue_static<P> lv_n;
    char* ksuf_m;
    char* ksuf_n;
    uint32_t ksuf_len_m;
    uint32_t ksuf_len_n;
  };

  //inline stcursor_merge(const basic_table<P>& table, const basic_table<P>& merge_table)
  inline stcursor_merge(basic_table<P>& table, basic_table<P>& merge_table)
    : root_(table.static_root()), 
      merge_root_(merge_table.static_root()) {
  }

  inline node_base<P>* get_root() const {
    return root_;
  }

  bool merge(threadinfo &ti, threadinfo &ti_merge);
  bool merge_nodes(merge_task t, threadinfo &ti, threadinfo &ti_merge);
  bool add_item_to_node(merge_task t, threadinfo &ti);
  bool create_node(merge_task t, threadinfo &ti);

private:
  key_type ka_;
  massnode<P>* n_;
  massnode<P>* m_;
  node_base<P>* root_;
  node_base<P>* merge_root_;

  std::vector<massnode<P>*> nodeTrace_;
  std::vector<int> posTrace_;

  std::vector<merge_task> task_;

  inline uint8_t convert_to_ikeylen(uint32_t len);
};


//huanchen-static-merge
//**********************************************************************************
// stcursor_merge_multivalue
//**********************************************************************************
template <typename P>
class stcursor_merge_multivalue {
public:
  typedef typename P::value_type value_type;
  typedef typename P::ikey_type ikey_type;
  typedef key<typename P::ikey_type> key_type;
  typedef typename P::threadinfo_type threadinfo;

  //task 0: merge m to n
  //task 1: merge item_m to n
  //task 2: create a new massnode to include item_m & item_n
  struct merge_task_multivalue {
    uint8_t task;
    massnode_multivalue<P>* parent_node;
    uint32_t parent_node_pos;
    massnode_multivalue<P>* m;
    massnode_multivalue<P>* n;
    uint8_t ikeylen_m;
    uint8_t ikeylen_n;
    ikey_type ikey_m;
    ikey_type ikey_n;
    leafvalue_static_multivalue<P> lv_m;
    leafvalue_static_multivalue<P> lv_n;
    char* ksuf_m;
    char* ksuf_n;
    uint32_t ksuf_len_m;
    uint32_t ksuf_len_n;
    char* value_m;
    char* value_n;
    //uint32_t value_len_m;
    //uint32_t value_len_n;
  };

  //inline stcursor_merge(const basic_table<P>& table, const basic_table<P>& merge_table)
  inline stcursor_merge_multivalue(basic_table<P>& table, basic_table<P>& merge_table)
    : root_(table.static_root()), 
      merge_root_(merge_table.static_root()) {
  }

  inline node_base<P>* get_root() const {
    return root_;
  }

  bool merge(threadinfo &ti, threadinfo &ti_merge);
  bool merge_nodes(merge_task_multivalue t, threadinfo &ti, threadinfo &ti_merge);
  bool add_item_to_node(merge_task_multivalue t, threadinfo &ti);
  bool create_node(merge_task_multivalue t, threadinfo &ti);

private:
  key_type ka_;
  massnode_multivalue<P>* n_;
  massnode_multivalue<P>* m_;
  node_base<P>* root_;
  node_base<P>* merge_root_;

  std::vector<massnode_multivalue<P>*> nodeTrace_;
  std::vector<int> posTrace_;

  std::vector<merge_task_multivalue> task_;

  inline uint8_t convert_to_ikeylen(uint32_t len);
};


//huanchen-static-merge
//**********************************************************************************
// stcursor_merge_dynamicvalue
//**********************************************************************************
template <typename P>
class stcursor_merge_dynamicvalue {
public:
  typedef typename P::value_type value_type;
  typedef typename P::ikey_type ikey_type;
  typedef key<typename P::ikey_type> key_type;
  typedef typename P::threadinfo_type threadinfo;

  //task 0: merge m to n
  //task 1: merge item_m to n
  //task 2: create a new massnode to include item_m & item_n
  struct merge_task_dynamicvalue {
    uint8_t task;
    massnode_dynamicvalue<P>* parent_node;
    uint32_t parent_node_pos;
    massnode_dynamicvalue<P>* m;
    massnode_dynamicvalue<P>* n;
    uint8_t ikeylen_m;
    uint8_t ikeylen_n;
    ikey_type ikey_m;
    ikey_type ikey_n;
    leafvalue<P> lv_m;
    leafvalue<P> lv_n;
    char* ksuf_m;
    char* ksuf_n;
    uint32_t ksuf_len_m;
    uint32_t ksuf_len_n;
  };

  //inline stcursor_merge(const basic_table<P>& table, const basic_table<P>& merge_table)
  inline stcursor_merge_dynamicvalue(basic_table<P>& table, basic_table<P>& merge_table)
    : root_(table.static_root()), 
      merge_root_(merge_table.static_root()) {
  }

  inline node_base<P>* get_root() const {
    return root_;
  }

  bool merge(threadinfo &ti, threadinfo &ti_merge);
  bool merge_nodes(merge_task_dynamicvalue t, threadinfo &ti, threadinfo &ti_merge);
  bool add_item_to_node(merge_task_dynamicvalue t, threadinfo &ti);
  bool create_node(merge_task_dynamicvalue t, threadinfo &ti);

private:
  key_type ka_;
  massnode_dynamicvalue<P>* n_;
  massnode_dynamicvalue<P>* m_;
  node_base<P>* root_;
  node_base<P>* merge_root_;

  std::vector<massnode_dynamicvalue<P>*> nodeTrace_;
  std::vector<int> posTrace_;

  std::vector<merge_task_dynamicvalue> task_;

  inline uint8_t convert_to_ikeylen(uint32_t len);
};



template <typename P>
class unlocked_tcursor {
  public:
    typedef typename P::value_type value_type;
    typedef key<typename P::ikey_type> key_type;
    typedef typename P::threadinfo_type threadinfo;
    typedef typename leaf<P>::nodeversion_type nodeversion_type;
    typedef typename nodeversion_type::value_type nodeversion_value_type;

    //huanchen-static
    typedef leafvalue_static<P> leafvalue_static_type;
    //huanchen-static-multivalue
    typedef leafvalue_static_multivalue<P> leafvalue_static_multivalue_type;

    inline unlocked_tcursor(const basic_table<P>& table)
        : lv_(leafvalue<P>::make_empty()),
          root_(table.root()) {
    }
    inline unlocked_tcursor(const basic_table<P>& table, Str str)
        : ka_(str), lv_(leafvalue<P>::make_empty()),
          root_(table.root()) {
    }
    inline unlocked_tcursor(basic_table<P>& table, Str str)
        : ka_(str), lv_(leafvalue<P>::make_empty()),
          root_(table.fix_root()) {
    }
    inline unlocked_tcursor(const basic_table<P>& table,
                            const char* s, int len)
        : ka_(s, len), lv_(leafvalue<P>::make_empty()),
          root_(table.root()) {
    }
    inline unlocked_tcursor(basic_table<P>& table,
                            const char* s, int len)
        : ka_(s, len), lv_(leafvalue<P>::make_empty()),
          root_(table.fix_root()) {
    }
    inline unlocked_tcursor(const basic_table<P>& table,
                            const unsigned char* s, int len)
        : ka_(reinterpret_cast<const char*>(s), len),
          lv_(leafvalue<P>::make_empty()), root_(table.root()) {
    }
    inline unlocked_tcursor(basic_table<P>& table,
                            const unsigned char* s, int len)
        : ka_(reinterpret_cast<const char*>(s), len),
          lv_(leafvalue<P>::make_empty()), root_(table.fix_root()) {
    }
  /*
  //huanchen-static
  inline unlocked_tcursor(const basic_table<P> &table)
    : lv_(leafvalue<P>::make_empty()), root_(table.root()) {
  }
  */
    bool find_unlocked(threadinfo& ti);

    inline value_type value() const {
        return lv_.value();
    }
    inline leaf<P>* node() const {
        return n_;
    }
    inline nodeversion_value_type full_version_value() const {
        static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(leaf<P>::permuter_type::size_bits), "not enough bits to add size to version");
        return (v_.version_value() << leaf<P>::permuter_type::size_bits) + perm_.size();
    }

  //huanchen-static
  massnode<P> *buildStatic(threadinfo &ti);
  massnode<P> *buildStatic_quick(int nkeys, threadinfo &ti);
  //huanchen-static-multivalue
  massnode_multivalue<P> *buildStaticMultivalue(threadinfo &ti);
  //huanchen-static-dynamicvalue
  massnode_dynamicvalue<P> *buildStaticDynamicvalue(threadinfo &ti);

  void stats(threadinfo &ti, std::vector<uint32_t> &nkeys_stats); //huanchen-stats

  private:
    leaf<P>* n_;
    key_type ka_;
    typename leaf<P>::nodeversion_type v_;
    typename leaf<P>::permuter_type perm_;
    leafvalue<P> lv_;
    const node_base<P>* root_;

    inline int lower_bound_binary() const;
    inline int lower_bound_linear() const;
};

template <typename P>
class tcursor {
  public:
    typedef node_base<P> node_type;
    typedef leaf<P> leaf_type;
    typedef internode<P> internode_type;
    typedef typename P::value_type value_type;
    typedef leafvalue<P> leafvalue_type;
    typedef typename leaf_type::permuter_type permuter_type;
    typedef typename P::ikey_type ikey_type;
    typedef key<ikey_type> key_type;
    typedef typename leaf<P>::nodeversion_type nodeversion_type;
    typedef typename nodeversion_type::value_type nodeversion_value_type;
    typedef typename P::threadinfo_type threadinfo;
    static constexpr int new_nodes_size = 1; // unless we make a new trie newnodes will have at most 1 item
    typedef local_vector<std::pair<leaf_type*, nodeversion_value_type>, new_nodes_size> new_nodes_type;

    tcursor(basic_table<P>& table, Str str)
        : ka_(str), root_(table.fix_root()) {
    }
    tcursor(basic_table<P>& table, const char* s, int len)
        : ka_(s, len), root_(table.fix_root()) {
    }
    tcursor(basic_table<P>& table, const unsigned char* s, int len)
        : ka_(reinterpret_cast<const char*>(s), len), root_(table.fix_root()) {
    }
    tcursor(node_base<P>* root, const char* s, int len)
        : ka_(s, len), root_(root) {
    }
    tcursor(node_base<P>* root, const unsigned char* s, int len)
        : ka_(reinterpret_cast<const char*>(s), len), root_(root) {
    }

    inline bool has_value() const {
        return kp_ >= 0;
    }
    inline value_type &value() const {
        return n_->lv_[kp_].value();
    }

    inline bool is_first_layer() const {
        return !ka_.is_shifted();
    }

    inline leaf<P>* node() const {
        return n_;
    }
    inline kvtimestamp_t node_timestamp() const {
        return n_->node_ts_;
    }
    inline kvtimestamp_t &node_timestamp() {
        return n_->node_ts_;
    }

    inline leaf_type *original_node() const {
        return original_n_;
    }

    inline nodeversion_value_type original_version_value() const {
        return original_v_;
    }

    inline nodeversion_value_type updated_version_value() const {
        return updated_v_;
    }

    inline const new_nodes_type &new_nodes() const {
        return new_nodes_;
    }

    inline bool find_locked(threadinfo& ti);
    inline bool find_insert(threadinfo& ti);

    inline void finish(int answer, threadinfo& ti);

    inline nodeversion_value_type previous_full_version_value() const;
    inline nodeversion_value_type next_full_version_value(int state) const;

  private:
    leaf_type *n_;
    key_type ka_;
    int ki_;
    int kp_;
    node_base<P>* root_;
    int state_;

    leaf_type *original_n_;
    nodeversion_value_type original_v_;
    nodeversion_value_type updated_v_;
    new_nodes_type new_nodes_;

    inline node_type* reset_retry() {
        ka_.unshift_all();
        return root_;
    }

    inline node_type* get_leaf_locked(node_type* root, nodeversion_type& v, threadinfo& ti);
    inline node_type* check_leaf_locked(node_type* root, nodeversion_type v, threadinfo& ti);
    inline node_type* check_leaf_insert(node_type* root, nodeversion_type v, threadinfo& ti);
    node_type* check_leaf_new_layer(nodeversion_type v, threadinfo& ti);
    static inline node_type* insert_marker() {
        return reinterpret_cast<node_type*>(uintptr_t(1));
    }
    static inline node_type* found_marker() {
        return reinterpret_cast<node_type*>(uintptr_t(0));
    }

    node_type* finish_split(threadinfo& ti);
    inline void finish_insert();
    inline bool finish_remove(threadinfo& ti);

    static void collapse(internode_type* p, ikey_type ikey,
                         node_type* root, Str prefix, threadinfo& ti);
    /** Remove @a leaf from the Masstree rooted at @a rootp.
     * @param prefix String defining the path to the tree containing this leaf.
     *   If removing a leaf in layer 0, @a prefix is empty.
     *   If removing, for example, the node containing key "01234567ABCDEF" in the layer-1 tree
     *   rooted at "01234567", then @a prefix should equal "01234567". */
    static bool remove_leaf(leaf_type* leaf, node_type* root,
                            Str prefix, threadinfo& ti);

    bool gc_layer(threadinfo& ti);
    friend struct gc_layer_rcu_callback<P>;
};

template <typename P>
inline typename tcursor<P>::nodeversion_value_type
tcursor<P>::previous_full_version_value() const {
    static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(leaf<P>::permuter_type::size_bits), "not enough bits to add size to version");
    return (n_->unlocked_version_value() << leaf<P>::permuter_type::size_bits) + n_->size();
}

template <typename P>
inline typename tcursor<P>::nodeversion_value_type
tcursor<P>::next_full_version_value(int state) const {
    static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(leaf<P>::permuter_type::size_bits), "not enough bits to add size to version");
    typename node_base<P>::nodeversion_type v(*n_);
    v.unlock();
    nodeversion_value_type result = (v.version_value() << leaf<P>::permuter_type::size_bits) + n_->size();
    if (state < 0 && (state_ & 1))
        return result - 1;
    else if (state > 0 && state_ == 2)
        return result + 1;
    else
        return result;
}

} // namespace Masstree
#endif
