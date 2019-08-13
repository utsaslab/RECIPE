/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2013 President and Fellows of Harvard College
 * Copyright (c) 2012-2013 Massachusetts Institute of Technology
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
#ifndef KVROW_HH
#define KVROW_HH 1
#include "kvthread.hh"
#include "kvproto.hh"
#include "log.hh"
#include "json.hh"
#include <algorithm>

#if MASSTREE_ROW_TYPE_ARRAY
# include "value_array.hh"
typedef value_array row_type;
#elif MASSTREE_ROW_TYPE_ARRAY_VER
# include "value_versioned_array.hh"
typedef value_versioned_array row_type;
#elif MASSTREE_ROW_TYPE_STR
# include "value_string.hh"
typedef value_string row_type;
#else
# include "value_bag.hh"
typedef value_bag<uint16_t> row_type;
//typedef value_bag<uint32_t> row_type;
#endif

template <typename R>
struct query_helper {
    inline const R* snapshot(const R* row, const std::vector<typename R::index_type>&, threadinfo&) {
        return row;
    }
};

template <typename R> class query_json_scanner;

template <typename R>
class query {
  public:
    typedef lcdf::Json Json;

    template <typename T>
    void run_get(T& table, Json& req, threadinfo& ti);
    template <typename T>
    bool run_get1(T& table, Str key, int col, Str& value, threadinfo& ti);

    template <typename T>
    result_t run_put(T& table, Str key,
                     const Json* firstreq, const Json* lastreq, threadinfo& ti);
    template <typename T>
    result_t run_replace(T& table, Str key, Str value, threadinfo& ti);
    template <typename T>
    bool run_remove(T& table, Str key, threadinfo& ti);

    template <typename T>
    void run_scan(T& table, Json& request, threadinfo& ti);
    template <typename T>
    void run_rscan(T& table, Json& request, threadinfo& ti);

  //huanchen
    template <typename T>
    bool run_replace_unique(T& table, Str key, Str value, threadinfo& ti);
    template <typename T>
    bool run_append(T& table, Str key, Str value, threadinfo& ti);
  //huanchen-stats
  template <typename T>
  void run_stats(T& table, threadinfo& ti, std::vector<uint32_t>& nkeys_stats);

  template <typename T>
  void run_static_stats(T& table);

  template <typename T>
  void run_static_multivalue_stats(T& table);

    const loginfo::query_times& query_times() const {
        return qtimes_;
    }
  //huanchen-static=================================================================
  template <typename T>
  bool run_get1_static(T &table, Str key, Str &value);

  template <typename T>
  void run_destroy_static(T &table, threadinfo &ti);

  template <typename T>
  void run_buildStatic(T &table, threadinfo &ti);

  template <typename T>
  void run_buildStatic_quick(T &table, int nkeys, threadinfo &ti);

  template <typename T>
  void run_merge(T &table, T &merge_table, threadinfo &ti, threadinfo &ti_merge);

  template <typename T>
  void run_destroy_static_multivalue(T &table, threadinfo &ti);

  template <typename T>
  void run_buildStatic_multivalue(T &table, threadinfo &ti);

  template <typename T>
  void run_merge_multivalue(T &table, T &merge_table, threadinfo &ti, threadinfo &ti_merge);

  template <typename T>
  void run_destroy_static_dynamicvalue(T &table, threadinfo &ti);

  template <typename T>
  void run_buildStatic_dynamicvalue(T &table, threadinfo &ti);

  template <typename T>
  void run_merge_dynamicvalue(T &table, T &merge_table, threadinfo &ti, threadinfo &ti_merge);
  //================================================================================

  private:
    std::vector<typename R::index_type> f_;
    loginfo::query_times qtimes_;
    query_helper<R> helper_;
    lcdf::String scankey_;
    int scankeypos_;

    void emit_fields(const R* value, Json& req, threadinfo& ti);
    void emit_fields1(const R* value, Json& req, threadinfo& ti);
    void assign_timestamp(threadinfo& ti);
    void assign_timestamp(threadinfo& ti, kvtimestamp_t t);
    inline bool apply_put(R*& value, bool found, const Json* firstreq,
                          const Json* lastreq, threadinfo& ti);
    inline bool apply_replace(R*& value, bool found, Str new_value,
                              threadinfo& ti);
    inline void apply_remove(R*& value, kvtimestamp_t& node_ts, threadinfo& ti);

    template <typename RR> friend class query_json_scanner;
};


template <typename R>
void query<R>::emit_fields(const R* value, Json& req, threadinfo& ti) {
    const R* snapshot = helper_.snapshot(value, f_, ti);
    if (f_.empty()) {
        for (int i = 0; i != snapshot->ncol(); ++i)
            req.push_back(lcdf::String::make_stable(snapshot->col(i)));
    } else {
        for (int i = 0; i != (int) f_.size(); ++i)
            req.push_back(lcdf::String::make_stable(snapshot->col(f_[i])));
    }
}

template <typename R>
void query<R>::emit_fields1(const R* value, Json& req, threadinfo& ti) {
    const R* snapshot = helper_.snapshot(value, f_, ti);
    if ((f_.empty() && snapshot->ncol() == 1) || f_.size() == 1)
        req = lcdf::String::make_stable(snapshot->col(f_.empty() ? 0 : f_[0]));
    else if (f_.empty()) {
        for (int i = 0; i != snapshot->ncol(); ++i)
            req.push_back(lcdf::String::make_stable(snapshot->col(i)));
    } else {
        for (int i = 0; i != (int) f_.size(); ++i)
            req.push_back(lcdf::String::make_stable(snapshot->col(f_[i])));
    }
}


template <typename R> template <typename T>
void query<R>::run_get(T& table, Json& req, threadinfo& ti) {
    typename T::unlocked_cursor_type lp(table, req[2].as_s());
    bool found = lp.find_unlocked(ti);
    if (found && row_is_marker(lp.value()))
        found = false;
    if (found) {
        f_.clear();
        for (int i = 3; i != req.size(); ++i)
            f_.push_back(req[i].as_i());
        req.resize(2);
        emit_fields(lp.value(), req, ti);
    }
}

template <typename R> template <typename T>
bool query<R>::run_get1(T& table, Str key, int col, Str& value, threadinfo& ti) {
    typename T::unlocked_cursor_type lp(table, key);
    bool found = lp.find_unlocked(ti);
    //huanchen
    //if (found && row_is_marker(lp.value()))
    //found = false;
    if (found)
        value = lp.value()->col(col);
    return found;
}


template <typename R>
inline void query<R>::assign_timestamp(threadinfo& ti) {
    qtimes_.ts = ti.update_timestamp();
    qtimes_.prev_ts = 0;
}

template <typename R>
inline void query<R>::assign_timestamp(threadinfo& ti, kvtimestamp_t min_ts) {
    qtimes_.ts = ti.update_timestamp(min_ts);
    qtimes_.prev_ts = min_ts;
}


template <typename R> template <typename T>
result_t query<R>::run_put(T& table, Str key,
                           const Json* firstreq, const Json* lastreq,
                           threadinfo& ti) {
    typename T::cursor_type lp(table, key);
    bool found = lp.find_insert(ti);
    if (!found)
        ti.advance_timestamp(lp.node_timestamp());
    bool inserted = apply_put(lp.value(), found, firstreq, lastreq, ti);
    lp.finish(1, ti);
    return inserted ? Inserted : Updated;
}

template <typename R>
inline bool query<R>::apply_put(R*& value, bool found, const Json* firstreq,
                                const Json* lastreq, threadinfo& ti) {
    if (loginfo* log = ti.logger()) {
	log->acquire();
	qtimes_.epoch = global_log_epoch;
    }

    if (!found) {
    insert:
	assign_timestamp(ti);
        value = R::create(firstreq, lastreq, qtimes_.ts, ti);
        return true;
    }

    R* old_value = value;
    assign_timestamp(ti, old_value->timestamp());
    if (row_is_marker(old_value)) {
	old_value->deallocate_rcu(ti);
	goto insert;
    }

    R* updated = old_value->update(firstreq, lastreq, qtimes_.ts, ti);
    if (updated != old_value) {
	value = updated;
	old_value->deallocate_rcu_after_update(firstreq, lastreq, ti);
    }
    return false;
}

//huanchen
/*
template <typename R> template <typename T>
bool query<R>::run_replace_unique(T& table, Str key, Str value, threadinfo& ti) {
  typename T::unlocked_cursor_type lp_get(table, key);
  bool found_get = lp_get.find_unlocked(ti);
  if (found_get && row_is_marker(lp_get.value()))
    found_get = false;
  if (found_get)
    return false;

  typename T::cursor_type lp(table, key);
  bool found = lp.find_insert(ti);
  if (!found)
    ti.advance_timestamp(lp.node_timestamp());
  bool inserted = apply_replace(lp.value(), found, value, ti);
  lp.finish(1, ti);
  return true;
}
*/

template <typename R> template <typename T>
bool query<R>::run_replace_unique(T& table, Str key, Str value, threadinfo& ti) {
  typename T::cursor_type lp(table, key);
  bool found = lp.find_insert(ti);
  if (!found)
    ti.advance_timestamp(lp.node_timestamp());
  else {
    lp.finish(1, ti);
    return false;
  }
  /*
  if (loginfo* log = ti.logger()) {
    log->acquire();
    qtimes_.epoch = global_log_epoch;
  }
  */
  assign_timestamp(ti);
  lp.value() = R::create1(value, qtimes_.ts, ti);
  lp.finish(1, ti);
  return true;
}

template <typename R> template <typename T>
result_t query<R>::run_replace(T& table, Str key, Str value, threadinfo& ti) {
    typename T::cursor_type lp(table, key);
    bool found = lp.find_insert(ti);
    if (!found)
        ti.advance_timestamp(lp.node_timestamp());
    bool inserted = apply_replace(lp.value(), found, value, ti);
    lp.finish(1, ti);
    return inserted ? Inserted : Updated;
}

template <typename R>
inline bool query<R>::apply_replace(R*& value, bool found, Str new_value,
                                    threadinfo& ti) {
    if (loginfo* log = ti.logger()) {
	log->acquire();
	qtimes_.epoch = global_log_epoch;
    }

    bool inserted = !found || row_is_marker(value);
    if (!found)
	assign_timestamp(ti);
    else {
        assign_timestamp(ti, value->timestamp());
        value->deallocate_rcu(ti);
    }

    value = R::create1(new_value, qtimes_.ts, ti);
    return inserted;
}

//huanchen
template <typename R> template <typename T>
bool query<R>::run_append(T& table, Str key, Str value, threadinfo& ti) {
  typename T::cursor_type lp(table, key);
  bool found = lp.find_insert(ti);
  if (!found)
    ti.advance_timestamp(lp.node_timestamp());
   /*
  if (loginfo* log = ti.logger()) {
    log->acquire();
    qtimes_.epoch = global_log_epoch;
  }
  */
  char *put_value_string;
  int put_value_len;
  if (!found) {
    assign_timestamp(ti);
    lp.value() = R::create1(value, qtimes_.ts, ti);
  }
  else {
    assign_timestamp(ti, lp.value()->timestamp());
    lp.value()->deallocate_rcu(ti);
    put_value_len = value.len + lp.value()->col(0).len;
    //put_value_string = (char*)malloc(put_value_len);
    char put_value_string[4096];
    memcpy(put_value_string, value.s, value.len);
    memcpy(put_value_string + value.len, lp.value()->col(0).s, lp.value()->col(0).len);
    lp.value() = R::create1(Str(put_value_string, put_value_len), qtimes_.ts, ti);
    //free(put_value_string);
  }
  lp.finish(1, ti);
  return true;
}

template <typename R> template <typename T>
bool query<R>::run_remove(T& table, Str key, threadinfo& ti) {
    typename T::cursor_type lp(table, key);
    bool found = lp.find_locked(ti);
    if (found)
        apply_remove(lp.value(), lp.node_timestamp(), ti);
    lp.finish(-1, ti);
    return found;
}

template <typename R>
inline void query<R>::apply_remove(R*& value, kvtimestamp_t& node_ts,
                                   threadinfo& ti) {
    if (loginfo* log = ti.logger()) {
	log->acquire();
	qtimes_.epoch = global_log_epoch;
    }

    R* old_value = value;
    assign_timestamp(ti, old_value->timestamp());
    if (circular_int<kvtimestamp_t>::less_equal(node_ts, qtimes_.ts))
	node_ts = qtimes_.ts + 2;
    old_value->deallocate_rcu(ti);
}


template <typename R>
class query_json_scanner {
  public:
    query_json_scanner(query<R> &q, lcdf::Json& request)
	: q_(q), nleft_(request[3].as_i()), request_(request) {
        std::swap(request[2].value().as_s(), firstkey_);
        request_.resize(2);
        q_.scankeypos_ = 0;
    }
    const lcdf::String& firstkey() const {
        return firstkey_;
    }
    template <typename SS, typename K>
    void visit_leaf(const SS&, const K&, threadinfo&) {
    }
    bool visit_value(Str key, R* value, threadinfo& ti) {
        if (row_is_marker(value))
            return true;
        // NB the `key` is not stable! We must save space for it.
        while (q_.scankeypos_ + key.length() > q_.scankey_.length()) {
            q_.scankey_ = lcdf::String::make_uninitialized(q_.scankey_.length() ? q_.scankey_.length() * 2 : 1024);
            q_.scankeypos_ = 0;
        }
        memcpy(const_cast<char*>(q_.scankey_.data() + q_.scankeypos_),
               key.data(), key.length());
        request_.push_back(q_.scankey_.substring(q_.scankeypos_, key.length()));
        q_.scankeypos_ += key.length();
        request_.push_back(lcdf::Json());
        q_.emit_fields1(value, request_.back(), ti);
        --nleft_;
        return nleft_ != 0;
    }
  private:
    query<R> &q_;
    int nleft_;
    lcdf::Json& request_;
    lcdf::String firstkey_;
};

template <typename R> template <typename T>
void query<R>::run_scan(T& table, Json& request, threadinfo& ti) {
    assert(request[3].as_i() > 0);
    f_.clear();
    for (int i = 4; i != request.size(); ++i)
        f_.push_back(request[i].as_i());
    query_json_scanner<R> scanf(*this, request);
    table.scan(scanf.firstkey(), true, scanf, ti);
}

template <typename R> template <typename T>
void query<R>::run_rscan(T& table, Json& request, threadinfo& ti) {
    assert(request[3].as_i() > 0);
    f_.clear();
    for (int i = 4; i != request.size(); ++i)
        f_.push_back(request[i].as_i());
    query_json_scanner<R> scanf(*this, request);
    table.rscan(scanf.firstkey(), true, scanf, ti);
}

//huanchen-stats
template <typename R> template <typename T>
void query<R>::run_stats(T& table, threadinfo& ti, std::vector<uint32_t>& nkeys_stats) {
  typename T::unlocked_cursor_type lp(table);
  lp.stats(ti, nkeys_stats);
}

template <typename R> template <typename T>
void query<R>::run_static_stats(T& table) {
  typename T::static_cursor_type lp(table);
  std::cout << "TREE SIZE = " << lp.tree_size() << "\n";
}

template <typename R> template <typename T>
void query<R>::run_static_multivalue_stats(T& table) {
  typename T::static_multivalue_cursor_type lp(table);
  std::cout << "TREE SIZE = " << lp.tree_size() << "\n";
  std::cout << "TREE VALUE SIZE = " << lp.tree_value_size() << "\n";
}

//huanchen-static===============================================================
template <typename R> template <typename T>
bool query<R>::run_get1_static(T &table, Str key, Str &value) {
  typename T::static_cursor_type lp(table, key);
  bool found = lp.find();
  if (found)
    value = Str(lp.value(), 8);
  return found;
}

template <typename R> template <typename T>
void query<R>::run_destroy_static(T &table, threadinfo &ti) {
  typename T::static_cursor_type lp(table);
  lp.destroy(ti);
  table.set_static_root(NULL);
}

template <typename R> template <typename T>
void query<R>::run_buildStatic(T &table, threadinfo &ti) {
  typename T::unlocked_cursor_type lp(table);
  table.set_static_root(lp.buildStatic(ti));
}

template <typename R> template <typename T>
void query<R>::run_buildStatic_quick(T &table, int nkeys, threadinfo &ti) {
  typename T::unlocked_cursor_type lp(table);
  table.set_static_root(lp.buildStatic_quick(nkeys, ti));
}

template <typename R> template <typename T>
void query<R>::run_merge(T &table, T &merge_table, threadinfo &ti, threadinfo &ti_merge) {
  typename T::static_cursor_merge_type lp(table, merge_table);
  lp.merge(ti, ti_merge);
  table.set_static_root(lp.get_root());
}

template <typename R> template <typename T>
void query<R>::run_destroy_static_multivalue(T &table, threadinfo &ti) {
  typename T::static_multivalue_cursor_type lp(table);
  lp.destroy(ti);
  table.set_static_root(NULL);
}

template <typename R> template <typename T>
void query<R>::run_buildStatic_multivalue(T &table, threadinfo &ti) {
  typename T::unlocked_cursor_type lp(table);
  table.set_static_root(lp.buildStaticMultivalue(ti));
}

template <typename R> template <typename T>
void query<R>::run_merge_multivalue(T &table, T &merge_table, threadinfo &ti, threadinfo &ti_merge) {
  typename T::static_cursor_merge_multivalue_type lp(table, merge_table);
  lp.merge(ti, ti_merge);
  table.set_static_root(lp.get_root());
}


template <typename R> template <typename T>
void query<R>::run_destroy_static_dynamicvalue(T &table, threadinfo &ti) {
  typename T::static_dynamicvalue_cursor_type lp(table);
  lp.destroy(ti);
  table.set_static_root(NULL);
}

template <typename R> template <typename T>
void query<R>::run_buildStatic_dynamicvalue(T &table, threadinfo &ti) {
  typename T::unlocked_cursor_type lp(table);
  table.set_static_root(lp.buildStaticDynamicvalue(ti));
}

template <typename R> template <typename T>
void query<R>::run_merge_dynamicvalue(T &table, T &merge_table, threadinfo &ti, threadinfo &ti_merge) {
  typename T::static_cursor_merge_dynamicvalue_type lp(table, merge_table);
  lp.merge(ti, ti_merge);
  table.set_static_root(lp.get_root());
}
//==============================================================================

#endif
