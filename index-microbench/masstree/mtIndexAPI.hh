#ifndef MTINDEXAPI_H
#define MTINDEXAPI_H

#include <stdio.h>
#include <stdarg.h>
#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/utsname.h>
#include <limits.h>
#if HAVE_NUMA_H
#include <numa.h>
#endif
#if HAVE_SYS_EPOLL_H
#include <sys/epoll.h>
#endif
#if HAVE_EXECINFO_H
#include <execinfo.h>
#endif
#if __linux__
#include <asm-generic/mman.h>
#endif
#include <fcntl.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <math.h>
#include <signal.h>
#include <errno.h>
#ifdef __linux__
#include <malloc.h>
#endif
#include "nodeversion.hh"
#include "kvstats.hh"
#include "query_masstree.hh"
#include "masstree_tcursor.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "timestamp.hh"
#include "json.hh"
#include "kvtest.hh"
#include "kvrandom.hh"
#include "kvrow.hh"
#include "kvio.hh"
#include "clp.h"
#include <algorithm>
#include <numeric>

#include <stdint.h>
#include "config.h"

#define GC_THRESHOLD 1000000
#define MERGE 0
#define MERGE_THRESHOLD 1000000
#define MERGE_RATIO 10
#define VALUE_LEN 8

#define LITTLEENDIAN 1
#define BITS_PER_KEY 8
#define K 2

#define SECONDARY_INDEX_TYPE 1

template <typename T>
class mt_index {
public:
  mt_index() {}
  ~mt_index() {
/*
    table_->destroy(*ti_);
    delete table_;
    ti_->rcu_clean();
    ti_->deallocate_ti();
    free(ti_);
    if (cur_key_)
      free(cur_key_);
*/    
    return;
  }

  //#####################################################################################
  // Initialize
  //#####################################################################################
  unsigned long long rdtsc_timer() {
    unsigned int lo,hi;
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return ((unsigned long long)hi << 32) | lo;
  }

  inline void setup(threadinfo *ti) {
    table_ = new T;
    table_->initialize(*ti);

    srand(rdtsc_timer());
  }

  //#####################################################################################
  // Garbage Collection
  //#####################################################################################
  inline void clean_rcu(threadinfo *ti) {
    ti->rcu_quiesce();
  }

  inline void gc_dynamic(threadinfo *ti) {
    if (ti->limbo >= GC_THRESHOLD) {
      clean_rcu(ti);
      ti->dealloc_rcu += ti->limbo;
      ti->limbo = 0;
    }
  }
  
  //#####################################################################################
  //Insert Unique
  //#####################################################################################
  inline bool put_uv(const Str &key, const Str &value, threadinfo *ti) {
    typename T::cursor_type lp(table_->table(), key);
    bool found = lp.find_insert(*ti);
    if (!found)
      ti->advance_timestamp(lp.node_timestamp());
    else {
      lp.finish(1, *ti);
      return false;
    }
    qtimes_.ts = ti->update_timestamp();
    qtimes_.prev_ts = 0;
    lp.value() = row_type::create1(value, qtimes_.ts, *ti);
    lp.finish(1, *ti);

    return true;
  }
  
  bool put_uv(const char *key, int keylen, const char *value, int valuelen, threadinfo *ti) {
    return put_uv(Str(key, keylen), Str(value, valuelen), ti);
  }

  //#####################################################################################
  // Upsert
  //#####################################################################################
  inline void put(const Str &key, const Str &value, threadinfo *ti) {
    typename T::cursor_type lp(table_->table(), key);
    bool found = lp.find_insert(*ti);
    if (!found) {
      ti->advance_timestamp(lp.node_timestamp());
      qtimes_.ts = ti->update_timestamp();
      qtimes_.prev_ts = 0;
    }
    else {
      qtimes_.ts = ti->update_timestamp(lp.value()->timestamp());
      qtimes_.prev_ts = lp.value()->timestamp();
      lp.value()->deallocate_rcu(*ti);
    }
    
    lp.value() = row_type::create1(value, qtimes_.ts, *ti);
    lp.finish(1, *ti);
  }

  void put(const char *key, int keylen, const char *value, int valuelen, threadinfo *ti) {
    put(Str(key, keylen), Str(value, valuelen), ti);
  }

  //#################################################################################
  // Get (unique value)
  //#################################################################################
  inline bool dynamic_get(const Str &key, Str &value, threadinfo *ti) {
    typename T::unlocked_cursor_type lp(table_->table(), key);
    bool found = lp.find_unlocked(*ti);
    if (found)
      value = lp.value()->col(0);
    return found;
  }
  
  bool get (const char *key, int keylen, Str &value, threadinfo *ti) {
    return dynamic_get(Str(key, keylen), value, ti);
  }

  //#################################################################################
  // Get Next (ordered)
  //#################################################################################
  bool dynamic_get_next(Str &value, char *cur_key, int *cur_keylen, threadinfo *ti) {
    Json req = Json::array(0, 0, Str(cur_key, *cur_keylen), 2);
    q_[0].run_scan(table_->table(), req, *ti);
    if (req.size() < 4)
      return false;
    value = req[3].as_s();
    if (req.size() < 6) {
      *cur_keylen = 0;
    }
    else {
      Str cur_key_str = req[4].as_s();
      memcpy(cur_key, cur_key_str.s, cur_key_str.len);
      *cur_keylen = cur_key_str.len;
    }
    
    return true;
  }

  //#################################################################################
  // Get Next N (ordered)
  //#################################################################################
  struct scanner {
    Str *values;
    int range;

    scanner(Str *values, int range)
      : values(values), range(range) {
    }

    template <typename SS2, typename K2>
    void visit_leaf(const SS2&, const K2&, threadinfo&) {}
    bool visit_value(Str key, const row_type* row, threadinfo&) {
        *values = row->col(0);
        ++values;
        --range;
        return range > 0;
    }
  };
  int get_next_n(Str *values, char *cur_key, int *cur_keylen, int range, threadinfo *ti) {
    if (range == 0)
      return 0;

    scanner s(values, range);
    int count = table_->table().scan(Str(cur_key, *cur_keylen), true, s, *ti);
    return count;
  }

private:
  T *table_;
  query<row_type> q_[1];
  loginfo::query_times qtimes_;
};

#endif //MTINDEXAPI_H
