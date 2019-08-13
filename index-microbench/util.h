
#include "indexkey.h"
#include "microbench.h"
#include "index.h"

#ifndef _UTIL_H
#define _UTIL_H

bool hyperthreading = false;

//This enum enumerates index types we support
enum {
  TYPE_BWTREE = 0,
  TYPE_MASSTREE,
  TYPE_ARTOLC,
  TYPE_BTREEOLC,
  TYPE_SKIPLIST,
  TYPE_BTREERTM,
  TYPE_NONE,
};

// These are workload operations
enum {
  OP_INSERT,
  OP_READ,
  OP_UPSERT,
  OP_SCAN,
};

// These are YCSB workloads
enum {
  WORKLOAD_A,
  WORKLOAD_C,
  WORKLOAD_E,
};

// These are key types we use for running the benchmark
enum {
  RAND_KEY,
  MONO_KEY,
  RDTSC_KEY,
  EMAIL_KEY,
};

//==============================================================
// GET INSTANCE
//==============================================================
template<typename KeyType, 
         typename KeyComparator=std::less<KeyType>, 
         typename KeyEuqal=std::equal_to<KeyType>, 
         typename KeyHash=std::hash<KeyType>>
Index<KeyType, KeyComparator> *getInstance(const int type, const uint64_t kt) {
  if (type == TYPE_BWTREE)
    return new BwTreeIndex<KeyType, KeyComparator, KeyEuqal, KeyHash>(kt);
  else if (type == TYPE_MASSTREE)
    return new MassTreeIndex<KeyType, KeyComparator>(kt);
  else if (type == TYPE_ARTOLC)
      return new ArtOLCIndex<KeyType, KeyComparator>(kt);
  else if (type == TYPE_BTREEOLC)
    return new BTreeOLCIndex<KeyType, KeyComparator>(kt);
  else if (type == TYPE_SKIPLIST)
    return new SkipListIndex<KeyType, KeyComparator>(kt);
  else if (type == TYPE_BTREERTM)
    return new BTreeRTMIndex<KeyType, KeyComparator>(kt);
  else {
    fprintf(stderr, "Unknown index type: %d\n", type);
    exit(1);
  }
  
  return nullptr;
}

inline double get_now() { 
struct timeval tv; 
  gettimeofday(&tv, 0); 
  return tv.tv_sec + tv.tv_usec / 1000000.0; 
} 

/*
 * Rdtsc() - This function returns the value of the time stamp counter
 *           on the current core
 */
inline uint64_t Rdtsc()
{
    uint32_t lo, hi;
    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
    return (((uint64_t) hi << 32) | lo);
}

// This is the order of allocation

static int core_alloc_map_hyper[] = {
  0, 2, 4, 6, 8, 10, 12, 14, 16, 18,
  20, 22, 24, 26, 28, 30, 32, 34, 36, 38,
  1, 3, 5, 7 ,9, 11, 13, 15, 17, 19,
  21, 23, 25, 27, 29, 31, 33, 35, 37, 39,  
};


static int core_alloc_map_numa[] = {
  0, 2, 4, 6, 8, 10, 12, 14, 16, 18,
  1, 3, 5, 7 ,9, 11, 13, 15, 17, 19,
  20, 22, 24, 26, 28, 30, 32, 34, 36, 38,
  21, 23, 25, 27, 29, 31, 33, 35, 37, 39,  
};


constexpr static size_t MAX_CORE_NUM = 40;

inline void PinToCore(size_t thread_id) {
  cpu_set_t cpu_set;
  CPU_ZERO(&cpu_set);

  size_t core_id = thread_id % MAX_CORE_NUM;

  if(hyperthreading == true) {
    CPU_SET(core_alloc_map_hyper[core_id], &cpu_set);
  } else {
    CPU_SET(core_alloc_map_numa[core_id], &cpu_set);
  }

  int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
  if(ret != 0) {
    fprintf(stderr, "PinToCore() returns non-0\n");
    exit(1);
  }

  return;
}

template <typename Fn, typename... Args>
void StartThreads(Index<keytype, keycomp> *tree_p,
                  uint64_t num_threads,
                  Fn &&fn,
                  Args &&...args) {
  std::vector<std::thread> thread_group;

  if(tree_p != nullptr) {
    tree_p->UpdateThreadLocal(num_threads);
  }

  auto fn2 = [tree_p, &fn](uint64_t thread_id, Args ...args) {
    if(tree_p != nullptr) {
      tree_p->AssignGCID(thread_id);
    }

    PinToCore(thread_id);
    fn(thread_id, args...);

    if(tree_p != nullptr) {
      tree_p->UnregisterThread(thread_id);
    }

    return;
  };

  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread{fn2, thread_itr, std::ref(args...)});
  }

  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  // Print statistical data before we destruct thread local data
#ifdef BWTREE_COLLECT_STATISTICS
  tree_p->CollectStatisticalCounter(num_threads);
#endif

  if(tree_p != nullptr) {
    tree_p->UpdateThreadLocal(1);
  }

  return;
}

/*
 * GetTxnCount() - Counts transactions and return 
 */
template <bool upsert_hack=true>
int GetTxnCount(const std::vector<int> &ops,
                int index_type) {
  int count = 0;
 
  for(auto op : ops) {
    switch(op) {
      case OP_INSERT:
      case OP_READ:
      case OP_SCAN:
        count++;
        break;
      case OP_UPSERT:
        count++;

        break;
      default:
        fprintf(stderr, "Unknown operation\n");
        exit(1);
        break;
    }
  }

  return count;
}


#endif
