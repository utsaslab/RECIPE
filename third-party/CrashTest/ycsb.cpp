#include <iostream>
#include <chrono>
#include <random>
#include <cstring>
#include <vector>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include "tbb/tbb.h"

using namespace std;

#include "ROWEX/Tree.h"
#include "FAST_FAIR/btree.h"
#include "CCEH/src/Level_hashing.h"
#include "CCEH/src/CCEH.h"
#include "masstree.h"
#include "Bwtree/src/bwtree.h"
#include "clht.h"
#include "ssmem.h"
#include "WORT/wort.h"
#include "WOART/woart.h"
#include "wbtree/wbtree.h"
#include "fptree/FPTree.h"
#ifdef PERF_PROFILE
#include "hw_events.h"
#endif

#ifdef HOT
#include <hot/rowex/HOTRowex.hpp>
//#include <idx/benchmark/Benchmark.hpp>
//#include <idx/benchmark/NoThreadInfo.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>
#endif

using namespace wangziqi2013::bwtree;

// index types
enum {
    TYPE_ART,
    TYPE_HOT,
    TYPE_BWTREE,
    TYPE_BLINK,
    TYPE_MASSTREE,
    TYPE_CLHT,
    TYPE_WORT,
    TYPE_WOART,
    TYPE_WBTREE,
    TYPE_FPTREE,
    TYPE_FASTFAIR,
    TYPE_LEVELHASH,
    TYPE_CCEH,
    TYPE_DUMMY,
};

enum {
    OP_INSERT,
    OP_READ,
    OP_SCAN,
    OP_DELETE,
};

enum {
    WORKLOAD_A,
    WORKLOAD_B,
    WORKLOAD_C,
    WORKLOAD_E,
};

enum {
    RANDINT_KEY,
    STRING_KEY,
};

enum {
    UNIFORM,
    ZIPFIAN,
};

namespace Dummy {
    inline void mfence() {asm volatile("mfence":::"memory");}

    inline void clflush(char *data, int len, bool front, bool back)
    {
        if (front)
            mfence();
        volatile char *ptr = (char *)((unsigned long)data & ~(64 - 1));
        for (; ptr < data+len; ptr += 64){
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
}

/*
 * class KeyComparator - Test whether BwTree supports context
 *                       sensitive key comparator
 *
 * If a context-sensitive KeyComparator object is being used
 * then it should follow rules like:
 *   1. There could be no default constructor
 *   2. There MUST be a copy constructor
 *   3. operator() must be const
 *
 */
class KeyComparator {
 public:
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 < k2;
  }

  inline bool operator()(const uint64_t k1, const uint64_t k2) const {
      return k1 < k2;
  }

  inline bool operator()(const char *k1, const char *k2) const {
      return memcmp(k1, k2, strlen(k1) > strlen(k2) ? strlen(k1) : strlen(k2)) < 0;
  }

  KeyComparator(int dummy) {
    (void)dummy;

    return;
  }

  KeyComparator() = delete;
  //KeyComparator(const KeyComparator &p_key_cmp_obj) = delete;
};

/*
 * class KeyEqualityChecker - Tests context sensitive key equality
 *                            checker inside BwTree
 *
 * NOTE: This class is only used in KeyEqual() function, and is not
 * used as STL template argument, it is not necessary to provide
 * the object everytime a container is initialized
 */
class KeyEqualityChecker {
 public:
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 == k2;
  }

  inline bool operator()(uint64_t k1, uint64_t k2) const {
      return k1 == k2;
  }

  inline bool operator()(const char *k1, const char *k2) const {
      if (strlen(k1) != strlen(k2))
          return false;
      else
          return memcmp(k1, k2, strlen(k1)) == 0;
  }

  KeyEqualityChecker(int dummy) {
    (void)dummy;

    return;
  }

  KeyEqualityChecker() = delete;
  //KeyEqualityChecker(const KeyEqualityChecker &p_key_eq_obj) = delete;
};

template<typename ValueType = Key *>
class KeyExtractor {
    public:
    typedef uint8_t* KeyType;

    inline KeyType operator()(ValueType const &value) const {
        return value->fkey;
    }
};

static uint64_t LOAD_SIZE = 64000000;
static uint64_t RUN_SIZE = 64000000;

void loadKey(TID tid, Key &key) {
    return ;
}

void ycsb_load_run_string(int index_type, int wl, int kt, int ap, int num_thread,
        std::vector<Key *> &init_keys,
        std::vector<Key *> &keys,
        std::vector<int> &ranges,
        std::vector<int> &ops)
{
    std::string init_file;
    std::string txn_file;

    if (ap == UNIFORM) {
        if (kt == STRING_KEY && wl == WORKLOAD_A) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/load_ycsbkey_workloada";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/txn_ycsbkey_workloada";
#else
            init_file = "./../index-microbench/workloads/ycsbkey_load_workloada";
            txn_file = "./../index-microbench/workloads/ycsbkey_run_workloada";
#endif
        } else if (kt == STRING_KEY && wl == WORKLOAD_B) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/load_ycsbkey_workloadb";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/txn_ycsbkey_workloadb";
#else
            init_file = "./../index-microbench/workloads/ycsbkey_load_workloadb";
            txn_file = "./../index-microbench/workloads/ycsbkey_run_workloadb";
#endif
        } else if (kt == STRING_KEY && wl == WORKLOAD_C) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/load_ycsbkey_workloadc";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/txn_ycsbkey_workloadc";
#else
            init_file = "./../index-microbench/workloads/ycsbkey_load_workloadc";
            txn_file = "./../index-microbench/workloads/ycsbkey_run_workloadc";
#endif
        } else if (kt == STRING_KEY && wl == WORKLOAD_E) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/load_ycsbkey_workloade";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/txn_ycsbkey_workloade";
#else
            init_file = "./../index-microbench/workloads/ycsbkey_load_workloade";
            txn_file = "./../index-microbench/workloads/ycsbkey_run_workloade";
#endif
        }
    } else {
        if (kt == STRING_KEY && wl == WORKLOAD_A) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/load_ycsbkey_workloada";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/txn_ycsbkey_workloada";
        } else if (kt == STRING_KEY && wl == WORKLOAD_B) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/load_ycsbkey_workloadb";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/txn_ycsbkey_workloadb";
        } else if (kt == STRING_KEY && wl == WORKLOAD_C) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/load_ycsbkey_workloadc";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/txn_ycsbkey_workloadc";
        } else if (kt == STRING_KEY && wl == WORKLOAD_E) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/load_ycsbkey_workloade";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/txn_ycsbkey_workloade";
        }
    }

    std::ifstream infile_load(init_file);

    std::string op;
    std::string key;
    int range;

    std::string insert("INSERT");
    std::string read("READ");
    std::string scan("SCAN");
    std::string maxKey("z");

    int count = 0;
    uint64_t val;
    while ((count < LOAD_SIZE) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return ;
        }
        val = std::stoul(key.substr(4, key.size()));
        init_keys.push_back(init_keys[count]->make_leaf((char *)key.c_str(), key.size()+1, val));
        count++;
    }

    fprintf(stderr, "Loaded %d keys\n", count);

    std::ifstream infile_txn(txn_file);

    count = 0;
    while ((count < RUN_SIZE) && infile_txn.good()) {
        infile_txn >> op >> key;
        if (op.compare(insert) == 0) {
            ops.push_back(OP_INSERT);
            val = std::stoul(key.substr(4, key.size()));
            keys.push_back(keys[count]->make_leaf((char *)key.c_str(), key.size()+1, val));
            ranges.push_back(1);
        } else if (op.compare(read) == 0) {
            ops.push_back(OP_READ);
            val = std::stoul(key.substr(4, key.size()));
            keys.push_back(keys[count]->make_leaf((char *)key.c_str(), key.size()+1, val));
            ranges.push_back(1);
        } else if (op.compare(scan) == 0) {
            infile_txn >> range;
            ops.push_back(OP_SCAN);
            keys.push_back(keys[count]->make_leaf((char *)key.c_str(), key.size()+1, 0));
            ranges.push_back(range);
        } else {
            std::cout << "UNRECOGNIZED CMD!\n";
            return;
        }
        count++;
    }

#ifdef PERF_PROFILE
    std::atomic<int> range_complete, range_incomplete;
    range_complete.store(0);
    range_incomplete.store(0);
    system("free -h");
    open_perf_event();
    do_ioctl_call(PERF_EVENT_IOC_RESET);
    do_ioctl_call(PERF_EVENT_IOC_ENABLE);
#endif

    if (index_type == TYPE_ART) {
        ART_ROWEX::Tree tree(loadKey);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree.getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    Key *key = key->make_leaf((char *)init_keys[i]->fkey, init_keys[i]->key_len, init_keys[i]->value);
                    tree.insert(key, t);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            Key *end = end->make_leaf((char *)maxKey.c_str(), maxKey.size()+1, 0);
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree.getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    Key *key;
                    if (ops[i] == OP_INSERT) {
                        key = key->make_leaf((char *)keys[i]->fkey, keys[i]->key_len, keys[i]->value);
                        tree.insert(key, t);
                    } else if (ops[i] == OP_READ) {
                        key = key->make_leaf((char *)keys[i]->fkey, keys[i]->key_len, keys[i]->value);
                        Key *val = reinterpret_cast<Key *>(tree.lookup(key, t));
                        if (val->value != keys[i]->value) {
                            std::cout << "[ART] wrong key read: " << val->value << " expected:" << keys[i]->value << std::endl;
                            throw;
                        }
                    } else if (ops[i] == OP_SCAN) {
                        Key *results[200];
                        Key *continueKey = NULL;
                        size_t resultsFound = 0;
                        size_t resultsSize = ranges[i];
                        Key *start = start->make_leaf((char *)keys[i]->fkey, keys[i]->key_len, keys[i]->value);
                        tree.lookupRange(start, end, continueKey, results, resultsSize, resultsFound, t);
#ifdef PERF_PROFILE
                        if (resultsFound != ranges[i])
                            range_incomplete.fetch_add(1);
                        else {
                            range_complete.fetch_add(1);
                        }
#endif
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            printf("Scan complete: %d, Scan incomplete: %d\n", range_complete.load(), range_incomplete.load());
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
#endif
        }
#ifdef HOT
    } else if (index_type == TYPE_HOT) {
        hot::rowex::HOTRowex<Key *, KeyExtractor> mTrie;

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    Key *key = key->make_leaf((char *)init_keys[i]->fkey, init_keys[i]->key_len, init_keys[i]->value);
                    Dummy::clflush((char *)key, sizeof(Key) + key->key_len, true, true);
                    if (!(mTrie.insert(key))) {
                        fprintf(stderr, "[HOT] load insert fail\n");
                        exit(1);
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        Key *key = key->make_leaf((char *)keys[i]->fkey, keys[i]->key_len, keys[i]->value);
                        Dummy::clflush((char *)key, sizeof(Key) + key->key_len, true, true);
                        if (!(mTrie.insert(key))) {
                            fprintf(stderr, "[HOT] run insert fail\n");
                            exit(1);
                        }
                    } else if (ops[i] == OP_READ) {
                        idx::contenthelpers::OptionalValue<Key *> result = mTrie.lookup((uint8_t *)keys[i]->fkey);
                        assert((result.mIsValid & result.mValue->value == keys[i]->value) == true);
                    } else if (ops[i] == OP_SCAN) {
                        idx::contenthelpers::OptionalValue<Key *> result = mTrie.scan((uint8_t *)keys[i]->fkey, ranges[i]);
#ifdef PERF_PROFILE
                        if (result.mIsValid)
                            range_complete.fetch_add(1);
                        else
                            range_incomplete.fetch_add(1);
#endif
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            printf("Scan complete: %d, Scan incomplete: %d\n", range_complete.load(), range_incomplete.load());
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
#endif
    } else if (index_type == TYPE_BWTREE) {
        auto t = new BwTree<char *, uint64_t, KeyComparator, KeyEqualityChecker>{true, KeyComparator{1}, KeyEqualityChecker{1}};
        t->UpdateThreadLocal(1);
        t->AssignGCID(0);
        std::atomic<int> next_thread_id;

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            next_thread_id.store(0);
            t->UpdateThreadLocal(num_thread);
#ifndef PERF_PROFILE
            auto func = [&]() {
#endif
                int thread_id = next_thread_id.fetch_add(1);
                uint64_t start_key = LOAD_SIZE / num_thread * (uint64_t)thread_id;
                uint64_t end_key = start_key + LOAD_SIZE / num_thread;

                t->AssignGCID(thread_id);
                for (uint64_t i = start_key; i < end_key; i++) {
                    Dummy::clflush((char *)init_keys[i]->fkey, init_keys[i]->key_len, true, true);
                    t->Insert((char *)init_keys[i]->fkey, init_keys[i]->value);
                }
                t->UnregisterThread(thread_id);
#ifndef PERF_PROFILE
            };

            std::vector<std::thread> thread_group;

            for (int i = 0; i < num_thread; i++)
                thread_group.push_back(std::thread{func});

            for (int i = 0; i < num_thread; i++)
                thread_group[i].join();
#endif
            t->UpdateThreadLocal(1);
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            next_thread_id.store(0);
            t->UpdateThreadLocal(num_thread);
#ifndef PERF_PROFILE
            auto func = [&]() {
#endif
                std::vector<uint64_t> v{};
                v.reserve(1);
                int thread_id = next_thread_id.fetch_add(1);
                uint64_t start_key = RUN_SIZE / num_thread * (uint64_t)thread_id;
                uint64_t end_key = start_key + RUN_SIZE / num_thread;

                t->AssignGCID(thread_id);
                for (uint64_t i = start_key; i < end_key; i++) {
                    if (ops[i] == OP_INSERT) {
                        Dummy::clflush((char *)keys[i]->fkey, keys[i]->key_len, true, true);
                        t->Insert((char *)keys[i]->fkey, keys[i]->value);
                    } else if (ops[i] == OP_READ) {
                        v.clear();
                        t->GetValue((char *)keys[i]->fkey, v);
                        if (v[0] != keys[i]->value) {
                            std::cout << "[BWTREE] wrong key read: " << v[0] << " expected:" << keys[i]->value << std::endl;
                        }
                    } else if (ops[i] == OP_SCAN) {
                        uint64_t buf[200];
                        auto it = t->Begin((char *)keys[i]->fkey);

                        int resultsFound = 0;
                        while (it.IsEnd() != true && resultsFound != ranges[i]) {
                            buf[resultsFound] = it->second;
                            resultsFound++;
                            it++;
                        }

#ifdef PERF_PROFILE
                        if (resultsFound != ranges[i])
                            range_incomplete.fetch_add(1);
                        else {
                            range_complete.fetch_add(1);
                        }
#endif
                    }
                }
                t->UnregisterThread(thread_id);
#ifndef PERF_PROFILE
            };

            std::vector<std::thread> thread_group;

            for (int i = 0; i < num_thread; i++)
                thread_group.push_back(std::thread{func});

            for (int i = 0; i < num_thread; i++)
                thread_group[i].join();
#endif
            t->UpdateThreadLocal(1);
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            printf("Scan complete: %d, Scan incomplete: %d\n", range_complete.load(), range_incomplete.load());
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
    } else if (index_type == TYPE_BLINK || index_type == TYPE_MASSTREE) {
        masstree::leafnode *init_root = new masstree::leafnode(0);
        masstree::masstree *tree = new masstree::masstree(init_root);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    tree->put((char *)init_keys[i]->fkey, init_keys[i]->value);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        tree->put((char *)keys[i]->fkey, keys[i]->value);
                    } else if (ops[i] == OP_READ) {
                        uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get((char *)keys[i]->fkey));
                        if (*ret != keys[i]->value) {
                            printf("[MASS] search key = %lu, search value = %lu\n", keys[i]->value, *ret);
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                        int resultsFound;
                        masstree::leafvalue *results[200];
                        resultsFound = tree->scan((char *)keys[i]->fkey, ranges[i], results);
#ifdef PERF_PROFILE
                        if (resultsFound == ranges[i])
                            range_complete.fetch_add(1);
                        else
                            range_incomplete.fetch_add(1);
#endif
                    } else if (ops[i] == OP_DELETE) {
                        tree->del((char *)keys[i]->fkey);
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            printf("Scan complete: %d, Scan incomplete: %d\n", range_complete.load(), range_incomplete.load());
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
#ifdef STRING_TYPE
    } else if (index_type == TYPE_WORT) {
        wort_tree *t = (wort_tree *)malloc(sizeof(wort_tree));
        wort_tree_init(t);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    wort_insert(t, init_keys[i]->fkey, init_keys[i]->key_len, &init_keys[i]->value);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        wort_insert(t, keys[i]->fkey, keys[i]->key_len, &keys[i]->value);
                    } else if (ops[i] == OP_READ) {
                        uint64_t *ret = reinterpret_cast<uint64_t *> (wort_search(t, keys[i]->fkey, keys[i]->key_len));
                        if (*ret != keys[i]->value) {
                            printf("[WORT] search key = %lu, search value = %lu\n", keys[i]->value, *ret);
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
    } else if (index_type == TYPE_WOART) {
        woart_tree *t = (woart_tree *)malloc(sizeof(woart_tree));
        woart_tree_init(t);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    woart_insert(t, init_keys[i]->fkey, init_keys[i]->key_len, &init_keys[i]->value);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        woart_insert(t, keys[i]->fkey, keys[i]->key_len, &keys[i]->value);
                    } else if (ops[i] == OP_READ) {
                        uint64_t *ret = reinterpret_cast<uint64_t *> (woart_search(t, keys[i]->fkey, keys[i]->key_len));
                        if (*ret != keys[i]->value) {
                            printf("[WOART] search key = %lu, search value = %lu\n", keys[i]->value, *ret);
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
    } else if (index_type == TYPE_WBTREE) {
        wbtree::tree *t = wbtree::initTree();

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    wbtree::Insert(t, init_keys[i]->fkey, init_keys[i]->key_len, &init_keys[i]->value);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        wbtree::Insert(t, keys[i]->fkey, keys[i]->key_len, &keys[i]->value);
                    } else if (ops[i] == OP_READ) {
                        uint64_t *ret = reinterpret_cast<uint64_t *> (wbtree::Lookup(t, keys[i]->fkey, keys[i]->key_len));
                        if (*ret != keys[i]->value) {
                            printf("[wBtree] search key = %lu, search value = %lu\n", keys[i]->value, *ret);
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
    } else if (index_type == TYPE_FPTREE) {
        fptree::tree *t = fptree::initTree();

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    fptree::Insert(t, init_keys[i]->fkey, init_keys[i]->key_len, &init_keys[i]->value);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        fptree::Insert(t, keys[i]->fkey, keys[i]->key_len, &keys[i]->value);
                    } else if (ops[i] == OP_READ) {
                        uint64_t *ret = reinterpret_cast<uint64_t *> (fptree::Lookup(t, keys[i]->fkey, keys[i]->key_len));
                        if (*ret != keys[i]->value) {
                            printf("[FPTree] search key = %lu, search value = %lu\n", keys[i]->value, *ret);
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
#endif
    } else if (index_type == TYPE_FASTFAIR) {
        fastfair::btree *bt = new fastfair::btree();

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    bt->btree_insert((char *)init_keys[i]->fkey, (char *) &init_keys[i]->value);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        bt->btree_insert((char *)keys[i]->fkey, (char *) &keys[i]->value);
                    } else if (ops[i] == OP_READ) {
                        uint64_t *ret = reinterpret_cast<uint64_t *> (bt->btree_search((char *)keys[i]->fkey));
                        if (ret == NULL) {
                            printf("Return value is NULL\n");
                        }else if (*ret != keys[i]->value) {
                            printf("[FASTFAIR] wrong value was returned: originally expected %lu\n", keys[i]->value);
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                        uint64_t buf[200];
                        int resultsFound = 0;
                        bt->btree_search_range ((char *)keys[i]->fkey, (char *)maxKey.c_str(), buf, ranges[i], resultsFound);
#ifdef PERF_PROFILE
                        if (ranges[i] == resultsFound)
                            range_complete.fetch_add(1);
                        else
                            range_incomplete.fetch_add(1);
#endif
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            printf("Scan complete: %d, Scan incomplete: %d\n", range_complete.load(), range_incomplete.load());
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
    } else if (TYPE_DUMMY) {
#ifndef PINCNT
        typedef struct upper_entry {
            uint8_t *key;
            void *value;
        } uentry;

        uentry *dummy;
        posix_memalign((void **)&dummy, 64, sizeof(uentry) * LOAD_SIZE * 2);
        memset(dummy, 0, sizeof(uentry) * LOAD_SIZE * 2);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    dummy[i].key = init_keys[i]->fkey;
                    dummy[i].value = &init_keys[i]->value;
                    Dummy::clflush((char *)dummy[i].key, init_keys[i]->key_len, true, true);
                    Dummy::clflush((char *)&dummy[i], sizeof(uentry), false, true);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        dummy[i + LOAD_SIZE].key = keys[i]->fkey;
                        dummy[i + LOAD_SIZE].value = &keys[i]->value;
                        Dummy::clflush((char *)dummy[i + LOAD_SIZE].key, keys[i]->key_len, true, true);
                        Dummy::clflush((char *)&dummy[i], sizeof(uentry), false, true);
                    } else if (ops[i] == OP_READ) {
                        if (memcmp(dummy[i].key, init_keys[i]->fkey, init_keys[i]->key_len) == 0) {
                            uint64_t *val = (uint64_t *)dummy[i].value;
                            if (*val != init_keys[i]->value) {
                                std::cout << "[Dummy] wrong value read: " << *val << " expected: " << init_keys[i]->value << std::endl;
                                exit(1);
                            }
                        } else {
                            printf("[Dummy] should not come here\n");
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
#endif
    }
}

void ycsb_load_run_randint(int index_type, int wl, int kt, int ap, int num_thread,
        std::vector<uint64_t> &init_keys,
        std::vector<uint64_t> &keys,
        std::vector<int> &ranges,
        std::vector<int> &ops)
{
    std::string init_file;
    std::string txn_file;

    if (ap == UNIFORM) {
        if (kt == RANDINT_KEY && wl == WORKLOAD_A) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/loada_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/txnsa_unif_int.dat";
#else
            init_file = "./../index-microbench/workloads/loada_unif_int.dat";
            txn_file = "./../index-microbench/workloads/txnsa_unif_int.dat";
#endif
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_B) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/loadb_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/txnsb_unif_int.dat";
#else
            init_file = "./../index-microbench/workloads/loadb_unif_int.dat";
            txn_file = "./../index-microbench/workloads/txnsb_unif_int.dat";
#endif
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_C) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/loadc_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/txnsc_unif_int.dat";
#else
            init_file = "./../index-microbench/workloads/loadc_unif_int.dat";
            txn_file = "./../index-microbench/workloads/txnsc_unif_int.dat";
#endif
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_E) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/loade_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/txnse_unif_int.dat";
#else
            init_file = "./../index-microbench/workloads/loade_unif_int.dat";
            txn_file = "./../index-microbench/workloads/txnse_unif_int.dat";
#endif
        }
    } else {
        if (kt == RANDINT_KEY && wl == WORKLOAD_A) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/loada_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/txnsa_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_B) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/loadb_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/txnsb_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_C) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/loadc_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/txnsc_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_E) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/loade_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/txnse_unif_int.dat";
        }
    }

    std::ifstream infile_load(init_file);

    std::string op;
    uint64_t key;
    int range;

    std::string insert("INSERT");
    std::string read("READ");
    std::string scan("SCAN");

    int count = 0;
    while ((count < LOAD_SIZE) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return ;
        }
        init_keys.push_back(key);
        count++;
    }

    fprintf(stderr, "Loaded %d keys\n", count);

    std::ifstream infile_txn(txn_file);

    count = 0;
    while ((count < RUN_SIZE) && infile_txn.good()) {
        infile_txn >> op >> key;
        if (op.compare(insert) == 0) {
            ops.push_back(OP_INSERT);
            keys.push_back(key);
            ranges.push_back(1);
        } else if (op.compare(read) == 0) {
            ops.push_back(OP_READ);
            keys.push_back(key);
            ranges.push_back(1);
        } else if (op.compare(scan) == 0) {
            infile_txn >> range;
            ops.push_back(OP_SCAN);
            keys.push_back(key);
            ranges.push_back(range);
        } else {
            std::cout << "UNRECOGNIZED CMD!\n";
            return;
        }
        count++;
    }

#ifdef PERF_PROFILE
    std::atomic<int> range_complete, range_incomplete;
    range_complete.store(0);
    range_incomplete.store(0);
    system("free -h");
    open_perf_event();
    do_ioctl_call(PERF_EVENT_IOC_RESET);
    do_ioctl_call(PERF_EVENT_IOC_ENABLE);
#endif

    if (index_type == TYPE_ART) {
        ART_ROWEX::Tree tree(loadKey);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree.getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    Key *key = key->make_leaf(init_keys[i], sizeof(uint64_t), init_keys[i]);
                    tree.insert(key, t);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            Key *end = end->make_leaf(UINT64_MAX, sizeof(uint64_t), 0);
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree.getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        Key *key = key->make_leaf(keys[i], sizeof(uint64_t), keys[i]);
                        tree.insert(key, t);
                    } else if (ops[i] == OP_READ) {
                        Key *key = key->make_leaf(keys[i], sizeof(uint64_t), 0);
                        uint64_t *val = reinterpret_cast<uint64_t *>(tree.lookup(key, t));
                        if (*val != keys[i]) {
                            std::cout << "[ART] wrong key read: " << val << " expected:" << keys[i] << std::endl;
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                        Key *results[200];
                        Key *continueKey = NULL;
                        size_t resultsFound = 0;
                        size_t resultsSize = ranges[i];
                        Key *start = start->make_leaf(keys[i], sizeof(uint64_t), 0);
                        tree.lookupRange(start, end, continueKey, results, resultsSize, resultsFound, t);
#ifdef PERF_PROFILE
                        if (resultsFound != ranges[i])
                            range_incomplete.fetch_add(1);
                        else {
                            range_complete.fetch_add(1);
                        }
#endif
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            printf("Scan complete: %d, Scan incomplete: %d\n", range_complete.load(), range_incomplete.load());
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
#ifdef HOT
    } else if (index_type == TYPE_HOT) {
        hot::rowex::HOTRowex<Key *, KeyExtractor> mTrie;

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    Key *key = key->make_leaf(init_keys[i], sizeof(uint64_t), init_keys[i]);
                    Dummy::clflush((char *)key, sizeof(Key) + sizeof(uint64_t), true, true);
                    if (!(mTrie.insert(key))) {
                        fprintf(stderr, "[HOT] load insert fail\n");
                        exit(1);
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    Key *key = key->make_leaf(keys[i], sizeof(uint64_t), keys[i]);
                    if (ops[i] == OP_INSERT) {
                        Dummy::clflush((char *)key, sizeof(Key) + sizeof(uint64_t), true, true);
                        if (!(mTrie.insert(key))) {
                            fprintf(stderr, "[HOT] run insert fail\n");
                            exit(1);
                        }
                    } else if (ops[i] == OP_READ) {
                        idx::contenthelpers::OptionalValue<Key *> result = mTrie.lookup((uint8_t *)key->fkey);
                        assert((result.mIsValid & (result.mValue->value == keys[i])) == true);
                    } else if (ops[i] == OP_SCAN) {
                        idx::contenthelpers::OptionalValue<Key *> result = mTrie.scan((uint8_t *)key->fkey, ranges[i]);
#ifdef PERF_PROFILE
                        if (result.mIsValid)
                            range_complete.fetch_add(1);
                        else
                            range_incomplete.fetch_add(1);
#endif
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            printf("Scan complete: %d, Scan incomplete: %d\n", range_complete.load(), range_incomplete.load());
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
#endif
    } else if (index_type == TYPE_BWTREE) {
        auto t = new BwTree<uint64_t, uint64_t, KeyComparator, KeyEqualityChecker>{true, KeyComparator{1}, KeyEqualityChecker{1}};
        t->UpdateThreadLocal(1);
        t->AssignGCID(0);
        std::atomic<int> next_thread_id;

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            next_thread_id.store(0);
            t->UpdateThreadLocal(num_thread);
#ifndef PERF_PROFILE
            auto func = [&]() {
#endif
                int thread_id = next_thread_id.fetch_add(1);
                uint64_t start_key = LOAD_SIZE / num_thread * (uint64_t)thread_id;
                uint64_t end_key = start_key + LOAD_SIZE / num_thread;

                t->AssignGCID(thread_id);
                for (uint64_t i = start_key; i < end_key; i++) {
                    t->Insert(init_keys[i], init_keys[i]);
                }
                t->UnregisterThread(thread_id);
#ifndef PERF_PROFILE
            };

            std::vector<std::thread> thread_group;

            for (int i = 0; i < num_thread; i++)
                thread_group.push_back(std::thread{func});

            for (int i = 0; i < num_thread; i++)
                thread_group[i].join();
#endif
            t->UpdateThreadLocal(1);
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            next_thread_id.store(0);
            t->UpdateThreadLocal(num_thread);
#ifndef PERF_PROFILE
            auto func = [&]() {
#endif
                std::vector<uint64_t> v{};
                v.reserve(1);
                int thread_id = next_thread_id.fetch_add(1);
                uint64_t start_key = RUN_SIZE / num_thread * (uint64_t)thread_id;
                uint64_t end_key = start_key + RUN_SIZE / num_thread;

                t->AssignGCID(thread_id);
                for (uint64_t i = start_key; i < end_key; i++) {
                    if (ops[i] == OP_INSERT) {
                        t->Insert(keys[i], keys[i]);
                    } else if (ops[i] == OP_READ) {
                        v.clear();
                        t->GetValue(keys[i], v);
                        if (v[0] != keys[i]) {
                            std::cout << "[BWTREE] wrong key read: " << v[0] << " expected:" << keys[i] << std::endl;
                        }
                    } else if (ops[i] == OP_SCAN) {
                        uint64_t buf[200];
                        auto it = t->Begin(keys[i]);

                        int resultsFound = 0;
                        while (it.IsEnd() != true && resultsFound != ranges[i]) {
                            buf[resultsFound] = it->second;
                            resultsFound++;
                            it++;
                        }
#ifdef PERF_PROFILE
                        if (resultsFound != ranges[i])
                            range_incomplete.fetch_add(1);
                        else
                            range_complete.fetch_add(1);
#endif
                    }
                }
                t->UnregisterThread(thread_id);
#ifndef PERF_PROFILE
            };

            std::vector<std::thread> thread_group;

            for (int i = 0; i < num_thread; i++)
                thread_group.push_back(std::thread{func});

            for (int i = 0; i < num_thread; i++)
                thread_group[i].join();
#endif
            t->UpdateThreadLocal(1);
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            printf("Scan complete: %d, Scan incomplete: %d\n", range_complete.load(), range_incomplete.load());
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
    } else if (index_type == TYPE_BLINK || index_type == TYPE_MASSTREE) {
        masstree::leafnode *init_root = new masstree::leafnode(0);
        masstree::masstree *tree = new masstree::masstree(init_root);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    tree->put(init_keys[i], &init_keys[i]);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        tree->put(keys[i], &keys[i]);
                    } else if (ops[i] == OP_READ) {
                        uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get(keys[i]));
                        if (*ret != keys[i]) {
                            printf("[MASS] search key = %lu, search value = %lu\n", keys[i], *ret);
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                        uint64_t buf[200];
                        int ret = tree->scan(keys[i], ranges[i], buf);
#ifdef PERF_PROFILE
                        if (ret != ranges[i])
                            range_incomplete.fetch_add(1);
                        else
                            range_complete.fetch_add(1);
#endif
                    } else if (ops[i] == OP_DELETE) {
                        tree->del(keys[i]);
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            printf("Scan complete: %d, Scan incomplete: %d\n", range_complete.load(), range_incomplete.load());
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
    } else if (index_type == TYPE_CLHT) {
        typedef struct thread_data {
            uint32_t id;
            clht_t *ht;
        } thread_data_t;

        clht_t *hashtable = clht_create(512);

        thread_data_t *tds = (thread_data_t *) malloc(num_thread * sizeof(thread_data_t));

        std::atomic<int> next_thread_id;

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            next_thread_id.store(0);
#ifndef PERF_PROFILE
            auto func = [&]() {
#endif
                int thread_id = next_thread_id.fetch_add(1);
                tds[thread_id].id = thread_id;
                tds[thread_id].ht = hashtable;

                uint64_t start_key = LOAD_SIZE / num_thread * (uint64_t)thread_id;
                uint64_t end_key = start_key + LOAD_SIZE / num_thread;

                clht_gc_thread_init(tds[thread_id].ht, tds[thread_id].id);
                ssmem_allocator_t *alloc = (ssmem_allocator_t *) malloc(sizeof(ssmem_allocator_t));
                ssmem_alloc_init_fs_size(alloc, SSMEM_DEFAULT_MEM_SIZE, SSMEM_GC_FREE_SET_SIZE, tds[thread_id].id);

                for (uint64_t i = start_key; i < end_key; i++) {
                    clht_put(tds[thread_id].ht, init_keys[i], init_keys[i]);
                }
#ifndef PERF_PROFILE
            };

            std::vector<std::thread> thread_group;

            for (int i = 0; i < num_thread; i++)
                thread_group.push_back(std::thread{func});

            for (int i = 0; i < num_thread; i++)
                thread_group[i].join();
#endif
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            next_thread_id.store(0);
#ifndef PERF_PROFILE
            auto func = [&]() {
#endif
                int thread_id = next_thread_id.fetch_add(1);
                tds[thread_id].id = thread_id;
                tds[thread_id].ht = hashtable;

                uint64_t start_key = RUN_SIZE / num_thread * (uint64_t)thread_id;
                uint64_t end_key = start_key + RUN_SIZE / num_thread;

                for (uint64_t i = start_key; i < end_key; i++) {
                    if (ops[i] == OP_INSERT) {
                        clht_put(tds[thread_id].ht, keys[i], keys[i]);
                    } else if (ops[i] == OP_READ) {
                        uintptr_t val = clht_get(tds[thread_id].ht->ht, keys[i]);
                        if (val != keys[i]) {
                            std::cout << "[CLHT] wrong key read: " << val << "expected: " << keys[i] << std::endl;
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                    }
                }
#ifndef PERF_PROFILE
            };

            std::vector<std::thread> thread_group;

            for (int i = 0; i < num_thread; i++)
                thread_group.push_back(std::thread{func});

            for (int i = 0; i < num_thread; i++)
                thread_group[i].join();
#endif
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
#ifndef STRING_TYPE
    } else if (index_type == TYPE_WORT) {
        wort_tree *t = (wort_tree *)malloc(sizeof(wort_tree));
        wort_tree_init(t);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    wort_insert(t, init_keys[i], sizeof(uint64_t), &init_keys[i]);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        wort_insert(t, keys[i], sizeof(uint64_t), &keys[i]);
                    } else if (ops[i] == OP_READ) {
                        uint64_t *ret = reinterpret_cast<uint64_t *> (wort_search(t, keys[i], sizeof(uint64_t)));
                        if (*ret != keys[i]) {
                            printf("[WORT] expected = %lu, search value = %lu\n", keys[i], *ret);
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
    } else if (index_type == TYPE_WOART) {
        woart_tree *t = (woart_tree *)malloc(sizeof(woart_tree));
        woart_tree_init(t);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    woart_insert(t, init_keys[i], sizeof(uint64_t), &init_keys[i]);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        woart_insert(t, keys[i], sizeof(uint64_t), &keys[i]);
                    } else if (ops[i] == OP_READ) {
                        uint64_t *ret = reinterpret_cast<uint64_t *> (woart_search(t, keys[i], sizeof(uint64_t)));
                        if (*ret != keys[i]) {
                            printf("[WOART] expected = %lu, search value = %lu\n", keys[i], *ret);
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
    } else if (index_type == TYPE_WBTREE) {
        wbtree::tree *t = wbtree::initTree();

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    wbtree::Insert(t, init_keys[i], &init_keys[i]);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        wbtree::Insert(t, keys[i], &keys[i]);
                    } else if (ops[i] == OP_READ) {
                        uint64_t *ret = reinterpret_cast<uint64_t *> (wbtree::Lookup(t, keys[i]));
                        if (*ret != keys[i]) {
                            printf("[wBtree] expected = %lu, search value = %lu\n", keys[i], *ret);
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
    } else if (index_type == TYPE_FPTREE) {
        fptree::tree *t = fptree::initTree();

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    fptree::Insert(t, init_keys[i], &init_keys[i]);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        fptree::Insert(t, keys[i], &keys[i]);
                    } else if (ops[i] == OP_READ) {
                        uint64_t *ret = reinterpret_cast<uint64_t *> (fptree::Lookup(t, keys[i]));
                        if (*ret != keys[i]) {
                            printf("[FPTree] expected = %lu, search value = %lu\n", keys[i], *ret);
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
#endif
    } else if (index_type == TYPE_FASTFAIR) {
        fastfair::btree *bt = new fastfair::btree();

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    bt->btree_insert(init_keys[i], (char *) &init_keys[i]);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        bt->btree_insert(keys[i], (char *) &keys[i]);
                    } else if (ops[i] == OP_READ) {
                        uint64_t *ret = reinterpret_cast<uint64_t *>(bt->btree_search(keys[i]));
                        if (ret == NULL)
                            printf("NULL is found\n");
                        else if (*ret != keys[i]) {
                            printf("[FASTFAIR] wrong value is returned: <expected> %lu\n", keys[i]);
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                        uint64_t buf[200];
                        int resultsFound = 0;
                        bt->btree_search_range (keys[i], UINT64_MAX, buf, ranges[i], resultsFound);
#ifdef PERF_PROFILE
                        if (ranges[i] == resultsFound)
                            range_complete.fetch_add(1);
                        else
                            range_incomplete.fetch_add(1);
#endif
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            printf("Scan complete: %d, Scan incomplete: %d\n", range_complete.load(), range_incomplete.load());
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
    } else if (index_type == TYPE_LEVELHASH) {
        Hash *table = new LevelHashing(10);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    table->Insert(init_keys[i], reinterpret_cast<const char*>(&init_keys[i]));
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        table->Insert(keys[i], reinterpret_cast<const char*>(&keys[i]));
                    } else if (ops[i] == OP_READ) {
                        auto val = table->Get(keys[i]);
                        if (val == NONE) {
                            std::cout << "[Level Hashing] wrong key read: " << *(uint64_t *)val << " expected: " << keys[i] << std::endl;
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
    } else if (index_type == TYPE_CCEH) {
        Hash *table = new CCEH(2);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    table->Insert(init_keys[i], reinterpret_cast<const char*>(&init_keys[i]));
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        table->Insert(keys[i], reinterpret_cast<const char*>(&keys[i]));
                    } else if (ops[i] == OP_READ) {
                        uint64_t *val = reinterpret_cast<uint64_t *>(const_cast<char *>(table->Get(keys[i])));
                        if (val == NULL) {
                            std::cout << "[CCEH] wrong value is read <expected:> " << keys[i] << std::endl;
                            //exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            system("free -h");
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
    } else if (TYPE_DUMMY) {
#ifndef PINCNT
        typedef struct upper_entry {
            uint64_t key;
            void *value;
        } uentry;

        uentry *dummy;
        posix_memalign((void **)&dummy, 64, sizeof(uentry) * LOAD_SIZE * 2);
        memset(dummy, 0, sizeof(uentry) * LOAD_SIZE * 2);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    dummy[i].key = init_keys[i];
                    dummy[i].value = &init_keys[i];
                    Dummy::clflush((char *)&dummy[i], sizeof(uentry), true, true);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: load, %f ,sec\n", duration.count() / 1000000.0);
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_RESET);
#endif
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        dummy[i + LOAD_SIZE].key = keys[i];
                        dummy[i + LOAD_SIZE].value = &keys[i];
                        Dummy::clflush((char *)&dummy[i], sizeof(uentry), true, true);
                    } else if (ops[i] == OP_READ) {
                        if (dummy[i].key == init_keys[i]) {
                            uint64_t *ret = reinterpret_cast<uint64_t *> (dummy[i].value);
                            if (*ret != init_keys[i]) {
                                std::cout << "[Dummy] wrong value read: " << *ret << " expected: " << init_keys[i] << std::endl;
                                exit(1);
                            }
                        } else {
                            printf("[Dummy] should not come here\n");
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
#ifdef PERF_PROFILE
            printf("Elapsed time: run, %f ,sec\n", duration.count() / 1000000.0);
            print_event();
            do_ioctl_call(PERF_EVENT_IOC_DISABLE);
            close_perf_desc();
#endif
        }
#endif
    }
}

int main(int argc, char **argv) {
#ifndef PINCNT
    if (argc != 6) {
#else
    if (argc != 7) {
#endif
        std::cout << "Usage: ./ycsb [index type] [ycsb workload type] [key distribution] [access pattern] [number of threads]\n";
        std::cout << "1. index type: art hot bwtree blink masstree clht dummy\n";
        std::cout << "               wort woart wbtree fptree fastfair levelhash cceh\n";
        std::cout << "2. ycsb workload type: a, b, c, e\n";
        std::cout << "3. key distribution: randint, string\n";
        std::cout << "4. access pattern: uniform, zipfian\n";
        std::cout << "5. number of threads (integer)\n";
#ifdef PINCNT
        std::cout << "6. Load size tested for performance counters (integer)\n";
#endif

        return 1;
    }

    printf("%s, workload%s, %s, %s, threads %s\n", argv[1], argv[2], argv[3], argv[4], argv[5]);

    int index_type;
    if (strcmp(argv[1], "art") == 0)
        index_type = TYPE_ART;
    else if (strcmp(argv[1], "hot") == 0)
        index_type = TYPE_HOT;
    else if (strcmp(argv[1], "bwtree") == 0)
        index_type = TYPE_BWTREE;
    else if (strcmp(argv[1], "blink") == 0)
        index_type = TYPE_BLINK;
    else if (strcmp(argv[1], "masstree") == 0)
        index_type = TYPE_MASSTREE;
    else if (strcmp(argv[1], "clht") == 0)
        index_type = TYPE_CLHT;
    else if (strcmp(argv[1], "wort") == 0)
        index_type = TYPE_WORT;
    else if (strcmp(argv[1], "woart") == 0)
        index_type = TYPE_WOART;
    else if (strcmp(argv[1], "wbtree") == 0)
        index_type = TYPE_WBTREE;
    else if (strcmp(argv[1], "fptree") == 0)
        index_type = TYPE_FPTREE;
    else if (strcmp(argv[1], "fastfair") == 0)
        index_type = TYPE_FASTFAIR;
    else if (strcmp(argv[1], "levelhash") == 0)
        index_type = TYPE_LEVELHASH;
    else if (strcmp(argv[1], "cceh") == 0)
        index_type = TYPE_CCEH;
    else if (strcmp(argv[1], "dummy") == 0)
        index_type = TYPE_DUMMY;
    else {
        fprintf(stderr, "Unknown index type: %s\n", argv[1]);
        exit(1);
    }

    int wl;
    if (strcmp(argv[2], "a") == 0) {
        wl = WORKLOAD_A;
    } else if (strcmp(argv[2], "b") == 0) {
        wl = WORKLOAD_B;
    } else if (strcmp(argv[2], "c") == 0) {
        wl = WORKLOAD_C;
    } else if (strcmp(argv[2], "e") == 0) {
        wl = WORKLOAD_E;
    } else {
        fprintf(stderr, "Unknown workload: %s\n", argv[2]);
        exit(1);
    }

    int kt;
    if (strcmp(argv[3], "randint") == 0) {
        kt = RANDINT_KEY;
    } else if (strcmp(argv[3], "string") == 0) {
        kt = STRING_KEY;
    } else {
        fprintf(stderr, "Unknown key type: %s\n", argv[3]);
        exit(1);
    }

    int ap;
    if (strcmp(argv[4], "uniform") == 0) {
        ap = UNIFORM;
    } else if (strcmp(argv[4], "zipfian") == 0) {
        ap = ZIPFIAN;
    } else {
        fprintf(stderr, "Unknown access pattern: %s\n", argv[4]);
        exit(1);
    }

    int num_thread = atoi(argv[5]);
    tbb::task_scheduler_init init(num_thread);

#ifdef PINCNT
    LOAD_SIZE = atoi(argv[6]);
    RUN_SIZE = 0;
#endif

    if (kt != STRING_KEY) {
        std::vector<uint64_t> init_keys;
        std::vector<uint64_t> keys;
        std::vector<int> ranges;
        std::vector<int> ops;

        init_keys.reserve(LOAD_SIZE);
        keys.reserve(RUN_SIZE);
        ranges.reserve(RUN_SIZE);
        ops.reserve(RUN_SIZE);

        memset(&init_keys[0], 0x00, LOAD_SIZE * sizeof(uint64_t));
        memset(&keys[0], 0x00, RUN_SIZE * sizeof(uint64_t));
        memset(&ranges[0], 0x00, RUN_SIZE * sizeof(int));
        memset(&ops[0], 0x00, RUN_SIZE * sizeof(int));

        ycsb_load_run_randint(index_type, wl, kt, ap, num_thread, init_keys, keys, ranges, ops);
    } else {
        std::vector<Key *> init_keys;
        std::vector<Key *> keys;
        std::vector<int> ranges;
        std::vector<int> ops;

        init_keys.reserve(LOAD_SIZE);
        keys.reserve(RUN_SIZE);
        ranges.reserve(RUN_SIZE);
        ops.reserve(RUN_SIZE);

        memset(&init_keys[0], 0x00, LOAD_SIZE * sizeof(Key *));
        memset(&keys[0], 0x00, RUN_SIZE * sizeof(Key *));
        memset(&ranges[0], 0x00, RUN_SIZE * sizeof(int));
        memset(&ops[0], 0x00, RUN_SIZE * sizeof(int));

        ycsb_load_run_string(index_type, wl, kt, ap, num_thread, init_keys, keys, ranges, ops);
    }

    return 0;
}
