#include <iostream>
#include <chrono>
#include <random>
#include "tbb/tbb.h"

using namespace std;

#include "OptimisticLockCoupling/Tree.h"
#include "ROWEX/Tree.h"
#include "ART/Tree.h"
#include "FAST_FAIR/btree.h"
#include "Hash_Table/external/level_hashing.hpp"
#include "Hash_Table/src/CCEH.h"
#include "masstree.h"

#include <hot/rowex/HOTRowex.hpp>
#include <idx/benchmark/Benchmark.hpp>
#include <idx/benchmark/NoThreadInfo.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

void loadKey(TID tid, Key &key) {
    // Store the key of the tuple into the key vector
    // Implementation is database specific
    key.setKeyLen(sizeof(tid));
    reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(tid);
}

void singlethreaded(char **argv) {
    std::cout << "single threaded:" << std::endl;

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];

    // Generate keys
    for (uint64_t i = 0; i < n; i++)
        // dense, sorted
        keys[i] = i + 1;
    if (atoi(argv[2]) == 1)
        // dense, random
        std::random_shuffle(keys, keys + n);
    if (atoi(argv[2]) == 2)
        // "pseudo-sparse" (the most-significant leaf bit gets lost)
        for (uint64_t i = 0; i < n; i++)
            keys[i] = (static_cast<uint64_t>(rand()) << 32) | static_cast<uint64_t>(rand());

    printf("operation,n,ops/s\n");
    ART_unsynchronized::Tree tree(loadKey);

    // Build tree
    {
        auto starttime = std::chrono::system_clock::now();
        for (uint64_t i = 0; i != n; i++) {
            Key key;
            loadKey(keys[i], key);
            tree.insert(key, keys[i]);
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("insert,%ld,%f\n", n, (n * 1.0) / duration.count());
    }

    {
        // Lookup
        auto starttime = std::chrono::system_clock::now();
        for (uint64_t i = 0; i != n; i++) {
            Key key;
            loadKey(keys[i], key);
            auto val = tree.lookup(key);
            if (val != keys[i]) {
                std::cout << "wrong key read: " << val << " expected:" << keys[i] << std::endl;
                throw;
            }
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
        printf("lookup,%ld,%f\n", n, (n * 1.0) / duration.count());
    }

    {
        auto starttime = std::chrono::system_clock::now();

        for (uint64_t i = 0; i != n; i++) {
            Key key;
            loadKey(keys[i], key);
            tree.remove(key, keys[i]);
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("remove,%ld,%f\n", n, (n * 1.0) / duration.count());
    }
    delete[] keys;

    std::cout << std::endl;
}

void multithreaded(char **argv) {
#ifdef MULTI_THREAD
    std::cout << "multi threaded: ART" << std::endl;
#else
    std::cout << "single threaded: ART" << std::endl;
#endif

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];
    uint64_t min,max;

    // Generate keys
    for (uint64_t i = 0; i < n; i++)
        // dense, sorted
        keys[i] = i + 1;
    if (atoi(argv[2]) == 1)
        // dense, random
        std::random_shuffle(keys, keys + n);
    if (atoi(argv[2]) == 2)
        // "pseudo-sparse" (the most-significant leaf bit gets lost)
        for (uint64_t i = 0; i < n; i++)
            keys[i] = (static_cast<uint64_t>(rand()) << 32) | static_cast<uint64_t>(rand());
    if (atoi(argv[2]) == 3) {
        std::default_random_engine generator;
        std::uniform_int_distribution<uint64_t> distribution(0, ((UINT64_MAX) ^ (1UL << 63)));
        for (uint64_t i = 0; i < n; i++)
            keys[i] = distribution(generator);
    }

    min = keys[0];
    max = UINT64_MAX >> 1UL;
    for (uint64_t i = 0; i < n; i++) {
        if (min > keys[i])
            min = keys[i];
    }

    printf("operation,n,ops/s\n");
    //ART_OLC::Tree tree(loadKey);
    ART_ROWEX::Tree tree(loadKey);

    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
#endif
            auto t = tree.getThreadInfo();
#ifdef MULTI_THREAD
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#else
            for (uint64_t i = 0; i != n; i++) {
#endif
                Key key;
                loadKey(keys[i], key);
                tree.insert(key, keys[i], t);
            }
#ifdef MULTI_THREAD
        });
#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: insert,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: insert,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }
#if !defined(PERFCNT) && !defined(WRITE_LATENCY)
    {
        // Lookup
        auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
#endif
            auto t = tree.getThreadInfo();
#ifdef MULTI_THREAD
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#else
            for (uint64_t i = 0; i != n; i++) {
#endif
                Key key;
                loadKey(keys[i], key);
                auto val = tree.lookup(key, t);
                if (val != keys[i]) {
                    std::cout << "wrong key read: " << val << " expected:" << keys[i] << std::endl;
                    throw;
                }
            }
#ifdef MULTI_THREAD
        });
#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: lookup,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: lookup,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }

    {
        // Scan
        auto t = tree.getThreadInfo();
        Key start;
        Key end;
        Key continueKey;
        TID *tid = new TID[n];
        size_t resultsFound = 0;
        size_t resultsSize = n;
        loadKey(min, start);
        loadKey(max, end);
        auto starttime = std::chrono::system_clock::now();
        tree.lookupRange(start, end, continueKey, tid, resultsSize, resultsFound, t);
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: scan,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: scan,%ld,%f sec\n", n, duration.count() / 1000000.0);
        delete[] tid;
    }

    {
        auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
#endif
            auto t = tree.getThreadInfo();
#ifdef MULTI_THREAD
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#else
            for (uint64_t i = 0; i != n; i++) {
#endif
                Key key;
                loadKey(keys[i], key);
                tree.remove(key, keys[i], t);
            }
#ifdef MULTI_THREAD
        });
#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: remove,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: remove,%ld,%f sec\n", n, duration.count() / 1000000.0);
        //printf("clflush count = %d\n", ART_ROWEX::clflush_cnt);
    }
#endif
    delete[] keys;
}

void multithreaded_btree(char **argv) {
#ifdef MULTI_THREAD
    std::cout << "multi threaded: FAST&FAIR" << std::endl;
#else
    std::cout << "single threaded: FAST&FAIR" << std::endl;
#endif

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];
    uint64_t min, max;

    // Generate keys
    for (uint64_t i = 0; i < n; i++)
        // dense, sorted
        keys[i] = i + 1;
    if (atoi(argv[2]) == 1)
        // dense, random
        std::random_shuffle(keys, keys + n);
    if (atoi(argv[2]) == 2)
        // "pseudo-sparse" (the most-significant leaf bit gets lost)
        for (uint64_t i = 0; i < n; i++)
            keys[i] = (static_cast<uint64_t>(rand()) << 32) | static_cast<uint64_t>(rand());
    if (atoi(argv[2]) == 3) {
        std::default_random_engine generator;
        std::uniform_int_distribution<uint64_t> distribution(0, ((UINT64_MAX) ^ (1UL << 63)));
        for (uint64_t i = 0; i < n; i++)
            keys[i] = distribution(generator);
    }

    min = keys[0];
    max = UINT64_MAX >> 1UL;
    for (uint64_t i = 0; i < n; i++) {
        if (min > keys[i])
            min = keys[i];
    }

    printf("operation,n,ops/s\n");
    btree *bt = new btree();

    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#else
            for (uint64_t i = 0; i != n; i++) {
#endif
                bt->btree_insert(keys[i], (char *) keys[i]);
            }
#ifdef MULTI_THREAD
        });
#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: insert,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: insert,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }
#if !defined(PERFCNT) && !defined(WRITE_LATENCY)
    {
        // Lookup
        auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#else
            for (uint64_t i = 0; i != n; i++) {
#endif
                auto val = bt->btree_search(keys[i]);
                //if (*val != keys[i]) {
                //    std::cout << "wrong key read: " << *val << " expected:" << keys[i] << std::endl;
                //    throw;
                //}
            }
#ifdef MULTI_THREAD
        });
#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: lookup,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: lookup,%ld,%f sec\n", n, duration.count() / 1000000.0);
        //printf("lookup,%ld,%f\n", n, ((n * 1.0) / duration.count()) * 1000000.0);
    }

    {
        // Scan
        unsigned long *buf = new unsigned long[n];
        auto starttime = std::chrono::system_clock::now();
        bt->btree_search_range(min, max, buf, n);
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: scan,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: scan,%ld,%f sec\n", n, duration.count() / 1000000.0);
        delete[] buf;
    }

    {
        // Delete
        auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#else
            for (uint64_t i = 0; i != n; i++) {
#endif
                bt->btree_delete(keys[i]);
            }
#ifdef MULTI_THREAD
        });
#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: remove,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: remove,%ld,%f sec\n", n, duration.count() / 1000000.0);
        //printf("clflush count = %d\n", clflush_cnt);
    }
#endif
    bt->~btree();
    delete[] keys;
}

void multithreaded_hash(char **argv) {
#ifdef MULTI_THREAD
#ifdef ENABLE_LEVEL
    std::cout << "multi threaded: level hashing" << std::endl;
#endif
#ifdef ENABLE_CCEH
    std::cout << "multi threaded: CCEH" << std::endl;
#endif
#else
#ifdef ENABLE_LEVEL
    std::cout << "single threaded: level hashing" << std::endl;
#endif
#ifdef ENABLE_CCEH
    std::cout << "single threaded: CCEH" << std::endl;
#endif
#endif

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];

    // Generate keys
    for (uint64_t i = 0; i < n; i++)
        // dense, sorted
        keys[i] = i + 1;
    if (atoi(argv[2]) == 1)
        // dense, random
        std::random_shuffle(keys, keys + n);
    if (atoi(argv[2]) == 2)
        // "pseudo-sparse" (the most-significant leaf bit gets lost)
        for (uint64_t i = 0; i < n; i++)
            keys[i] = (static_cast<uint64_t>(rand()) << 32) | static_cast<uint64_t>(rand());
    if (atoi(argv[2]) == 3) {
        std::default_random_engine generator;
        std::uniform_int_distribution<uint64_t> distribution(0, ((UINT64_MAX) ^ (1UL << 63)));
        for (uint64_t i = 0; i < n; i++)
            keys[i] = distribution(generator);
    }

    printf("operation,n,ops/s\n");
#ifdef  ENABLE_LEVEL
    Hash *table = new LevelHashing(12);
#endif
#ifdef  ENABLE_CCEH
    Hash *table = new CCEH((16*1024)/Segment::kNumSlot);
#endif

    {
        // Build hash table
        auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#else
            for (uint64_t i = 0; i != n; i++) {
#endif
                table->Insert(keys[i], reinterpret_cast<const char*>(&keys[i]));
            }
#ifdef MULTI_THREAD
        });
#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: insert,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: insert,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }
#if !defined(PERFCNT) && !defined(WRITE_LATENCY)
    {
        // Lookup
        auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#else
            for (uint64_t i = 0; i != n; i++) {
#endif
                auto val = table->Get(keys[i]);
                if (val == NONE) {
                    std::cout << "wrong key read: " << val << " expected:" << keys[i] << std::endl;
                    throw;
                }
            }
#ifdef MULTI_THREAD
        });
#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: lookup,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: lookup,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }

    {
        auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#else
            for (uint64_t i = 0; i != n; i++) {
#endif
                table->Delete(keys[i]);
            }
#ifdef MULTI_THREAD
        });
#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: remove,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: remove,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }
#endif
    table->~Hash();
    delete[] keys;
}

void multithreaded_hot(char **argv) {
#ifdef MULTI_THREAD
    std::cout << "multi threaded: HOT" << std::endl;
#else
    std::cout << "single threaded: HOT" << std::endl;
#endif

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];
    uint64_t min,max;

    // Generate keys
    for (uint64_t i = 0; i < n; i++)
        // dense, sorted
        keys[i] = i + 1;
    if (atoi(argv[2]) == 1)
        // dense, random
        std::random_shuffle(keys, keys + n);
    if (atoi(argv[2]) == 2)
        // "pseudo-sparse" (the most-significant leaf bit gets lost)
        for (uint64_t i = 0; i < n; i++)
            keys[i] = (static_cast<uint64_t>(rand()) << 32) | static_cast<uint64_t>(rand());
    if (atoi(argv[2]) == 3) {
        std::default_random_engine generator;
        std::uniform_int_distribution<uint64_t> distribution(0, ((UINT64_MAX) ^ (1UL << 63)));
        for (uint64_t i = 0; i < n; i++)
            keys[i] = distribution(generator);
    }

    min = keys[0];
    max = UINT64_MAX >> 1UL;
    for (uint64_t i = 0; i < n; i++) {
        if (min > keys[i])
            min = keys[i];
    }

    printf("operation,n,ops/s\n");
    hot::rowex::HOTRowex<uint64_t, idx::contenthelpers::IdentityKeyExtractor> mTrie;

    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
#endif
#ifdef MULTI_THREAD
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#else
            for (uint64_t i = 0; i != n; i++) {
#endif
                if (!(mTrie.insert(keys[i])))
                    printf("insert fail\n");
            }
#ifdef MULTI_THREAD
        });
#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: insert,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: insert,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }

#if !defined(PERFCNT) && !defined(WRITE_LATENCY)
    {
        // Lookup
        auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
#endif
#ifdef MULTI_THREAD
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#else
            for (uint64_t i = 0; i != n; i++) {
#endif
                idx::contenthelpers::OptionalValue<uint64_t> result = mTrie.lookup(keys[i]);
                assert((result.mIsValid & (result.mValue == keys[i])) == true);
            }
#ifdef MULTI_THREAD
        });
#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: lookup,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: lookup,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }

    {
        // Scan
        auto starttime = std::chrono::system_clock::now();
        mTrie.scan(min, n);
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: scan,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: scan,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }
#endif
    delete[] keys;
}

void multithreaded_masstree(char **argv) {
#ifdef MULTI_THREAD
    std::cout << "multi threaded: masstree" << std::endl;
#else
    std::cout << "single threaded: masstree" << std::endl;
#endif

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];
    uint64_t min,max;

    // Generate keys
    for (uint64_t i = 0; i < n; i++)
        // dense, sorted
        keys[i] = i + 1;
    if (atoi(argv[2]) == 1)
        // dense, random
        std::random_shuffle(keys, keys + n);
    if (atoi(argv[2]) == 2)
        // "pseudo-sparse" (the most-significant leaf bit gets lost)
        for (uint64_t i = 0; i < n; i++)
            keys[i] = (static_cast<uint64_t>(rand()) << 32) | static_cast<uint64_t>(rand());
    if (atoi(argv[2]) == 3) {
        std::default_random_engine generator;
        std::uniform_int_distribution<uint64_t> distribution(0, ((UINT64_MAX) ^ (1UL << 63)));
        for (uint64_t i = 0; i < n; i++)
            keys[i] = distribution(generator);
    }

    min = keys[0];
    max = UINT64_MAX >> 1UL;
    for (uint64_t i = 0; i < n; i++) {
        if (min > keys[i])
            min = keys[i];
    }

    printf("operation,n,ops/s\n");
    masstree::leafnode *init_root = new masstree::leafnode;
    masstree::masstree *tree = new masstree::masstree(init_root);

    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
#endif
#ifdef MULTI_THREAD
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#else
            for (uint64_t i = 0; i != n; i++) {
#endif
                tree->put(keys[i], &keys[i]);
            }
#ifdef MULTI_THREAD
        });
#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: insert,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: insert,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }

#if !defined(PERFCNT) && !defined(WRITE_LATENCY)
    {
        // Lookup
        auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
#endif
#ifdef MULTI_THREAD
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#else
            for (uint64_t i = 0; i != n; i++) {
#endif
                uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get(keys[i]));
                if (*ret != keys[i]) {
                    printf("search key = %lu, search value = %lu\n", keys[i], *ret);
                    exit(0);
                }
            }
#ifdef MULTI_THREAD
        });
#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: lookup,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: lookup,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }
#endif
    delete[] keys;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        printf("usage: %s n 0|1|2\nn: number of keys\n0: sorted keys\n1: dense keys\n2: sparse keys\n", argv[0]);
        return 1;
    }

//    singlethreaded(argv);

//    multithreaded(argv);
//    multithreaded_btree(argv);
//    multithreaded_hash(argv);
//    multithreaded_hot(argv);
    multithreaded_masstree(argv);

    return 0;
}