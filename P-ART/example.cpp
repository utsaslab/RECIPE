#include <iostream>
#include <chrono>
#include <random>
#include "tbb/tbb.h"

using namespace std;

#include "ROWEX/Tree.h"

void loadKey(TID tid, Key &key) {
    // Store the key of the tuple into the key vector
    // Implementation is database specific
    key.setKeyLen(sizeof(tid));
    reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(tid);
}

void multithreaded(char **argv) {
    std::cout << "multi threaded: ART" << std::endl;

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
    ART_ROWEX::Tree tree(loadKey);

    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t = tree.getThreadInfo();
            for (uint64_t i = range.begin(); i != range.end(); i++) {
                Key key;
                loadKey(keys[i], key);
                tree.insert(key, keys[i], t);
            }
        });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: insert,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: insert,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }

    {
        // Lookup
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t = tree.getThreadInfo();
            for (uint64_t i = range.begin(); i != range.end(); i++) {
                Key key;
                loadKey(keys[i], key);
                auto val = tree.lookup(key, t);
                if (val != keys[i]) {
                    std::cout << "wrong key read: " << val << " expected:" << keys[i] << std::endl;
                    throw;
                }
            }
        });
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
        // Remove
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t = tree.getThreadInfo();
            for (uint64_t i = range.begin(); i != range.end(); i++) {
                Key key;
                loadKey(keys[i], key);
                tree.remove(key, keys[i], t);
            }
        });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: remove,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: remove,%ld,%f sec\n", n, duration.count() / 1000000.0);
        //printf("clflush count = %d\n", ART_ROWEX::clflush_cnt);
    }
    delete[] keys;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        printf("usage: %s n 0|1|2\nn: number of keys\n0: sorted keys\n1: dense keys\n2: sparse keys\n", argv[0]);
        return 1;
    }

    multithreaded(argv);
    return 0;
}