#include <iostream>
#include <chrono>
#include <random>
#include "tbb/tbb.h"

using namespace std;

#include "masstree.h"

void multithreaded(char **argv) {
    std::cout << "multi threaded: P-Masstree" << std::endl;

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];

    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        // dense, sorted
        keys[i] = i + 1;
    }

    if (atoi(argv[2]) == 1) {
        // dense, random
        std::random_shuffle(keys, keys + n);
    }

    if (atoi(argv[2]) == 2) {
        // "pseudo-sparse" (the most-significant leaf bit gets lost)
        std::default_random_engine generator;
        std::uniform_int_distribution<uint64_t> distribution(0, ((UINT64_MAX) ^ (1UL << 63)));
        for (uint64_t i = 0; i < n; i++)
            keys[i] = distribution(generator);
    }

    int num_thread = atoi(argv[3]);
    tbb::task_scheduler_init init(num_thread);

    printf("operation,n,ops/s\n");
    masstree::leafnode *init_root = new masstree::leafnode(0);
    masstree::masstree *tree = new masstree::masstree(init_root);

    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            for (uint64_t i = range.begin(); i != range.end(); i++) {
                tree->put(keys[i], &keys[i]);
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
            for (uint64_t i = range.begin(); i != range.end(); i++) {
                uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get(keys[i]));
                if (*ret != keys[i]) {
                    std::cout << "wrong value read: " << *ret << " expected:" << keys[i] << std::endl;
                    throw;
                }
            }
        });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: lookup,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: lookup,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }

    delete[] keys;
}

int main(int argc, char **argv) {
    if (argc != 4) {
        printf("usage: %s n 0|1|2 nthreads\nn: number of keys\n0: sorted keys\n1: dense keys\n2: sparse keys\nnthreads: number of threads\n", argv[0]);
        return 1;
    }

    multithreaded(argv);
    return 0;
}