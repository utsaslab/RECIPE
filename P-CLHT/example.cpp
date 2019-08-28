#include <iostream>
#include <chrono>
#include <random>
#include <vector>
#include <thread>
#include <atomic>
#include "tbb/tbb.h"

using namespace std;

#include "clht.h"
#include "ssmem.h"

void multithreaded(char **argv) {
    std::cout << "multi threaded: P-CLHT" << std::endl;

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

    uint64_t num_thread = atoi(argv[3]);

    printf("operation,n,ops/s\n");
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
        auto func = [&]() {
            int thread_id = next_thread_id.fetch_add(1);
            tds[thread_id].id = thread_id;
            tds[thread_id].ht = hashtable;

            uint64_t start_key = n / num_thread * (uint64_t)thread_id;
            uint64_t end_key = start_key + n / num_thread;

            clht_gc_thread_init(tds[thread_id].ht, tds[thread_id].id);
            ssmem_allocator_t *alloc = (ssmem_allocator_t *) malloc(sizeof(ssmem_allocator_t));
            ssmem_alloc_init_fs_size(alloc, SSMEM_DEFAULT_MEM_SIZE, SSMEM_GC_FREE_SET_SIZE, tds[thread_id].id);

            for (uint64_t i = start_key; i < end_key; i++) {
                clht_put(tds[thread_id].ht, keys[i], keys[i]);
            }
        };

        std::vector<std::thread> thread_group;

        for (int i = 0; i < num_thread; i++)
            thread_group.push_back(std::thread{func});

        for (int i = 0; i < num_thread; i++)
            thread_group[i].join();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: load, %f ,ops/us\n", (n * 1.0) / duration.count());
    }

    {
        // Run
        auto starttime = std::chrono::system_clock::now();
        next_thread_id.store(0);
        auto func = [&]() {
            int thread_id = next_thread_id.fetch_add(1);
            tds[thread_id].id = thread_id;
            tds[thread_id].ht = hashtable;

            uint64_t start_key = n / num_thread * (uint64_t)thread_id;
            uint64_t end_key = start_key + n / num_thread;

            for (uint64_t i = start_key; i < end_key; i++) {
                    uintptr_t val = clht_get(tds[thread_id].ht->ht, keys[i]);
                    if (val != keys[i]) {
                        std::cout << "[CLHT] wrong key read: " << val << "expected: " << keys[i] << std::endl;
                        exit(1);
                    }
            }
        };

        std::vector<std::thread> thread_group;

        for (int i = 0; i < num_thread; i++)
            thread_group.push_back(std::thread{func});

        for (int i = 0; i < num_thread; i++)
            thread_group[i].join();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: run, %f ,ops/us\n", (n * 1.0) / duration.count());
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