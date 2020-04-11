#include <iostream>
#include <chrono>
#include <random>
#include <vector>
#include <thread>
#include <atomic>
#include "tbb/tbb.h"

#include <libpmemobj.h>

#include "clht_lb_res.h"
#include "ssmem.h"

typedef struct thread_data {
    uint32_t id;
    clht_t *ht;
} thread_data_t;

typedef struct barrier {
    pthread_cond_t complete;
    pthread_mutex_t mutex;
    int count;
    int crossing;
} barrier_t;

void barrier_init(barrier_t *b, int n) {
    pthread_cond_init(&b->complete, NULL);
    pthread_mutex_init(&b->mutex, NULL);
    b->count = n;
    b->crossing = 0;
}

void barrier_cross(barrier_t *b) {
    pthread_mutex_lock(&b->mutex);
    b->crossing++;
    if (b->crossing < b->count) {
        pthread_cond_wait(&b->complete, &b->mutex);
    } else {
        pthread_cond_broadcast(&b->complete);
        b->crossing = 0;
    }
    pthread_mutex_unlock(&b->mutex);
}

barrier_t barrier;

void run(char **argv) {
    std::cout << "Simple Example of P-CLHT" << std::endl;

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];

    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        keys[i] = i + 1;
    }

    int num_thread = atoi(argv[2]);

    printf("operation,n,ops/s\n");

    clht_t *hashtable = clht_create(512);

    barrier_init(&barrier, num_thread);

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
            barrier_cross(&barrier);

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
        printf("Throughput: load, %f ,ops/s\n", (n * 1.0) / (duration.count()/1000000.0));
    }

    barrier.crossing = 0;

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

            clht_gc_thread_init(tds[thread_id].ht, tds[thread_id].id);
            barrier_cross(&barrier);

            for (uint64_t i = start_key; i < end_key; i++) {
                clht_hashtable_t *ht = (clht_hashtable_t*)clht_ptr_from_off((tds[thread_id].ht)->ht_off);
                uintptr_t val = clht_get(ht, keys[i]);
                if (val != keys[i]) {
                    std::cout << "[CLHT] wrong key read: " << val << " expected: " << keys[i] << std::endl;
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
        printf("Throughput: run, %f ,ops/s\n", (n * 1.0) / (duration.count()/1000000.0));
    }
    // clht_gc_destroy(hashtable);

    delete[] keys;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        printf("usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: number of threads (integer)\n", argv[0]);
        return 1;
    }

    run(argv);
    return 0;
}