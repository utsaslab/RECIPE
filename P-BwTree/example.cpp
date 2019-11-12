#include <iostream>
#include <chrono>
#include <random>
#include "tbb/tbb.h"

using namespace std;

#include "src/bwtree.h"

using namespace wangziqi2013::bwtree;

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

void run(char **argv) {
    std::cout << "Simple Example of P-BwTree" << std::endl;

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];

    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        keys[i] = i + 1;
    }

    int num_thread = atoi(argv[2]);

    printf("operation,n,ops/s\n");
    auto t = new BwTree<uint64_t, uint64_t, KeyComparator, KeyEqualityChecker> {true, KeyComparator{1}, KeyEqualityChecker{1}};
    t->UpdateThreadLocal(1);
    t->AssignGCID(0);
    std::atomic<int> next_thread_id;

    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
        next_thread_id.store(0);
        t->UpdateThreadLocal(num_thread);
        auto func = [&]() {
            int thread_id = next_thread_id.fetch_add(1);
            uint64_t start_key = n / num_thread * (uint64_t)thread_id;
            uint64_t end_key = start_key + n / num_thread;

            t->AssignGCID(thread_id);
            for (uint64_t i = start_key; i < end_key; i++) {
                t->Insert(keys[i], keys[i]);
            }
            t->UnregisterThread(thread_id);
        };

        std::vector<std::thread> thread_group;

        for (int i = 0; i < num_thread; i++)
            thread_group.push_back(std::thread{func});

        for (int i = 0; i < num_thread; i++)
            thread_group[i].join();
        t->UpdateThreadLocal(1);
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: load, %f ,ops/us\n", (n * 1.0) / duration.count());
    }

    {
        // Run
        auto starttime = std::chrono::system_clock::now();
        next_thread_id.store(0);
        t->UpdateThreadLocal(num_thread);
        auto func = [&]() {
            std::vector<uint64_t> v{};
            v.reserve(1);
            int thread_id = next_thread_id.fetch_add(1);
            uint64_t start_key = n / num_thread * (uint64_t)thread_id;
            uint64_t end_key = start_key + n / num_thread;

            t->AssignGCID(thread_id);
            for (uint64_t i = start_key; i < end_key; i++) {
                v.clear();
                t->GetValue(keys[i], v);
                if (v[0] != keys[i]) {
                    std::cout << "[BwTree] wrong value read: " << v[0] << " expected:" << keys[i] << std::endl;
                }
            }
            t->UnregisterThread(thread_id);
        };

        std::vector<std::thread> thread_group;

        for (int i = 0; i < num_thread; i++)
            thread_group.push_back(std::thread{func});

        for (int i = 0; i < num_thread; i++)
            thread_group[i].join();
        t->UpdateThreadLocal(1);
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: run, %f ,ops/us\n", (n * 1.0) / duration.count());
    }

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