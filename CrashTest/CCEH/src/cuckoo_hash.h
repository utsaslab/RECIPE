#ifndef CUCKOO_HASH_H_
#define CUCKOO_HASH_H_

#include <stddef.h>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include "../util/pair.h"
#include "hash.h"

class CuckooHash : public Hash {
  size_t _seed = 0xc70f6907UL;
  const size_t kCuckooThreshold = 16;
  const size_t kNumHash = 2;
  const float kResizingFactor = 2;
  const size_t kMaxGrows = 128;

  public:
    CuckooHash(void);
    CuckooHash(size_t);
    ~CuckooHash(void);

    void Insert(Key_t&, Value_t);
    bool InsertOnly(Key_t&, Value_t);
    bool Delete(Key_t&);
    Value_t Get(Key_t&);
    double Utilization(void);
    size_t Capacity(void) {
      return capacity;
    }

    void print(void);


    void* operator new[](size_t size) {
      void *ret;
      posix_memalign(&ret, 64, size);
      return ret;
    }


    void* operator new(size_t size) {
      void *ret;
      posix_memalign(&ret, 64, size);
      return ret;
    }

  private:
    bool insert4resize(Key_t&, Value_t);
    bool resize(void);
    std::vector<std::pair<size_t, size_t>> find_path(size_t);
    bool validate_path(std::vector<size_t>&);
    bool execute_path(std::vector<std::pair<size_t,size_t>>&);
    bool execute_path(std::vector<std::pair<size_t,size_t>>&, Key_t&, Value_t);

    size_t capacity;
    Pair* table;
    Pair pushed[2];
    Pair temp;
    
    size_t old_cap;
    Pair* old_tab;

    int resizing_lock = 0;
    std::shared_mutex *mutex;
    int nlocks;
    int locksize;
};


#endif  // CUCKOO_HASH_H_
