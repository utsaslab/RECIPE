#ifndef LINEAR_HASH_H_
#define LINEAR_HASH_H_

#include <stddef.h>
#include <mutex>
#include <shared_mutex>
#include "../util/pair.h"
#include "hash.h"

class LinearProbingHash : public Hash {
  const float kResizingFactor = 2;
  const float kResizingThreshold = 0.95;
  public:
    LinearProbingHash(void);
    LinearProbingHash(size_t);
    ~LinearProbingHash(void);
    void Insert(Key_t&, Value_t);
    bool InsertOnly(Key_t&, Value_t);
    bool Delete(Key_t&);
    Value_t Get(Key_t&);
    double Utilization(void);
    size_t Capacity(void) {
      return capacity;
    }

    void* operator new[] (size_t size) {
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
    void resize(size_t);
    size_t getLocation(size_t, size_t, Pair*);

    size_t capacity;
    Pair* dict;

    size_t old_cap;
    Pair* old_dic;

    size_t size = 0;

    int resizing_lock = 0;
    std::shared_mutex *mutex;
    int nlocks;
    int locksize;
};


#endif  // LINEAR_HASH_H_
