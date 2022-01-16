#ifndef LEVEL_HASHING_H_
#define LEVEL_HASHING_H_

#include <stdint.h>
#include <mutex>
#include <shared_mutex>
#include "src/hash.h"
#include "util/pair.h"
#define ASSOC_NUM 3


struct Entry {
  Key_t key;
  Value_t value;
  Entry() {
    key = INVALID;
    value = NONE;
  }
  void* operator new[] (size_t size) {
    void* ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }

  void* operator new (size_t size) {
    void* ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }
};

struct Node {
  uint8_t token[ASSOC_NUM];
  Entry slot[ASSOC_NUM];
  char dummy[13];
  void* operator new[] (size_t size) {
    void* ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }

  void* operator new (size_t size) {
    void* ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }
};

class LevelHashing: public Hash {
  private:
    Node *buckets[2];
    Node *interim_level_buckets;
    uint64_t level_item_num[2];

    uint64_t levels;
    uint64_t addr_capacity;
    uint64_t total_capacity;
//    uint32_t occupied;
    uint64_t f_seed;
    uint64_t s_seed;
    uint32_t resize_num;
    int32_t resizing_lock = 0;
    std::shared_mutex *mutex;
    int nlocks;
    int locksize;


    void generate_seeds(void);
    void resize(void);
    int b2t_movement(uint64_t );
    uint8_t try_movement(uint64_t , uint64_t , Key_t& , Value_t);

  public:
    LevelHashing(void);
    LevelHashing(size_t);
    ~LevelHashing(void);

    bool InsertOnly(Key_t&, Value_t);
    void Insert(Key_t&, Value_t);
    bool Delete(Key_t&);
    Value_t Get(Key_t&);
    double Utilization(void);
    size_t Capacity(void) {
      return (addr_capacity + addr_capacity/2)*ASSOC_NUM;
    }
};

#endif  // LEVEL_HASHING_H_
