#ifndef PATH_HASHING_H_
#define PATH_HASHING_H_

#include <stdint.h>
#include <mutex>
#include <shared_mutex>
#include "../src/hash.h"
#include "../util/pair.h"

#define FIRST_HASH(hash, capacity) (hash % (capacity / 2))
#define SECOND_HASH(hash, capacity) ((hash % (capacity / 2)) + (capacity / 2))
#define F_IDX() FIRST_HASH(                       \
    h(&key, sizeof(key), f_seed), \
    addr_capacity)
#define S_IDX() SECOND_HASH(                       \
    h(&key, sizeof(key), s_seed), \
    addr_capacity)

struct Node {
  // uint8_t token;
  Key_t key;
  Value_t value;

  Node(void) {
    key = INVALID;
    value = NONE;
  }

  void* operator new[](size_t size) {
    void* ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }

  void* operator new (size_t size){
    void *ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }
};

class PathHashing : public Hash {
  private:
    uint32_t levels;                //  the number of levels of the complete binary tree in path hashing
    uint32_t reserved_levels;       //  the number of reserved levels in path hashing
    uint32_t addr_capacity;         //  the number of addressed cells in path hashing, i.e., the number of cells in the top level
    uint32_t total_capacity;        //  the total number of cells in path hashing
    uint32_t size;                  //  the number of stored items in path hashing
    uint64_t f_seed;
    uint64_t s_seed;
    int resizing_lock = 0;
    std::shared_mutex *mutex;
    int nlocks;
    int locksize;

    Node *table;

    uint64_t F_HASH(Key_t&);
    uint64_t S_HASH(Key_t&);

    uint32_t F_IDX_Re(size_t hashKey, size_t capacity) {
      return hashKey % (capacity/2);
    }
    uint32_t S_IDX_Re(size_t hashKey, size_t capacity) {
      return hashKey % (capacity/2) + (capacity/2);
    }

    void generate_seeds(void);
    void resize(void);

  public:
    PathHashing(void);
    PathHashing(size_t, size_t);
    ~PathHashing(void);

    bool InsertOnly(Key_t&, Value_t);
    void Insert(Key_t&, Value_t);
    bool Delete(Key_t&);
    Value_t Get(Key_t&);
    double Utilization(void);
    size_t Capacity(void) {
      return total_capacity;
    }
};


#endif  // PATH_HASHING_H_
