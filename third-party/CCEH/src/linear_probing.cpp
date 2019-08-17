#include <iostream>
#include <cstring>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include "../util/persist.h"
#include "../util/hash.h"
#include "linear_probing.h"


LinearProbingHash::LinearProbingHash(void)
  : Hash{}, capacity{0}, dict{nullptr} { }

LinearProbingHash::LinearProbingHash(size_t _capacity)
  : Hash{}, capacity{_capacity}, dict{new Pair[capacity]}
{
  locksize = 256;
  nlocks = (capacity)/locksize+1;
  mutex = new std::shared_mutex[nlocks];
}

LinearProbingHash::~LinearProbingHash(void) {
  if (dict != nullptr) delete[] dict;
}

void LinearProbingHash::Insert(Key_t& key, Value_t value) {
  using namespace std;
  auto key_hash = h(&key, sizeof(key));

RETRY:
  while (resizing_lock == 1) {
    asm("nop");
  }
  auto loc = key_hash % capacity;
  if (size < capacity * kResizingThreshold) {
    auto i = 0;
    while (i < capacity) {
      auto slot = (loc + i) % capacity;
      unique_lock<shared_mutex> lock(mutex[slot/locksize]);
      do {
        if (dict[slot].key == INVALID) {
          dict[slot].value = value;
          mfence();
          dict[slot].key = key;
          clflush((char*)&dict[slot].key, sizeof(Pair));
          auto _size = size;
          while (!CAS(&size, &_size, _size+1)) {
            _size = size;
          }
          return;
        }
        i++;
        slot = (loc + i) % capacity;
        if (!(i < capacity)) break;
      } while (slot % locksize != 0);
    }
  } else {
    auto lock = 0;
    if (CAS(&resizing_lock, &lock, 1)) {
      resize(capacity * kResizingFactor);
      resizing_lock = 0;
    }
  }
  goto RETRY;
}

bool LinearProbingHash::InsertOnly(Key_t& key, Value_t value) {
  auto key_hash = h(&key, sizeof(key)) % capacity;
  auto loc = getLocation(key_hash, capacity, dict);
  if (loc == INVALID) {
    return false;
  } else {
    dict[loc].value = value;
    mfence();
    dict[loc].key = key;
    clflush((char*)&dict[loc], sizeof(Pair));
    size++;
    return true;
  }
}

bool LinearProbingHash::Delete(Key_t& key) {
  return false;
}

Value_t LinearProbingHash::Get(Key_t& key) {
  auto key_hash = h(&key, sizeof(key)) % capacity;
  for (size_t i = 0; i < capacity; ++i) {
    auto id = (key_hash + i) % capacity;
    std::shared_lock<std::shared_mutex> lock(mutex[id/locksize]);
    if (dict[id].key == INVALID) return NONE;
    if (dict[id].key == key) return std::move(dict[id].value);
  }
}

double LinearProbingHash::Utilization(void) {
  size_t size = 0;
  for (size_t i = 0; i < capacity; ++i) {
    if (dict[i].key != INVALID) {
      ++size;
    }
  }
  return ((double)size)/((double)capacity)*100;
}

size_t LinearProbingHash::getLocation(size_t hash_value, size_t _capacity, Pair* _dict) {
  Key_t LOCK = INVALID;
  size_t cur = hash_value;
  size_t i = 0;
FAILED:
  while (_dict[cur].key != INVALID) {
    cur = (cur + 1) % _capacity;
    ++i;
    if (!(i < capacity)) {
      return INVALID;
    }
  }
  if (CAS(&_dict[cur].key, &LOCK, SENTINEL)) {
    return cur;
  } else {
    goto FAILED;
  }
}

void LinearProbingHash::resize(size_t _capacity) {
  timer.Start();
  std::unique_lock<std::shared_mutex> *lock[nlocks];
  for(int i=0; i<nlocks; i++){
    lock[i] = new std::unique_lock<std::shared_mutex>(mutex[i]);
  }
  int prev_nlocks = nlocks;
  nlocks = _capacity/locksize+1;
  std::shared_mutex* old_mutex = mutex;


  Pair* newDict = new Pair[_capacity];
  for (int i = 0; i < capacity; i++) {
    if (dict[i].key != INVALID) {
      auto key_hash = h(&dict[i].key, sizeof(Key_t)) % _capacity;
      auto loc = getLocation(key_hash, _capacity, newDict);
      newDict[loc].key = dict[i].key;
      newDict[loc].value = dict[i].value;
    }
  }
  mutex = new std::shared_mutex[nlocks];
  clflush((char*)&newDict[0], sizeof(Pair)*_capacity);
  old_cap = capacity;
  old_dic = dict;
  clflush((char*)&old_cap, sizeof(size_t));
  clflush((char*)&old_dic, sizeof(Pair*));
  dict = newDict;
  clflush((char*)&dict, sizeof(void*));
  capacity = _capacity;
  clflush((char*)&capacity, sizeof(size_t));
  auto tmp = old_dic;
  old_cap = 0;
  old_dic = nullptr;
  clflush((char*)&old_cap, sizeof(size_t));
  clflush((char*)&old_dic, sizeof(Pair*));

  delete [] tmp;
  for(int i=0; i<prev_nlocks; i++) {
    delete lock[i];
  }
  delete[] old_mutex;
  timer.Stop();
  breakdown += timer.GetSeconds();


}
