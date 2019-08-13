#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iostream>

#include "src/path_hashing.hpp"
#include "util/hash.h"
#include "util/persist.h"

using namespace std;

PathHashing::PathHashing(void) {
}

PathHashing::~PathHashing(void) {
  delete [] mutex;
  delete [] table;
}

PathHashing::PathHashing(size_t _levels, size_t _reserved_levels) 
  : levels{_levels},
  reserved_levels{_reserved_levels},
  addr_capacity{pow(2, levels-1)},
  total_capacity{pow(2, levels) - pow(2, levels - reserved_levels)},
  size{0},
  table{new Node[total_capacity]}
{
  locksize = 256;
  nlocks = (total_capacity)/locksize+1;
  mutex = new std::shared_mutex[nlocks];

  generate_seeds();
}

void PathHashing::Insert(Key_t& key, Value_t value) {
RETRY:
  while (resizing_lock == 1) {
    asm("nop");
  }
  auto f_idx = F_IDX();
  auto s_idx = S_IDX();

  auto sub_f_idx = f_idx;
  auto sub_s_idx = s_idx;
  auto capacity = 0;
  for(unsigned i = 0; i < reserved_levels; i ++){
    {
      std::unique_lock<std::shared_mutex> lock(mutex[(f_idx)/locksize]);
      if (table[f_idx].key == INVALID)
      {
        table[f_idx].value = value;
        mfence();
        table[f_idx].key = key;
        clflush((char*)&table[f_idx], sizeof(Node));
        auto _size = size;
        while (!CAS(&size, &_size, _size+1)) {
          _size = size;
        }
        return;
      }
    }
    {
      std::unique_lock<std::shared_mutex> lock(mutex[(s_idx)/locksize]);
      if (table[s_idx].key == INVALID)
      {
        table[s_idx].value = value;
        mfence();
        table[s_idx].key = key;
        clflush((char*)&table[s_idx], sizeof(Node));
        auto _size = size;
        while (!CAS(&size, &_size, _size+1)) {
          _size = size;
        }
        return;
      }
    }
    sub_f_idx = sub_f_idx/2;
    sub_s_idx = sub_s_idx/2;
    capacity = (int)pow(2, levels) - (int)pow(2, levels - i - 1);
    f_idx = sub_f_idx + capacity;
    s_idx = sub_s_idx + capacity;
  }
  auto lock = 0;
  if (CAS(&resizing_lock, &lock, 1)) {
    timer.Start();
    resize();
    timer.Stop();
    breakdown += timer.GetSeconds();
    resizing_lock = 0;
  }
  goto RETRY;
}

bool PathHashing::InsertOnly(Key_t& key, Value_t value) {
  auto f_idx = F_IDX();
  auto s_idx = S_IDX();

  auto sub_f_idx = f_idx;
  auto sub_s_idx = s_idx;
  auto capacity = 0;

  for(unsigned i = 0; i < reserved_levels; i ++){
    if (table[f_idx].key == INVALID)
    {
      table[f_idx].value = value;
      mfence();
      table[f_idx].key = key;
      clflush((char*)&table[f_idx], sizeof(Node));
      size++;
      return true;
    }
    if (table[s_idx].key == INVALID)
    {
      table[s_idx].value = value;
      mfence();
      table[s_idx].key = key;
      clflush((char*)&table[s_idx], sizeof(Node));
      size++;
      return true;
    }

    sub_f_idx = sub_f_idx/2;
    sub_s_idx = sub_s_idx/2;
    capacity = (int)pow(2, levels) - (int)pow(2, levels - i - 1);

    f_idx = sub_f_idx + capacity;
    s_idx = sub_s_idx + capacity;
  }

  return false;
}

bool PathHashing::Delete(Key_t& key) {
  return false;
}

Value_t PathHashing::Get(Key_t& key) {
  auto f_idx = F_IDX();
  auto s_idx = S_IDX();

  auto sub_f_idx = f_idx;
  auto sub_s_idx = s_idx;
  auto capacity = 0;

  for(int i = 0; i < reserved_levels; i ++){
    {
      std::shared_lock<std::shared_mutex> lock(mutex[f_idx/locksize]);
      if(table[f_idx].key == key){
        return table[f_idx].value;
      }
    }
    {
      std::shared_lock<std::shared_mutex> lock(mutex[s_idx/locksize]);
      if(table[s_idx].key == key){
        return table[s_idx].value;
      }
    }

    sub_f_idx = sub_f_idx/2;
    sub_s_idx = sub_s_idx/2;
    capacity = pow(2, levels) - pow(2, levels - i - 1);

    f_idx = sub_f_idx + capacity;
    s_idx = sub_s_idx + capacity;
  }

  return NONE;
}

void PathHashing::resize(void) {
  std::unique_lock<std::shared_mutex> *lock[nlocks];
  for(int i=0; i<nlocks; i++) {
    lock[i] = new std::unique_lock<std::shared_mutex>(mutex[i]);
  }
  std::shared_mutex* old_mutex = mutex;

  auto old_total_capacity = total_capacity;
  auto *oldBucket = table;
  levels ++;
  addr_capacity = pow(2, levels-1);
  total_capacity = pow(2, levels) - pow(2, levels - reserved_levels);
  table = new Node[total_capacity];

  int prev_nlocks = nlocks;
  nlocks = total_capacity/locksize+1;
  mutex = new std::shared_mutex[nlocks];

  for (unsigned old_idx = 0; old_idx < old_total_capacity; old_idx ++) {
    int i, j;
    if (oldBucket[old_idx].key != INVALID)
    {
      Key_t key = oldBucket[old_idx].key;
      Value_t value = oldBucket[old_idx].value;

      uint32_t f_idx = F_IDX_Re(F_HASH(key), addr_capacity);
      uint32_t s_idx = S_IDX_Re(S_HASH(key), addr_capacity);

      uint32_t sub_f_idx = f_idx;
      uint32_t sub_s_idx = s_idx;
      uint32_t capacity = 0;

      int insertSuccess = 0;
      for(i = 0; i < reserved_levels; i ++){

        if (table[f_idx].key == INVALID)
        {
          table[f_idx].value = value;
#ifndef BATCH
          mfence();
#endif
          table[f_idx].key = key;
#ifndef BATCH
          clflush((char*)&table[f_idx], sizeof(Node));
#endif
          insertSuccess = 1;
          break;
        }
        else if (table[s_idx].key == INVALID)
        {
          table[s_idx].value = value;
#ifndef BATCH
          mfence();
#endif
          table[s_idx].key = key;
#ifndef BATCH
          clflush((char*)&table[s_idx], sizeof(Node));
#endif
          insertSuccess = 1;
          break;
        }

        sub_f_idx = sub_f_idx/2;
        sub_s_idx = sub_s_idx/2;
        capacity = (int)pow(2, levels) - (int)pow(2, levels - i - 1);

        f_idx = sub_f_idx + capacity;
        s_idx = sub_s_idx + capacity;
      }
    }
  }

#ifdef BATCH
  clflush((char*)&table[0], sizeof(Node)*total_capacity);
#endif

  delete [] oldBucket;
  for(int i=0; i<prev_nlocks; i++){
    delete lock[i];
  }
  delete old_mutex;
}

void PathHashing::generate_seeds(void) {
  srand(time(NULL));

  do
  {
    f_seed = rand();
    s_seed = rand();
    f_seed = f_seed << (rand() % 63);
    s_seed = s_seed << (rand() % 63);
  } while (f_seed == s_seed);
}

uint64_t PathHashing::F_HASH(Key_t& key) {
  return (h(&key, sizeof(key), f_seed));
}

uint64_t PathHashing::S_HASH(Key_t& key) {
  return (h(&key, sizeof(key), s_seed));
}

double PathHashing::Utilization(void) {
  size_t sum = 0;
  for (unsigned i = 0; i < total_capacity; ++i) {
    if (table[i].key != INVALID) {
      sum++;
    }
  }
  return ((double)(sum)/(double)(total_capacity)*100);
  }
