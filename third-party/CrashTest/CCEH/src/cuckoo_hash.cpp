#include <iostream>  // debugging
#include <cstring>
#include <algorithm>
#include "../util/persist.h"
#include "../util/hash.h"
#include "cuckoo_hash.h"

using namespace std;

CuckooHash::CuckooHash(void)
  : capacity{0}, table{nullptr} {
    memset(&pushed, 0, sizeof(Pair)*2);
  }

CuckooHash::CuckooHash(size_t _capacity)
  : capacity{_capacity}, table{new Pair[capacity]} {
    memset(&pushed, 0, sizeof(Pair)*2);
    locksize = 256;
    nlocks = (capacity)/locksize+1;
    mutex = new std::shared_mutex[nlocks];
  }

CuckooHash::~CuckooHash() {
  delete [] table;
}

void CuckooHash::Insert(Key_t& key, Value_t value) {
  auto f_hash = hash_funcs[0](&key, sizeof(key), 0xc70f6907UL);
  auto s_hash = hash_funcs[1](&key, sizeof(key), 0xc70f6907UL);

RETRY:
  while (resizing_lock == 1) {
    asm("nop");
  }
  auto f_idx = f_hash % capacity;
  auto s_idx = s_hash % capacity;

  auto f_invalid = INVALID;
  auto s_invalid = INVALID;

  {
    unique_lock<shared_mutex> f_lock(mutex[f_idx/locksize]);
    if (CAS(&table[f_idx].key, &f_invalid, SENTINEL)) {
      table[f_idx].value = value;
      mfence();
      table[f_idx].key = key;
      clflush((char*)&table[f_idx], sizeof(Pair));
      return;
    }
  }
  {
    unique_lock<shared_mutex> s_lock(mutex[s_idx/locksize]);
    if (CAS(&table[s_idx].key, &s_invalid, SENTINEL)) {
      table[s_idx].value = value;
      mfence();
      table[s_idx].key = key;
      clflush((char*)&table[s_idx], sizeof(Pair));
      return;
    }
  }

  { // Failed to insert... Doing Cuckooing...
    int unlocked = 0;
    if (CAS(&resizing_lock, &unlocked, 1)) {
PATH_RETRY:
      auto path1 = find_path(f_idx);
      auto path2 = find_path(s_idx);
      if (path1.size() != 0 || path2.size() != 0) {
        auto path = &path1;
        if (path1.size() == 0
            || (path2.size() != 0 && path2.size() < path1.size())
            || (path2.size() != 0 && path1[0].second == INVALID) ) {
          path = &path2;
        }
        auto id = 0;
        vector<size_t> lock_loc;
        for (auto p: *path) {
          lock_loc.push_back(p.first/locksize);
        }
        sort(begin(lock_loc), end(lock_loc));
        lock_loc.erase( unique( lock_loc.begin(), lock_loc.end() ), lock_loc.end() );
        unique_lock<shared_mutex> *lock[kCuckooThreshold];
        for (auto i :lock_loc) {
          lock[id++] = new unique_lock<shared_mutex>(mutex[i]);
        }
        for (auto p : *path) {
          if (table[p.first].key != p.second) {
            for (int i = 0; i < id; ++i) {
              delete lock[i];
            }
            goto PATH_RETRY;
          }
        }
        resizing_lock = 0;
        execute_path(*path, key, value);
        for (int i = 0; i < id; ++i) {
          delete lock[i];
        }

        return;
      } else {
        resize();
        resizing_lock = 0;
      }
    }
  }
  goto RETRY;
}

bool CuckooHash::insert4resize(Key_t& key, Value_t value) {
  auto f_hash = hash_funcs[0](&key, sizeof(key), 0xc70f6907UL);
  auto s_hash = hash_funcs[1](&key, sizeof(key), 0xc70f6907UL);

  auto f_idx = f_hash % capacity;
  auto s_idx = s_hash % capacity;

  auto f_invalid = INVALID;
  auto s_invalid = INVALID;

  if (table[f_idx].key == INVALID) {
    table[f_idx].key = key;
    table[f_idx].value = value;
  } else if (table[s_idx].key == INVALID) {
    table[s_idx].key = key;
    table[s_idx].value = value;
  } else {
    auto path1 = find_path(f_idx);
    auto path2 = find_path(s_idx);
    pushed[0].key = key;
    pushed[0].value = value;
    if (path1.size() == 0 && path2.size() == 0) {
      return false;
    } else {
      if (path1.size() == 0) {
        execute_path(path2);
      } else if (path2.size() == 0) {
        execute_path(path1);
      } else if (path1.size() < path2.size()) {
        execute_path(path1);
      } else {
        execute_path(path2);
      }
    }
  }
  return true;
}

bool CuckooHash::InsertOnly(Key_t& key, Value_t value) {
  auto f_hash = hash_funcs[0](&key, sizeof(key), 0xc70f6907UL);
  auto s_hash = hash_funcs[1](&key, sizeof(key), 0xc70f6907UL);

  auto f_idx = f_hash % capacity;
  auto s_idx = s_hash % capacity;

  auto f_invalid = INVALID;
  auto s_invalid = INVALID;

  if (CAS(&table[f_idx].key, &f_invalid, SENTINEL)) {
    table[f_idx].value = value;
    mfence();
    table[f_idx].key = key;
    clflush((char*)&table[f_idx], sizeof(Pair));
    return true;
  } else if (CAS(&table[s_idx].key, &s_invalid, SENTINEL)) {
    table[s_idx].value = value;
    mfence();
    table[s_idx].key = key;
    clflush((char*)&table[s_idx], sizeof(Pair));
    return true;
  } else {
    return false;
  }
}


vector<pair<size_t,Key_t>> CuckooHash::find_path(size_t target) {
  vector<pair<size_t,size_t>> path;
  path.reserve(kCuckooThreshold);
  path.emplace_back(target, table[target].key);
  auto cur = target;
  auto i = 0;
  do {
    Key_t* key = &table[cur].key;
    if (*key == INVALID) break;
    auto f_hash = hash_funcs[0](key, sizeof(Key_t), 0xc70f6907UL);
    auto s_hash = hash_funcs[1](key, sizeof(Key_t), 0xc70f6907UL);

    auto f_idx = f_hash % capacity;
    auto s_idx = s_hash % capacity;

    if (f_idx == cur) {
      path.emplace_back(s_idx, table[s_idx].key);
      cur = s_idx;
    } else if (s_idx == cur) {
      path.emplace_back(f_idx, table[f_idx].key);
      cur = f_idx;
    } else {  // something terribly wrong
      cout << "E: " << f_idx << " " << s_idx << " " << cur << " " << target << endl;
      cout << *key << endl;
      exit(1);
    }
    i++;
  } while (i < kCuckooThreshold);

  if (i == kCuckooThreshold) {
    path.resize(0);
  }

  return move(path);
}

bool CuckooHash::execute_path(vector<pair<size_t,size_t>>& path) {
  auto i = 0;
  auto j = (i+1)%2;

  for (auto p: path) {
    pushed[j] = table[p.first];
    clflush((char*)& pushed[j], sizeof(Pair));
    table[p.first] = pushed[i];
    clflush((char*)& table[p.first], sizeof(Pair));
    i = (i+1)%2;
    j = (i+1)%2;
  }
  return true;
}

bool CuckooHash::execute_path(vector<pair<size_t,size_t>>& path, Key_t& key, Value_t value) {
  for (int i = path.size()-1; i > 0; --i) {
    table[path[i].first] = table[path[i-1].first];
    clflush((char*)&table[path[i].first], sizeof(Pair));
  }
  table[path[0].first].value = value;
  mfence();
  table[path[0].first].key = key;
  clflush((char*)&table[path[0].first], sizeof(Pair));
  return true;
}

bool CuckooHash::Delete(Key_t& key) {
  return false;
}

Value_t CuckooHash::Get(Key_t& key) {
  if (resizing_lock) {
    for (unsigned i = 0; i < kNumHash; ++i) {
      auto idx = hash_funcs[i](&key, sizeof(key), 0xc70f6907UL) % old_cap;
      std::shared_lock<std::shared_mutex> lock(mutex[idx/locksize]);
      if (old_tab[idx].key == key) {
        return old_tab[idx].value;
      }
    }
    return NONE;
  } else {
    for (int i = 0; i < kNumHash; i++) {
      auto idx = hash_funcs[i](&key, sizeof(key), 0xc70f6907UL) % capacity;
      std::shared_lock<std::shared_mutex> lock(mutex[idx/locksize]);
      if (table[idx].key == key) {
        return table[idx].value;
      }
    }
    return NONE;
  }
}

double CuckooHash::Utilization(void) {
  size_t n = 0;
  for (int i = 0; i < capacity; i++) {
    if (table[i].key != INVALID) {
      n += 1;
    }
  }
  return ((double)n)/((double)capacity)*100;
}

bool CuckooHash::resize(void) {
  old_cap = capacity;
  old_tab = table;
  clflush((char*)&old_cap, sizeof(size_t));
  clflush((char*)&old_tab, sizeof(Pair*));

  std::unique_lock<std::shared_mutex> *lock[nlocks];
  for(int i=0;i<nlocks;i++){
    lock[i] = new std::unique_lock<std::shared_mutex>(mutex[i]);
  }
  std::shared_mutex* old_mutex = mutex;

  int prev_nlocks = nlocks;

  bool success = true;
  size_t i = 0;

  do {
    success = true;
    if (table != old_tab) delete [] table;
    capacity = capacity * kResizingFactor;
    table = new Pair[capacity];
    if (table == nullptr) {
      cerr << "error: memory allocation failed." << endl;
      exit(1);
    }
    clflush((char*)&table, sizeof(size_t));
    clflush((char*)&capacity, sizeof(Pair*));

    for (unsigned i = 0; i < old_cap; ++i) {
      if (old_tab[i].key != INVALID) {
        if (!insert4resize(old_tab[i].key, old_tab[i].value)) {
          success = false;
          break;
        }
      }
    }
    ++i;
  } while (!success && i < kMaxGrows);

  for (int i = 0; i < prev_nlocks; ++i) {
    delete lock[i];
  }

  if (success) {
    nlocks = capacity/locksize+1;
    mutex = new std::shared_mutex[nlocks];
    delete old_mutex;
    clflush((char*)&table[0], sizeof(Pair*)*capacity);
  } else {
    exit(1);
  }
  return success;
}

void CuckooHash::print(void) {
}
