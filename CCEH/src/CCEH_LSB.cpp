#include <iostream>
#include <cmath>
#include <thread>
#include <bitset>
#include <cassert>
#include <unordered_map>
#include "util/persist.h"
#include "util/hash.h"
#include "src/CCEH.h"

extern size_t perfCounter;

int Segment::Insert(Key_t& key, Value_t value, size_t loc, size_t key_hash) {
#ifdef INPLACE
  if (sema == -1) return 2;
  if ((key_hash & (size_t)pow(2, local_depth)-1) != pattern) return 2;
  auto lock = sema;
  int ret = 1;
  while (!CAS(&sema, &lock, lock+1)) {
    lock = sema;
  }
  Key_t LOCK = INVALID;
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto slot = (loc + i) % kNumSlot;
    if ((h(&_[slot].key,sizeof(Key_t)) & (size_t)pow(2, local_depth)-1) != pattern) {
      _[slot].key = INVALID;
    }
    if (CAS(&_[slot].key, &LOCK, SENTINEL)) {
      _[slot].value = value;
      mfence();
      _[slot].key = key;
      ret = 0;
      break;
    } else {
      LOCK = INVALID;
    }
  }
  lock = sema;
  while (!CAS(&sema, &lock, lock-1)) {
    lock = sema;
  }
  return ret;
#else
  if (sema == -1) return 2;
  if ((key_hash & (size_t)pow(2, local_depth)-1) != pattern) return 2;
  auto lock = sema;
  int ret = 1;
  while (!CAS(&sema, &lock, lock+1)) {
    lock = sema;
  }
  Key_t LOCK = INVALID;
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto slot = (loc + i) % kNumSlot;
    if (CAS(&_[slot].key, &LOCK, SENTINEL)) {
      _[slot].value = value;
      mfence();
      _[slot].key = key;
      ret = 0;
      break;
    } else {
      LOCK = INVALID;
    }
  }
  lock = sema;
  while (!CAS(&sema, &lock, lock-1)) {
    lock = sema;
  }
  return ret;
#endif
}

void Segment::Insert4split(Key_t& key, Value_t value, size_t loc) {
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto slot = (loc+i) % kNumSlot;
    if (_[slot].key == INVALID) {
      _[slot].key = key;
      _[slot].value = value;
      return;
    }
  }
}

Segment** Segment::Split(void) {
  using namespace std;
  int64_t lock = 0;
  if (!CAS(&sema, &lock, -1)) return nullptr;

#ifdef INPLACE
  Segment** split = new Segment*[2];
  split[0] = this;
  split[1] = new Segment(local_depth+1);

  for (unsigned i = 0; i < kNumSlot; ++i) {
    auto key_hash = h(&_[i].key, sizeof(Key_t));
    if (key_hash & ((size_t) 1 << local_depth)) {
      split[1]->Insert4split
        (_[i].key, _[i].value, (key_hash >> (8*sizeof(key_hash)-kShift))*kNumPairPerCacheLine);
    }
  }

  clflush((char*)split[1], sizeof(Segment));
  local_depth = local_depth + 1;
  clflush((char*)&local_depth, sizeof(size_t));

  return split;
#else
  Segment** split = new Segment*[2];
  split[0] = new Segment(local_depth+1);
  split[1] = new Segment(local_depth+1);

  for (unsigned i = 0; i < kNumSlot; ++i) {
    auto key_hash = h(&_[i].key, sizeof(Key_t));
    if (key_hash & ((size_t) 1 << (local_depth))) {
      split[1]->Insert4split
        (_[i].key, _[i].value, (key_hash >> (8*sizeof(key_hash)-kShift))*kNumPairPerCacheLine);
    } else {
      split[0]->Insert4split
        (_[i].key, _[i].value, (key_hash >> (8*sizeof(key_hash)-kShift))*kNumPairPerCacheLine);
    }
  }

  clflush((char*)split[0], sizeof(Segment));
  clflush((char*)split[1], sizeof(Segment));

  return split;
#endif
}


CCEH::CCEH(void)
: dir{1}, global_depth{0}
{
  for (unsigned i = 0; i < dir.capacity; ++i) {
    dir._[i] = new Segment(global_depth);
    dir._[i]->pattern = i;
  }
}

CCEH::CCEH(size_t initCap)
: dir{initCap}, global_depth{static_cast<size_t>(log2(initCap))}
{
  for (unsigned i = 0; i < dir.capacity; ++i) {
    dir._[i] = new Segment(global_depth);
    dir._[i]->pattern = i;
  }
}

CCEH::~CCEH(void)
{ }

void Directory::LSBUpdate(int local_depth, int global_depth, int dir_cap, int x, Segment** s) {
  int depth_diff = global_depth - local_depth;
  if (depth_diff == 0) {
    if ((x % dir_cap) >= dir_cap/2) {
      _[x-dir_cap/2] = s[0];
      clflush((char*)&_[x-dir_cap/2], sizeof(Segment*));
      _[x] = s[1];
      clflush((char*)&_[x], sizeof(Segment*));
    } else {
      _[x] = s[0];
      clflush((char*)&_[x], sizeof(Segment*));
      _[x+dir_cap/2] = s[1];
      clflush((char*)&_[x+dir_cap/2], sizeof(Segment*));
    }
  } else {
    if ((x%dir_cap) >= dir_cap/2) {
      LSBUpdate(local_depth+1, global_depth, dir_cap/2, x-dir_cap/2, s);
      LSBUpdate(local_depth+1, global_depth, dir_cap/2, x, s);
    } else {
      LSBUpdate(local_depth+1, global_depth, dir_cap/2, x, s);
      LSBUpdate(local_depth+1, global_depth, dir_cap/2, x+dir_cap/2, s);
    }
  }
  return;
}

void CCEH::Insert(Key_t& key, Value_t value) {
STARTOVER:
  auto key_hash = h(&key, sizeof(key));
  auto y = (key_hash >> (sizeof(key_hash)*8-kShift)) * kNumPairPerCacheLine;

RETRY:
  auto x = (key_hash % dir.capacity);
  auto target = dir._[x];
  auto ret = target->Insert(key, value, y, key_hash);

  if (ret == 1) {
    timer.Start();
    Segment** s = target->Split();
    timer.Stop();
    breakdown += timer.GetSeconds();
    if (s == nullptr) {
      // another thread is doing split
      goto RETRY;
    }

    s[0]->pattern = (key_hash % (size_t)pow(2, s[0]->local_depth-1));
    s[1]->pattern = s[0]->pattern + (1 << (s[0]->local_depth-1));

    // Directory management
    while (!dir.Acquire()) {
      asm("nop");
    }
    { // CRITICAL SECTION - directory update
      x = (key_hash % dir.capacity);
#ifdef INPLACE
      if (dir._[x]->local_depth-1 < global_depth) {  // normal split
#else
      if (dir._[x]->local_depth < global_depth) {  // normal split
#endif
        dir.LSBUpdate(s[0]->local_depth, global_depth, dir.capacity, x, s);
      } else {  // directory doubling
        auto d = dir._;
        auto _dir = new Segment*[dir.capacity*2];
        memcpy(_dir, d, sizeof(Segment*)*dir.capacity);
        memcpy(_dir+dir.capacity, d, sizeof(Segment*)*dir.capacity);
        _dir[x] = s[0];
        _dir[x+dir.capacity] = s[1];
        clflush((char*)&dir._[0], sizeof(Segment*)*dir.capacity);
        dir._ = _dir;
        clflush((char*)&dir._, sizeof(void*));
        dir.capacity *= 2;
        clflush((char*)&dir.capacity, sizeof(size_t));
        global_depth += 1;
        clflush((char*)&global_depth, sizeof(global_depth));
        delete d;
        // TODO: requiered to do this atomically
      }
#ifdef INPLACE
      s[0]->sema = 0;
#endif
    }  // End of critical section
    while (!dir.Release()) {
      asm("nop");
    }
    goto RETRY;
  } else if (ret == 2) {
    // Insert(key, value);
    goto STARTOVER;
  } else {
    clflush((char*)&dir._[x]->_[y], 64);
  }
}

// This function does not allow resizing
bool CCEH::InsertOnly(Key_t& key, Value_t value) {
  auto key_hash = h(&key, sizeof(key));
  auto x = (key_hash % dir.capacity);
  auto y = (key_hash >> (sizeof(key_hash)*8-kShift)) * kNumPairPerCacheLine;

  auto ret = dir._[x]->Insert(key, value, y, key_hash);
  if (ret == 0) {
    clflush((char*)&dir._[x]->_[y], 64);
    return true;
  }

  return false;
}

// TODO
bool CCEH::Delete(Key_t& key) {
  return false;
}

Value_t CCEH::Get(Key_t& key) {
  auto key_hash = h(&key, sizeof(key));
  const size_t mask = dir.capacity-1;
  auto x = (key_hash & mask);
  auto y = (key_hash >> (sizeof(key_hash)*8-kShift)) * kNumPairPerCacheLine;

  auto dir_ = dir._[x];

#ifdef INPLACE
  auto sema = dir._[x]->sema;
  while (!CAS(&dir._[x]->sema, &sema, sema+1)) {
    sema = dir._[x]->sema;
  }
#endif

  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto slot = (y+i) % Segment::kNumSlot;
    if (dir_->_[slot].key == key) {
#ifdef INPLACE
      sema = dir._[x]->sema;
      while (!CAS(&dir._[x]->sema, &sema, sema-1)) {
        sema = dir._[x]->sema;
      }
#endif
      return dir_->_[slot].value;
    }
  }

#ifdef INPLACE
  sema = dir._[x]->sema;
  while (!CAS(&dir._[x]->sema, &sema, sema-1)) {
    sema = dir._[x]->sema;
  }
#endif
  return NONE;
}

double CCEH::Utilization(void) {
  size_t sum = 0;
  std::unordered_map<Segment*, bool> set;
  for (size_t i = 0; i < dir.capacity; ++i) {
    set[dir._[i]] = true;
  }
  for (auto& elem: set) {
    for (unsigned i = 0; i < Segment::kNumSlot; ++i) {
      if (elem.first->_[i].key != INVALID) sum++;
    }
  }
  return ((double)sum)/((double)set.size()*Segment::kNumSlot)*100.0;
}

size_t CCEH::Capacity(void) {
  std::unordered_map<Segment*, bool> set;
  for (size_t i = 0; i < dir.capacity; ++i) {
    set[dir._[i]] = true;
  }
  return set.size() * Segment::kNumSlot;
}

size_t Segment::numElem(void) {
  size_t sum = 0;
  for (unsigned i = 0; i < kNumSlot; ++i) {
    if (_[i].key != INVALID) {
      sum++;
    }
  }
  return sum;
}

bool CCEH::Recovery(void) {
  return false;
}

// for debugging
Value_t CCEH::FindAnyway(Key_t& key) {
  using namespace std;
  for (size_t i = 0; i < dir.capacity; ++i) {
     for (size_t j = 0; j < Segment::kNumSlot; ++j) {
       if (dir._[i]->_[j].key == key) {
         auto key_hash = h(&key, sizeof(key));
         auto x = (key_hash >> (8*sizeof(key_hash)-global_depth));
         auto y = (key_hash & kMask) * kNumPairPerCacheLine;
         cout << bitset<32>(i) << endl << bitset<32>((x>>1)) << endl << bitset<32>(x) << endl;
         return dir._[i]->_[j].value;
       }
     }
  }
  return NONE;
}

void Directory::SanityCheck(void* addr) {
  using namespace std;
  for (unsigned i = 0; i < capacity; ++i) {
    if (_[i] == addr) {
      cout << i << " " << _[i]->sema << endl;
      exit(1);
    }
  }
}
