#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include "src/Level_hashing.h"
#include "util/hash.h"
#include "util/persist.h"

using namespace std;

#define F_HASH(key) (h(&key, sizeof(Key_t), f_seed))
#define S_HASH(key) (h(&key, sizeof(Key_t), s_seed))
#define F_IDX(hash, capacity) (hash % (capacity/2))
#define S_IDX(hash, capacity) ((hash % (capacity/2)) + (capacity/2))

void LevelHashing::generate_seeds(void) {
  srand(time(NULL));

  do
  {
    f_seed = rand();
    s_seed = rand();
    f_seed = f_seed << (rand() % 63);
    s_seed = s_seed << (rand() % 63);
  } while (f_seed == s_seed);
}

LevelHashing::LevelHashing(void){
}

LevelHashing::~LevelHashing(void){
  delete [] mutex;
  delete [] buckets;
}

LevelHashing::LevelHashing(size_t _levels)
  : levels{_levels},
  addr_capacity{pow(2, levels)},
  total_capacity{pow(2, levels) + pow(2, levels-1)},
  resize_num{0}
{
  locksize = 256;
  nlocks = (3*addr_capacity/2)/locksize+1;
  mutex = new std::shared_mutex[nlocks];

  generate_seeds();
  buckets[0] = new Node[addr_capacity];
  buckets[1] = new Node[addr_capacity/2];
  level_item_num[0] = 0;
  level_item_num[1] = 0;
  interim_level_buckets = NULL;
}


void LevelHashing::Insert(Key_t& key, Value_t value) {
RETRY:
  while (resizing_lock == 1) {
    asm("nop");
  }
  uint64_t f_hash = F_HASH(key);
  uint64_t s_hash = S_HASH(key);
  uint32_t f_idx = F_IDX(f_hash, addr_capacity);
  uint32_t s_idx = S_IDX(s_hash, addr_capacity);

  int i, j;

  for(i = 0; i < 2; i ++){
    for(j = 0; j < ASSOC_NUM; j ++){
      {
        std::unique_lock<std::shared_mutex> lock(mutex[f_idx/locksize]);
	if(buckets[i][f_idx].token[j] == 0){
          buckets[i][f_idx].slot[j].value = value;
          mfence();
          buckets[i][f_idx].slot[j].key = key;
	  buckets[i][f_idx].token[j] = 1;
	  clflush((char*)&buckets[i][f_idx], sizeof(Node));
	  level_item_num[i]++;
          return;
        }
      }
      {
        std::unique_lock<std::shared_mutex> lock(mutex[s_idx/locksize]);
	if(buckets[i][s_idx].token[j] == 0){
          buckets[i][s_idx].slot[j].value = value;
          mfence();
          buckets[i][s_idx].slot[j].key = key;
	  buckets[i][s_idx].token[j] = 1;
	  clflush((char*)&buckets[i][s_idx], sizeof(Node));
	  level_item_num[i]++;
          return;
        }
      }
    }
    f_idx = F_IDX(f_hash, addr_capacity / 2);
    s_idx = S_IDX(s_hash, addr_capacity / 2);
  }

  f_idx = F_IDX(f_hash, addr_capacity);
  s_idx = S_IDX(s_hash, addr_capacity);

  int empty_loc;
  auto lock = 0;
  if (CAS(&resizing_lock, &lock, 1)) {
    for(i=0; i<2; i++){
      if(!try_movement(f_idx, i, key, value)){
        resizing_lock = 0;
        return;
      }
      if(!try_movement(s_idx, i, key, value)){
        resizing_lock = 0;
        return;
      }
      f_idx = F_IDX(f_hash, addr_capacity / 2);
      s_idx = S_IDX(s_hash, addr_capacity / 2);
    }

    if(resize_num>0){
      {
        std::unique_lock<std::shared_mutex> lock(mutex[f_idx/locksize]);
#ifdef TIME
	cuck_timer.Start();
#endif
        empty_loc = b2t_movement(f_idx);
#ifdef TIME
	cuck_timer.Stop();
	displacement += cuck_timer.GetSeconds();
#endif
        if(empty_loc != -1){
          buckets[1][f_idx].slot[empty_loc].value = value;
          mfence();
          buckets[1][f_idx].slot[empty_loc].key = key;
	  buckets[1][f_idx].token[empty_loc] = 1;
	  clflush((char*)&buckets[1][f_idx], sizeof(Node));
	  level_item_num[1]++;
          resizing_lock = 0;
          return;
        }
      }
      {
        std::unique_lock<std::shared_mutex> lock(mutex[s_idx/locksize]);
#ifdef TIME
	cuck_timer.Start();
#endif
        empty_loc = b2t_movement(s_idx);
#ifdef TIME
	cuck_timer.Stop();
	displacement += cuck_timer.GetSeconds();
#endif
        if(empty_loc != -1){
          buckets[1][s_idx].slot[empty_loc].value = value;
          mfence();
          buckets[1][s_idx].slot[empty_loc].key = key;
	  buckets[1][s_idx].token[empty_loc] = 1;
	  clflush((char*)&buckets[1][s_idx], sizeof(Node));
	  level_item_num[1]++;
          resizing_lock = 0;
          return;
        }
      }
    }
    timer.Start();
    resize();
    timer.Stop();
    breakdown += timer.GetSeconds();
    resizing_lock = 0;
  }
  goto RETRY;
}

bool LevelHashing::InsertOnly(Key_t& key, Value_t value) {
}

void LevelHashing::resize(void) {
  std::unique_lock<std::shared_mutex> *lock[nlocks];
  for(int i=0;i<nlocks;i++){
    lock[i] = new std::unique_lock<std::shared_mutex>(mutex[i]);
  }
  std::shared_mutex* old_mutex = mutex;

  int prev_nlocks = nlocks;
  nlocks = nlocks + 2*addr_capacity/locksize+1;
  mutex= new std::shared_mutex[nlocks];

  size_t new_addr_capacity = pow(2, levels + 1);
  interim_level_buckets = new Node[new_addr_capacity];
  if(!interim_level_buckets){
	  perror("The expanding fails");
  }
  clflush((char*)&interim_level_buckets, sizeof(Node));

  uint64_t new_level_item_num = 0;
  uint64_t old_idx;
  for (old_idx = 0; old_idx < pow(2, levels - 1); old_idx ++) {
    uint64_t i, j;
    for(i = 0; i < ASSOC_NUM; i ++){
      if (buckets[1][old_idx].token[i] == 1)
      {
        Key_t key = buckets[1][old_idx].slot[i].key;
        Value_t value = buckets[1][old_idx].slot[i].value;

        uint32_t f_idx = F_IDX(F_HASH(key), new_addr_capacity);
        uint32_t s_idx = S_IDX(S_HASH(key), new_addr_capacity);

        uint8_t insertSuccess = 0;
        for(j = 0; j < ASSOC_NUM; j ++){
          if (interim_level_buckets[f_idx].token[j] == 0)
          {
            interim_level_buckets[f_idx].slot[j].value = value;
#ifndef BATCH
            mfence();
#endif
            interim_level_buckets[f_idx].slot[j].key = key;
	    interim_level_buckets[f_idx].token[j] = 1;
#ifndef BATCH
	    clflush((char*)&interim_level_buckets[f_idx], sizeof(Node));
#endif
            insertSuccess = 1;
	    new_level_item_num++;
            break;
          }
          else if (interim_level_buckets[s_idx].token[j] == 0)
          {
            interim_level_buckets[s_idx].slot[j].value = value;
#ifndef BATCH
            mfence();
#endif
            interim_level_buckets[s_idx].slot[j].key = key;
	    interim_level_buckets[s_idx].token[j] = 1;
#ifndef BATCH
	    clflush((char*)&interim_level_buckets[s_idx], sizeof(Node));
#endif
            insertSuccess = 1;
	    new_level_item_num++;
            break;
          }
        }

	buckets[1][old_idx].token[i] = 0;
#ifndef BATCH
	clflush((char*)&buckets[1][old_idx].token[i], sizeof(uint8_t));
#endif
      }
    }
  }

#ifdef BATCH
  clflush((char*)&buckets[1][0],sizeof(Node)*pow(2,levels-1));
  clflush((char*)&interim_level_buckets[0], sizeof(Node)*new_addr_capacity);
#endif


  levels++;
  resize_num++;

  delete [] buckets[1];
  buckets[1] = buckets[0];
  buckets[0] = interim_level_buckets;
  interim_level_buckets = NULL;

  level_item_num[1] = level_item_num[0];
  level_item_num[0] = new_level_item_num;

  addr_capacity = new_addr_capacity;
  total_capacity = pow(2, levels) + pow(2, levels - 1);

  for(int i=0;i<prev_nlocks;i++){
    delete lock[i];
  }
  delete[] old_mutex;
}

uint8_t LevelHashing::try_movement(uint64_t idx, uint64_t level_num, Key_t& key, Value_t value) {
#ifdef TIME
cuck_timer.Start();
#endif
  uint64_t i, j, jdx;
  {
    std::unique_lock<std::shared_mutex> *lock[2];
    lock[0] = new std::unique_lock<std::shared_mutex>(mutex[idx/locksize]);
    for(i=0; i<ASSOC_NUM; i++){
      Key_t m_key = buckets[level_num][idx].slot[i].key;
      Value_t m_value = buckets[level_num][idx].slot[i].value;
      uint64_t f_hash = F_HASH(m_key);
      uint64_t s_hash = S_HASH(m_key);
      uint64_t f_idx = F_IDX(f_hash, addr_capacity/(1+level_num));
      uint64_t s_idx = S_IDX(s_hash, addr_capacity/(1+level_num));

      if(f_idx == idx) jdx = s_idx;
      else jdx = f_idx;

      if((jdx/locksize)!=(idx/locksize)){
        lock[1] = new std::unique_lock<std::shared_mutex>(mutex[jdx/locksize]);
      }

      for(j=0; j<ASSOC_NUM; j++){
	  if(buckets[level_num][jdx].token[j] == 0){
          buckets[level_num][jdx].slot[j].value = m_value;
          mfence();
          buckets[level_num][jdx].slot[j].key = m_key;
	  buckets[level_num][jdx].token[j] = 1;
	  clflush((char*)&buckets[level_num][jdx], sizeof(Node));
	  buckets[level_num][idx].token[i] = 0;
	  clflush((char*)&buckets[level_num][idx].token[i], sizeof(uint8_t));

	  buckets[level_num][idx].slot[i].value = value;
          mfence();
          buckets[level_num][idx].slot[i].key = key;
	  buckets[level_num][idx].token[i] = 1;
	  clflush((char*)&buckets[level_num][idx], sizeof(Node));
	  level_item_num[level_num]++;

          if((jdx/locksize) != (idx/locksize)) delete lock[1];
          delete lock[0];
#ifdef TIME
	  cuck_timer.Stop();
	  displacement += cuck_timer.GetSeconds();
#endif
          return 0;
        }
      }
      if((jdx/locksize) != (idx/locksize)) delete lock[1];
    }
    delete lock[0];
  }
#ifdef TIME
  cuck_timer.Stop();
  displacement += cuck_timer.GetSeconds();
#endif
  return 1;
}



int LevelHashing::b2t_movement(uint64_t idx){
  Key_t key;
  Value_t value;
  uint64_t s_hash, f_hash;
  uint64_t s_idx, f_idx;
  uint64_t i, j;

  std::unique_lock<shared_mutex> *lock;
  for(i=0; i<ASSOC_NUM; i++){
    key = buckets[1][idx].slot[i].key;
    value = buckets[1][idx].slot[i].value;
    f_hash = F_HASH(key);
    s_hash = S_HASH(key);
    f_idx = F_IDX(f_hash, addr_capacity);
    s_idx = S_IDX(s_hash, addr_capacity);

    for(j=0; j<ASSOC_NUM; j++){
      if((idx/locksize) != (f_idx/locksize))
        lock = new std::unique_lock<std::shared_mutex>(mutex[f_idx/locksize]);
      if(buckets[0][f_idx].token[j] == 0){
        buckets[0][f_idx].slot[j].value = value;
        mfence();
        buckets[0][f_idx].slot[j].key = key;
	buckets[0][f_idx].token[j] = 1;
	clflush((char*)&buckets[0][f_idx], sizeof(Node));
	buckets[1][idx].token[i] = 0;
	clflush((char*)&buckets[1][idx].token[i], sizeof(uint8_t));
	level_item_num[0]++;
	level_item_num[1]--;

        if((idx/locksize) != (f_idx/locksize)) delete lock;
        return i;
      }
      if((idx/locksize)!=(f_idx/locksize)) delete lock;
      if((idx/locksize)!=(s_idx/locksize))
        lock = new std::unique_lock<std::shared_mutex>(mutex[s_idx/locksize]);

      if(buckets[0][s_idx].token[j] == 0){
        buckets[0][s_idx].slot[j].value = value;
        mfence();
        buckets[0][s_idx].slot[j].key = key;
	buckets[0][s_idx].token[j] = 1;
	clflush((char*)&buckets[0][s_idx], sizeof(Node));
	buckets[1][idx].token[i] = 0;
	clflush((char*)&buckets[0][s_idx].token[j], sizeof(uint8_t));

	level_item_num[0]++;
	level_item_num[1]--;

        if((idx/locksize) != (s_idx/locksize)) delete lock;
        return i;
      }
      if((idx/locksize)!=(s_idx/locksize)) delete lock;
    }
  }
  return -1;
}





Value_t LevelHashing::Get(Key_t& key) {
  uint64_t f_hash = F_HASH(key);
  uint64_t s_hash = S_HASH(key);
  uint32_t f_idx = F_IDX(f_hash, addr_capacity);
  uint32_t s_idx = S_IDX(s_hash, addr_capacity);
  int i = 0, j;

  for(i = 0; i < 2; i ++){
    {
      std::shared_lock<std::shared_mutex> lock(mutex[f_idx/locksize]);
      for(j = 0; j < ASSOC_NUM; j ++){
        if (buckets[i][f_idx].token[j] == 1 && buckets[i][f_idx].slot[j].key == key)
        {
          return buckets[i][f_idx].slot[j].value;
        }
      }
    }
    {
      std::shared_lock<std::shared_mutex> lock(mutex[s_idx/locksize]);
      for(j = 0; j < ASSOC_NUM; j ++){
        if (buckets[i][s_idx].token[j] == 1 && buckets[i][s_idx].slot[j].key == key)
        {
          return buckets[i][s_idx].slot[j].value;
        }
      }
    }
    f_idx = F_IDX(f_hash, addr_capacity/2);
    s_idx = S_IDX(s_hash, addr_capacity/2);
  }

  return NONE;
}

bool LevelHashing::Delete(Key_t& key) {
  return false;
}

double LevelHashing::Utilization(void) {
  size_t sum = 0;
  for (unsigned i = 0; i < addr_capacity; ++i) {
    for(unsigned j = 0; j < ASSOC_NUM; ++j) {
      if (buckets[0][i].slot[j].key != INVALID) {
        sum++;
      }
    }
  }
  for (unsigned i = 0; i < addr_capacity/2; ++i) {
    for(unsigned j = 0; j < ASSOC_NUM; ++j) {
      if (buckets[1][i].slot[j].key != INVALID) {
        sum++;
      }
    }
  }
  return ((double)(sum)/(double)(total_capacity*ASSOC_NUM)*100);
}
