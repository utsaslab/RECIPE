
#define BTREE_SLOWER_LAYOUT

#include <iostream>
#include "indexkey.h"
#include "ARTOLC/Tree.h"
#ifndef BTREE_SLOWER_LAYOUT
#include "BTreeOLC/BTreeOLC.h"
#else
#include "BTreeOLC/BTreeOLC_child_layout.h"
#endif
#include "BwTree/bwtree.h"
#include <byteswap.h>

#include "masstree/mtIndexAPI.hh"
#include "btree-rtm/btree.h"

#include "./nohotspot-skiplist/intset.h"
#include "./nohotspot-skiplist/background.h"
#include "./nohotspot-skiplist/nohotspot_ops.h"


#ifndef _INDEX_H
#define _INDEX_H

using namespace wangziqi2013;
using namespace bwtree;

template<typename KeyType, class KeyComparator>
class Index
{
 public:
  virtual bool insert(KeyType key, uint64_t value, threadinfo *ti) = 0;

  virtual uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) = 0;

  virtual uint64_t find_bwtree_fast(KeyType key, std::vector<uint64_t> *v) {};

  // Used for bwtree only
  virtual bool insert_bwtree_fast(KeyType key, uint64_t value) {};

  virtual bool upsert(KeyType key, uint64_t value, threadinfo *ti) = 0;

  virtual uint64_t scan(KeyType key, int range, threadinfo *ti) = 0;

  virtual int64_t getMemory() const = 0;

  // This initializes the thread pool
  virtual void UpdateThreadLocal(size_t thread_num) = 0;
  virtual void AssignGCID(size_t thread_id) = 0;
  virtual void UnregisterThread(size_t thread_id) = 0;
  
  // After insert phase perform this action
  // By default it is empty
  // This will be called in the main thread
  virtual void AfterLoadCallback() {}
  
  // This is called after threads finish but before the thread local are
  // destroied by the thread manager
  virtual void CollectStatisticalCounter(int) {}
  virtual size_t GetIndexSize() { return 0UL; }

  // Destructor must also be virtual
  virtual ~Index() {}
};

/////////////////////////////////////////////////////////////////////
// Skiplist
/////////////////////////////////////////////////////////////////////

extern thread_local long skiplist_steps;
extern std::atomic<long> skiplist_total_steps;

template<typename KeyType, class KeyComparator>
class BTreeRTMIndex : public Index<KeyType, KeyComparator>
{
 public:
  ~BTreeRTMIndex() {
    bt_free(tree);
  }

  void UpdateThreadLocal(size_t thread_num) {}
  void AssignGCID(size_t thread_id) {}
  void UnregisterThread(size_t thread_id) {}

  bool insert(KeyType key, uint64_t value, threadinfo *ti) {
    bt_insert(tree, (uint64_t)key, value);
    return true;
  }

  uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) {
    uint64_t result;
    int success;
    result = bt_find(tree, key, &success);
    v->clear();
    v->push_back(result);
    return 0;
  }

  bool upsert(KeyType key, uint64_t value, threadinfo *ti) {
    bt_upsert(tree, (uint64_t)key, value);
    return true;
  }

  void incKey(uint64_t& key) { key++; };
  void incKey(GenericKey<31>& key) { key.data[strlen(key.data)-1]++; };

  uint64_t scan(KeyType key, int range, threadinfo *ti) {
    return 0;
  }

  int64_t getMemory() const {
    return 0;
  }

  void merge() {}

  BTreeRTMIndex(uint64_t kt) {
    tree = bt_init(bt_intcmp);
  }

 private:
  btree_t *tree;
};

template<typename KeyType, class KeyComparator>
class SkipListIndex : public Index<KeyType, KeyComparator> {
 public:
  set_t *set;
 public:
  /*
   * Constructor - Allocate memory and initialize the skip list index
   */
  SkipListIndex(uint64_t key_type) {
    (void)key_type;
    skiplist_total_steps.store(0L);

    ptst_subsystem_init();
    gc_subsystem_init();
    set_subsystem_init();
    set = set_new(1);

    return;
  }

  /*
   * Destructor - We need to stop the background thread and also to 
   *              free the index object
   */
  ~SkipListIndex() {
    // Stop the background thread
    bg_stop();
    gc_subsystem_destroy();
    // Delete index
    set_delete(set);
    return;
  }

  bool insert(KeyType key, uint64_t value, threadinfo *ti) {
    sl_insert(&skiplist_steps, set, key, &value);
    (void)ti;
    return true;
  }

  uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) {
    // Note that skiplist only supports membership check
    // This is fine, because it still traverses to the location that
    // the key is stored. We just call push_back() with an arbitraty
    // number to compensate for lacking a value
    sl_contains(&skiplist_steps, set, key);
    (void)v; (void)ti;
    v->clear();
    v->push_back(0);
    return 0UL;
  }

  bool upsert(KeyType key, uint64_t value, threadinfo *ti) {
    // Upsert is implemented as two operations. In practice if we change
    // the internals of the skiplist, we can make it one atomic step
    sl_delete(&skiplist_steps, set, key);
    sl_insert(&skiplist_steps, set, key, &value);
    (void)ti;
    return true;
  }

  uint64_t scan(KeyType key, int range, threadinfo *ti) {
    sl_scan(&skiplist_steps, set, key, range);
    (void)ti;
    return 0UL;
  }

  int64_t getMemory() const {
    return 0L;
  }
  
  // Returns the size of the skiplist
  size_t GetIndexSize() {
    return (size_t)set_size(set, 1);
  }
  
  // Not actually used
  void UpdateThreadLocal(size_t thread_num) { (void)thread_num; }

  // Before thread starts we set the steps to 0
  void AssignGCID(size_t thread_id) { 
    (void)thread_id; 
    skiplist_steps = 0L; 
  }

  // Before thread exits we aggregate the steps into the global counter
  void UnregisterThread(size_t thread_id) { 
    (void)thread_id; 
    skiplist_total_steps.fetch_add(skiplist_steps);
    return;
  }
};

/////////////////////////////////////////////////////////////////////
// ARTOLC
/////////////////////////////////////////////////////////////////////

template<typename KeyType, class KeyComparator>
class ArtOLCIndex : public Index<KeyType, KeyComparator>
{
 public:

  ~ArtOLCIndex() {
    delete idx;
  }

  void UpdateThreadLocal(size_t thread_num) {}
  void AssignGCID(size_t thread_id) {}
  void UnregisterThread(size_t thread_id) {}

  void setKey(Key& k, uint64_t key) { k.setInt(key); }
  void setKey(Key& k, GenericKey<31> key) { k.set(key.data,31); }

  bool insert(KeyType key, uint64_t value, threadinfo *ti) {
    auto t = idx->getThreadInfo();
    Key k; setKey(k, key);
    idx->insert(k, value, t);
    return true;
  }

  uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) {
    auto t = idx->getThreadInfo();
    Key k; setKey(k, key);
    uint64_t result=idx->lookup(k, t);
    v->clear();
    v->push_back(result);
    return 0;
  }

  bool upsert(KeyType key, uint64_t value, threadinfo *ti) {
    auto t = idx->getThreadInfo();
    Key k; setKey(k, key);
    idx->insert(k, value, t);
  }

  uint64_t scan(KeyType key, int range, threadinfo *ti) {
    auto t = idx->getThreadInfo();
    Key startKey; setKey(startKey, key);

    TID results[range];
    size_t resultCount;
    Key continueKey;
    idx->lookupRange(startKey, maxKey, continueKey, results, range, resultCount, t);

    return resultCount;
  }

  int64_t getMemory() const {
    return 0;
  }

  void merge() {
  }

  ArtOLCIndex(uint64_t kt) {
    if (sizeof(KeyType)==8) {
      idx = new ART_OLC::Tree([](TID tid, Key &key) { key.setInt(*reinterpret_cast<uint64_t*>(tid)); });
      maxKey.setInt(~0ull);
    } else {
      idx = new ART_OLC::Tree([](TID tid, Key &key) { key.set(reinterpret_cast<char*>(tid),31); });
      uint8_t m[] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		     0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		     0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		     0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
      maxKey.set((char*)m,31);
    }
  }

 private:
  Key maxKey;
  ART_OLC::Tree *idx;
};

template<typename KeyType, class KeyComparator>
class BTreeOLCIndex : public Index<KeyType, KeyComparator>
{
 public:

  ~BTreeOLCIndex() {
  }

  void UpdateThreadLocal(size_t thread_num) {}
  void AssignGCID(size_t thread_id) {}
  void UnregisterThread(size_t thread_id) {}

  bool insert(KeyType key, uint64_t value, threadinfo *ti) {
    idx.insert(key, value);
    return true;
  }

  uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) {
    uint64_t result;
    idx.lookup(key,result);
    v->clear();
    v->push_back(result);
    return 0;
  }

  bool upsert(KeyType key, uint64_t value, threadinfo *ti) {
    idx.insert(key, value);
    return true;
  }

  void incKey(uint64_t& key) { key++; };
  void incKey(GenericKey<31>& key) { key.data[strlen(key.data)-1]++; };

  uint64_t scan(KeyType key, int range, threadinfo *ti) {
    uint64_t results[range];
    uint64_t count = idx.scan(key, range, results);
    if (count==0)
       return 0;

    while (count < range) {
      KeyType nextKey = *reinterpret_cast<KeyType*>(results[count-1]);
      incKey(nextKey); // hack: this only works for fixed-size keys

      uint64_t nextCount = idx.scan(nextKey, range - count, results + count);
      if (nextCount==0)
        break; // no more entries
      count += nextCount;
    }
    return count;
  }

  int64_t getMemory() const {
    return 0;
  }

  void merge() {}

  BTreeOLCIndex(uint64_t kt) {}

 private:

  btreeolc::BTree<KeyType,uint64_t> idx;
};

template<typename KeyType, 
         typename KeyComparator,
         typename KeyEqualityChecker=std::equal_to<KeyType>,
         typename KeyHashFunc=std::hash<KeyType>>
class BwTreeIndex : public Index<KeyType, KeyComparator>
{
 public:
  using index_type = BwTree<KeyType, uint64_t, KeyComparator, KeyEqualityChecker, KeyHashFunc>;
  using BaseNode = typename index_type::BaseNode;

  BwTreeIndex(uint64_t kt) {
    index_p = new index_type{};
    assert(index_p != nullptr);
    (void)kt;

    // Print the size of preallocated storage
    fprintf(stderr, "Inner prealloc size = %lu; Leaf prealloc size = %lu\n",
            index_type::INNER_PREALLOCATION_SIZE,
            index_type::LEAF_PREALLOCATION_SIZE);

    return;
  }

  ~BwTreeIndex() {
    delete index_p;
    return;
  }

#ifdef BWTREE_COLLECT_STATISTICS
  void CollectStatisticalCounter(int thread_num) {
    static constexpr int counter_count = \
      BwTreeBase::GCMetaData::CounterType::COUNTER_COUNT;
    int counters[counter_count];
    
    // Aggregate on the array of counters
    memset(counters, 0x00, sizeof(counters));
    
    for(int i = 0;i < thread_num;i++) {
      for(int j = 0;j < counter_count;j++) {
        counters[j] += index_p->GetGCMetaData(i)->counters[j];
      }
    }

    fprintf(stderr, "Statistical counters:\n");
    for(int j = 0;j < counter_count;j++) {
      fprintf(stderr,
              "    counter %s = %d\n",
              BwTreeBase::GCMetaData::COUNTER_NAME_LIST[j],
              counters[j]);
    }
    
    return;
  }
#endif
  
  void AfterLoadCallback() {
    int inner_depth_total = 0,
        leaf_depth_total = 0,
        inner_node_total = 0,
        leaf_node_total = 0;
    int inner_size_total = 0, leaf_size_total = 0;
    size_t inner_alloc_total = 0, inner_used_total = 0;
    size_t leaf_alloc_total = 0, leaf_used_total = 0;
   
    uint64_t index_root_id = index_p->root_id.load();
    fprintf(stderr, "BwTree - Start consolidating delta chains...\n");
    int ret = index_p->DebugConsolidateAllRecursive(
      index_root_id,
      &inner_depth_total,
      &leaf_depth_total,
      &inner_node_total,
      &leaf_node_total,
      &inner_size_total,
      &leaf_size_total,
      &inner_alloc_total,
      &inner_used_total,
      &leaf_alloc_total,
      &leaf_used_total);
    fprintf(stderr, "BwTree - Finished consolidating %d delta chains\n", ret);
   
    fprintf(stderr,
            "    Inner Avg. Depth: %f (%d / %d)\n",
            (double)inner_depth_total / (double)inner_node_total,
            inner_depth_total,
            inner_node_total);
    fprintf(stderr,
            "    Inner Avg. Size: %f (%d / %d)\n",
            (double)inner_size_total / (double)inner_node_total,
            inner_size_total,
            inner_node_total);
    fprintf(stderr,
            "    Leaf Avg. Depth: %f (%d / %d)\n",
            (double)leaf_depth_total / (double)leaf_node_total,
            leaf_depth_total,
            leaf_node_total);
    fprintf(stderr,
            "    Leaf Avg. Size: %f (%d / %d)\n",
            (double)leaf_size_total / (double)leaf_node_total,
            leaf_size_total,
            leaf_node_total);
    
    fprintf(stderr,
            "Inner Alloc. Util: %f (%lu / %lu)\n",
            (double)inner_used_total / (double)inner_alloc_total,
            inner_used_total,
            inner_alloc_total);
    
    fprintf(stderr,
            "Leaf Alloc. Util: %f (%lu / %lu)\n",
            (double)leaf_used_total / (double)leaf_alloc_total,
            leaf_used_total,
            leaf_alloc_total);

// Only do thid after the consolidation, because the mapping will change
// during the consolidation

#ifndef BWTREE_USE_MAPPING_TABLE
    fprintf(stderr, "Replacing all NodeIDs to BaseNode *\n");
    BaseNode *node_p = (BaseNode *)index_p->GetNode(index_p->root_id.load());
    index_p->root_id = reinterpret_cast<NodeID>(node_p);
    index_p->DebugReplaceNodeIDRecursive(node_p);
#endif
    return;
  }
  
  void UpdateThreadLocal(size_t thread_num) { 
    index_p->UpdateThreadLocal(thread_num); 
  }
  
  void AssignGCID(size_t thread_id) {
    index_p->AssignGCID(thread_id); 
  }
  
  void UnregisterThread(size_t thread_id) {
    index_p->UnregisterThread(thread_id); 
  }

  bool insert(KeyType key, uint64_t value, threadinfo *) {
    return index_p->Insert(key, value);
  }

  uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *) {
    index_p->GetValue(key, *v);

    return 0UL;
  }

#ifndef BWTREE_USE_MAPPING_TABLE
  uint64_t find_bwtree_fast(KeyType key, std::vector<uint64_t> *v) {
    index_p->GetValueNoMappingTable(key, *v);

    return 0UL;
  }
#endif

#ifndef BWTREE_USE_DELTA_UPDATE
  bool insert_bwtree_fast(KeyType key, uint64_t value) {
    index_p->InsertInPlace(key, value);
    return true;
  }
#endif

  bool upsert(KeyType key, uint64_t value, threadinfo *) {
    //index_p->Delete(key, value);
    //index_p->Insert(key, value);
    
    index_p->Upsert(key, value);

    return true;
  }

  uint64_t scan(KeyType key, int range, threadinfo *) {
    auto it = index_p->Begin(key);

    if(it.IsEnd() == true) {
      std::cout << "Iterator reaches the end\n";
      return 0UL;
    }

    uint64_t sum = 0;
    for(int i = 0;i < range;i++) {
      if(it.IsEnd() == true) {
        return sum;
      }

      sum += it->second;
      it++;
    }

    return sum;
  }

  int64_t getMemory() const {
    return 0L;
  }

 private:
  BwTree<KeyType, uint64_t, KeyComparator, KeyEqualityChecker, KeyHashFunc> *index_p;

};


template<typename KeyType, class KeyComparator>
class MassTreeIndex : public Index<KeyType, KeyComparator>
{
 public:

  typedef mt_index<Masstree::default_table> MapType;

  ~MassTreeIndex() {
    delete idx;
  }

  inline void swap_endian(uint64_t &i) {
    // Note that masstree internally treat input as big-endian
    // integer values, so we need to swap here
    // This should be just one instruction
    i = __bswap_64(i);
  }

  inline void swap_endian(GenericKey<31> &) {
    return;
  }
  
  void UpdateThreadLocal(size_t thread_num) {}
  void AssignGCID(size_t thread_id) {}
  void UnregisterThread(size_t thread_id) {}

  bool insert(KeyType key, uint64_t value, threadinfo *ti) {
    swap_endian(key);
    idx->put((const char*)&key, sizeof(KeyType), (const char*)&value, 8, ti);

    return true;
  }

  uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) {
    Str val;
    swap_endian(key);
    idx->get((const char*)&key, sizeof(KeyType), val, ti);

    v->clear();
    if (val.s)
      v->push_back(*(uint64_t *)val.s);

    return 0;
  }

  bool upsert(KeyType key, uint64_t value, threadinfo *ti) {
    swap_endian(key);
    idx->put((const char*)&key, sizeof(KeyType), (const char*)&value, 8, ti);
    return true;
  }

  // uint64_t scan(KeyType key, int range, threadinfo *ti) {
  //   Str val;

  //   swap_endian(key);
  //   int key_len = sizeof(KeyType);

  //   for (int i = 0; i < range; i++) {
  //     idx->dynamic_get_next(val, (char *)&key, &key_len, ti);
  //   }

  //   return 0UL;
  // }

  uint64_t scan(KeyType key, int range, threadinfo *ti) {
    Str results[range];

    swap_endian(key);
    int key_len = sizeof(KeyType);

    int resultCount = idx->get_next_n(results, (char *)&key, &key_len, range, ti);
    //printf("scan: requested: %d, actual: %d\n", range, resultCount);
    return resultCount;
  }

  int64_t getMemory() const {
    return 0;
  }

  MassTreeIndex(uint64_t kt) {
    idx = new MapType{};

    threadinfo *main_ti = threadinfo::make(threadinfo::TI_MAIN, -1);
    idx->setup(main_ti);

    return;
  }

  MapType *idx;
};

#endif


