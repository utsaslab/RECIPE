
#include "./pcm/pcm-memory.cpp"
#include "./pcm/pcm-numa.cpp"
#include "./papi_util.cpp"

#include "microbench.h"

#include <cstring>
#include <cctype>
#include <atomic>

thread_local long skiplist_steps = 0;
std::atomic<long> skiplist_total_steps;

//#define USE_TBB

#ifdef USE_TBB
#include "tbb/tbb.h"
#endif

// Enable this if you need pre-allocation utilization
//#define BWTREE_CONSOLIDATE_AFTER_INSERT

#ifdef BWTREE_CONSOLIDATE_AFTER_INSERT
  #ifdef USE_TBB
  #warning "Could not use TBB and BwTree consolidate together"
  #endif
#endif

#ifdef BWTREE_COLLECT_STATISTICS
  #ifdef USE_TBB
  #warning "Could not use TBB and BwTree statistics together"
  #endif
#endif

// Whether insert interleaves
//#define INTERLEAVED_INSERT

// Whether read operatoin miss will be counted
//#define COUNT_READ_MISS

typedef uint64_t keytype;
typedef std::less<uint64_t> keycomp;

static const uint64_t key_type=0;
static const uint64_t value_type=1; // 0 = random pointers, 1 = pointers to keys

extern bool hyperthreading;

// This is the flag for whather to measure memory bandwidth
static bool memory_bandwidth = false;
// Whether to measure NUMA Throughput
static bool numa = false;
// Whether we only perform insert
static bool insert_only = false;

// We could set an upper bound of the number of loaded keys
static int64_t max_init_key = -1;

#include "util.h"

/*
 * MemUsage() - Reads memory usage from /proc file system
 */
size_t MemUsage() {
  FILE *fp = fopen("/proc/self/statm", "r");
  if(fp == nullptr) {
    fprintf(stderr, "Could not open /proc/self/statm to read memory usage\n");
    exit(1);
  }

  unsigned long unused;
  unsigned long rss;
  if (fscanf(fp, "%ld %ld %ld %ld %ld %ld %ld", &unused, &rss, &unused, &unused, &unused, &unused, &unused) != 7) {
    perror("");
    exit(1);
  }

  (void)unused;
  fclose(fp);

  return rss * (4096 / 1024); // in KiB (not kB)
}

//==============================================================
// LOAD
//==============================================================
inline void load(int wl,
                 int kt,
                 int index_type,
                 std::vector<keytype> &init_keys,
                 std::vector<keytype> &keys,
                 std::vector<uint64_t> &values,
                 std::vector<int> &ranges,
                 std::vector<int> &ops) {
  std::string init_file;
  std::string txn_file;

  if (kt == RAND_KEY && wl == WORKLOAD_A) {
    init_file = "workloads/loada_zipf_int_100M.dat";
    txn_file = "workloads/txnsa_zipf_int_100M.dat";
  } else if (kt == RAND_KEY && wl == WORKLOAD_C) {
    init_file = "workloads/loadc_zipf_int_100M.dat";
    txn_file = "workloads/txnsc_zipf_int_100M.dat";
  } else if (kt == RAND_KEY && wl == WORKLOAD_E) {
    init_file = "workloads/loade_zipf_int_100M.dat";
    txn_file = "workloads/txnse_zipf_int_100M.dat";
  } else if (kt == MONO_KEY && wl == WORKLOAD_A) {
    init_file = "workloads/mono_inc_loada_zipf_int_100M.dat";
    txn_file = "workloads/mono_inc_txnsa_zipf_int_100M.dat";
  } else if (kt == MONO_KEY && wl == WORKLOAD_C) {
    init_file = "workloads/mono_inc_loadc_zipf_int_100M.dat";
    txn_file = "workloads/mono_inc_txnsc_zipf_int_100M.dat";
  } else if (kt == MONO_KEY && wl == WORKLOAD_E) {
    init_file = "workloads/mono_inc_loade_zipf_int_100M.dat";
    txn_file = "workloads/mono_inc_txnse_zipf_int_100M.dat";
  } else {
    fprintf(stderr, "Unknown workload type or key type: %d, %d\n", wl, kt);
    exit(1);
  }

  std::ifstream infile_load(init_file);

  std::string op;
  keytype key;
  int range;

  std::string insert("INSERT");
  std::string read("READ");
  std::string update("UPDATE");
  std::string scan("SCAN");

  int count = 0;
  while ((count < INIT_LIMIT) && infile_load.good()) {
    infile_load >> op >> key;
    if (op.compare(insert) != 0) {
      std::cout << "READING LOAD FILE FAIL!\n";
      return;
    }
    init_keys.push_back(key);
    count++;

    // If we have reached the max init key limit then just break
    if(max_init_key > 0 && count == max_init_key) {
      break;
    }
  }

  fprintf(stderr, "Loaded %d keys\n", count);

  count = 0;
  uint64_t value = 0;
  void *base_ptr = malloc(8);
  uint64_t base = (uint64_t)(base_ptr);
  free(base_ptr);

  keytype *init_keys_data = init_keys.data();

  if (value_type == 0) {
    while (count < INIT_LIMIT) {
      value = base + rand();
      values.push_back(value);
      count++;
    }
  }
  else {
    while (count < INIT_LIMIT) {
      values.push_back(reinterpret_cast<uint64_t>(init_keys_data+count));
      count++;
    }
  }

  // If we do not perform other transactions, we can skip txn file
  if(insert_only == true) {
    return;
  }

  // If we also execute transaction then open the
  // transacton file here
  std::ifstream infile_txn(txn_file);

  count = 0;
  while ((count < LIMIT) && infile_txn.good()) {
    infile_txn >> op >> key;
    if (op.compare(insert) == 0) {
      ops.push_back(OP_INSERT);
      keys.push_back(key);
      ranges.push_back(1);
    }
    else if (op.compare(read) == 0) {
      ops.push_back(OP_READ);
      keys.push_back(key);
    }
    else if (op.compare(update) == 0) {
      ops.push_back(OP_UPSERT);
      keys.push_back(key);
    }
    else if (op.compare(scan) == 0) {
      infile_txn >> range;
      ops.push_back(OP_SCAN);
      keys.push_back(key);
      ranges.push_back(range);
    }
    else {
      std::cout << "UNRECOGNIZED CMD!\n";
      return;
    }
    count++;
  }


  // Average and variation
  long avg = 0, var = 0;
  // If it is YSCB-E workload then we compute average and stdvar
  if(ranges.size() != 0) {
    for(int r : ranges) {
      avg += r;
    }

    avg /= (long)ranges.size();

    for(int r : ranges) {
      var += ((r - avg) * (r - avg));
    }

    var /= (long)ranges.size();

    fprintf(stderr, "YCSB-E scan Avg length: %ld; Variance: %ld\n",
            avg, var);
  }

}

//==============================================================
// EXEC
//==============================================================
inline void exec(int wl, 
                 int index_type, 
                 int num_thread,
                 std::vector<keytype> &init_keys, 
                 std::vector<keytype> &keys, 
                 std::vector<uint64_t> &values, 
                 std::vector<int> &ranges, 
                 std::vector<int> &ops) {

  Index<keytype, keycomp> *idx = getInstance<keytype, keycomp>(index_type, key_type);

  //WRITE ONLY TEST-----------------
  int count = (int)init_keys.size();
  fprintf(stderr, "Populating the index with %d keys using %d threads\n", count, num_thread);

#ifdef USE_TBB  
  tbb::task_scheduler_init init{num_thread};

  std::atomic<int> next_thread_id;
  next_thread_id.store(0);
  
  auto func = [idx, &init_keys, &values, &next_thread_id](const tbb::blocked_range<size_t>& r) {
    size_t start_index = r.begin();
    size_t end_index = r.end();
   
    threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);

    int thread_id = next_thread_id.fetch_add(1);
    idx->AssignGCID(thread_id);
    
    int gc_counter = 0;
    for(size_t i = start_index;i < end_index;i++) {
      idx->insert(init_keys[i], values[i], ti);
      gc_counter++;
      if(gc_counter % 4096 == 0) {
        ti->rcu_quiesce();
      }
    }

    ti->rcu_quiesce();
    idx->UnregisterThread(thread_id);
    
    return;
  };

  idx->UpdateThreadLocal(num_thread);
  tbb::parallel_for(tbb::blocked_range<size_t>(0, count), func);
  idx->UpdateThreadLocal(1);
#else

  auto func = [idx, &init_keys, num_thread, &values, index_type] \
              (uint64_t thread_id, bool) {
    size_t total_num_key = init_keys.size();
    size_t key_per_thread = total_num_key / num_thread;
    size_t start_index = key_per_thread * thread_id;
    size_t end_index = start_index + key_per_thread;
   
    threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);

    int gc_counter = 0;
#ifdef INTERLEAVED_INSERT
    for(size_t i = thread_id;i < total_num_key;i += num_thread) {
#else
    for(size_t i = start_index;i < end_index;i++) {
#endif
      if(index_type == TYPE_SKIPLIST) {
        idx->insert(init_keys[start_index + end_index - 1 - i], 
                    values[start_index + end_index - 1 - i], 
                    ti);
      } else {
#ifdef BWTREE_USE_DELTA_UPDATE
        idx->insert(init_keys[i], values[i], ti);
#else
        idx->insert_bwtree_fast(init_keys[i], values[i]);
#endif
      }
      gc_counter++;
      if(gc_counter % 4096 == 0) {
        ti->rcu_quiesce();
      }
    } 

    ti->rcu_quiesce();
    
    return;
  };
 
  if(memory_bandwidth == true) {
    PCM_memory::StartMemoryMonitor();
  }

  if(numa == true) {
    PCM_NUMA::StartNUMAMonitor();
  }
 
  double start_time = get_now(); 
  StartThreads(idx, num_thread, func, false);
  double end_time = get_now();

  if(index_type == TYPE_SKIPLIST) {
    fprintf(stderr, "SkipList size = %lu\n", idx->GetIndexSize());
    fprintf(stderr, "Skiplist avg. steps = %f\n", (double)skiplist_total_steps / (double)init_keys.size());
  }
 
  if(memory_bandwidth == true) {
    PCM_memory::EndMemoryMonitor();
  }

  if(numa == true) {
    PCM_NUMA::EndNUMAMonitor();
  }
#endif   
  
  double tput = count / (end_time - start_time) / 1000000; //Mops/sec

  std::cout << "\033[1;32m";
  std::cout << "insert " << tput << "\033[0m" << "\n";

  // Only execute consolidation if BwTree delta chain is used
#ifdef BWTREE_CONSOLIDATE_AFTER_INSERT
  fprintf(stderr, "Starting consolidating delta chain on each level\n");
  idx->AfterLoadCallback();
#endif

  // If the workload only executes load phase then we return here
  if(insert_only == true) {
    delete idx;
    return;
  }

  //READ/UPDATE/SCAN TEST----------------
  int txn_num = GetTxnCount(ops, index_type);
  uint64_t sum = 0;
  uint64_t s = 0;

  if(values.size() < keys.size()) {
    fprintf(stderr, "Values array too small\n");
    exit(1);
  }

  fprintf(stderr, "# of Txn: %d\n", txn_num);
  
  // This is used to count how many read misses we have found
  std::atomic<size_t> read_miss_counter{}, read_hit_counter{};
  read_miss_counter.store(0UL);
  read_hit_counter.store(0UL);

  auto func2 = [num_thread, 
                idx, 
                &read_miss_counter,
                &read_hit_counter,
                &keys,
                &values,
                &ranges,
                &ops](uint64_t thread_id, bool) {
    size_t total_num_op = ops.size();
    size_t op_per_thread = total_num_op / num_thread;
    size_t start_index = op_per_thread * thread_id;
    size_t end_index = start_index + op_per_thread;
   
    std::vector<uint64_t> v;
    v.reserve(10);
 
    threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);

    int counter = 0;
    for(size_t i = start_index;i < end_index;i++) {
      int op = ops[i];
      if (op == OP_INSERT) { //INSERT
        idx->insert(keys[i], values[i], ti);
      }
      else if (op == OP_READ) { //READ
        v.clear();

#ifdef BWTREE_USE_MAPPING_TABLE
        idx->find(keys[i], &v, ti);
#else
        idx->find_bwtree_fast(keys[i], &v);
#endif
        
        // If we count read misses then increment the 
        // counter here if the vetor is empty
#ifdef COUNT_READ_MISS
        if(v.size() == 0UL) {  
          read_miss_counter.fetch_add(1);
        } else {
          read_hit_counter.fetch_add(1);
        }
#endif
      }
      else if (op == OP_UPSERT) { //UPDATE
        idx->upsert(keys[i], reinterpret_cast<uint64_t>(&keys[i]), ti);
      }
      else if (op == OP_SCAN) { //SCAN
        idx->scan(keys[i], ranges[i], ti);
      }

      counter++;
      if(counter % 4096 == 0) {
        ti->rcu_quiesce();
      }
    }

    // Perform GC after all operations
    ti->rcu_quiesce();
    
    return;
  };

  if(memory_bandwidth == true) {
    PCM_memory::StartMemoryMonitor();
  }

  if(numa == true) {
    PCM_NUMA::StartNUMAMonitor();
  }

  start_time = get_now();  
  StartThreads(idx, num_thread, func2, false);
  end_time = get_now();

  if(memory_bandwidth == true) {
    PCM_memory::EndMemoryMonitor();
  }

  if(numa == true) {
    PCM_NUMA::EndNUMAMonitor();
  }

  // Print out how many reads have missed in the index (do not have a value)
#ifdef COUNT_READ_MISS
  fprintf(stderr, 
          "  Read misses: %lu; Read hits: %lu\n", 
          read_miss_counter.load(),
          read_hit_counter.load());
#endif

  tput = txn_num / (end_time - start_time) / 1000000; //Mops/sec

  std::cout << "sum = " << sum << "\n";
  std::cout << "\033[1;31m";

  if (wl == WORKLOAD_A) {  
    std::cout << "read/update " << (tput + (sum - sum));
  } else if (wl == WORKLOAD_C) {
    std::cout << "read " << (tput + (sum - sum));
  } else if (wl == WORKLOAD_E) {
    std::cout << "insert/scan " << (tput + (sum - sum));
  } else {
    fprintf(stderr, "Unknown workload type: %d\n", wl);
    exit(1);
  }

  std::cout << "\033[0m" << "\n";

  if(index_type == TYPE_SKIPLIST) {
    fprintf(stderr, "SkipList size = %lu\n", idx->GetIndexSize());
    fprintf(stderr, "Skiplist avg. steps = %f\n", (double)skiplist_total_steps / (double)init_keys.size());
  }

  delete idx;

  return;
}

/*
 * run_rdtsc_benchmark() - This function runs the RDTSC benchmark which is a high
 *                         contention insert-only benchmark
 *
 * Note that key num is the total key num
 */
void run_rdtsc_benchmark(int index_type, int thread_num, int key_num) {
  Index<keytype, keycomp> *idx = getInstance<keytype, keycomp>(index_type, key_type);

  auto func = [idx, thread_num, key_num](uint64_t thread_id, bool) {
    size_t key_per_thread = key_num / thread_num;

    threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);

    uint64_t *values = new uint64_t[key_per_thread];

    int gc_counter = 0;
    for(size_t i = 0;i < key_per_thread;i++) {
      // Note that RDTSC may return duplicated keys from different cores
      // to counter this we combine RDTSC with thread IDs to make it unique
      // The counter value on a single core is always unique, though
      uint64_t key = (Rdtsc() << 6) | thread_id;
      values[i] = key;
      //fprintf(stderr, "%lx\n", key);
      idx->insert(key, reinterpret_cast<uint64_t>(values + i), ti);
      gc_counter++;
      if(gc_counter % 4096 == 0) {
        ti->rcu_quiesce();
      }
    }

    ti->rcu_quiesce();

    delete [] values;

    return;
  };

  if(numa == true) {
    PCM_NUMA::StartNUMAMonitor();
  }

  double start_time = get_now();
  StartThreads(idx, thread_num, func, false);
  double end_time = get_now();

  if(numa == true) {
    PCM_NUMA::EndNUMAMonitor();
  }

  // Only execute consolidation if BwTree delta chain is used
#ifdef BWTREE_CONSOLIDATE_AFTER_INSERT
  idx->AfterLoadCallback();
#endif
  
  double tput = key_num * 1.0 / (end_time - start_time) / 1000000; //Mops/sec
  std::cout << "insert " << tput << "\n";

  return;
}

int main(int argc, char *argv[]) {

  if (argc < 5) {
    std::cout << "Usage:\n";
    std::cout << "1. workload type: a, c, e, none\n";
    std::cout << "   \"none\" type means we just load the file and exit. \n"
                 "This serves as the base line for microbenchamrks\n";
    std::cout << "2. key distribution: rand, mono\n";
    std::cout << "3. index type: bwtree skiplist masstree artolc btreeolc btreertm\n";
    std::cout << "4. number of threads (integer)\n";
    std::cout << "   --hyper: Whether to pin all threads on NUMA node 0\n";
    std::cout << "   --mem: Whether to monitor memory access\n";
    std::cout << "   --numa: Whether to monitor NUMA throughput\n";
    std::cout << "   --insert-only: Whether to only execute insert operations\n";
    
    return 1;
  }

  // Then read the workload type
  int wl;
  if (strcmp(argv[1], "a") == 0) {
    wl = WORKLOAD_A;
  } else if (strcmp(argv[1], "c") == 0) {
    wl = WORKLOAD_C;
  } else if (strcmp(argv[1], "e") == 0) {
    wl = WORKLOAD_E;
  } else {
    fprintf(stderr, "Unknown workload: %s\n", argv[1]);
    exit(1);
  }

  // Then read key type
  int kt;
  if (strcmp(argv[2], "rand") == 0) {
    kt = RAND_KEY;
  } else if (strcmp(argv[2], "mono") == 0) {
    kt = MONO_KEY;
  } else if (strcmp(argv[2], "rdtsc") == 0) {
    kt = RDTSC_KEY;
  } else {
    fprintf(stderr, "Unknown key type: %s\n", argv[2]);
    exit(1);
  }

  int index_type;
  if (strcmp(argv[3], "bwtree") == 0)
    index_type = TYPE_BWTREE;
  else if (strcmp(argv[3], "masstree") == 0)
    index_type = TYPE_MASSTREE;
  else if (strcmp(argv[3], "artolc") == 0)
    index_type = TYPE_ARTOLC;
  else if (strcmp(argv[3], "btreeolc") == 0)
    index_type = TYPE_BTREEOLC;
  else if (strcmp(argv[3], "skiplist") == 0)
    index_type = TYPE_SKIPLIST;
  else if (strcmp(argv[3], "btreertm") == 0)
    index_type = TYPE_BTREERTM;
  else if (strcmp(argv[3], "none") == 0)
    // This is a special type used for measuring base cost (i.e.
    // only loading the workload files but do not invoke the index)
    index_type = TYPE_NONE;
  else {
    fprintf(stderr, "Unknown index type: %d\n", index_type);
    exit(1);
  }

  // Then read number of threads using command line
  int num_thread = atoi(argv[4]);
  if(num_thread < 1 || num_thread > 40) {
    fprintf(stderr, "Do not support %d threads\n", num_thread);
    exit(1);
  } else {
    fprintf(stderr, "Number of threads: %d\n", num_thread);
  }

  // Then read all remianing arguments
  int repeat_counter = 1;
  char **argv_end = argv + argc;
  for(char **v = argv + 5;v != argv_end;v++) {
    if(strcmp(*v, "--hyper") == 0) {
      // Enable hyoerthreading for scheduling threads
      hyperthreading = true;
    } else if(strcmp(*v, "--mem") == 0) {
      // Enable memory bandwidth measurement
      memory_bandwidth = true;
    } else if(strcmp(*v, "--numa") == 0) {
      numa = true;
    } else if(strcmp(*v, "--insert-only") == 0) {
      insert_only = true;
    } else if(strcmp(*v, "--repeat") == 0) {
      // If we repeat, then exec() will be called for 5 times
      repeat_counter = 5;
    } else if(strcmp(*v, "--max-init-key") == 0) {
      max_init_key = atoll(*(v + 1));
      if(max_init_key <= 0) {
        fprintf(stderr, "Illegal maximum init keys: %ld\n", max_init_key);
        exit(1);
      }

      // Ignore the next argument
      v++;
    } else {
      fprintf(stderr, "Unknown switch: %s\n", *v);
      exit(1);
    }
  }

  if(max_init_key != -1) {
    fprintf(stderr, "Maximum init keys: %ld\n", max_init_key);
    fprintf(stderr, "  NOTE: Memory is not affected in this case\n");
  }

#ifdef COUNT_READ_MISS
  fprintf(stderr, "  Counting read misses\n");
#endif

#ifdef BWTREE_CONSOLIDATE_AFTER_INSERT
  fprintf(stderr, "  BwTree will considate after insert phase\n");
#endif

#ifdef USE_TBB
  fprintf(stderr, "  Using Intel TBB to run concurrent tasks\n");
#endif

#ifdef INTERLEAVED_INSERT
  fprintf(stderr, "  Interleaved insert\n");
#endif

#ifdef BWTREE_COLLECT_STATISTICS
  fprintf(stderr, "  BwTree will collect statistics\n");
#endif

  fprintf(stderr, "Leaf delta chain threshold: %d; Inner delta chain threshold: %d\n",
          LEAF_DELTA_CHAIN_LENGTH_THRESHOLD,
          INNER_DELTA_CHAIN_LENGTH_THRESHOLD);

#ifndef BWTREE_USE_MAPPING_TABLE
  fprintf(stderr, "  BwTree does not use mapping table\n");
  if(wl != WORKLOAD_C) {
    fprintf(stderr, "Could only use workload C\n");
    exit(1);
  }

  if(index_type != TYPE_BWTREE) {
    fprintf(stderr, "Could only use BwTree\n");
    exit(1);
  }
#endif

#ifndef BWTREE_USE_CAS
  fprintf(stderr, "  BwTree does not use CAS\n");
#endif

#ifndef BWTREE_USE_DELTA_UPDATE
  fprintf(stderr, "  BwTree does not use delta update\n");
  if(index_type != TYPE_BWTREE) {
    fprintf(stderr, "Could only use BwTree\n");
  }
#endif

#ifdef USE_OLD_EPOCH
  fprintf(stderr, "  BwTree uses old epoch\n");
#endif

  // If we do not interleave threads on two sockets then this will be printed
  if(hyperthreading == true) {
    fprintf(stderr, "  Hyperthreading for thread 10 - 19, 30 - 39\n");
  }

  if(repeat_counter != 1) {
    fprintf(stderr, "  Repeat for %d times (NOTE: Memory number may not be correct)\n",
            repeat_counter);
  }

  if(memory_bandwidth == true) {
    if(geteuid() != 0) {
      fprintf(stderr, "Please run the program as root in order to measure memory bandwidth\n");
      exit(1);
    }

    fprintf(stderr, "  Measuring memory bandwidth\n");

    PCM_memory::InitMemoryMonitor();
  }

  if(numa == true) {
    if(geteuid() != 0) {
      fprintf(stderr, "Please run the program as root in order to measure NUMA operations\n");
      exit(1);
    }

    fprintf(stderr, "  Measuring NUMA operations\n");

    // Call init here to avoid calling it mutiple times
    PCM_NUMA::InitNUMAMonitor();
  }

  if(insert_only == true) {
    fprintf(stderr, "Program will exit after insert operation\n");
  }


  fprintf(stderr, "  BTree element pair count: %lu\n", 
          (uint64_t)btreeolc::BTreeLeaf<uint64_t, uint64_t>::maxEntries);

  // If the key type is RDTSC we just run the special function
  if(kt != RDTSC_KEY) {
    std::vector<keytype> init_keys;
    std::vector<keytype> keys;
    std::vector<uint64_t> values;
    std::vector<int> ranges;
    std::vector<int> ops; //INSERT = 0, READ = 1, UPDATE = 2

    init_keys.reserve(50000000);
    keys.reserve(10000000);
    values.reserve(10000000);
    ranges.reserve(10000000);
    ops.reserve(10000000);

    memset(&init_keys[0], 0x00, 50000000 * sizeof(keytype));
    memset(&keys[0], 0x00, 10000000 * sizeof(keytype));
    memset(&values[0], 0x00, 10000000 * sizeof(uint64_t));
    memset(&ranges[0], 0x00, 10000000 * sizeof(int));
    memset(&ops[0], 0x00, 10000000 * sizeof(int));

    load(wl, kt, index_type, init_keys, keys, values, ranges, ops);
    printf("Finished loading workload file (mem = %lu)\n", MemUsage());
    if(index_type != TYPE_NONE) {
      // Then repeat executing the same workload
      while(repeat_counter > 0) {
        exec(wl, index_type, num_thread, init_keys, keys, values, ranges, ops);
        repeat_counter--;
        printf("Finished running benchmark (mem = %lu)\n", MemUsage());
      }
    } else {
      fprintf(stderr, "Type None is selected - no execution phase\n");
    }
  } else {
    fprintf(stderr, "Running RDTSC benchmark...\n");
    run_rdtsc_benchmark(index_type, num_thread, 50 * 1000 * 1000);
  }

  exit_cleanup();

  return 0;
}
