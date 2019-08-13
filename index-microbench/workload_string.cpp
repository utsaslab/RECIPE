#include "microbench.h"
#include "index.h"

// Used for skiplist
thread_local long skiplist_steps = 0;
std::atomic<long> skiplist_total_steps;

typedef GenericKey<31> keytype;
typedef GenericComparator<31> keycomp;

extern bool hyperthreading;

using KeyEuqalityChecker = GenericEqualityChecker<31>;
using KeyHashFunc = GenericHasher<31>;

static const uint64_t key_type=0;
static const uint64_t value_type=1; // 0 = random pointers, 1 = pointers to keys

#include "util.h"

#define USE_27MB_FILE

// Whether to exit after insert operation
static bool insert_only = false;

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

  // If we do not use the 27MB file then use old set of files; Otherwise 
  // use 27 MB email workload
#ifndef USE_27MB_FILE
  // 0 = a, 1 = c, 2 = e
  if (kt == EMAIL_KEY && wl == WORKLOAD_A) {
    init_file = "workloads/email_loada_zipf_int_100M.dat";
    txn_file = "workloads/email_txnsa_zipf_int_100M.dat";
  } else if (kt == EMAIL_KEY && wl == WORKLOAD_C) {
    init_file = "workloads/email_loadc_zipf_int_100M.dat";
    txn_file = "workloads/email_txnsc_zipf_int_100M.dat";
  } else if (kt == EMAIL_KEY && wl == WORKLOAD_E) {
    init_file = "workloads/email_loade_zipf_int_100M.dat";
    txn_file = "workloads/email_txnse_zipf_int_100M.dat";
  } else {
    fprintf(stderr, "Unknown workload or key type: %d, %d\n", wl, kt);
    exit(1);
  }
#else
  if (kt == EMAIL_KEY && wl == WORKLOAD_A) {
    init_file = "workloads/email_load.dat";
    txn_file = "workloads/email_a.dat";
  } else if (kt == EMAIL_KEY && wl == WORKLOAD_C) {
    init_file = "workloads/email_load.dat";
    txn_file = "workloads/email_c.dat";
  } else if (kt == EMAIL_KEY && wl == WORKLOAD_E) {
    init_file = "workloads/email_load.dat";
    txn_file = "workloads/email_e.dat";
  } else {
    fprintf(stderr, "Unknown workload or key type: %d, %d\n", wl, kt);
    exit(1);
  }
#endif

  std::ifstream infile_load(init_file);

  std::string op;
  std::string key_str;
  keytype key;
  int range;

  std::string insert("INSERT");
  std::string read("READ");
  std::string update("UPDATE");
  std::string scan("SCAN");

  int count = 0;
  while ((count < INIT_LIMIT) && infile_load.good()) {
    infile_load >> op >> key_str;
    if (op.compare(insert) != 0) {
      std::cout << "READING LOAD FILE FAIL!\n";
      return;
    }
    key.setFromString(key_str);
    init_keys.push_back(key);
    count++;
  }

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
      values.push_back((uint64_t)init_keys_data[count].data);
      count++;
    }
  }

  fprintf(stderr, "Number of init entries: %lu\n", init_keys.size());

  // For insert only mode we return here
  if(insert_only == true) {
    return;
  }

  std::ifstream infile_txn(txn_file);
  count = 0;
  while ((count < LIMIT) && infile_txn.good()) {
    infile_txn >> op >> key_str;
    key.setFromString(key_str);
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

  std::cout << "Finished loading workload file\n";

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

  Index<keytype, keycomp> *idx = \
    getInstance<keytype, keycomp, KeyEuqalityChecker, KeyHashFunc>(index_type, key_type);

  // WRITE ONLY TEST--------------
  int count = (int)init_keys.size();
  double start_time = get_now();
  auto func = [idx, &init_keys, num_thread, &values](uint64_t thread_id, bool) {
    size_t total_num_key = init_keys.size();
    size_t key_per_thread = total_num_key / num_thread;
    size_t start_index = key_per_thread * thread_id;
    size_t end_index = start_index + key_per_thread;

    threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);

    int counter = 0;
    for(size_t i = start_index;i < end_index;i++) {
      idx->insert(init_keys[i], values[i], ti);
      counter++;
      if(counter % 4096 == 0) {
        ti->rcu_quiesce();
      }
    }

    ti->rcu_quiesce();

    return;
  };

  StartThreads(idx, num_thread, func, false);

  double end_time = get_now();
  double tput = count / (end_time - start_time) / 1000000; //Mops/sec

  if(index_type == TYPE_SKIPLIST) {
    fprintf(stderr, "SkipList size = %lu\n", idx->GetIndexSize());
  }
  
  std::cout << "\033[1;32m";
  std::cout << "insert " << tput;
  std::cout << "\033[0m" << "\n";

  if(insert_only == true) {
    delete idx;
    return;
  }

  //READ/UPDATE/SCAN TEST----------------
  start_time = get_now();
  int txn_num = GetTxnCount(ops, index_type);
  uint64_t sum = 0;

#ifdef PAPI_IPC
  //Variables for PAPI
  float real_time, proc_time, ipc;
  long long ins;
  int retval;

  if((retval = PAPI_ipc(&real_time, &proc_time, &ins, &ipc)) < PAPI_OK) {    
    printf("PAPI error: retval: %d\n", retval);
    exit(1);
  }
#endif

#ifdef PAPI_CACHE
  int events[3] = {PAPI_L1_TCM, PAPI_L2_TCM, PAPI_L3_TCM};
  long long counters[3] = {0, 0, 0};
  int retval;

  if ((retval = PAPI_start_counters(events, 3)) != PAPI_OK) {
    fprintf(stderr, "PAPI failed to start counters: %s\n", PAPI_strerror(retval));
    exit(1);
  }
#endif

  if(values.size() < keys.size()) {
    fprintf(stderr, "Values array too small\n");
    exit(1);
  }

  fprintf(stderr, "# of Txn: %d\n", txn_num);

  auto func2 = [num_thread,
                idx,
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
        idx->find(keys[i], &v, ti);
      }
      else if (op == OP_UPSERT) { //UPDATE
        idx->upsert(keys[i], (uint64_t)keys[i].data, ti);
      }
      else if (op == OP_SCAN) { //SCAN
        idx->scan(keys[i], ranges[i], ti);
      }

      counter++;
      if(counter % 4096 == 0) {
        ti->rcu_quiesce();
      }
    }

    ti->rcu_quiesce();

    return;
  };

  StartThreads(idx, num_thread, func2, false);

  end_time = get_now();

#ifdef PAPI_IPC
  if((retval = PAPI_ipc(&real_time, &proc_time, &ins, &ipc)) < PAPI_OK) {    
    printf("PAPI error: retval: %d\n", retval);
    exit(1);
  }

  std::cout << "Time = " << real_time << "\n";
  std::cout << "Tput = " << LIMIT/real_time << "\n";
  std::cout << "Inst = " << ins << "\n";
  std::cout << "IPC = " << ipc << "\n";
#endif

#ifdef PAPI_CACHE
  if ((retval = PAPI_read_counters(counters, 3)) != PAPI_OK) {
    fprintf(stderr, "PAPI failed to read counters: %s\n", PAPI_strerror(retval));
    exit(1);
  }

  std::cout << "L1 miss = " << counters[0] << "\n";
  std::cout << "L2 miss = " << counters[1] << "\n";
  std::cout << "L3 miss = " << counters[2] << "\n";
#endif

  tput = txn_num / (end_time - start_time) / 1000000; //Mops/sec

  std::cout << "\033[1;31m";

  if (wl == WORKLOAD_A) {  
    std::cout << "read/update " << (tput + (sum - sum));
  }
  else if (wl == WORKLOAD_C) {
    std::cout << "read " << (tput + (sum - sum));
  }
  else if (wl == WORKLOAD_E) {
    std::cout << "insert/scan " << (tput + (sum - sum));
  }
  else {
    std::cout << "read/update " << (tput + (sum - sum));
  }

  std::cout << "\033[0m" << "\n";

  delete idx;

  return;
}

int main(int argc, char *argv[]) {

  if (argc < 5) {
    std::cout << "Usage:\n";
    std::cout << "1. workload type: a, c, e\n";
    std::cout << "2. key distribution: email\n";
    std::cout << "3. index type: bwtree skiplist masstree artolc btreeolc\n";
    std::cout << "4. Number of threads: (1 - 40)\n";
    std::cout << "   --hyper: Whether to pin all threads on NUMA node 0\n";
    std::cout << "   --insert-only: Whether to only execute insert operations\n";
    std::cout << "   --repeat: Repeat 5 times\n";
    return 1;
  }

  int wl;
  if (strcmp(argv[1], "a") == 0) {
    wl = WORKLOAD_A;
  } else if (strcmp(argv[1], "c") == 0) {
    wl = WORKLOAD_C;
  } else if (strcmp(argv[1], "e") == 0) {
    wl = WORKLOAD_E;
  } else {
    fprintf(stderr, "Unknown workload type: %s\n", argv[1]);
    exit(1);
  }

  int kt = EMAIL_KEY;
  // The second argument must be exactly "email"
  if(strcmp(argv[2], "email") != 0) {
    fprintf(stderr, "Unknown key type: %s\n", argv[2]);
    exit(1);
  }

  int index_type;
  if (strcmp(argv[3], "bwtree") == 0) {
    index_type = TYPE_BWTREE;
  } else if (strcmp(argv[3], "masstree") == 0) {
    index_type = TYPE_MASSTREE;
  } else if (strcmp(argv[3], "artolc") == 0) {
    index_type = TYPE_ARTOLC;
  } else if (strcmp(argv[3], "btreeolc") == 0) {
    index_type = TYPE_BTREEOLC;
  } else if (strcmp(argv[3], "skiplist") == 0) { 
    index_type = TYPE_SKIPLIST;
  } else {
    fprintf(stderr, "Unknown index type: %d\n", index_type);
    exit(1);
  } 
 
  // Then read number of threads using command line
  int num_thread = atoi(argv[4]);
  if(num_thread < 1 || num_thread > 40) {
    fprintf(stderr, "Do not support %d threads\n", num_thread);

    return 1;
  } else {
    fprintf(stderr, "Number of threads: %d\n", num_thread);
  }

  fprintf(stderr, "Leaf delta chain threshold: %d; Inner delta chain threshold: %d\n",
          LEAF_DELTA_CHAIN_LENGTH_THRESHOLD,
          INNER_DELTA_CHAIN_LENGTH_THRESHOLD);


  // Then read all remianing arguments
  int repeat_counter = 1;
  char **argv_end = argv + argc;
  for(char **v = argv + 5;v != argv_end;v++) {
    if(strcmp(*v, "--hyper") == 0) {
      // Enable hyoerthreading for scheduling threads
      hyperthreading = true;
    } else if(strcmp(*v, "--insert-only") == 0) {
      insert_only = true;
    } else if(strcmp(*v, "--repeat") == 0) {
      repeat_counter = 5;
    }
  }

  if(hyperthreading == true) {
    fprintf(stderr, "  Hyperthreading enabled\n");
  }

  if(insert_only == true) {
    fprintf(stderr, "  Insert-only mode\n");
  }

#ifdef USE_27MB_FILE
  fprintf(stderr, "  Using 27MB workload file\n");
#endif 

  if(repeat_counter != 1) {
    fprintf(stderr, "  We run the workload part for %d times\n", repeat_counter);
  }

  fprintf(stderr, "index type = %d\n", index_type);

  std::vector<keytype> init_keys;
  std::vector<keytype> keys;
  std::vector<uint64_t> values;
  std::vector<int> ranges;
  std::vector<int> ops; //INSERT = 0, READ = 1, UPDATE = 2

  load(wl, kt, index_type, init_keys, keys, values, ranges, ops);
  fprintf(stderr, "Finish loading (Mem = %lu)\n", MemUsage());

  while(repeat_counter > 0) {
    exec(wl, index_type, num_thread, init_keys, keys, values, ranges, ops);
    fprintf(stderr, "Finished execution (Mem = %lu)\n", MemUsage());
    repeat_counter--;
  }

  return 0;
}
