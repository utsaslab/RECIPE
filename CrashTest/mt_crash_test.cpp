#include <iostream>
#include <chrono>
#include <random>
#include <cstring>
#include <vector>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include "tbb/tbb.h"
#include "mt_crash_test.h"

using namespace std;

//bool simulateCrash;

#include "ROWEX/Tree.h"
#include "FAST_FAIR/btree.h"
#include "CCEH/src/CCEH-crash.h"
#include "masstree.h"
#include "Bwtree/src/bwtree.h"
#include "clht.h"
#include "ssmem.h"
#include "WORT/wort.h"
#include "WOART/woart.h"
#include "wbtree/wbtree.h"
#include "fptree/FPTree.h"
#ifdef PERF_PROFILE
#include "hw_events.h"
#endif

#ifdef HOT
#include <hot/rowex/HOTRowex.hpp>
//#include <idx/benchmark/Benchmark.hpp>
//#include <idx/benchmark/NoThreadInfo.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>
#endif

using namespace wangziqi2013::bwtree;


// index types
enum {
    TYPE_ART,
    TYPE_HOT,
    TYPE_BWTREE,
    TYPE_BLINK,
    TYPE_MASSTREE,
    TYPE_CLHT,
    TYPE_WORT,
    TYPE_WOART,
    TYPE_WBTREE,
    TYPE_FPTREE,
    TYPE_FASTFAIR,
    TYPE_LEVELHASH,
    TYPE_CCEH,
    TYPE_DUMMY,
};

enum {
    OP_INSERT,
    OP_READ,
    OP_SCAN,
    OP_DELETE,
};

enum {
    WORKLOAD_A,
    WORKLOAD_B,
    WORKLOAD_C,
    WORKLOAD_E,
};

enum {
    RANDINT_KEY,
    STRING_KEY,
};

enum {
    UNIFORM,
    ZIPFIAN,
};

class Error {
    typedef struct {
        int missingValues = 0;
        int incorrectValues = 0;
    } errorSummary;

    private:
        errorSummary es;

    public:
        void IncrementMissingValue() {
            es.missingValues ++;
        }

        void IncrementIncorrectValue(){
            es.incorrectValues ++;
        }

        void PrintErrorStat(){
            fprintf(stderr, "Missing values = %d, incorrect values = %d\n", es.missingValues, es.incorrectValues);
        }

        bool IsError(){
            if (es.missingValues == 0 && es.incorrectValues == 0)
                return false;
            else
                return true;
        }
};


// global variables
Error errorStat;
int CRASH = 0; 

namespace Dummy {
    inline void mfence() {asm volatile("mfence":::"memory");}

    inline void clflush(char *data, int len, bool front, bool back)
    {
        if (front)
            mfence();
        volatile char *ptr = (char *)((unsigned long)data & ~(64 - 1));
        for (; ptr < data+len; ptr += 64){
#ifdef CLFLUSH
            asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
            asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
            asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
        }
        if (back)
            mfence();
    }
}

/*
 * class KeyComparator - Test whether BwTree supports context
 *                       sensitive key comparator
 *
 * If a context-sensitive KeyComparator object is being used
 * then it should follow rules like:
 *   1. There could be no default constructor
 *   2. There MUST be a copy constructor
 *   3. operator() must be const
 *
 */
class KeyComparator {
 public:
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 < k2;
  }

  inline bool operator()(const uint64_t k1, const uint64_t k2) const {
      return k1 < k2;
  }

  inline bool operator()(const char *k1, const char *k2) const {
      return memcmp(k1, k2, strlen(k1) > strlen(k2) ? strlen(k1) : strlen(k2)) < 0;
  }

  KeyComparator(int dummy) {
    (void)dummy;

    return;
  }

  KeyComparator() = delete;
  //KeyComparator(const KeyComparator &p_key_cmp_obj) = delete;
};

/*
 * class KeyEqualityChecker - Tests context sensitive key equality
 *                            checker inside BwTree
 *
 * NOTE: This class is only used in KeyEqual() function, and is not
 * used as STL template argument, it is not necessary to provide
 * the object everytime a container is initialized
 */
class KeyEqualityChecker {
 public:
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 == k2;
  }

  inline bool operator()(uint64_t k1, uint64_t k2) const {
      return k1 == k2;
  }

  inline bool operator()(const char *k1, const char *k2) const {
      if (strlen(k1) != strlen(k2))
          return false;
      else
          return memcmp(k1, k2, strlen(k1)) == 0;
  }

  KeyEqualityChecker(int dummy) {
    (void)dummy;

    return;
  }

  KeyEqualityChecker() = delete;
  //KeyEqualityChecker(const KeyEqualityChecker &p_key_eq_obj) = delete;
};

template<typename ValueType = Key *>
class KeyExtractor {
    public:
    typedef uint8_t* KeyType;

    inline KeyType operator()(ValueType const &value) const {
        return value->fkey;
    }
};


static uint64_t LOAD_SIZE = 10;
static uint64_t RUN_SIZE = 0;

void loadKey(TID tid, Key &key) {
    return ;
}

int ycsb_load_run_string(int index_type, int wl, int kt, int ap, int num_thread,
        std::vector<Key *> &init_keys,
        std::vector<Key *> &keys,
        std::vector<int> &ranges,
        std::vector<int> &ops)
{
    std::string init_file;
    std::string txn_file;

    if (ap == UNIFORM) {
        if (kt == STRING_KEY && wl == WORKLOAD_A) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/load_ycsbkey_workloada";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/txn_ycsbkey_workloada";
#else
            init_file = "./../index-microbench/workloads/ycsbkey_load_workloada";
            txn_file = "./../index-microbench/workloads/ycsbkey_run_workloada";
#endif
        } else if (kt == STRING_KEY && wl == WORKLOAD_B) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/load_ycsbkey_workloadb";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/txn_ycsbkey_workloadb";
#else
            init_file = "./../index-microbench/workloads/ycsbkey_load_workloadb";
            txn_file = "./../index-microbench/workloads/ycsbkey_run_workloadb";
#endif
        } else if (kt == STRING_KEY && wl == WORKLOAD_C) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/load_ycsbkey_workloadc";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/txn_ycsbkey_workloadc";
#else
            init_file = "./../index-microbench/workloads/ycsbkey_load_workloadc";
            txn_file = "./../index-microbench/workloads/ycsbkey_run_workloadc";
#endif
        } else if (kt == STRING_KEY && wl == WORKLOAD_E) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/load_ycsbkey_workloade";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/uniform/txn_ycsbkey_workloade";
#else
            init_file = "./../index-microbench/workloads/ycsbkey_load_workloade";
            txn_file = "./../index-microbench/workloads/ycsbkey_run_workloade";
#endif
        }
    } else {
        if (kt == STRING_KEY && wl == WORKLOAD_A) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/load_ycsbkey_workloada";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/txn_ycsbkey_workloada";
        } else if (kt == STRING_KEY && wl == WORKLOAD_B) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/load_ycsbkey_workloadb";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/txn_ycsbkey_workloadb";
        } else if (kt == STRING_KEY && wl == WORKLOAD_C) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/load_ycsbkey_workloadc";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/txn_ycsbkey_workloadc";
        } else if (kt == STRING_KEY && wl == WORKLOAD_E) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/load_ycsbkey_workloade";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/string/zipfian/txn_ycsbkey_workloade";
        }
    }

    std::ifstream infile_load(init_file);

    std::string op;
    std::string key;
    int range;

    std::string insert("INSERT");
    std::string read("READ");
    std::string scan("SCAN");
    std::string maxKey("z");


	// vector of inserted keys
	tbb::concurrent_vector<Key*> insertedKeys;

    int count = 0;
    uint64_t val;
    while ((count < LOAD_SIZE) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return -1;
        }
        val = std::stoul(key.substr(4, key.size()));
        init_keys.push_back(init_keys[count]->make_leaf((char *)key.c_str(), key.size()+1, val));
        count++;
    }

    //fprintf(stderr, "Loaded %d keys\n", count);

    std::ifstream infile_txn(txn_file);

    count = 0;
    while ((count < RUN_SIZE) && infile_txn.good()) {
        infile_txn >> op >> key;
        if (op.compare(insert) == 0) {
            ops.push_back(OP_INSERT);
            val = std::stoul(key.substr(4, key.size()));
            keys.push_back(keys[count]->make_leaf((char *)key.c_str(), key.size()+1, val));
            ranges.push_back(1);
        } else if (op.compare(read) == 0) {
            ops.push_back(OP_READ);
            val = std::stoul(key.substr(4, key.size()));
            keys.push_back(keys[count]->make_leaf((char *)key.c_str(), key.size()+1, val));
            ranges.push_back(1);
        } else if (op.compare(scan) == 0) {
            infile_txn >> range;
            ops.push_back(OP_SCAN);
            keys.push_back(keys[count]->make_leaf((char *)key.c_str(), key.size()+1, 0));
            ranges.push_back(range);
        } else {
            std::cout << "UNRECOGNIZED CMD!\n";
            return -1;
        }
        count++;
    }


/*------------------------------------- ART - STRING TYPE KEYS -----------------------------------*/

    if (index_type == TYPE_ART) {
        ART_ROWEX::Tree tree(loadKey);
		srand(time(NULL));
		int total_keys = 0;
		bool success;
        {

		// LOAD using single thread
		simulateCrash = (bool)CRASH;
		std::cout << "LOAD with CrashSimulation set to "<< simulateCrash << std::endl;
            // Load
                auto t = tree.getThreadInfo();
                for (uint64_t i = 0; i < LOAD_SIZE; i++) {
                    Key *key = key->make_leaf((char *)init_keys[i]->fkey, init_keys[i]->key_len, init_keys[i]->value);
                    success = tree.insert(key, t);
					if (!success){
						std::cout << "\t\t" << i << " : Crash state. Continuing to next insert" << std::endl;
						// We might want to construct complex crash states.
						// To do allow allow crashes on crashed states and return probabilistically
						if (rand()%2){
							std::cout << "\t" << i << " : Breaking from LOAD phase due to crash" << std::endl;
							break;
						}
						continue;
					}
					insertedKeys.push_back(key);
					total_keys ++;
                }
				std::cout << "LOADED " << total_keys << " keys. Now at a crash state " << std::endl;

        }
		simulateCrash = false;
		tbb::task_scheduler_init init(num_thread);
		std::cout << "RUN with CrashSimulation set to "<<simulateCrash << " with "<< num_thread << " threads " << std::endl;

        {
            // RUN using num_thread
			std::atomic<int> run_count;
			run_count.store(0);
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree.getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    Key *key;
                    if (ops[i] == OP_INSERT) {
                        key = key->make_leaf((char *)keys[i]->fkey, keys[i]->key_len, keys[i]->value);
                        success = tree.insert(key, t);
						if (!success){
							std::cout << "\t Insertion of key " << key->fkey << " failed!" << std::endl;
							continue;
						}
						insertedKeys.push_back(key);
						run_count.fetch_add(1);
                      }
				}
			});
			total_keys += run_count;	
			std::cout << "RUN phase : Inserted " << run_count << " keys" << std::endl;

        }
    		int read_count = 0;
			auto t = tree.getThreadInfo();
			for (int i=0; i< insertedKeys.size(); i++){
				Key *key;
				key = key->make_leaf((char *)insertedKeys[i]->fkey, insertedKeys[i]->key_len, insertedKeys[i]->value);  
				Key *val = reinterpret_cast<Key *>(tree.lookup(key, t));
				
				if (val == NULL){
						std::cout << "\t [ART] null key read: " << std::endl;
						errorStat.IncrementMissingValue();
						continue;
				}
                if (val->value != insertedKeys[i]->value) {
                       std::cout << "\t [ART] wrong key read: " << val->value << " expected:" << insertedKeys[i]->value << std::endl;
						errorStat.IncrementIncorrectValue();
						continue;
                }
				read_count ++;
             }

		std::cout << "Total keys = " << total_keys << ", and read " << read_count << " keys successfully" << std::endl;



/*------------------------------------- HOT - STRING TYPE KEYS -----------------------------------*/

#ifdef HOT
    } else if (index_type == TYPE_HOT) {
        hot::rowex::HOTRowex<Key *, KeyExtractor> mTrie;

        srand(time(NULL));
        int total_keys = 0;
        bool success;

        // LOAD using single thread
		simulateCrash = (bool)CRASH;
        std::cout << "LOAD with CrashSimulation set to "<< simulateCrash << std::endl;


            // Load
            for (uint64_t i = 0; i < LOAD_SIZE ; i++) {
                    Key *key = key->make_leaf((char *)init_keys[i]->fkey, init_keys[i]->key_len, init_keys[i]->value);
                    Dummy::clflush((char *)key, sizeof(Key) + key->key_len, true, true);
                    success = mTrie.insert(key); 
                    if (!success){
                        std::cout << "\t\t" << i << " : Crash state. Continuing to next insert" << std::endl;
                        // We might want to construct complex crash states.
                        // To do allow allow crashes on crashed states and return probabilistically
                        if (rand()%2){
                            std::cout << "\t" << i << " : Breaking from LOAD phase due to crash" << std::endl;
                            break;
                        }
                        continue;
                    }
                    insertedKeys.push_back(key);
                    total_keys ++;
                }
                std::cout << "LOADED " << total_keys << " keys. Now at a crash state " << std::endl;



        	simulateCrash = false;
        	tbb::task_scheduler_init init(num_thread);
        	std::cout << "RUN with CrashSimulation set to "<<simulateCrash << " with "<< num_thread << " threads " << std::endl;

            // RUN using num_thread
            std::atomic<int> run_count;
            run_count.store(0);


            // Run
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        Key *key = key->make_leaf((char *)keys[i]->fkey, keys[i]->key_len, keys[i]->value);
                        Dummy::clflush((char *)key, sizeof(Key) + key->key_len, true, true);
                        success  = mTrie.insert(key); 
                        if (!success){
                            std::cout << "\t Insertion of key " << key->fkey << " failed!" << std::endl;
                            continue;
                        }
                        insertedKeys.push_back(key);
                        run_count.fetch_add(1);
                      }
				}
            });
            total_keys += run_count;
            std::cout << "RUN phase : Inserted " << run_count << " keys" << std::endl;
	
			// Now READ all inserted keys
            int read_count = 0;
            for (int i=0; i< insertedKeys.size(); i++){
                Key *key;
                key = key->make_leaf((char *)insertedKeys[i]->fkey, insertedKeys[i]->key_len, insertedKeys[i]->value);
                idx::contenthelpers::OptionalValue<Key *> result = mTrie.lookup((uint8_t *)insertedKeys[i]->fkey);

                if (!result.mIsValid){
                        std::cout << "\t [HOT] null key read: " << std::endl;
						errorStat.IncrementMissingValue();
                        continue;
                }
                if (result.mValue->value != insertedKeys[i]->value) {
                       std::cout << "\t [HOT] wrong key read: " << result.mValue->value << " expected:" << insertedKeys[i]->value << std::endl;
                        errorStat.IncrementIncorrectValue();
						continue;
                }
                read_count ++;
             }

        std::cout << "Total keys = " << total_keys << ", and read " << read_count << " keys successfully" << std::endl;
#endif



/*------------------------------------- BWTREE - STRING TYPE KEYS -----------------------------------*/

    } else if (index_type == TYPE_BWTREE) {
		
		auto t = new BwTree<char *, uint64_t, KeyComparator, KeyEqualityChecker>{true, KeyComparator{1}, KeyEqualityChecker{1}};
        t->UpdateThreadLocal(1);
        t->AssignGCID(0);
        std::atomic<int> next_thread_id;

        srand(time(NULL));
        int total_keys = 0;
        bool success;
        
        // LOAD using single thread
        simulateCrash = (bool)CRASH;
        std::cout << "LOAD with CrashSimulation set to "<< simulateCrash << std::endl;
            // Load
				next_thread_id.store(0);
				t->UpdateThreadLocal(num_thread);

				int thread_id = next_thread_id.fetch_add(1);
				t->AssignGCID(thread_id);
                for (uint64_t i = 0; i < LOAD_SIZE; i++) {
                    Key *key = key->make_leaf((char *)init_keys[i]->fkey, init_keys[i]->key_len, init_keys[i]->value);
					Dummy::clflush((char *)init_keys[i]->fkey, init_keys[i]->key_len, true, true);
                    success = t->Insert((char *)init_keys[i]->fkey, init_keys[i]->value);
					if (!success){
                        std::cout << i << " : Crash state. Continuing to next insert" << std::endl;
                        // We might want to construct complex crash states.
                        // To do allow allow crashes on crashed states and return probabilistically
                        if (rand()%2){
                            std::cout << "\t" << i << " : Breaking from LOAD phase due to crash" << std::endl;
                            break;
                        }
                        continue;
                    }
                    insertedKeys.push_back(key);
                    total_keys ++;
                }
				t->UnregisterThread(thread_id);
                std::cout << "LOADED " << total_keys << " keys. Now at a crash state " << std::endl;
		t->UpdateThreadLocal(1);       
 
        simulateCrash = false;
        std::cout << "RUN with CrashSimulation set to "<<simulateCrash << " with "<< num_thread << " threads " << std::endl;


            // RUN using num_thread
            std::atomic<int> run_count;
            run_count.store(0);
			
			next_thread_id.store(0);
			t->UpdateThreadLocal(num_thread);
			auto func = [&]() {
				int thread_id = next_thread_id.fetch_add(1);
				uint64_t start_key = RUN_SIZE / num_thread * (uint64_t)thread_id;
				uint64_t end_key = start_key + RUN_SIZE / num_thread;
				t->AssignGCID(thread_id);
				for (uint64_t i = start_key; i < end_key; i++) {
					if (ops[i] == OP_INSERT) {
                    	Key *key = key->make_leaf((char *)keys[i]->fkey, keys[i]->key_len, keys[i]->value);
						Dummy::clflush((char *)keys[i]->fkey, keys[i]->key_len, true, true);
						success = t->Insert((char *)keys[i]->fkey, keys[i]->value);	
                        if (!success){
                            std::cout << "\t Insertion of key " << key->fkey << " failed!" << std::endl;
                            continue;
                        }
                        insertedKeys.push_back(key);
                        run_count.fetch_add(1);
                      }
                  }
				  t->UnregisterThread(thread_id);
				};
				
				std::vector<std::thread> thread_group;
				for (int i = 0; i < num_thread; i++)
					thread_group.push_back(std::thread{func});
				
				for (int i = 0; i < num_thread; i++)
					thread_group[i].join();

				t->UpdateThreadLocal(1);

            total_keys += run_count;
            std::cout << "RUN phase : Inserted " << run_count << " keys" << std::endl;

            int read_count = 0;
			std::vector<uint64_t> v{};
			v.reserve(1);

            for (int i=0; i< insertedKeys.size(); i++){
				v.clear();
				t->GetValue((char *)insertedKeys[i]->fkey, v);
                if (v.size() == 0){
                        std::cout << "\t [BWTREE] null key read: " << std::endl;
						errorStat.IncrementMissingValue();
                        continue;
                }
                if (v[0] != insertedKeys[i]->value) {
                       std::cout << "\t [BWTREE] wrong key read: " << v[0] << " expected:" << insertedKeys[i]->value << std::endl;
						errorStat.IncrementIncorrectValue();
                        continue;
                }
                read_count ++;
             }

        std::cout << "Total keys = " << total_keys << ", and read " << read_count << " keys successfully" << std::endl;



/*------------------------------------- MASSTREE - STRING TYPE KEYS -----------------------------------*/

    } else if (index_type == TYPE_BLINK || index_type == TYPE_MASSTREE) {
        masstree::leafnode *init_root = new masstree::leafnode(0);
        masstree::masstree *tree = new masstree::masstree(init_root);
		srand(time(NULL));
        int total_keys = 0;
        bool success;
        // LOAD using single thread
        simulateCrash = (bool)CRASH;
        std::cout << "LOAD with CrashSimulation set to "<< simulateCrash << std::endl;
                
			for (uint64_t i = 0; i < LOAD_SIZE; i++) {
					Key *key = key->make_leaf((char *)init_keys[i]->fkey, init_keys[i]->key_len, init_keys[i]->value);
                    success = tree->put((char *)init_keys[i]->fkey, init_keys[i]->value);
					if (!success){
                        std::cout << "\t\t" << i << " : Crash state. Continuing to next insert" << std::endl;
                        // We might want to construct complex crash states.
                        // To do allow allow crashes on crashed states and return probabilistically
                        if (rand()%2){
                            std::cout << "\t" << i << " : Breaking from LOAD phase due to crash" << std::endl;
                            break;
                        }
                        continue;
                    }
                    insertedKeys.push_back(key);
                    total_keys ++;
                }
                std::cout << "LOADED " << total_keys << " keys. Now at a crash state " << std::endl;

        simulateCrash = false;
        tbb::task_scheduler_init init(num_thread);
        std::cout << "RUN with CrashSimulation set to "<<simulateCrash << " with "<< num_thread << " threads " << std::endl;


            // RUN using num_thread
            std::atomic<int> run_count;
            run_count.store(0);

            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
					Key *key = key->make_leaf((char *)keys[i]->fkey, keys[i]->key_len, keys[i]->value);
                    if (ops[i] == OP_INSERT) {
                        success = tree->put((char *)keys[i]->fkey, keys[i]->value);
						if (!success){
                            std::cout << "\t Insertion of key " << key->fkey << " failed!" << std::endl;
                            continue;
                        }
                        insertedKeys.push_back(key);
                        run_count.fetch_add(1);
                      }

                  }
			});

            total_keys += run_count;
            std::cout << "RUN phase : Inserted " << run_count << " keys" << std::endl;
		
			int read_count = 0;
			for (int i=0; i< insertedKeys.size(); i++){
				void * result = tree->get((char *)insertedKeys[i]->fkey);
				if (result == NULL){
					std::cout << "\t [MASSTREE] null key read: " << std::endl;
					errorStat.IncrementMissingValue();
                    continue;
				}
 				uint64_t *ret = reinterpret_cast<uint64_t *> (result);
                if (*ret != insertedKeys[i]->value) {
                	std::cout << "\t [MASSTREE] wrong key read: " << ret << " expected:" << insertedKeys[i]->value << std::endl;
						errorStat.IncrementIncorrectValue();
                    continue;
                }
				read_count ++;
             }

        std::cout << "Total keys = " << total_keys << ", and read " << read_count << " keys successfully" << std::endl;

/*------------------------------------- FAST AND FAIR - STRING TYPE KEYS -----------------------------------*/

    } else if (index_type == TYPE_FASTFAIR) {
        fastfair::btree *bt = new fastfair::btree();
        srand(time(NULL));
        int total_keys = 0;
        bool success;
        simulateCrash = (bool)CRASH;
        std::cout << "LOAD with CrashSimulation set to "<< simulateCrash << std::endl;


		for (uint64_t i = 0; i < LOAD_SIZE; i++) {
			Key *key = key->make_leaf((char *)init_keys[i]->fkey, init_keys[i]->key_len, init_keys[i]->value);
			success = bt->btree_insert((char *)init_keys[i]->fkey, (char *) &init_keys[i]->value);
			if (!success){
				std::cout << "\t\t" << i << " : Crash state. Continuing to next insert" << std::endl;
				// We might want to construct complex crash states.                        
				// To do allow allow crashes on crashed states and return probabilistically
                if (rand()%2){
                	std::cout << "\t" << i << " : Breaking from LOAD phase due to crash" << std::endl;
                    break;
                 }
                 continue;
             }
             insertedKeys.push_back(key);
             total_keys ++;
          }
          std::cout << "LOADED " << total_keys << " keys. Now at a crash state " << std::endl;

        simulateCrash = false;
        tbb::task_scheduler_init init(num_thread);
        std::cout << "RUN with CrashSimulation set to "<<simulateCrash << " with "<< num_thread << " threads " << std::endl;

            // RUN using num_thread
            std::atomic<int> run_count;
            run_count.store(0);
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                       	Key * key = key->make_leaf((char *)keys[i]->fkey, keys[i]->key_len, keys[i]->value);
						success = bt->btree_insert((char *)keys[i]->fkey, (char *) &keys[i]->value);
						if (!success){
                            std::cout << "\t Insertion of key " << key->fkey << " failed!" << std::endl;
                            continue;
                        }
                        insertedKeys.push_back(key);
                        run_count.fetch_add(1);
                      }
                }
            });
            total_keys += run_count;
			std::cout << "RUN phase : Inserted " << run_count << " keys" << std::endl;
                        
			int read_count = 0;
			for (int i=0; i< insertedKeys.size(); i++){
				char * result = bt->btree_search((char *)insertedKeys[i]->fkey);
				if (result == NULL) {
					std::cout << "\t [FASTFAIR] null key read: " << std::endl;
					errorStat.IncrementMissingValue();
					continue;
				}
				uint64_t *ret = reinterpret_cast<uint64_t *> (result);
                if (*ret != insertedKeys[i]->value) {
                	std::cout << "\t [ART] wrong key read: " << *ret << " expected:" << insertedKeys[i]->value << std::endl;
					errorStat.IncrementIncorrectValue();
					continue;
				}
				read_count ++;
            }
			std::cout << "Total keys = " << total_keys << ", and read " << read_count << " keys successfully" << std::endl;

}	// end else if

errorStat.PrintErrorStat();
return errorStat.IsError();
} // end function load_run_string



/*======================================= RANDINT ===================================== */


int ycsb_load_run_randint(int index_type, int wl, int kt, int ap, int num_thread,
        std::vector<uint64_t> &init_keys,
        std::vector<uint64_t> &keys,
        std::vector<int> &ranges,
        std::vector<int> &ops)
{
    std::string init_file;
    std::string txn_file;

    if (ap == UNIFORM) {
        if (kt == RANDINT_KEY && wl == WORKLOAD_A) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/loada_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/txnsa_unif_int.dat";
#else
            init_file = "./../index-microbench/workloads/loada_unif_int.dat";
            txn_file = "./../index-microbench/workloads/txnsa_unif_int.dat";
#endif
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_B) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/loadb_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/txnsb_unif_int.dat";
#else
            init_file = "./../index-microbench/workloads/loadb_unif_int.dat";
            txn_file = "./../index-microbench/workloads/txnsb_unif_int.dat";
#endif
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_C) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/loadc_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/txnsc_unif_int.dat";
#else
            init_file = "./../index-microbench/workloads/loadc_unif_int.dat";
            txn_file = "./../index-microbench/workloads/txnsc_unif_int.dat";
#endif
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_E) {
#ifdef LOCAL_DATA
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/loade_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/uniform/txnse_unif_int.dat";
#else
            init_file = "./../index-microbench/workloads/loade_unif_int.dat";
            txn_file = "./../index-microbench/workloads/txnse_unif_int.dat";
#endif
        }
    } else {
        if (kt == RANDINT_KEY && wl == WORKLOAD_A) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/loada_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/txnsa_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_B) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/loadb_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/txnsb_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_C) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/loadc_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/txnsc_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_E) {
            init_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/loade_unif_int.dat";
            txn_file = "/home/sekwon/radix-tree-for-pm/ARTSynchronized/workloads/integer/zipfian/txnse_unif_int.dat";
        }
    }

    std::ifstream infile_load(init_file);

    std::string op;
    uint64_t key;
    int range;

	tbb::concurrent_vector<uint64_t> insertedKeys;

    std::string insert("INSERT");
    std::string read("READ");
    std::string scan("SCAN");

    int count = 0;
    while ((count < LOAD_SIZE) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return -1 ;
        }
        init_keys.push_back(key);
        count++;
    }

    fprintf(stderr, "Loaded %d keys\n", count);

    std::ifstream infile_txn(txn_file);

    count = 0;
    while ((count < RUN_SIZE) && infile_txn.good()) {
        infile_txn >> op >> key;
        if (op.compare(insert) == 0) {
            ops.push_back(OP_INSERT);
            keys.push_back(key);
            ranges.push_back(1);
        } else if (op.compare(read) == 0) {
            ops.push_back(OP_READ);
            keys.push_back(key);
            ranges.push_back(1);
        } else if (op.compare(scan) == 0) {
            infile_txn >> range;
            ops.push_back(OP_SCAN);
            keys.push_back(key);
            ranges.push_back(range);
        } else {
            std::cout << "UNRECOGNIZED CMD!\n";
            return -1;
        }
        count++;
    }

/*------------------------------------- CLHT - INT TYPE KEYS -----------------------------------*/

  if (index_type == TYPE_CLHT) {
        typedef struct thread_data {
            uint32_t id;
            clht_t *ht;
        } thread_data_t;

        srand(time(NULL));
        int total_keys = 0;
        bool success;

        clht_t *hashtable = clht_create(10);

        thread_data_t *tds = (thread_data_t *) malloc((num_thread) * sizeof(thread_data_t));

        std::atomic<int> next_thread_id;

                
		{
        // LOAD using single thread
        simulateCrash = (bool)CRASH;
        std::cout << "LOAD with CrashSimulation set to "<< simulateCrash << std::endl;

		next_thread_id.store(0);
			
        	//clht_gc_thread_init(hashtable, 0);
        	//ssmem_allocator_t *alloc = (ssmem_allocator_t *) malloc(sizeof(ssmem_allocator_t));
        	//ssmem_alloc_init_fs_size(alloc, SSMEM_DEFAULT_MEM_SIZE, SSMEM_GC_FREE_SET_SIZE, 20);

        for (uint64_t i = 0; i < LOAD_SIZE; i++) {
            	success = clht_put(hashtable, init_keys[i], init_keys[i]);
				if (!success){
					std::cout << "\t\t" << i << " : Crash state. Continuing to next insert" << std::endl;
					// We might want to construct complex crash states.
					// To do allow allow crashes on crashed states and return probabilistically
					if (rand()%2){
						std::cout << "\t" << i << " : Breaking from LOAD phase due to crash" << std::endl;
						break;
					}
					continue;
				}
				insertedKeys.push_back(init_keys[i]);
				total_keys ++;
		}
		std::cout << "LOADED " << total_keys << " keys. Now at a crash state " << std::endl;
		}

		{
		simulateCrash = false;
		std::cout << "RUN with CrashSimulation set to "<<simulateCrash << " with "<< num_thread << " threads " << std::endl;

		// Run
		std::atomic<int> run_count;
		run_count.store(0);
        next_thread_id.store(0);
        auto func = [&]() {
				int thread_id = next_thread_id.fetch_add(1);
                tds[thread_id].id = thread_id;
                tds[thread_id].ht = hashtable;

                uint64_t start_key = RUN_SIZE / num_thread * (uint64_t)(thread_id);
                uint64_t end_key = start_key + RUN_SIZE / num_thread;

                for (uint64_t i = start_key; i < end_key; i++) {
                    if (ops[i] == OP_INSERT) {
                        success = clht_put(tds[thread_id].ht, keys[i], keys[i]);
						if (!success){
							std::cout << "\t Insertion of key " << keys[i] << " failed!" << std::endl;
							continue;
						}
						insertedKeys.push_back(keys[i]);
						run_count.fetch_add(1);
					}
				}
			};

            std::vector<std::thread> thread_group;

            for (int i = 0; i < num_thread; i++)
                thread_group.push_back(std::thread{func});

            for (int i = 0; i < num_thread; i++)
                thread_group[i].join();

			total_keys += run_count;
			std::cout << "RUN phase : Inserted " << run_count << " keys" << std::endl;
		}
			 int read_count = 0;
			for (int i=0; i< insertedKeys.size(); i++){
				uintptr_t val = clht_get(hashtable->ht, insertedKeys[i]);
                if (val == 0){
					std::cout << "\t [CLHT] null key read for : " << insertedKeys[i] << std::endl;
					errorStat.IncrementMissingValue();
					continue;
				}
				if (val != insertedKeys[i]) {
                 	std::cout << "[CLHT] wrong key read: " << val << "expected: " << insertedKeys[i] << std::endl;
					errorStat.IncrementIncorrectValue();
                	continue;        
				}
				read_count ++;

			}
        std::cout << "Total keys = " << total_keys << ", and read " << read_count << " keys successfully" << std::endl;

/*------------------------------------- CCEH - INT TYPE KEYS -----------------------------------*/
} else if (index_type == TYPE_CCEH) {
        Hash *table = new CCEH(2);

        srand(time(NULL));
        int total_keys = 0;
        bool success;


        // LOAD using single thread
        simulateCrash = (bool)CRASH;
        std::cout << "LOAD with CrashSimulation set to "<< simulateCrash << std::endl;

        // Load
        for (uint64_t i = 0; i < LOAD_SIZE; i++) {
        	success = table->Insert(init_keys[i], reinterpret_cast<const char*>(&init_keys[i]));
			if (!success){
                 std::cout << "\t\t" << i << " : Crash state. Continuing to next insert" << std::endl;
            	// We might want to construct complex crash states.
             	// To do allow allow crashes on crashed states and return probabilistically
             	if (rand()%2){
             		std::cout << "\t" << i << " : Breaking from LOAD phase due to crash" << std::endl;
                    break;
                 }
                 continue;
              }
              insertedKeys.push_back(init_keys[i]);
              total_keys ++;
            }
            std::cout << "LOADED " << total_keys << " keys. Now at a crash state " << std::endl;

         

            simulateCrash = false;
            tbb::task_scheduler_init init(num_thread);
            std::cout << "RUN with CrashSimulation set to "<<simulateCrash << " with "<< num_thread << " threads " << std::endl;

            // RUN using num_thread
            std::atomic<int> run_count;
            run_count.store(0);tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
             for (uint64_t i = scope.begin(); i != scope.end(); i++) {
             	if (ops[i] == OP_INSERT) {
					success = table->Insert(keys[i], reinterpret_cast<const char*>(&keys[i]));
                    if (!success){
                     	std::cout << "\t Insertion of key " << keys[i] << " failed!" << std::endl;
                    	continue;
                    }
                    insertedKeys.push_back(keys[i]);
                    run_count.fetch_add(1);
                  }
                }
            });						                        

            total_keys += run_count;
            std::cout << "RUN phase : Inserted " << run_count << " keys" << std::endl;

           // Now READ all inserted keys
            int read_count = 0;
            for (int i=0; i< insertedKeys.size(); i++){
				const char* value = table->Get(insertedKeys[i]);
				if (value == NONE){
					std::cout << "\t [CCEH] null key read: " << std::endl;
					errorStat.IncrementMissingValue();
                    continue;
				}
				uint64_t *val = reinterpret_cast<uint64_t *>(const_cast<char *>(value));
                if (*val != insertedKeys[i]) {
                	std::cout << "[CCEH] wrong key read: " << *val << " expected " << insertedKeys[i]  << std::endl;
					errorStat.IncrementIncorrectValue();
					continue;
                }
				read_count ++;
             }
        std::cout << "Total keys = " << total_keys << ", and read " << read_count << " keys successfully" << std::endl;
        
	}// end else if
	errorStat.PrintErrorStat();
	return errorStat.IsError();
}// end function



int main(int argc, char **argv) {
#ifndef PINCNT
    if (argc != 9) {
#else
    if (argc != 10) {
#endif
        std::cout << "Usage: ./ycsb [index type] [ycsb workload type] [key distribution] [access pattern] [number of threads]\n";
        std::cout << "1. index type: art hot bwtree blink masstree clht dummy\n";
        std::cout << "               wort woart wbtree fptree fastfair levelhash cceh\n";
        std::cout << "2. ycsb workload type: a, b, c, e\n";
        std::cout << "3. key distribution: randint, string\n";
        std::cout << "4. access pattern: uniform, zipfian\n";
        std::cout << "5. number of threads (integer)\n";
        std::cout << "6. LOAD_SIZE (integer)\n";
        std::cout << "7. RUN_SIZE (integer)\n";
        std::cout << "8. CRASH (int 0,1)\n";
#ifdef PINCNT
        std::cout << "9. Load size tested for performance counters (integer)\n";
#endif

        return -1;
    }

    printf("%s, workload%s, %s, %s, threads %s\n", argv[1], argv[2], argv[3], argv[4], argv[5], argv[6], argv[7]);

    int index_type;
    if (strcmp(argv[1], "art") == 0)
        index_type = TYPE_ART;
    else if (strcmp(argv[1], "hot") == 0)
        index_type = TYPE_HOT;
    else if (strcmp(argv[1], "bwtree") == 0)
        index_type = TYPE_BWTREE;
    else if (strcmp(argv[1], "blink") == 0)
        index_type = TYPE_BLINK;
    else if (strcmp(argv[1], "masstree") == 0)
        index_type = TYPE_MASSTREE;
    else if (strcmp(argv[1], "clht") == 0)
        index_type = TYPE_CLHT;
    else if (strcmp(argv[1], "wort") == 0)
        index_type = TYPE_WORT;
    else if (strcmp(argv[1], "woart") == 0)
        index_type = TYPE_WOART;
    else if (strcmp(argv[1], "wbtree") == 0)
        index_type = TYPE_WBTREE;
    else if (strcmp(argv[1], "fptree") == 0)
        index_type = TYPE_FPTREE;
    else if (strcmp(argv[1], "fastfair") == 0)
        index_type = TYPE_FASTFAIR;
    else if (strcmp(argv[1], "levelhash") == 0)
        index_type = TYPE_LEVELHASH;
    else if (strcmp(argv[1], "cceh") == 0)
        index_type = TYPE_CCEH;
    else if (strcmp(argv[1], "dummy") == 0)
        index_type = TYPE_DUMMY;
    else {
        fprintf(stderr, "Unknown index type: %s\n", argv[1]);
        exit(1);
    }

    int wl;
    if (strcmp(argv[2], "a") == 0) {
        wl = WORKLOAD_A;
    } else if (strcmp(argv[2], "b") == 0) {
        wl = WORKLOAD_B;
    } else if (strcmp(argv[2], "c") == 0) {
        wl = WORKLOAD_C;
    } else if (strcmp(argv[2], "e") == 0) {
        wl = WORKLOAD_E;
    } else {
        fprintf(stderr, "Unknown workload: %s\n", argv[2]);
        exit(1);
    }

    int kt;
    if (strcmp(argv[3], "randint") == 0) {
        kt = RANDINT_KEY;
    } else if (strcmp(argv[3], "string") == 0) {
        kt = STRING_KEY;
    } else {
        fprintf(stderr, "Unknown key type: %s\n", argv[3]);
        exit(1);
    }

    int ap;
    if (strcmp(argv[4], "uniform") == 0) {
        ap = UNIFORM;
    } else if (strcmp(argv[4], "zipfian") == 0) {
        ap = ZIPFIAN;
    } else {
        fprintf(stderr, "Unknown access pattern: %s\n", argv[4]);
        exit(1);
    }

    int num_thread = atoi(argv[5]);
    tbb::task_scheduler_init init(num_thread);

	LOAD_SIZE = atoi(argv[6]);
	RUN_SIZE = atoi(argv[7]);
	CRASH = atoi(argv[8]);
#ifdef PINCNT
    LOAD_SIZE = atoi(argv[9]);
    RUN_SIZE = 0;
#endif


	bool status;

    if (kt != STRING_KEY) {
        std::vector<uint64_t> init_keys;
        std::vector<uint64_t> keys;
        std::vector<int> ranges;
        std::vector<int> ops;

        init_keys.reserve(LOAD_SIZE);
        keys.reserve(RUN_SIZE);
        ranges.reserve(RUN_SIZE);
        ops.reserve(RUN_SIZE);

        memset(&init_keys[0], 0x00, LOAD_SIZE * sizeof(uint64_t));
        memset(&keys[0], 0x00, RUN_SIZE * sizeof(uint64_t));
        memset(&ranges[0], 0x00, RUN_SIZE * sizeof(int));
        memset(&ops[0], 0x00, RUN_SIZE * sizeof(int));

        status = ycsb_load_run_randint(index_type, wl, kt, ap, num_thread, init_keys, keys, ranges, ops);
    } else {
        std::vector<Key *> init_keys;
        std::vector<Key *> keys;
        std::vector<int> ranges;
        std::vector<int> ops;

        init_keys.reserve(LOAD_SIZE);
        keys.reserve(RUN_SIZE);
        ranges.reserve(RUN_SIZE);
        ops.reserve(RUN_SIZE);

        memset(&init_keys[0], 0x00, LOAD_SIZE * sizeof(Key *));
        memset(&keys[0], 0x00, RUN_SIZE * sizeof(Key *));
        memset(&ranges[0], 0x00, RUN_SIZE * sizeof(int));
        memset(&ops[0], 0x00, RUN_SIZE * sizeof(int));

        status = ycsb_load_run_string(index_type, wl, kt, ap, num_thread, init_keys, keys, ranges, ops);
    }

    std::cout << string(50, '_') << std::endl;
	return (int)status;
}
