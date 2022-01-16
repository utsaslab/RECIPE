
/*
 * crash_test.cpp
 *
 */

#include <fstream>
#include <iostream>
#include <sys/ipc.h> 
#include <sys/shm.h> 
#include <stdio.h>
#include <string>
#include "tbb/tbb.h"

#include "ROWEX/Tree.h"
#include "FAST_FAIR/btree.h"
#include "CCEH/src/CCEH-crash.h"
#include "masstree-crash.h"
#include "Bwtree/src/bwtree.h"
#include "Bwtree/test/test_suite.h"
#include "clht.h"
#include "ssmem.h"
#include "WORT/wort.h"
#include "WOART/woart.h"
#include "wbtree/wbtree.h"
#include "fptree/FPTree.h"

#ifdef HOT
#include <hot/rowex/HOTRowex.hpp>
#include <idx/benchmark/Benchmark.hpp>
#include <idx/benchmark/NoThreadInfo.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>
#endif

#define OFFSET 10000
#define stringify( type ) # type

using namespace std;

template<typename ValueType = Key *>
class KeyExtractor {
    public:
    typedef uint8_t* KeyType;

    inline KeyType operator()(ValueType const &value) const {
        return value->fkey;
    }
};

// key types
enum {
    RANDINT_KEY,
    STRING_KEY,
};

// index types
enum {
    	TYPE_PART,
    	TYPE_PHOT,
    	TYPE_PBWTREE,
    	TYPE_PMASSTREE,
    	TYPE_PCLHT,
    	TYPE_WORT,
    	TYPE_WOART,
    	TYPE_WBTREE,
    	TYPE_FPTREE,
    	TYPE_FASTFAIR,
    	TYPE_LEVELHASH,
    	TYPE_CCEH,
};

const char* typeIndexName[]= {
	stringify(TYPE_PART),
	stringify(TYPE_PHOT),
	stringify(TYPE_PBWTREE),
	stringify(TYPE_PMASSTREE),
	stringify(TYPE_PCLHT),
	stringify(TYPE_WORT),
	stringify(TYPE_WOART), 
	stringify(TYPE_WBTREE), 
	stringify(TYPE_FPTREE), 
	stringify(TYPE_FASTFAIR), 
	stringify(TYPE_LEVELHASH), 
	stringify(TYPE_CCEH) 
};

const char* typeKeyName[]={
	stringify(RANDINT_KEY),
	stringify(STRING_KEY)
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

struct globalSummary{
	int numCrashStates = 0;	
	int numFailedStates = 0;
};


// global variables
Error errorStat;
globalSummary *gs;

void loadKey(TID tid, Key &key) {
    return ;
}

int indexType(string index){
	if (!index.compare("p-art"))
		return TYPE_PART;
	else if (!index.compare("p-hot"))
		return TYPE_PHOT;
	else if (!index.compare("p-bwtree"))
		return TYPE_PBWTREE;
	else if (!index.compare("p-clht"))
		return TYPE_PCLHT;
	else if (!index.compare("p-masstree"))
		return TYPE_PMASSTREE;
	else if (!index.compare("cceh"))
		return TYPE_CCEH;
	else
		return -1;
}

int keyType(string key_type){
	if (!key_type.compare("randint"))
		return RANDINT_KEY;
	else if (!key_type.compare("string"))
		return STRING_KEY;
	else
		return -1;
}  

void testMASSTREE(int total_keys, int num_threads, bool perform_delete, int delete_key_start, int num_delete_keys,
		std::vector<Key *> &keysToInsert,
		std::vector<Key *> &insertedKeys) {

	fprintf(stderr, "\n-------Testing MASSTREE with %d insert keys and %d deletes-----\n", total_keys, num_delete_keys);
	std::vector<Key *>::iterator it;
	int inserted = 0, deleted = 0, searched = 0;
    bool success = true;

	masstree::leafnode *init_root = new masstree::leafnode(0);
	masstree::masstree *tree = new masstree::masstree(init_root);

	// insert keys now
	tbb::parallel_for(tbb::blocked_range<uint64_t>(0, total_keys), [&](const tbb::blocked_range<uint64_t> &scope) {
		for (uint64_t i = scope.begin(); i != scope.end(); i++) {
			//std::cout << "Inserting key " << keysToInsert[i]->fkey << std::endl;
			success = tree->put((char *)keysToInsert[i]->fkey, keysToInsert[i]->value);
			if (! success){
				std::cout << "Inserting key " << keysToInsert[i]->fkey << " failed" << std::endl;
				it = find(insertedKeys.begin(), insertedKeys.end(), keysToInsert[i]);
				if ( it != insertedKeys.end()){
					insertedKeys.erase(it);
				}
			}
		}
	});


	inserted = insertedKeys.size();

	// delete keys now
	tbb::parallel_for(tbb::blocked_range<uint64_t>(delete_key_start, delete_key_start + num_delete_keys), [&](const tbb::blocked_range<uint64_t> &scope) {
		for (uint64_t i = scope.begin(); i != scope.end() ; i++) {
			//std::cout << "Deleting key " << keysToInsert[i]->fkey << std::endl;
			success = tree->del((char *)keysToInsert[i]->fkey);
			if (!success){
				std::cout << "Deleting key " << keysToInsert[i]->fkey << " failed" << std::endl;
				continue;
			}
			it = find(insertedKeys.begin(), insertedKeys.end(), keysToInsert[i]);
			if ( it != insertedKeys.end()){
				insertedKeys.erase(it);
			}
			//std::cout << "Removed key "<< keysToInsert[i]->fkey << std::endl;
			deleted ++;
		}
	});

      // Reading all keys for now
	tbb::parallel_for(tbb::blocked_range<uint64_t>(0, insertedKeys.size()), [&](const tbb::blocked_range<uint64_t> &scope) {
		for (uint64_t i = scope.begin(); i != scope.end(); i++) {
			//std::cout << " Searching for key " << insertedKeys[i]->fkey << std::endl;
			uint64_t *val = reinterpret_cast<uint64_t *> (tree->get((char *)insertedKeys[i]->fkey));
			if (!val){
				std::cout << "Missing key " << insertedKeys[i]->fkey << std::endl;
				errorStat.IncrementMissingValue();	
			}
			else if (*val != insertedKeys[i]->value) {
				std::cout << "[MASSTREE] Incorrect value : " << *val << " expected:" << insertedKeys[i]->value << std::endl;
				errorStat.IncrementIncorrectValue();
			}
			//std::cout << "Value for key " << insertedKeys[i]->fkey << " is " << *val << std::endl;
			searched ++;
		}
	});


	cout << "Total keys inserted = " << inserted << endl;
	cout << "Total keys deleted = " << deleted << endl;
	cout << "Total keys searched = " << searched << endl;

	gs->numCrashStates++;
	errorStat.PrintErrorStat();

	if(errorStat.IsError()) {
		gs->numFailedStates++;
		cout << "ERROR ENCOUNTERED IN THIS CRASH STATE" << endl;
	} else {
		cout << "Passed clean" << endl;
	}
	cout << "********* Global summary *********" << endl;
	cout << "Crash States : " << gs->numCrashStates << endl;
	cout << "Failed States : " << gs->numFailedStates << endl;
	cout << "-----------------------------------------------" << endl;

}

#ifdef HOT
void testHOT(int total_keys, int num_threads, bool perform_delete, int delete_key_start, int num_delete_keys,
		std::vector<Key *> &keysToInsert,
		std::vector<Key *> &insertedKeys) {

	fprintf(stderr, "\n-------Testing HOT with %d insert keys and %d deletes-----\n", total_keys, num_delete_keys);
	std::vector<Key *>::iterator it;
	int inserted = 0, deleted = 0, searched = 0;
	bool success;

	hot::rowex::HOTRowex<Key *, KeyExtractor> mTrie;

    // insert keys now
	tbb::parallel_for(tbb::blocked_range<uint64_t>(0, total_keys), [&](const tbb::blocked_range<uint64_t> &scope) {
		for (uint64_t i = scope.begin(); i != scope.end(); i++) {
			Key *key = key->make_leaf((char *)keysToInsert[i]->fkey, keysToInsert[i]->key_len, keysToInsert[i]->value);
			//std::cout << "Inserting key " << keysToInsert[i]->fkey << " : " << keysToInsert[i]->value << std::endl;
			success = mTrie.insert(keysToInsert[i]);
			if (!success){
				std::cout << "Inserting key " << i << " : " << keysToInsert[i]->fkey << " failed" << std::endl;
				it = find(insertedKeys.begin(), insertedKeys.end(), keysToInsert[i]);
                if ( it != insertedKeys.end()){
                    insertedKeys.erase(it);
                }
			}
		}
	});


	inserted = insertedKeys.size();


      // Reading all keys for now
	tbb::parallel_for(tbb::blocked_range<uint64_t>(0, insertedKeys.size()), [&](const tbb::blocked_range<uint64_t> &scope) {
		for (uint64_t i = scope.begin(); i != scope.end(); i++) {
			idx::contenthelpers::OptionalValue<Key *> val = mTrie.lookup((uint8_t *)insertedKeys[i]->fkey);
			//std::cout << " Value for key " << insertedKeys[i]->fkey << " is " << val.mValue->value << std::endl;
			if (! val.mIsValid){
				std::cout << "Missing key " << insertedKeys[i]->fkey << std::endl;
				errorStat.IncrementMissingValue();	
			}
			//std::cout << "Value for key " << insertedKeys[i]->fkey << " is " << val->value << std::endl;
			else if (val.mValue->value != insertedKeys[i]->value) {
				std::cout << "[HOT] Incorrect value : " << val.mValue->value << " expected:" << insertedKeys[i]->value << std::endl;
				errorStat.IncrementIncorrectValue();
			}
			searched ++;
		}
	});


	cout << "Total keys inserted = " << inserted << endl;
	cout << "Total keys deleted = " << deleted << endl;
	cout << "Total keys searched = " << searched << endl;

	gs->numCrashStates++;
	errorStat.PrintErrorStat();

	if(errorStat.IsError()) {
		gs->numFailedStates++;
		cout << "ERROR ENCOUNTERED IN THIS CRASH STATE" << endl;
	} else {
		cout << "Passed clean" << endl;
	}
	cout << "********* Global summary *********" << endl;
	cout << "Crash States : " << gs->numCrashStates << endl;
	cout << "Failed States : " << gs->numFailedStates << endl;
	cout << "-----------------------------------------------" << endl;

}
#endif


void testART(int total_keys, int num_threads, bool perform_delete, int delete_key_start, int num_delete_keys,
		std::vector<Key *> &keysToInsert,
		std::vector<Key *> &insertedKeys) {

	fprintf(stderr, "\n-------Testing ART with %d insert keys and %d deletes-----\n", total_keys, num_delete_keys);
	std::vector<Key *>::iterator it;
	int inserted = 0, deleted = 0, searched = 0;

	ART_ROWEX::Tree tree(loadKey);

    // insert keys now
	tbb::parallel_for(tbb::blocked_range<uint64_t>(0, total_keys), [&](const tbb::blocked_range<uint64_t> &scope) {
		auto t = tree.getThreadInfo();
		for (uint64_t i = scope.begin(); i != scope.end(); i++) {
			Key *key = key->make_leaf((char *)keysToInsert[i]->fkey, keysToInsert[i]->key_len, keysToInsert[i]->value);
			tree.insert(key, t);
		}
	});


	inserted = insertedKeys.size();

	// delete keys now
	tbb::parallel_for(tbb::blocked_range<uint64_t>(delete_key_start, delete_key_start + num_delete_keys), [&](const tbb::blocked_range<uint64_t> &scope) {
		auto t = tree.getThreadInfo();
		for (uint64_t i = scope.begin(); i != scope.end() ; i++) {
			Key *key = key->make_leaf((char *)keysToInsert[i]->fkey, keysToInsert[i]->key_len, keysToInsert[i]->value);
			tree.remove(key, t);
			it = find(insertedKeys.begin(), insertedKeys.end(), keysToInsert[i]);
			if ( it != insertedKeys.end()){
				insertedKeys.erase(it);
			}
			//std::cout << "Removed key "<< keysToInsert[i]->fkey << std::endl;
			deleted ++;
		}
	});

      // Reading all keys for now
	tbb::parallel_for(tbb::blocked_range<uint64_t>(0, insertedKeys.size()), [&](const tbb::blocked_range<uint64_t> &scope) {
		auto t = tree.getThreadInfo();
		for (uint64_t i = scope.begin(); i != scope.end(); i++) {
			Key *key = key->make_leaf((char *)insertedKeys[i]->fkey, insertedKeys[i]->key_len, insertedKeys[i]->value);
			Key *val = reinterpret_cast<Key *>(tree.lookup(key, t));
			if (!val){
				std::cout << "Missing key " << insertedKeys[i]->fkey << std::endl;
				errorStat.IncrementMissingValue();	
			}
			//std::cout << "Value for key " << insertedKeys[i]->fkey << " is " << val->value << std::endl;
			else if (val->value != insertedKeys[i]->value) {
				std::cout << "[ART] wrong key read: " << val->value << " expected:" << insertedKeys[i]->value << std::endl;
				errorStat.IncrementIncorrectValue();
			}
			searched ++;
		}
	});


	cout << "Total keys inserted = " << inserted << endl;
	cout << "Total keys deleted = " << deleted << endl;
	cout << "Total keys searched = " << searched << endl;

	gs->numCrashStates++;
	errorStat.PrintErrorStat();

	if(errorStat.IsError()) {
		gs->numFailedStates++;
		cout << "ERROR ENCOUNTERED IN THIS CRASH STATE" << endl;
	} else {
		cout << "Passed clean" << endl;
	}
	cout << "********* Global summary *********" << endl;
	cout << "Crash States : " << gs->numCrashStates << endl;
	cout << "Failed States : " << gs->numFailedStates << endl;
	cout << "-----------------------------------------------" << endl;

}


void testCCEH(int total_keys, int num_threads, bool perform_delete, int delete_key_start, int num_delete_keys,
		std::vector<uint64_t> &keysToInsert,
		std::vector<uint64_t> &insertedKeys) {

	fprintf(stderr, "\n-------Testing CCEH with %d insert keys and %d deletes-----\n", total_keys, num_delete_keys);
	std::vector<uint64_t>::iterator it;
	int inserted = 0, deleted = 0, searched = 0;
	bool success;

	// kNumSlot = 1024
	Hash *table = new CCEH((2 * 1024)/Segment::kNumSlot);

    // insert keys now
	tbb::parallel_for(tbb::blocked_range<uint64_t>(0, total_keys), [&](const tbb::blocked_range<uint64_t> &scope) {
		for (uint64_t i = scope.begin(); i != scope.end(); i++) {
			std::cout << "Key " << i << " : Inserting " << keysToInsert[i]  << std::endl;
			success = table->Insert(keysToInsert[i], reinterpret_cast<const char *>(&keysToInsert[i]));
			if (! success){
                std::cout << "Inserting key " << i << " : " << keysToInsert[i] << " failed" << std::endl;
                it = find(insertedKeys.begin(), insertedKeys.end(), keysToInsert[i]);
                if ( it != insertedKeys.end()){
                    insertedKeys.erase(it);
                }
            }
		}
	});

	inserted = insertedKeys.size();

	// delete keys now
	tbb::parallel_for(tbb::blocked_range<uint64_t>(delete_key_start, delete_key_start + num_delete_keys), [&](const tbb::blocked_range<uint64_t> &scope) {
		for (uint64_t i = scope.begin(); i != scope.end() ; i++) {
			success = table->Delete(keysToInsert[i]);
			if (!success){
				std::cout << "Deleting key " << i << " : " << keysToInsert[i] << " failed" << std::endl;
				continue;
			}
			it = find(insertedKeys.begin(), insertedKeys.end(), keysToInsert[i]);
			if ( it != insertedKeys.end()){
				insertedKeys.erase(it);
			}
			//std::cout << "Removed key "<< keysToInsert[i]->fkey << std::endl;
			deleted ++;
		}
	});

	std::cout << " Searching keys now" << std::endl;
      // Reading all keys for now
	tbb::parallel_for(tbb::blocked_range<uint64_t>(0, insertedKeys.size()), [&](const tbb::blocked_range<uint64_t> &scope) {
		for (uint64_t i = scope.begin(); i != scope.end(); i++) {
			const char* value = table->Get(insertedKeys[i]);
			if (value == NONE){
				std::cout << "Missing key " << insertedKeys[i] << std::endl;
				errorStat.IncrementMissingValue();	
				continue;
			}
			//uint64_t *val = reinterpret_cast<uint64_t *>(const_cast<char *>(table->Get(insertedKeys[i])));
			uint64_t *val = reinterpret_cast<uint64_t *>(const_cast<char*>(value));
			//std::cout << " Key " << i << " : Value for key " << insertedKeys[i]<< " is " << *val << std::endl;	
			if (*val != insertedKeys[i]) {
				std::cout << "[CLHT] Incorrect value: " << *val << " expected:" << insertedKeys[i] << std::endl;
				errorStat.IncrementIncorrectValue();
				continue;
			}
			searched ++;
		}
	});

	cout << "Total keys inserted = " << inserted << endl;
	cout << "Total keys deleted = " << deleted << endl;
	cout << "Total keys searched = " << searched << endl;

	gs->numCrashStates++;
	errorStat.PrintErrorStat();

	if(errorStat.IsError()) {
		gs->numFailedStates++;
		cout << "ERROR ENCOUNTERED IN THIS CRASH STATE" << endl;
	} else {
		cout << "Passed clean" << endl;
	}
	cout << "********* Global summary *********" << endl;
	cout << "Crash States : " << gs->numCrashStates << endl;
	cout << "Failed States : " << gs->numFailedStates << endl;
	cout << "-----------------------------------------------" << endl;

}




void testCLHT(int total_keys, int num_threads, bool perform_delete, int delete_key_start, int num_delete_keys,
		std::vector<uint64_t> &keysToInsert,
		std::vector<uint64_t> &insertedKeys) {


	fprintf(stderr, "\n-------Testing CLHT with %d insert keys and %d deletes-----\n", total_keys, num_delete_keys);
	std::vector<uint64_t>::iterator it;
	int inserted =0, deleted = 0, searched = 0;


	// start with 512 buckets	  
	clht_t *ht = clht_create(1);
	clht_gc_thread_init(ht, 0);
	ssmem_allocator_t *alloc = (ssmem_allocator_t *) malloc(sizeof(ssmem_allocator_t));
	ssmem_alloc_init_fs_size(alloc, SSMEM_DEFAULT_MEM_SIZE, SSMEM_GC_FREE_SET_SIZE, 0);

	// insert keys now
	for (int i = 0; i < total_keys; i++) {
		//std::cout << "Inserting key "<< keysToInsert[i] << std::endl;
		clht_put(ht, keysToInsert[i], keysToInsert[i]);
	}

	inserted = insertedKeys.size();

	// delete keys now
	for (int i = delete_key_start; i != delete_key_start + num_delete_keys ; i++) {
		uintptr_t val = clht_remove(ht, keysToInsert[i]);
		if (val != keysToInsert[i])
			continue;
		it = find(insertedKeys.begin(), insertedKeys.end(), keysToInsert[i]);
		if ( it != insertedKeys.end()){
			insertedKeys.erase(it);
		}
		//std::cout << "Removed key "<< keysToInsert[i]->fkey << std::endl;
		deleted ++;
	}

	// Reading all keys for now
	for (int i = 0; i < insertedKeys.size(); i++) {
		uintptr_t val = clht_get(ht->ht, insertedKeys[i]);
		if (val == 0){
			std::cout << "Missing key " << insertedKeys[i] << std::endl;
			errorStat.IncrementMissingValue();	
		}
		//cout << "Value for key " << insertedKeys[i] << " is " << val << endl;
		if (val != insertedKeys[i]) {
			std::cout << "[CLHT] Incorrect Value: " << val << " expected:" << insertedKeys[i] << std::endl;
			errorStat.IncrementIncorrectValue();
		}
		searched ++;
	}
	

	cout << "Total keys inserted = " << inserted << endl;
	cout << "Total keys deleted = " << deleted << endl;
	cout << "Total keys searched = " << searched << endl;

	gs->numCrashStates++;
	errorStat.PrintErrorStat();

	if(errorStat.IsError()) {
		gs->numFailedStates++;
		cout << "ERROR ENCOUNTERED IN THIS CRASH STATE" << endl;
	} else {
		cout << "Passed clean" << endl;
	}
	cout << "********* Global summary *********" << endl;
	cout << "Crash States : " << gs->numCrashStates << endl;
	cout << "Failed States : " << gs->numFailedStates << endl;
	cout << "-----------------------------------------------" << endl;

}


int main(int argc, char** argv)
{
  	// Parsing arguments
	int total_keys = 1, delete_key_start = 0, num_delete_keys = 0;
	int num_threads = 1;
	bool perform_delete = false;
 	std::string input_keyfile = std::string("../sample.txt").data();
	print_flag = true;
	int index = 0;
    int key_type = 0;

  	int c;
  	while((c = getopt(argc, argv, "n:t:i:d:m:p:k:")) != -1) {
    	switch(c) {
      		case 'n':
        		total_keys = atoi(optarg);
        		break;
      		case 't':
        		num_threads = atoi(optarg);
        		break;
			case 'd':
				cout << "In delete option" << endl;
				perform_delete = true;
				delete_key_start = atoi(optarg);
				break;
			case 'm':
				num_delete_keys = atoi(optarg);
				break;
      			case 'i':
        			input_keyfile = optarg;
				break;
			case 'p':
				index = indexType(optarg);
				if (index == -1){
					fprintf(stderr, "Unknown index type %s\n", optarg);
					exit(1);
				}
				break;
			case 'k':
				key_type = keyType(optarg);
				if (key_type == -1){
					fprintf(stderr, "Unknown key type %s\n", optarg);		
					exit(1);
				}	
				break;	
      		default:
        		break;
    	}
  	}
	

	
    // ftok to generate unique key 
    key_t shmkey = ftok("shmfile",65); 
  
    // shmget returns an identifier in shmid 
    int shmid = shmget(shmkey, 1024, 0666|IPC_CREAT); 
  
    // shmat to attach to shared memory 
    gs = (globalSummary*) shmat(shmid, (void*)0, 0); 

	//initialize before the start of any process
	gs->numCrashStates = 0;
	gs->numFailedStates = 0;  
      

	// hashmap of inserted keys to facilitate deletes and search after crash
	/*map<string, string> keysInserted;
	map<string, string>::iterator it;
	*/

    int count = 0;
	ifstream ifs(input_keyfile);
  	cout << "Reading keys from " << input_keyfile << endl;
  	if(!ifs) {
    	cout << "Input key load error" << endl;
    	exit(-1);
  	}

	if (key_type == STRING_KEY){

		std::vector<Key *> keysToInsert;
		keysToInsert.reserve(total_keys);
		memset(&keysToInsert[0], 0x00, total_keys * sizeof(Key *));

		std::vector<Key *> insertedKeys;
		insertedKeys.reserve(total_keys);
		memset(&insertedKeys[0], 0x00, total_keys * sizeof(Key *));


   		//Preload keys into the array
    	std::string key;
    	uint64_t val;
    	while ((count < total_keys) && ifs.good()) {
        	ifs >> key;
        	val = std::stoul(key.substr(4, key.size()));
        	keysToInsert.push_back(keysToInsert[count]->make_leaf((char *)key.c_str(), key.size()+1, val));
        	count++;
    	}
		fprintf(stdout, "Loaded %d keys\n", count);
    
    	for (int i = 0; i < keysToInsert.size(); i++)
        	insertedKeys.push_back(keysToInsert[i]);

		// init scheduler
		tbb::task_scheduler_init init(num_threads);

		// if type ART
		if (index == TYPE_PART)
			testART(total_keys, num_threads, perform_delete, delete_key_start, num_delete_keys, keysToInsert, insertedKeys);
		else if (index == TYPE_PMASSTREE)
			testMASSTREE(total_keys, num_threads, perform_delete, delete_key_start, num_delete_keys, keysToInsert, insertedKeys);
#ifdef HOT
		else if (index == TYPE_PHOT)
			testHOT(total_keys, num_threads, perform_delete, delete_key_start, num_delete_keys, keysToInsert, insertedKeys);
#endif
		else {
			fprintf(stderr, "\nUnknown combination %s, %s\n", typeIndexName[index], typeKeyName[key_type]);
			exit(0);
		}
	}

	else {
		std::vector<uint64_t> keysToInsert;
		keysToInsert.reserve(total_keys);
		memset(&keysToInsert[0], 0x00, total_keys * sizeof(uint64_t));

		std::vector<uint64_t> insertedKeys;
		insertedKeys.reserve(total_keys);
		memset(&insertedKeys[0], 0x00, total_keys * sizeof(uint64_t));

		std::vector<uint64_t>::iterator it;
		
		// Preload keys into the array
		uint64_t key;
		while ((count < total_keys) && ifs.good()) {
			ifs >> key;
			keysToInsert.push_back(key);
			count++;
		}
		fprintf(stdout, "Loaded %d keys\n", count);
		
		for (int i = 0; i < keysToInsert.size(); i++)
			insertedKeys.push_back(keysToInsert[i]);


		// init scheduler
		tbb::task_scheduler_init init(num_threads);

		if (index == TYPE_PCLHT)
			testCLHT(total_keys, num_threads, perform_delete, delete_key_start, num_delete_keys, keysToInsert, insertedKeys);
		
		if (index == TYPE_CCEH)
			testCCEH(total_keys, num_threads, perform_delete, delete_key_start, num_delete_keys, keysToInsert, insertedKeys);
		
		else {
			fprintf(stderr, "\nUnknown combination %s, %s\n", typeIndexName[index], typeKeyName[key_type]);
			exit(0);
		}
	}
	


	// detach every terminating process from shared memory
  	shmdt(gs);
	return 0;
}
