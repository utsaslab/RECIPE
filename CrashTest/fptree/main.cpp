#include <future>
#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <malloc.h>
#include <vector>
#include <time.h>
#include <unistd.h>
#include "FPTree.h"

using namespace std;

//#define INPUT_NUM	1000000
int latency=0;

extern unsigned long long search_time_in_insert;
extern unsigned long long clflush_time_in_insert;
extern unsigned long gettime_cnt;
int main(int argc,char**argv)
{
	struct timespec t1, t2,tmp;
	int perf,INPUT_NUM=0;
	char *dummy;
	unsigned long *keys, *new_value;
	unsigned long elapsed_time;
	FILE *fp;
	unsigned long max;
	unsigned long min;
  INPUT_NUM=atoi(argv[1]);
  latency = atoi(argv[2]);
  int n_threads = atoi(argv[3]);

	if ((fp = fopen("/home/okie90/nvram/clf-btree/input/input_1b.txt","r")) == NULL)
	{
		puts("error");
		exit(0);
	}

	keys = (unsigned long *)malloc(sizeof(unsigned long) * INPUT_NUM);
	for(int i = 0; i < INPUT_NUM; i++) {
//		keys[i] = i;
		fscanf(fp, "%lu", &keys[i]);
	}
	fclose(fp);

	max = keys[0];
	min = keys[0];
	for(int i = 1; i < INPUT_NUM; i++) {
		if (keys[i] > max)
			max = keys[i];
		if (keys[i] < min)
			min = keys[i];
	}

	tree *t = initTree();

	/* Insertion */
  dummy = (char *)malloc(15*1024*1024);
  memset(dummy, 0, 15*1024*1024);
  flush_buffer_nocount((void *)dummy, 15*1024*1024, true);
  search_time_in_insert = 0;
  clflush_time_in_insert = 0;
  gettime_cnt=0;
  clflush_count=0;
  clock_gettime(CLOCK_MONOTONIC, &t1);

  long half_num_data = INPUT_NUM / 2;

  // Warm-up! Insert half of input size
  for(int i=0;i<half_num_data;i++)
		Insert(t, keys[i], &keys[i]);

	clock_gettime(CLOCK_MONOTONIC, &t2);
	elapsed_time = (t2.tv_sec - t1.tv_sec) * 1000000000;
	elapsed_time += (t2.tv_nsec - t1.tv_nsec);
	printf("Insertion Time = %lu us\n",elapsed_time/1000);
	//printf("search time in Insert= %lu us\n",search_time_in_insert/1000);
  //printf("clflush time in Insert= %lu us\n",clflush_time_in_insert/1000);
  //printf("Total space = %lu byte\n", (IN_count * sizeof(IN)) + (LN_count * sizeof(LN)));

  clock_gettime(CLOCK_MONOTONIC, &t1);
  for(int i = 0; i < gettime_cnt; i++) {
    clock_gettime(CLOCK_MONOTONIC, &tmp);
  }
  clock_gettime(CLOCK_MONOTONIC, &t2);

  elapsed_time = (t2.tv_sec - t1.tv_sec) * 1000000000;
  elapsed_time += (t2.tv_nsec - t1.tv_nsec);
	//printf("gettime overhead = %lu us\n",elapsed_time/1000);
	//printf("clflush count = %lu\n", clflush_count);
	//printf("LN count = %lu\n", LN_count);
	printf("sizeof(LN) = %d\n", sizeof(LN));
	printf("sizeof(IN) = %d\n", sizeof(IN));

  vector<future<void>> futures(n_threads);

#ifndef MIXED
  // Search
  long data_per_thread = half_num_data / n_threads;
  clock_gettime(CLOCK_MONOTONIC,&t1);

  for(int tid = 0; tid < n_threads; tid++) {
    int from = data_per_thread * tid;
    int to = (tid == n_threads - 1) ? half_num_data : from + data_per_thread;

    auto f = async(launch::async, [&t, &keys](int from, int to){
      for(int i = from; i < to; i++) {
		    void *ret = Lookup(t, keys[i]);
        if(ret == NULL)
          cout << keys[i] << ": NOT FOUND" << endl;
      }
      }, from, to);
    futures.push_back(move(f));
  }
  for(auto &&f : futures) 
    if(f.valid())
      f.get();
    
  clock_gettime(CLOCK_MONOTONIC,&t2);
  elapsed_time = (t2.tv_sec-t1.tv_sec)*1000000000 + (t2.tv_nsec-t1.tv_nsec);
  cout<<"Concurrent searching with " << n_threads << " threads (usec) : "<< elapsed_time / 1000 << endl; 
  
  // Remove cache
	memset(dummy, 0, 15*1024*1024);
	flush_buffer_nocount((void *)dummy, 15*1024*1024, true);

  futures.clear();

  // Insert
  clock_gettime(CLOCK_MONOTONIC,&t1);

  for(int tid = 0; tid < n_threads; tid++) {
    int from = half_num_data + data_per_thread * tid;
    int to = (tid == n_threads - 1) ? INPUT_NUM : from + data_per_thread;

    //cout << "thread " << tid << " from " << from << " to " << to << endl;

    auto f = async(launch::async, [&t, &keys](int from, int to){
      for(int i = from; i < to; i++)
		    Insert(t, keys[i], &keys[i]);
      }, from, to);
    futures.push_back(move(f));
  }
  for(auto &&f : futures) 
    if(f.valid())
      f.get();
    
  clock_gettime(CLOCK_MONOTONIC,&t2);
  elapsed_time = (t2.tv_sec-t1.tv_sec)*1000000000 + (t2.tv_nsec-t1.tv_nsec);
  cout<<"Concurrent inserting with " << n_threads << " threads (usec) : "<< elapsed_time / 1000 << endl; 
#else
  // Mixed Workload
  float ratio = 0.5;
  if(argv[4] != NULL)
    ratio = atof(argv[4]);

/*
  long divided_data = half_num_data / n_threads;

  for(int tid = 0; tid < n_threads; tid++) {
    int from = half_num_data + divided_data * tid;
    int to = (tid == n_threads - 1) ? INPUT_NUM : from + divided_data;

    cout << "insert thread " << tid << " from " << from << " to " << to << endl;

    if(tid % 2 == 0) {
      auto f = async(launch::async, [&t, &keys, &half_num_data](int from, int to){
        for(int i = from; i < to; i++) {
		      Insert(t, keys[i], &keys[i]);
          void *ret = Lookup(t, keys[i-half_num_data]);
          if(ret == NULL)
            cout << keys[i] << ": NOT FOUND" << endl;
        }
      }, from, to);
      futures.push_back(move(f));
    }
    else {
      auto f = async(launch::async, [&t, &keys, &half_num_data](int from, int to){
        for(int i = from; i < to; i++) {
		      void *ret = Lookup(t, keys[i-half_num_data]);
          if(ret == NULL)
            cout << keys[i] << ": NOT FOUND" << endl;
		      Insert(t, keys[i], &keys[i]);
          }
      }, from, to);
      futures.push_back(move(f));
    }
  }

  for(auto &&f : futures) 
    if(f.valid())
      f.get();
      */
  long divided_data = half_num_data / n_threads;
  void *ret = NULL;
  int idx = 0;

  for(int tid = 0; tid < n_threads; tid++) {
    int from = half_num_data + divided_data * tid;
    int to = (tid == n_threads - 1) ? INPUT_NUM : from + divided_data;

    auto f = async(launch::async, [&t, &keys, &half_num_data, &ret, &idx]  
      (int from, int to) mutable {
        for(int i = from; i < to; i++) {
          int sidx = i - half_num_data;

          int jid = i % 4;
          switch(jid) {
            case 0:
              Insert(t, keys[i], (char*)keys[i]);
              for(int j = 0; j < 4; j++) {
                idx = (sidx + j + jid * 8) % half_num_data;
                ret = Lookup(t, keys[idx]);
                if(ret == NULL) printf("NOT FOUND: %s\n", keys[idx]);
              }
              break;
            case 1:
              for(int j = 1; j < 4; j++) {
                idx = (sidx + j + jid * 8) % half_num_data;
                ret = Lookup(t, keys[idx]);
                if(ret == NULL) printf("NOT FOUND: %s\n", keys[idx]);
              }

              Insert(t, keys[i], (char*) keys[i]);

              idx = (sidx + 0 + jid * 8) % half_num_data;
              ret = Lookup(t, keys[idx]);
              if(ret == NULL) printf("NOT FOUND: %s\n", keys[idx]);
              break;
            case 2:
              for(int j = 0; j < 2; j++) {
                idx = (sidx + j + jid * 8) % half_num_data;
                ret = Lookup(t, keys[idx]);
                if(ret == NULL) printf("NOT FOUND: %s\n", keys[idx]);
              }

              Insert(t, keys[i], (char*) keys[i]);

              for(int j = 2; j < 4; j++) {
                idx = (sidx + j + jid * 8) % half_num_data;
                ret = Lookup(t, keys[idx]);
                if(ret == NULL) printf("NOT FOUND: %s\n", keys[idx]);
              }
              break;
            case 3:
              for(int j = 0; j < 4; j++) {
                idx = (sidx + j + jid * 8) % half_num_data;
                ret = Lookup(t, keys[idx]);
                if(ret == NULL) printf("NOT FOUND: %s\n", keys[idx]);
              }

              Insert(t, keys[i], (char*) keys[i]);
              break;
            default:
              break;  
          }
        }
    }, from, to);
    futures.push_back(move(f));
  }

  for(auto &&f : futures) 
    if(f.valid())
      f.get();

  clock_gettime(CLOCK_MONOTONIC,&t2);
  elapsed_time = (t2.tv_sec-t1.tv_sec)*1000000000 + (t2.tv_nsec-t1.tv_nsec);
  cout<<"Concurrent inserting and searching with " << n_threads << " threads (usec) : "<< elapsed_time / 1000 << endl; 
#endif

	// Lookup
	memset(dummy, 0, 15*1024*1024);
	flush_buffer_nocount((void *)dummy, 15*1024*1024, true);
	clock_gettime(CLOCK_MONOTONIC, &t1);

	for(int i = 0; i < INPUT_NUM; i++)  {
    void *ret = Lookup(t, keys[i]);
    if(ret == NULL)
      cout << keys[i] << ": NOT FOUND" << endl;
  }
	
	clock_gettime(CLOCK_MONOTONIC, &t2);
	elapsed_time = (t2.tv_sec - t1.tv_sec) * 1000000000;
	elapsed_time += (t2.tv_nsec - t1.tv_nsec);
	printf("Search Time = %lu ns\n", elapsed_time);
	printf("Avg. depth = %lu\n", sum_depth / INPUT_NUM);
	printf("Total sum of depth = %lu\n", sum_depth);

	memset(dummy, 0, 15*1024*1024);
	flush_buffer_nocount((void *)dummy, 15*1024*1024, true);
	clock_gettime(CLOCK_MONOTONIC, &t1);

  //free(dummy);
  //free(keys);
	return 0;
}
