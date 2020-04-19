## P-CLHT: Persistent Cache-Line Hash Table - PMDK

`P-CLHT` is a recoverable and crash-consistent version of [Cache-Line Hash Table](https://dl.acm.org/citation.cfm?id=2694359) (CLHT).
CLHT is a cache-friendly hash table which restricts each bucket to be of the size of a cache line. 
CLHT is an unordered index only supporting point queries.

**Conversion**. `CLHT-LB` using lock-based writes for concurrency is converted into `P-CLHT` by adding cache line flushes and memory fences after each critical volatile store. 

**Performance**. Compared with [CCEH](https://www.usenix.org/conference/fast19/presentation/nam) that is a 
state-of-the-art unordered index, `P-CLHT` shows **2.38x**, **1.35x**, and **1.25x** better performance in 
YCSB workload A, B, C respectively using random integer keys while **0.37x** worse in Load workload.

**Support**. `P-CLHT` supports Insert, Delete, and Point Lookup operations. Each operation works for only integer keys.

**Use Case**. `P-CLHT` provides the superior performance of insertion and point lookup, even if not supporting
range scans. Therefore, it would be appropriate to be used for the applications only consisting of point queries.

This branch of P-CLHT also uses PMDK to ensure the persistence and recoverability of the cache-line hash table. All other details of this data structure are the same (cache line flushing, alignment, etc) except for the backend library used to ensure persistence.

**Motivation** The published implementation does not have a way of recovering permanent memory leaks during a crash. The PMDK library, specifically `libpmemobj`, gives us useful internal structures such as `pmemobj_root`, which is a stored offset within the persistent memory pool that can be used to recover any data that was left in a partial state, etc.

**How We Used PMDK** The entire conversion required us to replace any data structure pointers to point to the persistent memory pool using the non-transactional, atomic allocation functions such as `pmemobj_alloc`. Since the `PMEMoid` structs (which store the pool offset and id) were 16 bytes, some code manipulation was required to ensure the cache-line alignment of the data structure. Finally, transactions were used for major hashtable operations such as insertion, resizing, and deletion. This part is still being tested and is a work-in-progress. If you look through the code and compare it with the `master` branch, you can see that the changes follow a logical pattern, and the modifications are relatively minor. 

**How to test recoverability?** The best way to recover your hashtable is following the paradigm presented in `clht_open` where all the user has to do is use `pmemobj_root` to recover the root (a clht_t object basically) of the persistent memory pool. Please make sure that you are opening the same pool with the correct pool layout! 
```
...
PMEMoid my_root = pmemobj_root(pop, sizeof(clht_t));
if (pmemobj_direct(my_root) == NULL)
{
    perror("root pointer is null\n");
} 
...
clht_t* w = pmemobj_direct(my_root);
...
```

## Build & Run
### How to enable PM?
1. Install PMDK
```$ git clone https://github.com/pmem/pmdk.git 
$ cd pmdk
$ git checkout tags/1.6
$ make -j
$ cd ..  
```
2. Emulate PM with Ext4-DAX mount
```$ sudo mkfs.ext4 -b 4096 -E stride=512 -F /dev/pmem0
$ sudo mount -o dax /dev/pmem0 /mnt/pmem
```

3.  Set pool_size and pool name appropriately using `pmemobj_create`. For example:
```
// Size of the memory pool
size_t pool_size = 2*1024*1024*1024UL;
if( access("/mnt/pmem/pool", F_OK ) != -1 ) 
{
    // If the pool already exists, open it
    pop = pmemobj_open("/mnt/pmem/pool", POBJ_LAYOUT_NAME(clht));
} else 
{
    // If the pool does not exist, create it
    pop = pmemobj_create("/mnt/pmem/pool", POBJ_LAYOUT_NAME(clht), pool_size, 0666);
}
```

4. Make accordingly and run the example. 

#### Build

```
$ mkdir build
$ cd build
$ cmake ..
$ make -j
```

#### Run

```
$ ./example 10000 4

usage: ./example [n] [nthreads]
n: number of keys (integer)
nthreads: number of threads (integer)
```