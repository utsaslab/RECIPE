## PMDK

This branch of P-CLHT uses PMDK to ensure the persistence and recoverability of the persistent cache-line hash table. All other details of this data structure are the same (cache line flushing, alignment, etc) except for the backend library used to ensure persistence.

**Motivation** The current implementation does not have a way of recovering permanent memory leaks during a crash. The PMDK library, specifically `libpmemobj`, gives us useful internal structures such as `pmemobj_root`, which is a stored offset within the persistent memory pool that can be used to recover any data that was left in a partial state, etc.

**How We Used PMDK** The entire conversion required us to replace any data structure pointers to point to the persistent memory pool using the non-transactional, atomic allocation functions such as `pmemobj_alloc`. Since the `PMEMoid` structs (which store the pool offset and id) were 16 bytes, some code manipulation was required to ensure the cache-line alignment of the data structure. Finally, transactions were used for major data structure modifications. This part is still being tested and is a work-in-progress. 

**How to test recoverability?** The best way to recover your hashtable is following the paradigm presented in `clht_open` within the P-CLHT implementation, where all the user has to do is use `pmemobj_root` to recover the root (a clht_t object basically) of the persistent memory pool. Please make sure that you are opening the same pool with the correct pool layout! 

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

3. Set pool_size and pool name appropriately using `pmemobj_open` or `pmemobj_create`.

4. Make accordingly and run your code.