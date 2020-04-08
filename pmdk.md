## PMDK Version

The `pmdk` branch of RECIPE uses PMDK to ensure the persistence and recoverability of all RECIPE data structures. All other details of this data structure are the same (cache line flushing, alignment, etc) except for the backend library used to ensure persistence.

**Motivation** The current implementation does not have a way of recovering permanent memory leaks during a crash. The PMDK library, specifically `libpmemobj`, gives us useful internal structures such as `pmemobj_root`, which is a stored offset within the persistent memory pool that can be used to recover any data that was left in a partial state, etc.

**How We Used PMDK** The entire conversion required us to replace any data structure pointers to point to the persistent memory pool using the non-transactional, atomic allocation functions such as `pmemobj_alloc`. Since the `PMEMoid` structs (which store the pool offset and id) were 16 bytes, some code manipulation was required to ensure the cache-line alignment of the data structure. Finally, transactions were used for major data structure modifications. This part is still being tested and is a work-in-progress. 

**How to test recoverability?** The best way to recover your hashtable is following the paradigm presented in `clht_open` within the P-CLHT implementation, where all the user has to do is use `pmemobj_root` to recover the root (a clht_t object basically) of the persistent memory pool. Please make sure that you are opening the same pool with the correct pool layout! Here's an example code snippet:
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

**Limitations** Currently, CLHT is the only data structure that has been converted to PMDK. We plan on updating the other data structures in the near future.

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

3. Set pool_size and pool name appropriately using `pmemobj_open` or `pmemobj_create`. For example:
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

4. Make accordingly and run your code.