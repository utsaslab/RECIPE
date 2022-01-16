CLHT
====

CLHT is a very fast and scalable concurrent, resizable hash table. It comes in two (main) variants, a lock-based and a lock-free.
The main idea behind CLHT is to, if possible, use cache-line-sized buckets so that update operations (`put` and `remove`) complete with at most one cache-line transfer. Furthermore, CLHT is based on two ideas:
  1. operations parse (go through) the elements of a bucket taking a snapshot of each key/value pair,
  2. `get` operations are read-only (i.e., they do not perform any stores) and are wait-free (i.e., they never have to restart), and 
  2. update operations perform in place updates, hence they do not require any memory allocation.

The result is very low latency operations. For example, a single thread on an Intel Ivy Bridge processor achieves the following latencies (in cycles):
  * srch-suc: 58
  * srch-fal: 21
  * insr-suc: 74
  * insr-fal: 62
  * remv-suc: 91
  * remv-fal: 19

This translates to more than 50 Mega-operations/sec in our benchmarks (the actual throughput of CLHT is even higher, but the benchmarking code takes a significant portion of the execution).

In concurrent settings, CLHT gives more than 2 Billion-operations/sec with read-only executions on 40 threads on a 2-socket Ivy Bridge, more than 1 Billion-operations/sec with 1% updates, 900 Mega-operations/sec with 10%, and 300 Mega-operations/sec with update operations only.


* Website             : http://lpd.epfl.ch/site/ascylib
* Author              : Vasileios Trigonakis <github@trigonakis.com>
* Related Publications: CLHT was developed for:
  *Asynchronized Concurrency: The Secret to Scaling Concurrent Search Data Structures*,
  Tudor David, Rachid Guerraoui, Vasileios Trigonakis (alphabetical order),
  ASPLOS '15

You can also find a detailed description of the algorithms and correctness sketches in the following technical report: https://infoscience.epfl.ch/record/203822

CLHT-LB (Lock-based version)
----------------------------

CLHT-LB synchronizes update operations (`put` and `remove`) with locks. In short, an update first checks whether the operation can be successful (i.e., if the given key exists for a removal, or if the key is not already in there for an insertion), and if it is, it grabs the corresponding lock and performs the update.

If a `put` find the bucket full, then either the bucket is expanded (linked to another bucket), or the hash table is resized (for the variants of CLHT-LB that support resizing).

We have implemented the following variants of CLHT-LB:
  1. `clht_lb_res`: the default version, supports resizing. 
  2. `clht_lb`: as (1), but w/o resizing.
  3. `clht_lb_res_no_next`: as (1), but the hash table is immediately resized when there is no space for a `put`.
  4. `clht_lb_packed`: as (2), but elements are "packed" in the first slots of the bucket. Removals move elements to avoid leaving any empty slots.
  5. `clht_lb_linked`: as (1), but the buckets are linked to their next buckets (b0 to b1, b1 to b2, ...), so that if there is no space in a bucket, the next one is used. If the hash table is too full, it is resized.
  6. `clht_lb_lock_ins`: as (2), but `remove` operations do not lock. The work using `compare-and-swap` operations.

CLHT-LF (Lock-free version)
---------------------------

CLHT-LF synchronizes update operations (`put` and `remove`) in a lock-free manner. Instead of locks, CLHT-LF uses the `snapshot_t` structure. `snapshot_t` is an 8-byte structure with two fields: a version number and an array of bytes (a map). The map is used to indicate whether a key/value pair in the bucket is valid, invalid, or is being inserted. The version field is used to indicate that the `snapshot_t` object has been changed by a concurrent update.

We have implemented the following variants of CLHT-LF:
  1. `clht_lf_res`: the default version, supports resizing.
  2. `clht_lf`: as (1), but w/o resizing. NB. CLHT-LF cannot expand/link bucket, thus, if there is not enough space for a `put`, the operation might never complete.
  3. `clht_lf_only_map_rem`: as (2), but `remove` operations do not increment the `snapshot_t`'s version number.


Compilation
-----------

You can install the dependencies of CLHT with `make dependencies`.

CLHT requires the ssmem memory allocator (https://github.com/LPD-EPFL/ssmem).
Clone ssmem, do `make libssmem.a` and then copy `libssmem.a` in `CLHT/external/lib` and `include/smmem.h` in `CLHT/external/include`.

Additionally, the sspfd profiler library is required (https://github.com/trigonak/sspfd).
Clone sspfd, do `make` and then copy `libsspfd.a` in `CLHT/external/lib` and `include/sspfd.h` in `CLHT/external/include`.

You can compile the different variants of CLHT with the corresponding target. For example:
`make libclht_lb_res.a` will build the `clht_lb_res` version.

The compilation always produces the `libclht.a`, regardless of the variant that is built.

Various parameters can be set for each variant in the corresponding header file (e.g., `include/clht_lb_res.h` for the `clht_lb_res` version).

To make a debug build of CLHT, you can do `make target DEBUG=1`.

Tests
-----

CLHT comes with various microbenchmarks. `make all` builds a performance benchmark for each variant of CLHT.
You can control which test file to be used in Makefile by changing `MAIN_BMARK` variable.
You can set it to:
  * `test.c`: for a test w/o memory allocation for the values
  * `test_mem.c`: for a test with memory allocation for the values
  * `test_ro.c`: for a read-only test
  
Call any of the executables with `-h` to get the available options of the tests.

Additionally, you can build `make rest` that builds some correctness tests.


Using CLHT
----------

To use CLHT you need to include the `clht.h` file in your source code and then link with `-lclht -lssmem` (ssmem is used in the CLHT implementation).

The following functions can be used to create and use a new hash table:
  * `clht_t* clht_create(uint32_t num_buckets)`: creates a new CLHT instance.
  * `void clht_gc_thread_init(clht_t* hashtable, int id)`: initializes the GC for hash table resizing. Every thread should make this call before using the hash table.
  * `void clht_gc_destroy(clht_t* hashtable)`: frees up the hash table
  * `clht_val_t clht_get(clht_hashtable_t* hashtable, clht_addr_t key)`: gets the value for a give key, or return 0
  * `int clht_put(clht_t* hashtable, clht_addr_t key, clht_val_t val)`: inserts a new key/value pair (if the key is not already present)
  * `clht_val_t clht_remove(clht_t* hashtable, clht_addr_t key)`: removes the key from the hash table (if the key is present)
  * `void clht_print(clht_hashtable_t* hashtable)`: prints the hash talble
  * `const char* clht_type_desc()`: return the type of CLHT. For example, CLHT-LB-RESIZE.


Details
-------

### Resizing  

Hash-table resizing is implemented in a pessimistic manner, both on CLHT-LB and CLHT-LF. Thus, CLHT-LF resizing is not lock-free.

On CLHT-LB resizing is pretty straightforward: lock and then copy each bucket. Concurrent `get` operations can proceed while resizing is ongoing. CLHT-LB supports *helping* (i.e., other threads than the one starting the resizing help with the procedure). Helping is controlled by the `CLHT_HELP_RESIZE` define in the `clht_lb_res.h` file. In our experiments, helping proved beneficial only on huge hash tables. Due to the structure of CLHT-LB, copying data is very fast, as the buckets are an array in memory.

On CLHT-LF resizing is implemented with a global lock. To resize a thread grabs the lock and waits for all threads to indicate that they are "aware" that a resize is in progress. This is done by a thread-local flag that indicates whether there is an ongoing update operation on the current hash table or not.
