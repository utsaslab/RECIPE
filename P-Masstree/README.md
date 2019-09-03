## P-Masstree: Persistent Masstree

`P-Masstree` is a crash consistent version of [Masstree](https://dl.acm.org/citation.cfm?id=2168855) that is a cache-efficient, highly concurrent trie-like 
concatenation of B+Tree nodes. The implementation of `P-Masstree` is based on the compact variant of Masstree where the original internal nodes are replaced 
with the same structure of leaf nodes. Compared with original Masstree, `P-Masstree` has some limitations in providing various functionalities; for example, 
differently from original Masstree, `P-Masstree` does not support storing duplicated keys. `P-Masstree` is an ordered index supporting both point and range queries. 

**Conversion**. In order to apply RECIPE approach to Masstree, we first modify the B+tree structure of original Masstree to be B-link tree by extending leaf node 
structures across all levels. After this, Masstree is converted into `P-Masstree` by adding crash detection using `try lock` and recovery mechanisms reusing node split 
algorithms, along with cache line flushes and memory fences.

**Performance**. Compared with [FAST&FAIR](https://www.usenix.org/conference/fast18/presentation/hwang) that is a state-of-the-art ordered index, 
`P-Masstree` shows **1.51x**, **1.34x**, **1.06x** better performance in YCSB Load and workload A, B respectively using random integer keys while 
similar in workload C and **0.75x** worse in workload E.

**Support**. `P-Masstree` supports Insert, Update, Delete, Point Lookup, and Range Scan operations. 
Each operation works for both integer and string keys.

**Use Case**. `P-Masstree` provides the well-balanced performance of insertion, lookup, and range scan operations regardless of integer or string keys.
`P-Masstree` would be suitable for the application using workloads consisting of variable length keys.

**Maturity**. `P-Masstree` is still in alpha state. The basic functionalities are working, but performance is not fully optimized, and more work to 
stabilize it is required. Use it at your own risk, but we will improve it soon.

## Build & Run

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
