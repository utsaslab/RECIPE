## P-BwTree: Persistent BwTree

`P-BwTree` is a crash consistent version of [BwTree](https://ieeexplore.ieee.org/abstract/document/6544834). 
BwTree is a variant of B+tree that provides non-blocking reads and writes for synchronization. 
It increases concurrency by prepending delta records to nodes. It uses a mapping table that enables
atomically installing delta updates using a single Compare-And-Swap (CAS) operation.
BwTree is an ordered index supporting both point and range queries.

**Conversion**. BwTree is converted into `P-BwTree` by adding cache line flushes and memory fences after each critical volatile store. 

**Performance**. Compared with [FAST&FAIR](https://www.usenix.org/conference/fast18/presentation/hwang) that is a state-of-the-art ordered index, 
`P-BwTree` shows similar performance to `FAST&FAIR` in all YCSB workloads using random integer keys (**1.13x**, **1.04x**, **0.93x**, **0.92x** 
and **0.92x** performance in Load and workload A, B, C, E respectively).

**Use case**. `P-BwTree` can provide the well-balanced performance of insertion, lookup, and range scan operations in workloads using integer keys.
However, it is not suitable to be applied for the application employing string keys due to the inefficiencies caused by the overhead of string 
comparison and additional pointer dereferences.


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
