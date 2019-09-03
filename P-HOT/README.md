## P-HOT: Persistent Height-Optimized Trie

`P-HOT` is a crash consistent version of [Height-Optimized Trie](https://dl.acm.org/citation.cfm?id=3196896) (HOT). 
HOT is a lookup and space-optimized variant of a trie. HOT achieves cache efficiency, dynamically varying the number of
prefix bits mapped by a node to maintain consistent high fanout. HOT is an ordered index supporting both point and range queries.


**Conversion**. HOT is converted into `P-HOT` by adding cache line flushes and memory fences after each critical volatile store.

**Performance**. Compared with [FAST&FAIR](https://www.usenix.org/conference/fast18/presentation/hwang) that is a state-of-the-art 
ordered index, `P-HOT` shows **1.4x**, **1.27x**, **1.34x**, and **1.49x** better performance in YCSB Load and workload A, B, C respectively
using random integer keys while **0.70x** worse in workload E.

**Support**. `P-HOT` provides Insert, Update, Point Lookup, and Range Scan operations. Each operation works for both integer and string keys.
However, note that the length of string keys are restricted to 255 bytes.

**Use Case**. `P-HOT` would be useful to be employed for the applications consisting of read-dominated workloads mixed with point and range queries.
`P-HOT` would not be suitable for insertion-dominated workloads as it requires excessive cache line flushes for persistency due to copy-on-write scheme.

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
