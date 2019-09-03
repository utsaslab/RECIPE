## P-ART: Persistent Adaptive Radix Tree

`P-ART` is a crash consistent version of [Adaptive Radix Tree](https://dl.acm.org/citation.cfm?id=2933352) 
(ART). ART is a radix tree variant reducing space consumption by adaptively varying node sizes 
based on varid key entries. `P-ART` (ART) is an ordered index supporting both point and range queries.

**Conversion**. `ART` using `ROWEX` for concurrency is converted into `P-ART` by applying crash detection 
and recovery mechanisms, along with cache line flushes and memory fences.

**Performance**. Compared with [FAST&FAIR](https://www.usenix.org/conference/fast18/presentation/hwang) 
that is a state-of-the-art ordered index, `P-ART` shows **1.8x**, **1.6x**, **1.12x**, and **1.04x** better performance 
in Load and workload A, B, C respectively using random integer keys while **0.54x** worse in workload E.

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
