## P-ART: Persistent Adaptive Radix Tree

`P-ART` is a crash consistent version of Adaptive Radix Tree (ART) that is a 
radix tree variant reducing space consumption by adaptively varying node sizes 
based on varid key entries. `ART` using `ROWEX` for concurrency is converted 
into `P-ART` by adding cache line flushes and memory fences after each critical store 
and applying additional crash detection and recovery mechanisms.
`P-ART` is an ordered index supporting both point and range queries.
Compared with `FAST&FAIR` that is a state-of-the-art ordered index, `P-ART` shows 
1.8x, 1.6x, 1.12x, and 1.04x better performance in Load and workload A, B, C 
respectively using random integer keys while 0.54x worse in workload E.

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
