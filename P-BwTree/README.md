## P-BwTree: Persistent BwTree

`P-BwTree` is a crash consistent version of `BwTree` that is a lookup and space-optimized variant of a trie. `BwTree` is converted into `P-BwTree` by adding cache line flushes and memory fences after each critical volatile store. `P-BwTree` is an ordered index supporting both point and range queries. Compared with `FAST&FAIR` that is a state-of-the-art ordered index, `P-BwTree` shows similar performance to `FAST&FAIR` in all workloads using random integer keys (1.13x, 1.04x, 0.93x, 0.92x and 0.92x performance in Load and workload A, B, C, E respectively).

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
