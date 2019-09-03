## P-Masstree: Persistent Masstree

`P-Masstree` is a crash consistent version of Masstree that is a cache-efficient, highly concurrent trie-like concatenation of B+Tree nodes. The implementation of `P-Masstree` is based on the compact variant of `Masstree` where the original internal nodes are replaced with the same structure of leaf nodes. Compare with original `Masstree`, `P-Masstree` has lsome imitations in providing various functionalities; for example, differently from original `Masstree`, `P-Masstree` does not support storing duplicated keys. `P-Masstree` is an ordered index supporting both point and range queries. Compared with `FAST&FAIR` that is a state-of-the-art ordered index, `P-Masstree` shows 1.51x, 1.34x, 1.06x better performance in Load and workload A, B respectively using random integer keys while similar in workload C and 0.75x worse in workload E.

## Maturity

`P-Masstree` is still in alpha state. The basic functionalities are working, but performance is not fully optimized, and more work to stabilize it is required. Use it at your own risk, but we will improve it soon.

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
