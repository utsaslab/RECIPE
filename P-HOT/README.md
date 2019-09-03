## P-HOT: Persistent Height-Optimized Trie

`P-HOT` is a crash consistent version of Height-Optimized Trie (HOT) that is a 
lookup and space-optimized variant of a trie. `HOT` is converted into `P-HOT` by
adding cache line flushes and memory fences after each critical volatile store.
`P-HOT` is an ordered index supporting both point and range queries.
Compared with `FAST&FAIR` that is a state-of-the-art ordered index, `P-HOT` shows
1.4x, 1.27x, 1.34x, and 1.49x better performance in Load and workload A, B, C respectively
using random integer keys while 0.70x worse in workload E.

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
