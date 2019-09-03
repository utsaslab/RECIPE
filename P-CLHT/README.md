## P-CLHT: Persistent Cache-Line Hash Table

`P-CLHT` is a crash consistent version of Cache-Line Hash Table (CLHT) that is a cache-friendly hash table which restricts each bucket to be of the size of a cache line. `CLHT-LB` using lock-based writes for concurrency is converted into `P-CLHT` by adding cache line flushes and memory fences after each critical volatile store. `P-CLHT` is an unordered index only supporting point queries. Compared with `CCEH` that is a state-of-the-art unordered index, `P-CLHT` shows 2.38x, 1.35x, and 1.25x better performance in workload A, B, C respectively using random integer keys while 0.37x worse in Load workload.


## Build & Run

#### Build

```
$ bash compile.sh lb
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
