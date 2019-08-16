## RECIPE: Reusing Concurrent In-Memory Indexes for Persistent Memory (SOSP 2019)

RECIPE proposes a principled approach for converting concurrent indexes built for DRAM into crash-consistent indexes for persistent memory. This repository includes the implementations of the index structures for persistent memory converted from the existing concurrent DRAM indexes by following RECIPE. For performance evaluations, this repository also provides the microbenchmarks for index structures based on YCSB.

## Artifact Evaluation

For artifact evaluation, we will evaluates again the performance of the index structures presented in the paper by using YCSB benchmark. The index structures tested for artifact evaluation include `P-ART`, `P-HOT`, `P-Masstree`, `P-Bwtree`, `FAST&FAIR`, `CCEH`, and `Level hashing`. The evaluation results will be stored in `./results` directory as csv files. Please make sure to check the contents at least by `checklists` subsection in `Benchmark detail` below, before beginning artifact evaluation. Note that the evaluations re-generated for artifact evaluation will be based on DRAM because Optane DC persistent memory machine used for the evaluations presented in the paper has the hard access limitation from external users. For more detail, please refer to [experiments.md](https://github.com/utsaslab/RECIPE/blob/master/experiments.md).

## Benchmark Details

### Desired system configurations for Artifact Evaluation
- Ubuntu 18.04.1 LTS
- At least 32GB DRAM
- x86-64 CPU supporting at least 16 threads
- x86-64 CPU supporting at least the AVX-2 and BMI-2 instruction sets (Haswell and newer)

### Required compile packages
- cmake
- Compiler: g++-7, gcc-7, c++17

### Dependencies
Install build packages
```
$ sudo apt-get install build-essential cmake libboost-all-dev libpapi-dev
```
Install jemalloc and tbb
```
$ sudo apt-get install libtbb-dev libjemalloc-dev
```

### Checklists
Configuration for workload size.
- Change `LOAD_SIZE` and `RUN_SIZE` variables to be same with the generated workload size, which are hard-coded in `ycsb.cpp` (Default is 64000000).
```
$ vi ycsb.cpp
```
Configuration for cache line flush instruction.
- Check supported cache line flush instructions. Current default configurations are based on `CLFLUSH` instruction to flush the dirty cache lines. If your CPU ISA supports `CLWB` or `CLFLUSHOPT`, please make sure to change the options in `./CMakeLists.txt` and `./CLHT/Makefile`. There are three options (clflush, clflushopt, clwb).
```
$ lscpu | grep clflush
$ lscpu | grep clflushopt
$ lscpu | grep clwb
```
- Overall
```
$ vi CMakeLists.txt
- add_definitions(-DCLFLUSH) (default configuration)
or
- add_definitions(-DCLFLUSH_OPT)
or
- add_definitions(-DCLWB)
```
- CLHT
```
$ vi CLHT/Makefile
- CFLAGS= -D_GNU_SOURCE -DCLFLUSH (default configuration)
or
- CFLAGS= -D_GNU_SOURCE -DCLFLUSH_OPT
or
- CFLAGS= -D_GNU_SOURCE -DCLWB
```

Check if your machine supports AVX-2 and BMI-2.
```
$ lscpu | grep avx2
$ lscpu | grep bmi2
```
AVX-2 and BMI-2 are required to run HOT ([Height Optimized Trie](https://github.com/speedskater/hot)).
If your machine does not provide those primitives, please disable HOT from `CMakeLists.txt`.
You can complete compile and run other index structures, except for HOT.
```
$ vi CMakeLists.txt
set(HOT TRUE) --> set(HOT FALSE)
```

### Generating YCSB workloads
Download YCSB source code
```
$ cd ./index-microbench
$ curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.11.0/ycsb-0.11.0.tar.gz
$ tar xfvz ycsb-0.11.0.tar.gz
$ mv ycsb-0.11.0 YCSB
```
How to configure and generate workloads
- Configure the options of each workloads (a, b, c, e), would only need to change `$recordcount` and `$operationcount`.
```
$ vi ./index-microbench/workload_spec/<workloada or workloadb or workloadc or workloade>
```
- Select which workloads to be generated. Default configuration will generate all workloads (a, b, c, e). Change the code line `for WORKLOAD_TYPE in <a b c e>; do`, depending on which workload you want to generate.
```
$ vi ./index-microbench/generate_all_workloads.sh
```
- Generate the workloads. This will generate both random integer keys and string ycsb keys with the specified key distribution.
```
$ cd ./index-microbench/
$ mkdir workloads
$ make generate_workload
```

### Build & Run
Build CLHT
```
$ cd ./CLHT
$ bash compile.sh lb
$ cd ..
```
Build all
```
$ mkdir build
$ cd build
$ cmake ..
$ make
```
Run
```
$ ./ycsb art a randint uniform 4
Usage: ./ycsb [index type] [ycsb workload type] [key distribution] [access pattern] [number of threads]
       1. index type: art hot bwtree masstree clht
                      fastfair levelhash cceh
       2. ycsb workload type: a, b, c, e
       3. key distribution: randint, string
       4. access pattern: uniform, zipfian
       5. number of threads (integer)
```

## Contact

Please contact us at `sklee@cs.utexas.edu` and `vijayc@utexas.edu` with any questions.
