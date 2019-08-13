## RECIPE

RECIPE: Reusing Concurrent In-Memory Indexes for Persistent Memory (SOSP 2019)

## Description
RECIPE proposes a principled approach for converting concurrent indexes built for DRAM into crash-consistent indexes for persistent memory. This repository includes the implementations of the index structures for persistent memory converted from the existing concurrent DRAM indexes by following RECIPE. For performance evaluations, this repository also provides the microbenchmarks for index structures based on YCSB.

## Artifact Evaluation
```
$ bash ./build_run.sh
```
This script generates YCSB workloads and evaluates the performance of the index structures presented in paper. The evaluation results are stored in ./results directory as csv files. Please make sure to check the lists by checklists subsection in Benchmark detail section below, before running this script.

## Benchmark Detail

### Desired system configurations
- Ubuntu 18.04.1 LTS
- Linux-4.15.0-47-generic

### Required compile packages
- cmake
- Compiler: g++-7, gcc-7, c++17

### Dependencies
- Install packages
```
$ sudo apt-get install build-essential cmake libboost-all-dev libpapi-dev
```
- Install jemalloc and tbb
```
$ sudo apt-get install libtbb-dev libjemalloc-dev
```

### Checklists
- Change LOAD_SIZE and RUN_SIZE variables to be same with the generated workload size, which are hard-coded in ycsb.cc (Default is 64000000).
```
$ vi ycsb.cpp
```
- Check supported cache line flush instructions. Current default configurations are based on CLFLUSH instruction to flush the dirty cache lines. If your CPU ISA supports CLWB or CLFLUSHOPT, please make sure to change the options in ./CMakeLists.txt and ./CLHT/Makefile. There are three options (clflush, clflushopt, clwb).
```
$ cat /proc/cpuinfo | grep clflush
$ cat /proc/cpuinfo | grep clflushopt
$ cat /proc/cpuinfo | grep clwb
```
Overall
```
$ vi CMakeLists.txt
- add_definitions(-DCLFLUSH) (default configuration)
or
- add_definitions(-DCLFLUSH_OPT)
or
- add_definitions(-DCLWB)
```
CLHT
```
$ vi CLHT/Makefile
- CFLAGS= -D_GNU_SOURCE -DCLFLUSH (default configuration)
or
- CFLAGS= -D_GNU_SOURCE -DCLFLUSH_OPT
or
- CFLAGS= -D_GNU_SOURCE -DCLWB
```

### Generating YCSB workloads
- Download YCSB source code
```
$ cd ./index-microbench
$ curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.11.0/ycsb-0.11.0.tar.gz
$ tar xfvz ycsb-0.11.0.tar.gz
$ mv ycsb-0.11.0 YCSB
```
- Configure the options of each workloads (a, b, c, e), would only need to change $recordcount and $operationcount.
```
$ vi ./index-microbench/workload_spec/<workloada or workloadb or workloadc or workloade>
```
- Select which workloads to be generated. Default configuration will generate all workloads (a, b, c, e). Change the code line "for WORKLOAD_TYPE in <a b c e>; do", depending on which workload you want to generate.
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
- Build CLHT
```
$ cd ./CLHT
$ bash compile.sh lb
$ cd ..
```
- Build all
```
$ mkdir build
$ cd build
$ cmake ..
$ make
```
- Run
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
