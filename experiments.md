## Desired system configurations for artifact evaluation
- Ubuntu 18.04.1 LTS
- At least 32GB DRAM
- x86-64 CPU supporting at least 16 threads
- x86-64 CPU supporting at least the AVX-2 and BMI-2 instruction sets (Haswell and newer)

## Performance Benchmarks
We evaluate the performance of indexes converted using the RECIPE against state-of-the-art PM indexes with YCSB ([Yahoo! Cloud Serving Benchmark](https://github.com/brianfrankcooper/YCSB)). We split our evaluation based on the data structure into ordered indexes and unordered indexes. An ordered index aims to support both point and range queries, but an unordered index only provides point queries. `P-BwTree`, `P-Masstree`, `P-ART`, and `P-HOT` are the ordered indexes, while `CCEH`, `Level Hashing`, and `P-CLHT` are the unordered indexes.

We use [index microbenchmarks](https://github.com/wangziqi2016/index-microbench) to generate workload files for YCSB and statically split them across multiple threads. For each workload, we test two key types, 8-byte random integer and 24 byte string, all uniformly distributed. To generate workload files, please run the below script. This script file will generate 64M size workload(A, B, C, E) files for Load and Run respectively. Workload`A` consists of 50% insertion / 50% search operations. Workload`B` is made by 95% search / 5% insertion operations. Workload`C` is organized with 100% search operations while Workload`E` is based on 95% range scan / 5% insertion operations. These workload files will be stored in `index-microbench/workloads/`.

```
bash scripts/gen_workload.sh
```

The list of benchmarks and the major performance results presented in the paper are as follows:

### Ordered Indexes
We evaluate the converted indexes `P-ART`, `P-HOT`, `P-Masstree`, and `P-Bwtree` against `FAST&FAIR`, which is the state-of-the-art PM B+Tree. Each experiment will run with 16 threads. Four kinds of workloads (A, B, C, E) will be used for evaluating tree indexes.

![YCSB-Randint-Tree](https://github.com/utsaslab/RECIPE/blob/master/graphs/ycsb-tree-multithread-randint.png)
<p align="center"> Figure 1 - YCSB Integer Keys: Tree indexes </p>

#### Integer type keys
In this section, we will evaluate the performance of ordered indexes with YCSB workload(A, B, C, E) where the random integer keys are used. For running experiments, please run below script. The script will compile the source code, run the experiments, and store the performance results into `results/ycsb_int_tree.csv` directory with csv format. Figure 1 shows the performance results measured on Optane DC Persistent Memory (PM). Although our artifact evaluations are based on DRAM, the overall trends of the results measured on DRAM should be similar to the results made on real PM.
```
bash scripts/ycsb_int_tree.sh
```

![YCSB-String-Tree](https://github.com/utsaslab/RECIPE/blob/master/graphs/ycsb-tree-multithread.png)
<p align="center"> Figure 2 - YCSB String Keys: Tree indexes </p>

#### String type keys
In this section, we will measure the performance of ordered index with the same workload patterns while using the string type keys. For running experiments, please run below script. The script will compile the source code, run the experiments, and store the results into `results/ycsb_str_tree.csv` directory with csv format. Figure 2 also shows the results measured on Optane DC PM, but the overall trends meausred on DRAM should be similar.
```
bash scripts/ycsb_str_tree.sh
```

![YCSB-Randint-Hash](https://github.com/utsaslab/RECIPE/blob/master/graphs/ycsb-hash-multithread.png)
<p align="center"> Figure 3 - YCSB Integer Keys: Hash tables </p>

### Unordered Indexes
We evaluate `P-CLHT` against two persistent hash tables, `CCEH` and `Level hashing`. Each hash table will be evaluated by using 16 threads and three kinds of workloads (A, B, C). Samely with previous sections, for running experiments, please run below script. The script will compile the source code, run the experiments, and store the results into `results/ycsb_int_hash.csv` directory with csv format. Figure 3 shows the performance results measured on Optane DC PM, but our results based on DRAM should have similar trend with it.

```
bash scripts/ycsb_int_hash.sh
```

## WOART
In this section, we will evaluate the performance of WOART with YCSB workload (A, B, C) where the random integer and string keys are used. Note that we exclude Workload E from this evaluation because the open-source WOART does not provide the implementation for range scan. In order to run experiments, please run below script. It will compile codes, run experiments, and store the results to `results/ycsb_int_tree.csv` and `results/ycsb_str_tree.csv`. WOART showed 2-20X lower performance than P-ART on Optane-DC Persistent Memory. This trend should be similar even on DRAM environment.
```
bash scripts/ycsb_woart.sh
```

## Crash Test
In this section, we will do crash tests for the concurrent indexes including state-of-art PM indexes (`FAST&FAIR` and `CCEH`) and RECIPE-converted indexes (`P-ART`, `P-HOT`, `P-BwTree`, and `P-Masstree`). We test each index for 10K crash states. We load 10K entries into the index, allowing it to crash probabilistically. We then perform a mixed workload consisting of a total of 10K inserts and reads into the index using 16 concurrent threads. Finally, we read back all successfully inserted keys from the index. For testing, please execute following script. This script will compile source codes, execute our crash tests for each index. This script also will show the overall results of testing in prompt messages, while the detailed results are stored in `CrashTest/results` directory.

- Testing for ordered indexes

```
cd CrashTest
./mt_crash_test.sh ordered string

```

- Testing for unordered indexes

```
cd CrashTest
./mt_crash_test.sh unordered randint
```
