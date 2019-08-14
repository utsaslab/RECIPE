## Performance Benchmarks
We evaluate the performance of indexes converted using the RECIPE against state-of-the-art PM indexes with YCSB [Yahoo! Cloud Serving Benchmark](https://github.com/brianfrankcooper/YCSB). We split our evaluation based on the data structure into ordered indexes and unordered indexes. An ordered index aims to support both point and range queries, but an unordered index only provides point queries. `P-BwTree`, `P-Masstree`, `P-ART`, and `P-HOT` are the ordered indexes, while `CCEH`, `Level Hashing`, and `P-CLHT` are the unordered indexes.

We use [index microbenchmarks](https://github.com/wangziqi2016/index-microbench) to generate workload files for YCSB and statically split them across multiple threads. For each workload, we test two key types, 8-byte random integer and 24 byte string, all uniformly distributed. To generate workload files, please run the below script. This script file will generate 64M size workload(A, B, C, E) files for Load and Run respectively. Workload`A` consists of 50% insertion / 50% search operations. Workload`B` is made by 95% search / 5% insertion operations. Workload`C` is organized with 100% search operations while Workload`E` is based on 95% range scan / 5% insertion operations. These workload files will be stored in `index-microbench/workloads/`.

```
bash scripts/gen_workload.sh
```

The list of benchmarks and the major performance results presented in the paper are as follows:

### Ordered Indexes
We evaluate the converted indexes `P-ART`, `P-HOT`, `P-Masstree`, and `P-Bwtree` against `FAST&FAIR`, which is the state-of-the-art PM B+Tree. Each experiment will run with 16 threads.

![YCSB-Randint-Tree|100x100](https://github.com/utsaslab/RECIPE/blob/master/graphs/ycsb-tree-multithread-randint.png)
<p align="center"> Figure 1 - YCSB Integer Keys: Tree indexes </p>

#### Integer type keys
In this section, we will evaluate the performance of ordered indexes with YCSB workload(A, B, C, E) where the random integer keys are used. For running experiments, please run below script. The script will compile the source code, run the experiments, and store the performance results into `results/ordered/int/` directory with csv format. Figure 1 shows the performance results measured on Optane DC Persistent Memory (PM). Although our artifact evaluations are based on DRAM, the overall trends of the results measured on DRAM should be similar to the results made on real PM.
```
bash scripts/build_run_int_ordered.sh
```

![YCSB-String-Tree](https://github.com/utsaslab/RECIPE/blob/master/graphs/ycsb-tree-multithread.png)
<p align="center"> Figure 2 - YCSB String Keys: Tree indexes </p>

#### String type keys
In this section, we will measure the performance of ordered index with the same workload patterns while using the string type keys. For running experiments, please run below script. The script will compile the source code, run the experiments, and store the results into `results/ordered/string/` directory with csv format. Figure 2 also shows the results measured on Optane DC PM, but the overall trends meausred on DRAM should be similar.
```
bash scripts/build_run_str_ordered.sh
```

![YCSB-Randint-Hash](https://github.com/utsaslab/RECIPE/blob/master/graphs/ycsb-hash-multithread.png)
<p align="center"> Figure 3 - YCSB Integer Keys: Hash tables </p>

### Unordered Indexes
We evaluate P-CLHT against two persistent hash tables, CCEH and Level hashing. Samely with previous sections, for running experiments, please run below script. The script will compile the source code, run the experiments, and store the results into `results/unordered/int/` directory with csv format. Figure 3 shows the performance results measured on Optane DC PM and our results based on DRAM should have similar trend with it.

```
bash scripts/build_run_int_unordered.sh
```
