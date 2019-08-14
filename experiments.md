## Performance Benchmarks
We evaluate the performance of indexes converted using the RECIPE against state-of-the-art PM indexes with YCSB [Yahoo! Cloud Serving Benchmark](https://github.com/brianfrankcooper/YCSB). We split our evaluation based on the data structure into ordered indexes and unordered indexes. An ordered index aims to support both point and range queries, but an unordered index only provides point queries. `P-BwTree`, `P-Masstree`, `P-ART`, and `P-HOT` are the ordered indexes, while `CCEH`, `Level Hashing`, and `P-CLHT` are the unordered indexes.

We use [index microbenchmarks](https://github.com/wangziqi2016/index-microbench) to generate workload files for YCSB and statically split them across multiple threads. For each workload, we test two key types, 8-byte random integer and 24 byte string, all uniformly distributed. To generate workload files, please run the below script. This script file will generate 64M size workload(A, B, C, E) files for Load and Run respectively. These workload files will be stored in `index-microbench/workloads/`.

```
bash scripts/gen_workload.sh
```

The list of benchmarks and the major performance results presented in the paper are as follows:

### Ordered Indexes

#### Integer type keys

![YCSB-Randint-Tree](https://github.com/utsaslab/RECIPE/blob/master/graphs/ycsb-tree-multithread-randint.png)
<p align="center"> Figure 1 - YCSB Integer Keys: Tree indexes </p>

#### String type keys

![YCSB-String-Tree](https://github.com/utsaslab/RECIPE/blob/master/graphs/ycsb-tree-multithread.png)
<p align="center"> Figure 2 - YCSB String Keys: Tree indexes </p>

