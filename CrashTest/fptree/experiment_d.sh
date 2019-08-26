#!/bin/bash

WL=300

# Compile
make clean; make;

printf "FPTree Concurrency\n" > FPTree_new_mixed.result

# Mixed
printf "Mixed Workload\n" >> FPTree_new_mixed.result
for nthreads in 1 2 4 8 16 32
do
  printf "bash numactl -N 1 ./FPTree_lock_mixed ${1} ${WL} ${nthreads}\n" >> FPTree_new_mixed.result;
  bash numactl -N 1 ./FPTree_lock_mixed $1 ${WL} $nthreads >> FPTree_new_mixed.result;
  printf "\n" >> FPTree_new_mixed.result;
done
