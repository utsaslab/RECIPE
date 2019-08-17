#!/bin/bash

cd ./P-CLHT
bash compile.sh lb
cd ..
mkdir ./build
cd ./build
rm -rf *
cmake ..
make -j
cd ..

for idx_type in art bwtree masstree hot
do
    for workload in a b c e
    do
        for nthreads in 16
        do
            ./build/ycsb ${idx_type} ${workload} string uniform ${nthreads} &>> ./results/ycsb_str_tree.csv
            sleep 3
        done
        printf "\n\n" &>> ./results/ycsb_str_tree.csv
    done
done

cd ./build
rm -rf *
cmake -DCMAKE_CXX_FLAGS_RELEASE="-O0" ..
make -j
cd ..

for idx_type in fastfair
do
    for workload in a b c e
    do
        for nthreads in 16
        do
            ./build/ycsb ${idx_type} ${workload} string uniform ${nthreads} &>> ./results/ycsb_str_tree.csv
            sleep 3
        done
        printf "\n\n" &>> ./results/ycsb_str_tree.csv
    done
done
