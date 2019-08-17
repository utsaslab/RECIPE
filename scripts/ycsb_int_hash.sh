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

for idx_type in clht levelhash cceh
do
    for workload in a b c
    do
        for nthreads in 16
        do
            ./build/ycsb ${idx_type} ${workload} randint uniform ${nthreads} &>> ./results/ycsb_int_hash.csv
            sleep 3
        done
        printf "\n\n" &>> ./results/ycsb_int_hash.csv
    done
done
