#!/bin/bash

source ./compile_config.sh

sudo apt-get install --assume yes build-essential cmake libboost-all-dev libpapi-dev
sudo apt-get install --assume yes libtbb-dev libjemalloc-dev

cd ./index-microbench
curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.11.0/ycsb-0.11.0.tar.gz
tar xfvz ycsb-0.11.0.tar.gz
mv ycsb-0.11.0 YCSB
mkdir ./workloads
make generate_workload
cd ..

cd ./CLHT
bash compile.sh lb
cd ..
mkdir ./results
mkdir ./build
cd ./build
rm -rf *
cmake ..
make -j
cd ..

for idx_type in art bwtree masstree hot clht levelhash cceh
do
    for workload in a b c e
    do
        for nthreads in 16
        do
            ./build/ycsb ${idx_type} ${workload} randint uniform ${nthreads} &>> ./results/randint_${workload}.csv
            sleep 3
        done
        printf "\n\n" &>> ./results/randint_${workload}.csv
    done
done

for idx_type in art bwtree masstree hot
do
    for workload in a b c e
    do
        for nthreads in 16
        do
            ./build/ycsb ${idx_type} ${workload} string uniform ${nthreads} &>> ./results/string_${workload}.csv
            sleep 3
        done
        printf "\n\n" &>> ./results/string_${workload}.csv
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
            ./build/ycsb ${idx_type} ${workload} randint uniform ${nthreads} &>> ./results/randint_${workload}.csv
            sleep 3
        done
        printf "\n\n" &>> ./results/randint_${workload}.csv
    done
done

for idx_type in fastfair
do
    for workload in a b c e
    do
        for nthreads in 16
        do
            ./build/ycsb ${idx_type} ${workload} string uniform ${nthreads} &>> ./results/string_${workload}.csv
            sleep 3
        done
        printf "\n\n" &>> ./results/string_${workload}.csv
    done
done
