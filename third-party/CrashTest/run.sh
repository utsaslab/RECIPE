#!/bin/bash

DIR="perf_results/"
#PIN="./extra/pin-tool/pin -t ./extra/pin-tool/source/tools/SimpleExamples/obj-intel64/pm_count.so -- "
#SET_SIZE="10000000"
PIN=""
SET_SIZE=""

#for idx_type in art bwtree masstree hot wort woart wbtree fptree clht levelhash cceh dummy
for idx_type in clht
do
    for workload in a b c
    do
        for nthreads in 1
        do
            ${PIN}./build/ycsb ${idx_type} ${workload} randint uniform ${nthreads} ${SET_SIZE} &>> ./results/${DIR}randint_${workload}.csv
            sleep 3
        done
        printf "\n\n" &>> ./results/${DIR}randint_${workload}.csv
    done
done

for idx_type in bwtree
do
    for workload in a b c e
    do
        for nthreads in 1
        do
            ${PIN}./build/ycsb ${idx_type} ${workload} randint uniform ${nthreads} ${SET_SIZE} &>> ./results/${DIR}randint_${workload}.csv
            sleep 3
        done
        printf "\n\n" &>> ./results/${DIR}randint_${workload}.csv
    done
done

for idx_type in bwtree
do
    for workload in a b c e
    do
        for nthreads in 1
        do
            ${PIN}./build/ycsb ${idx_type} ${workload} string uniform ${nthreads} ${SET_SIZE} &>> ./results/${DIR}string_${workload}.csv
            sleep 3
        done
        printf "\n\n" &>> ./results/${DIR}string_${workload}.csv
    done
done

#cd ./build
#rm -rf *
#cmake -DCMAKE_CXX_FLAGS_RELEASE="-O0" ..
#make -j
#cd ..
#
#for idx_type in fastfair
#do
#    for workload in a
#    do
#        for nthreads in 1
#        do
#            ${PIN}./build/ycsb ${idx_type} ${workload} randint uniform ${nthreads} ${SET_SIZE} &>> ./results/${DIR}randint_${workload}.csv
#            sleep 3
#        done
#        printf "\n\n" &>> ./results/${DIR}randint_${workload}.csv
#    done
#done
#
#
#cp CMakeLists_string.txt CMakeLists.txt
#cd ./build
#rm -rf ./*
#cmake ..
#make -j
#cd ..
#
#for idx_type in art bwtree masstree hot wort woart wbtree fptree dummy
#do
#    for workload in a
#    do
#        for nthreads in 1
#        do
#            ${PIN}./build/ycsb ${idx_type} ${workload} string uniform ${nthreads} ${SET_SIZE} &>> ./results/${DIR}string_${workload}.csv
#            sleep 3
#        done
#        printf "\n\n" &>> ./results/${DIR}string_${workload}.csv
#    done
#done
#
#cd ./build
#rm -rf *
#cmake -DCMAKE_CXX_FLAGS_RELEASE="-O0" ..
#make -j
#cd ..
#
#for idx_type in fastfair
#do
#    for workload in a
#    do
#        for nthreads in 1
#        do
#            ${PIN}./build/ycsb ${idx_type} ${workload} string uniform ${nthreads} ${SET_SIZE} &>> ./results/${DIR}string_${workload}.csv
#            sleep 3
#        done
#        printf "\n\n" &>> ./results/${DIR}string_${workload}.csv
#    done
#done
