# Cacheline-Concious Extendible Hashing (CCEH)

## Introduction 

Low latency storage media such as byte-addressable persistent memory (PM) 
requires rethinking of various data structures in terms of optimization. One of
the main challenges in implementing hash-based indexing structures on PM is
how to achieve efficiency by making effective use of cachelines while 
guaranteeing failure-atomicity for dynamic hash expansion and shrinkage. In
this paper, we present Cacheline-Conscious Extendible Hashing (CCEH) that
reduces the overhead of dynamic memory block management while guaranteeing
constant hash table lookup time. CCEH guarantees failure-atomicity without
making use of explicit logging. Our experiments show that CCEH effectively
adapts its size as the demand increases under the fine-grained
failure-atomicity constraint and its maximum query latency is an order of
magnitude lower compared to the state- of-the-art hashing techniques.

For more details about CCEH, please refer to USENIX FAST 2019 paper - 
"[Write-Optimized Dynamic Hashing for Persistent Memory](https://www.usenix.org/conference/fast19/presentation/nam)"

## Compilation

```
git clone https://github.com/DICL/CCEH
cd CCEH
make ALL_CCEH
```

## Contirubtors
* Moohyeon Nam (moohyeon.nam@gmail.com)
* Hokeun Cha (chahg0129@skku.edu)
