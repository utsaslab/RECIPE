#!/bin/bash

#KEY_TYPE=monoint
#for WORKLOAD_TYPE in e c a; do
#  echo workload${WORKLOAD_TYPE} > workload_config.inp
#  echo ${KEY_TYPE} >> workload_config.inp
#  python gen_workload.py workload_config.inp
#  mv workloads/load_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/mono_inc_load${WORKLOAD_TYPE}_zipf_int_100M.dat
#  mv workloads/txn_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/mono_inc_txns${WORKLOAD_TYPE}_zipf_int_100M.dat
#done

KEY_TYPE=randint
for WORKLOAD_TYPE in a b c e; do
  echo workload${WORKLOAD_TYPE} > workload_config.inp
  echo ${KEY_TYPE} >> workload_config.inp
  python gen_workload.py workload_config.inp
  mv workloads/load_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/load${WORKLOAD_TYPE}_unif_int.dat
  mv workloads/txn_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/txns${WORKLOAD_TYPE}_unif_int.dat
done

