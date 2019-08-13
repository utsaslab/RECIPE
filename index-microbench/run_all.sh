#!/bin/bash

RUNS=1

for RUN in `seq 1 $RUNS`; do
  for KEY_TYPE in mono rand rdtsc; do
    for WORKLOAD_TYPE in c a e; do
      for THREAD_COUNT in 1 2 20 40; do
        # Use only one workload and many threads for rdtsc keys
        if [ "$KEY_TYPE" = "rdtsc" ] && ([ "$WORKLOAD_TYPE" != "c" ] || [ "$THREAD_COUNT" -eq 1 ] || [ "$THREAD_COUNT" -eq 2 ]); then
          continue
        fi

        for INDEX_TYPE in bwtree masstree btreeolc artolc; do
          # broken?
          if [ "$INDEX_TYPE" = "btreeolc" ] && [ "$WORKLOAD_TYPE" == "e" ]; then
            continue
          fi
          if [ "$INDEX_TYPE" = "artolc" ] && [ "$WORKLOAD_TYPE" == "e" ] && [ "$THREAD_COUNT" -eq 40 ]; then
            continue
          fi

          CMD="./workload $WORKLOAD_TYPE $KEY_TYPE $INDEX_TYPE $THREAD_COUNT"
          OUTPUT="result_${THREAD_COUNT}_${KEY_TYPE}_${WORKLOAD_TYPE}_${INDEX_TYPE}_${RUN}"
            echo
          echo ===========================================
          echo RUN=$RUN  CMD=$CMD
          echo ===========================================
          if [ -e "$OUTPUT" ]; then
            echo skipping
            continue
          fi

          $CMD 2>&1 | tee ${OUTPUT}.tmp
          if [ $? -ne 0 ]; then
            echo exiting; status=$?
            exit 1
          fi

          mv ${OUTPUT}.tmp ${OUTPUT}

          echo
        done
      done
    done
  done
done
