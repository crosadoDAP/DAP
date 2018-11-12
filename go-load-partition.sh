#!/usr/bin/env bash
#this is a loop to re-run load partition backfill
# sample testing
# to be removed once confirmed to work in production

declare -a arr=("adobe_analytics_cinemax_v1")
subtask='load_partition'
yr=2018
mo=08

for sub in "${arr[@]}"
do
    for i in {01..31}
    do
      airflow test $sub $subtask $yr-$mo-$i
    done
done
