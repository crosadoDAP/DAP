#!/usr/bin/env bash

declare -a arr=("adobe_analytics_cinemax_v1")
subtask='load_partition'
yr=2018
mo=08

for sub in "${arr[@]}"
do
    for i in {01..30}
    do
      airflow test $sub $subtask $yr-$mo-$i
    done
done
