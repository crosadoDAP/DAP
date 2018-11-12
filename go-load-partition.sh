#!/usr/bin/env bash
#this is a loop to re-run load partition backfill
# sample testing
# to be removed once confirmed to work in production
# free to use

declare -a arr=("file_path_name_v1")
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
