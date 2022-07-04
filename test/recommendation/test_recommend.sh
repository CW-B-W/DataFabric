#!/bin/bash

if [ "$1" != "help" ]; then
    n_proc=${1:-1}
    n_times=${2:-1000}
    blocking=${3:-"False"}
else
    echo -e "Usage:"
    echo -e "\t./test_search.sh {n_proc=1} {n_times=1000} {blocking=False}"
    exit
fi

rm proc_*.log
for ((i=0; i<$n_proc; i++)); do
    ./curl_recommend.sh proc_$i.log $n_times $blocking &
done