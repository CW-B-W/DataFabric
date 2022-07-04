#!/bin/bash

if [ ! -z $1 ]; then
    search_text=$1
    n_proc=${2:-1}
    n_times=${3:-1000}
    use_cache=${4:-"True"}
    blocking=${5:-"False"}
else
    echo -e "Usage:"
    echo -e "\t./test_search.sh {search_text} {n_proc=1} {n_times=1000} {use_cache=True} {blocking=False}"
    exit
fi

rm proc_*.log
for ((i=0; i<$n_proc; i++)); do
    ./curl_search.sh proc_$i.log $search_text $n_times $use_cache $blocking &
done