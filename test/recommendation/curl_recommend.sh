#!/bin/bash

if [ ! -z $2 ]; then
    log_file=$1
    n_times=$2
    blocking=${3:-"False"}
else
    echo -e "Usage:"
    echo -e "\n./curl_search.sh {log_file} {n_times} {blocking=False}"
    exit
fi

for ((i=0; i<$n_times; i++)); do
    ((page=i%10+1))
    # echo 'http://127.0.0.1:5000/recommender/recommend'
    if [ "$blocking" == "False" ]; then
        curl -w "@curl_format_total.txt" -o /dev/null -s 'http://127.0.0.1:5000/recommender/recommend' >> $log_file 2>&1 &
    else
        curl -w "@curl_format_total.txt" -o /dev/null -s 'http://127.0.0.1:5000/recommender/recommend' >> $log_file 2>&1
    fi
done