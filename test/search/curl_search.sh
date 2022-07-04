#!/bin/bash

if [ ! -z $3 ]; then
    log_file=$1
    search_text=$2
    n_times=$3
    use_cache=${4:-"True"}
    blocking=${5:-"False"}
else
    echo -e "Usage:"
    echo -e "\n./curl_search.sh {log_file} {search_text} {n_times} {use_cache=True} {blocking=False}"
    exit
fi

for ((i=0; i<$n_times; i++)); do
    ((page=i%10+1))
    # echo 'http://127.0.0.1:5000/search?text='$search_text'&page='$page
    if [ "$blocking" == "False" ]; then
        curl -w "@curl_format_total.txt" -o /dev/null -s 'http://127.0.0.1:5000/search?text='$search_text'&page='$page'&cache='$use_cache >> $log_file 2>&1 &
    else
        curl -w "@curl_format_total.txt" -o /dev/null -s 'http://127.0.0.1:5000/search?text='$search_text'&page='$page'&cache='$use_cache >> $log_file 2>&1
    fi
done