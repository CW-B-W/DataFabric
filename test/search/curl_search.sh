#!/bin/bash

if [ ! -z $2 ]; then
    log_file=$1
    search_text=$2
    n_times=$3
    use_cache=${4:-"True"}
else
    echo -e "Usage:"
    echo -e "\n./curl_search.sh {log_file} {search_text} {n_times} {use_cache=True}"
    exit
fi

for ((i=0; i<$n_times; i++)); do
    ((page=i%10+1))
    # echo 'http://127.0.0.1:5000/search?text='$search_text'&page='$page
    curl -w "@curl_format_total.txt" -o /dev/null -s 'http://127.0.0.1:5000/search?text='$search_text'&page='$page'&cache='$use_cache >> $log_file 2>&1 &
done