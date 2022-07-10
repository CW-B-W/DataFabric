#!/bin/bash

if [ "$1" == "help" ]; then
    echo -e "Usage:"
    echo -e "\t./curl_search.sh {log_file} {min_rank=10} {max_rank=100} {min_iter=10} {max_iter=100}"
    exit
fi

min_rank=${1:-10}
max_rank=${2:-30}
min_iter=${3:-10}
max_iter=${4:-100}

rm train.log
for ((n_rank=min_rank; n_rank<=$max_rank; n_rank+=2)); do
    for ((n_iter=min_iter; n_iter<=$max_iter; n_iter+=10)); do
        echo 'http://127.0.0.1:5000/recommender/train?max_iter='$n_iter'&rank='$n_rank
        echo 'max_iter='$n_iter'&rank='$n_rank >> train.log
        curl -w "@curl_format_total.txt" -o /dev/null -s 'http://127.0.0.1:5000/recommender/train?max_iter='$n_iter'&rank='$n_rank >> train.log 2>&1
    done
done
