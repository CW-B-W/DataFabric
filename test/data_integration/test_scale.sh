#!/bin/bash

if [ "$EUID" -ne 0 ]
  then echo "Please run as root"
  exit
fi

for ((i = 1; i < $1; i += 2)); do
    docker-compose --project-name datafabric scale data-integration=$i
    python3 test.py Join_r1310720_c10_437714537b.json -n $2 > scale_$i.log 2>&1
done