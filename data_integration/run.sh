#!/bin/bash

trap 'kill -s INT $child' EXIT INT TERM

python3 microservice.py > microservice.log 2>&1 &
child=$!
wait "$child"
