#!/bin/bash
trap 'kill -s INT $child' EXIT INT TERM

python3 datafabric.py > datafabric.log 2>&1 &
child=$!
wait "$child"
