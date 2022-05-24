#!/bin/bash
trap 'kill -s INT $child' EXIT INT TERM

flask run --host 0.0.0.0 &
child=$!
wait "$child"
