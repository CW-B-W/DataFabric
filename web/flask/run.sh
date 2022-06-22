#!/bin/bash
trap 'kill -s INT $child' EXIT INT TERM

gunicorn --workers=4 --bind=0.0.0.0:5000 datafabric:app > datafabric.log 2>&1 &
child=$!
wait "$child"
