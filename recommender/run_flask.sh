#!/bin/bash

trap 'kill -s INT $child' EXIT INT TERM

export FLASK_APP=recommender_microservice
export FLASK_ENV=production
export PYTHONPATH=/
export LC_ALL=C.UTF-8
export LANG=C.UTF-8
flask run --host 0.0.0.0 > recommender_microservice.log 2>&1 &
child=$!
wait "$child"
