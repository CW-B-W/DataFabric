#!/bin/bash
set -e
python3 recommender_microservice.py > recommender_microservice.log 2>&1 &
start-notebook.sh &
wait